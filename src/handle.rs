use rusqlite::TransactionBehavior;

use super::*;

#[derive(Default, Debug)]
#[repr(C)]
pub struct Limits {
    pub max_value_length_sum: Option<u64>,
    // Invert this logic when there are defaults and mutators.
    pub disable_hole_punching: bool,
}

type DeletedValuesSender = std::sync::mpsc::SyncSender<Vec<NonzeroValueLocation>>;

/// Provides access to a storage directory. Manages manifest access, file cloning, file writers,
/// configuration, value eviction etc.
#[derive(Debug)]
pub struct Handle {
    pub(crate) conn: Mutex<Connection>,
    pub(crate) exclusive_files: Mutex<HashMap<FileId, ExclusiveFile>>,
    pub(crate) dir: Dir,
    pub(crate) clones: Mutex<FileCloneCache>,
    pub(crate) instance_limits: Limits,
    deleted_values: Option<DeletedValuesSender>,
    _value_puncher: Option<std::thread::JoinHandle<()>>,
    value_puncher_done: ValuePuncherDone,
}

/// 4 bytes stored in the database header https://sqlite.org/fileformat2.html#database_header.
type ManifestUserVersion = u32;

impl Handle {
    /// Whether file cloning should be attempted.
    pub fn dir_supports_file_cloning(&self) -> bool {
        self.dir.supports_file_cloning()
    }

    pub fn set_instance_limits(&mut self, limits: Limits) -> Result<()> {
        self.instance_limits = limits;
        self.start_deferred_transaction()?.apply_limits()
    }

    pub fn dir(&self) -> &Path {
        self.dir.as_ref()
    }

    pub(crate) fn get_exclusive_file(&self) -> Result<ExclusiveFile> {
        {
            let mut files = self.exclusive_files.lock().unwrap();
            // How do we avoid cloning the key and skipping the unnecessary remove check? Do we need a
            // pop method on HashMap?
            if let Some(id) = files.keys().next().cloned() {
                let file = files.remove(&id).unwrap();
                debug_assert_eq!(id, file.id);
                debug!("using exclusive file {} from handle", &file.id);
                return Ok(file);
            }
        }
        trace!("about to open existing files");
        if let Some(file) = self.open_existing_exclusive_file()? {
            debug!("opened existing values file {}", file.id);
            return Ok(file);
        }
        trace!("here");
        let ret = ExclusiveFile::new(&self.dir);
        if let Ok(file) = &ret {
            debug!("created new exclusive file {}", file.id);
        }
        ret
    }

    fn open_existing_exclusive_file(&self) -> Result<Option<ExclusiveFile>> {
        for res in read_dir(&self.dir)? {
            let entry = res?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            if !ExclusiveFile::valid_file_name(entry.file_name().to_str().unwrap()) {
                continue;
            }
            let path = entry.path();
            debug!(?path, "opening existing file");
            match ExclusiveFile::open(path.clone()) {
                Ok(ef) => return Ok(ef),
                Err(err) => {
                    debug!(?path, ?err, "open");
                }
            }
        }
        Ok(None)
    }

    // Expected manifest sqlite user version field value.
    const USER_VERSION: u32 = 3;

    pub fn new(dir: PathBuf) -> Result<Self> {
        let sqlite_version = rusqlite::version_number();
        // TODO: Why?
        if sqlite_version < 3042000 {
            bail!(
                "sqlite version {} below minimum {}",
                rusqlite::version(),
                "3.42"
            );
        }
        let dir = Dir::new(dir)?;
        let mut conn = Connection::open(dir.path().join(MANIFEST_DB_FILE_NAME))?;
        Self::init_sqlite_conn(&mut conn, &dir)?;
        let (deleted_values, receiver) = std::sync::mpsc::sync_channel(10);
        let (value_puncher_done_sender, value_puncher_done) = std::sync::mpsc::sync_channel(0);
        let value_puncher_done = ValuePuncherDone(Arc::new(Mutex::new(value_puncher_done)));
        let handle = Self {
            conn: Mutex::new(conn),
            exclusive_files: Default::default(),
            dir: dir.clone(),
            clones: Default::default(),
            instance_limits: Default::default(),
            deleted_values: Some(deleted_values),
            // Don't wait on this, at least in the Drop handler, because it stays alive until it
            // succeeds in punching everything.
            _value_puncher: Some(std::thread::spawn(move || -> () {
                let _value_puncher_done_sender = value_puncher_done_sender;
                if let Err(err) = Self::value_puncher(dir, receiver) {
                    error!("value puncher thread failed with {err:?}");
                }
            })),
            value_puncher_done,
        };
        Ok(handle)
    }

    fn retry_while_busy<T>(mut f: impl FnMut() -> rusqlite::Result<T>) -> rusqlite::Result<T> {
        loop {
            match f() {
                Err(rusqlite::Error::SqliteFailure(err, _))
                    if err.code == rusqlite::ErrorCode::DatabaseBusy =>
                {
                    std::thread::sleep(Duration::from_secs(1));
                }
                default => return default,
            }
        }
    }

    fn init_sqlite_conn(conn: &mut Connection, dir: &Dir) -> anyhow::Result<()> {
        Self::retry_while_busy(|| conn.pragma_update(None, "synchronous", "off"))?;

        let get_user_version = |conn: &Connection| -> Result<ManifestUserVersion, _> {
            conn.pragma_query_value(None, "user_version", |row| row.get(0))
        };
        let user_version: ManifestUserVersion = get_user_version(conn)?;
        // I'm not sure about the greater than case. Should we return an error?
        if user_version >= Self::USER_VERSION {
            return Ok(());
        }
        // This initialization/upgrade process doesn't seem to be safe to perform when there's
        // multiple connections to a database. To avoid issues upgrading databases, you might want
        // to create a single synchronous handle to do the upgrade before creating new handles
        // concurrently.

        // If the journal mode is WAL, we seem to get database locking errors even when we think we
        // have the lock.
        conn.pragma_update(None, "journal_mode", "delete")?;
        // After the next read/write, we should be the only ones working on the database. Since we
        // can't use transactions there's no other choice.
        conn.pragma_update(None, "locking_mode", "exclusive")?;
        let user_version = get_user_version(conn)?;
        if user_version < Self::USER_VERSION {
            use rusqlite::config::DbConfig::SQLITE_DBCONFIG_RESET_DATABASE;
            conn.set_db_config(SQLITE_DBCONFIG_RESET_DATABASE, true)?;
            // This can't be done in a transaction, an exclusive one would have been nice.
            conn.execute("vacuum", [])?;
            conn.set_db_config(SQLITE_DBCONFIG_RESET_DATABASE, false)?;
            Self::delete_all_values_files(dir)?;
            // It might be possible special case doing this from user_version=0 (a brand new database).
            init_manifest_schema(conn)?;
            conn.pragma_update(None, "user_version", Self::USER_VERSION)?;
        }
        let mode: String =
            conn.pragma_update_and_check(None, "locking_mode", "normal", |row| row.get(0))?;
        assert_eq!(mode, "normal");
        // This is sticky and sync-safe. Must access the database after setting the locking mode to
        // make the change, so now is a good time to set the journal mode. I think if WAL is already
        // set, this doesn't change the locking_mode.
        conn.pragma_update(None, "journal_mode", "wal")?;
        Ok(())
    }

    /// Delete all values files, ensuring they're not in use first.
    fn delete_all_values_files(dir: &Dir) -> anyhow::Result<()> {
        for entry in dir.walk_dir()? {
            let path = &entry.path;
            if !matches!(entry.entry_type, EntryType::ValuesFile) {
                continue;
            }
            let file = OpenOptions::new().write(true).open(path)?;
            if !file.lock_max_segment(LockExclusiveNonblock)? {
                warn!(?path, "file for deletion is locked. blocking");
                assert!(file.lock_max_segment(LockExclusive)?);
            }
            debug!(?path, "deleting file");
            remove_file(path)?;
        }
        Ok(())
    }

    pub fn cleanup_snapshots(&self) -> PubResult<()> {
        delete_unused_snapshots(self.dir.path()).map_err(Into::into)
    }

    pub fn block_size(&self) -> u64 {
        self.dir.block_size()
    }

    pub fn new_writer(&self) -> Result<BatchWriter> {
        Ok(BatchWriter {
            handle: self,
            exclusive_files: Default::default(),
            pending_writes: Default::default(),
            value_renames: Default::default(),
        })
    }

    fn start_transaction<'h, T, O>(
        &'h self,
        make_tx: impl FnOnce(&'h mut Connection, &'h Handle) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<O>
    where
        O: From<OwnedTxInner<'h, T>>,
    {
        let guard = self.conn.lock().unwrap();
        Ok(owned_cell::OwnedCell::try_make(guard, |conn| make_tx(conn, self))?.into())
    }

    pub(crate) fn start_immediate_transaction(&self) -> rusqlite::Result<OwnedTx> {
        self.start_writable_transaction_with_behaviour(TransactionBehavior::Immediate)
    }

    pub(crate) fn start_writable_transaction_with_behaviour(
        &self,
        behaviour: TransactionBehavior,
    ) -> rusqlite::Result<OwnedTx> {
        self.start_transaction(|conn, handle| {
            let rtx = conn.transaction_with_behavior(behaviour)?;
            Ok(Transaction::new(rtx, handle))
        })
    }

    /// Starts a deferred transaction (the default). There is no guaranteed read-only transaction
    /// mode. There might be pragmas that can limit to read only statements.
    pub fn start_deferred_transaction_for_read(&self) -> rusqlite::Result<OwnedReadTx> {
        self.start_transaction(|conn, _handle| {
            let rtx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
            Ok(ReadTransaction {
                tx: ReadOnlyRusqliteTransaction { conn: rtx },
            })
        })
    }

    /// Starts a deferred transaction (the default). This might upgrade to a write transaction if
    /// appropriate. I'm not sure about the semantics of doing that yet. This might be useful for
    /// operations that become writes depending on certain conditions, but could violate some
    /// expectations around locking. TBD.
    pub(crate) fn start_deferred_transaction(&self) -> rusqlite::Result<OwnedTx> {
        self.start_writable_transaction_with_behaviour(TransactionBehavior::Deferred)
    }

    /// Begins a read transaction.
    pub fn read(&self) -> rusqlite::Result<Reader> {
        let reader = Reader {
            owned_tx: self.start_deferred_transaction()?,
            handle: self,
            reads: Default::default(),
        };
        Ok(reader)
    }

    pub fn read_single(&self, key: &[u8]) -> Result<Option<SnapshotValue<Value>>> {
        let mut reader = self.read()?;
        let Some(value) = reader.add(key)? else {
            return Ok(None);
        };
        let snapshot = reader.begin()?;
        Ok(Some(snapshot.value(value)))
    }

    pub fn single_write_from(
        &self,
        key: Vec<u8>,
        r: impl Read,
    ) -> Result<(u64, WriteCommitResult)> {
        let mut writer = self.new_writer()?;
        let mut value = writer.new_value().begin()?;
        trace!("got value writer");
        let n = value.copy_from(r)?;
        writer.stage_write(key, value)?;
        let commit = writer.commit()?;
        Ok((n, commit))
    }

    pub fn single_delete(&self, key: &[u8]) -> PubResult<Option<c_api::PossumStat>> {
        let mut tx = self.start_deferred_transaction()?;
        let deleted = tx.delete_key(key)?;
        // Maybe it's okay just to commit anyway, since we have a deferred transaction and sqlite
        // might know nothing has changed.
        if deleted.is_some() {
            tx.commit(())?.complete()?;
        }
        Ok(deleted)
    }

    pub fn clone_from_file(&mut self, key: Vec<u8>, file: &mut File) -> Result<u64> {
        let mut writer = self.new_writer()?;
        let mut value = writer.new_value().clone_file(file)?;
        let n = value.value_length()?;
        writer.stage_write(key, value)?;
        writer.commit()?;
        Ok(n)
    }

    pub fn rename_item(&mut self, from: &[u8], to: &[u8]) -> PubResult<Timestamp> {
        let mut tx = self.start_immediate_transaction()?;
        let last_used = tx.rename_item(from, to)?;
        Ok(tx.commit(last_used)?.complete()?)
    }

    /// Walks the underlying files in the possum directory.
    pub fn walk_dir(&self) -> Result<Vec<walk::Entry>> {
        crate::walk::walk_dir(&self.dir)
    }

    pub fn list_items(&self, prefix: &[u8]) -> PubResult<Vec<Item>> {
        self.start_deferred_transaction_for_read()?
            .list_items(prefix)
    }

    /// Punches values in batches with its own dedicated connection and read-only transactions.
    fn value_puncher(
        dir: Dir,
        values_receiver: std::sync::mpsc::Receiver<Vec<NonzeroValueLocation>>,
    ) -> Result<()> {
        let manifest_path = dir.path().join(MANIFEST_DB_FILE_NAME);
        use rusqlite::OpenFlags;
        let mut conn = Connection::open_with_flags(
            manifest_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_URI,
        )?;
        const RETRY_DURATION: Duration = Duration::from_secs(1);
        let mut pending_values: Vec<_> = Default::default();
        let mut values_receiver_opt = Some(values_receiver);
        while values_receiver_opt.is_some() || !pending_values.is_empty() {
            match &values_receiver_opt {
                Some(values_receiver) => {
                    let timeout = if pending_values.is_empty() {
                        Duration::MAX
                    } else {
                        RETRY_DURATION
                    };
                    let recv_result = values_receiver.recv_timeout(timeout);
                    use std::sync::mpsc::RecvTimeoutError;
                    match recv_result {
                        Ok(mut values) => {
                            pending_values.append(&mut values);
                            // Drain the channel
                            while let Ok(more_values) = values_receiver.try_recv() {
                                pending_values.extend(more_values);
                            }
                        }
                        Err(RecvTimeoutError::Timeout) => {}
                        Err(RecvTimeoutError::Disconnected) => {
                            // Don't try receiving again.
                            values_receiver_opt = None;
                        }
                    }
                }
                None => {
                    std::thread::sleep(RETRY_DURATION);
                }
            }
            let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
            let tx = ReadTransaction {
                tx: ReadOnlyRusqliteTransaction { conn: tx },
            };
            pending_values = Self::punch_values(&dir, pending_values, &tx)?;
        }
        Ok(())
    }

    /// Starts a read transaction to determine punch boundaries. Since punching is never expanded to
    /// offsets above the targeted values, ongoing writes should not be affected.
    pub(crate) fn punch_values(
        dir: &Dir,
        values: Vec<NonzeroValueLocation>,
        transaction: &ReadTransactionOwned,
    ) -> PubResult<Vec<NonzeroValueLocation>> {
        let mut failed = Vec::with_capacity(values.len());
        for v in values {
            let NonzeroValueLocation {
                file_id,
                file_offset,
                length,
            } = &v;
            let value_length = length;
            let msg = format!(
                "deleting value at {:?} {} {}",
                file_id, file_offset, value_length
            );
            debug!("{}", msg);
            // self.handle.clones.lock().unwrap().remove(&file_id);
            if !punch_value(PunchValueOptions {
                dir: dir.path(),
                file_id,
                offset: *file_offset,
                length: *value_length,
                tx: transaction,
                block_size: dir.block_size(),
                constraints: Default::default(),
            })
            .context(msg)?
            {
                failed.push(v);
            }
        }
        Ok(failed)
    }

    pub(crate) fn send_values_for_delete(&self, values: Vec<NonzeroValueLocation>) {
        use std::sync::mpsc::TrySendError::*;
        let sender = self.deleted_values.as_ref().unwrap();
        match sender.try_send(values) {
            Ok(()) => (),
            Err(Disconnected(values)) => {
                error!("sending {values:?}: channel disconnected");
            }
            Err(Full(values)) => {
                warn!("channel full while sending values. blocking.");
                sender.send(values).unwrap()
            }
        }
    }

    /// Returns something that can be used to test if the value puncher routine for this Handle has returned.
    pub fn get_value_puncher_done(&self) -> ValuePuncherDone {
        ValuePuncherDone(Arc::clone(&self.value_puncher_done.0))
    }

    pub fn move_prefix(&self, from: &[u8], to: &[u8]) -> Result<()> {
        let mut tx = self.start_deferred_transaction()?;
        let items = tx.read().list_items(from)?;
        let mut to_vec = to.to_vec();
        for item in items {
            to_vec.truncate(to.len());
            to_vec.extend_from_slice(item.key.strip_prefix(from).unwrap());
            tx.rename_item(&item.key, &to_vec)?;
        }
        tx.commit(())?.complete()
    }
}

use item::Item;

use crate::dir::Dir;
use crate::ownedtx::{OwnedReadTx, OwnedTxInner};
use crate::tx::{ReadOnlyRusqliteTransaction, ReadTransaction};
use crate::walk::EntryType;

impl Drop for Handle {
    fn drop(&mut self) {
        // self.deleted_values.take();
        // if let Some(join_handle) = self.value_puncher.take() {
        //     join_handle.thread().unpark();
        //     join_handle.join().unwrap()
        // }
    }
}

#[derive(Debug)]
pub struct ValuePuncherDone(Arc<Mutex<std::sync::mpsc::Receiver<()>>>);

impl ValuePuncherDone {
    pub fn wait(&self) {
        assert!(matches!(
            self.0.lock().unwrap().recv(),
            Err(std::sync::mpsc::RecvError)
        ))
    }
}
