use rusqlite::TransactionBehavior;

use super::*;

#[derive(Default)]
#[repr(C)]
pub struct Limits {
    pub max_value_length_sum: Option<u64>,
}

pub struct Handle {
    pub(crate) conn: Mutex<Connection>,
    pub(crate) exclusive_files: Mutex<HashMap<FileId, ExclusiveFile>>,
    pub(crate) dir: PathBuf,
    pub(crate) clones: Mutex<FileCloneCache>,
    pub(crate) instance_limits: Limits,
}

/// 4 bytes stored in the database header https://sqlite.org/fileformat2.html#database_header.
type ManifestUserVersion = u32;

impl Handle {
    pub fn set_instance_limits(&mut self, limits: Limits) -> Result<()> {
        self.instance_limits = limits;
        self.start_deferred_transaction()?.apply_limits()
    }

    pub fn dir(&self) -> &PathBuf {
        &self.dir
    }

    pub(crate) fn get_exclusive_file(&self) -> Result<ExclusiveFile> {
        let mut files = self.exclusive_files.lock().unwrap();
        // How do we avoid cloning the key and skipping the unnecessary remove check? Do we need a
        // pop method on HashMap?
        if let Some(id) = files.keys().next().cloned() {
            let file = files.remove(&id).unwrap();
            debug_assert_eq!(id, file.id);
            debug!("using exclusive file {} from handle", &file.id);
            return Ok(file);
        }
        if let Some(file) = self.open_existing_exclusive_file()? {
            debug!("opened existing values file {}", file.id);
            return Ok(file);
        }
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
            if !valid_file_name(entry.file_name().to_str().unwrap()) {
                continue;
            }
            if let Ok(ef) = ExclusiveFile::open(entry.path()) {
                return Ok(Some(ef));
            }
        }
        Ok(None)
    }

    // Expected manifest sqlite user version field value.
    const USER_VERSION: u32 = 1;

    pub fn new(dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&dir)?;
        let sqlite_version = rusqlite::version_number();
        // TODO: Why?
        if sqlite_version < 3042000 {
            bail!(
                "sqlite version {} below minimum {}",
                rusqlite::version(),
                "3.42"
            );
        }
        let conn = Connection::open(dir.join(MANIFEST_DB_FILE_NAME))?;
        Self::init_sqlite_conn(&conn)?;
        conn.pragma_update(None, "synchronous", "off")?;
        let handle = Self {
            conn: Mutex::new(conn),
            exclusive_files: Default::default(),
            dir,
            clones: Default::default(),
            instance_limits: Default::default(),
        };
        Ok(handle)
    }

    fn init_sqlite_conn(conn: &Connection) -> rusqlite::Result<()> {
        let user_version: ManifestUserVersion =
            conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        conn.pragma_update(None, "synchronous", "off")?;
        if user_version < Self::USER_VERSION {
            conn.pragma_update(None, "journal_mode", "wal")?;
            init_manifest_schema(conn)?;
            conn.pragma_update(None, "user_version", Self::USER_VERSION)?;
        }
        if false {
            conn.pragma_update(None, "locking_mode", "exclusive")?;
        }
        Ok(())
    }

    pub fn cleanup_snapshots(&self) -> PubResult<()> {
        delete_unused_snapshots(&self.dir).map_err(Into::into)
    }

    pub fn block_size(&self) -> u64 {
        4096
    }

    pub fn new_writer(&self) -> Result<BatchWriter> {
        Ok(BatchWriter {
            handle: self,
            exclusive_files: Default::default(),
            pending_writes: Default::default(),
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
            files: Default::default(),
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

    pub fn clone_from_fd(&mut self, key: Vec<u8>, fd: RawFd) -> Result<u64> {
        let mut writer = self.new_writer()?;
        let mut value = writer.new_value().clone_fd(fd, 0)?;
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
    pub fn walk_dir(&self) -> Result<Vec<WalkEntry>> {
        crate::walk::walk_dir(&self.dir)
    }

    pub fn list_items(&self, prefix: &[u8]) -> PubResult<Vec<Item>> {
        self.start_deferred_transaction_for_read()?
            .list_items(prefix)
    }

    /// Starts a read transaction to determine punch boundaries. Since punching is never expanded to
    /// offsets above the targeted values, ongoing writes should not be affected.
    pub(crate) fn punch_values<V>(&self, values: &[V]) -> PubResult<()>
    where
        V: AsRef<Value>,
    {
        let transaction = self.start_deferred_transaction_for_read()?;
        for v in values {
            let Value {
                file_id,
                file_offset,
                length,
                ..
            } = v.as_ref();
            let value_length = length;
            let msg = format!(
                "deleting value at {:?} {} {}",
                file_id, file_offset, value_length
            );
            debug!("{}", msg);
            // self.handle.clones.lock().unwrap().remove(&file_id);
            punch_value(PunchValueOptions {
                dir: &self.dir,
                file_id,
                offset: *file_offset,
                length: *value_length,
                tx: &transaction,
                block_size: self.block_size(),
                constraints: Default::default(),
            })
            .context(msg)?;
        }
        Ok(())
    }
}

use item::Item;

use crate::ownedtx::{OwnedReadTx, OwnedTxInner};
use crate::tx::{ReadOnlyRusqliteTransaction, ReadTransaction};
