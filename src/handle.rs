use log::error;

use std::sync::LockResult;

use super::*;

pub struct HandleOpts {
    pub max_value_length: Option<u64>,
}

impl Default for HandleOpts {
    fn default() -> Self {
        Self {
            // TODO: Expose this to the C API instead!
            max_value_length: Some(200 << 20),
        }
    }
}

pub(crate) type HandleExclusiveFiles = Arc<Mutex<HashMap<FileId, ExclusiveFile>>>;

pub struct Handle {
    pub(crate) conn: Mutex<Connection>,
    pub(crate) exclusive_files: HandleExclusiveFiles,
    pub(crate) dir: PathBuf,
    pub(crate) clones: Mutex<FileCloneCache>,
    pub(crate) greedy_holes: bool,
    pub(crate) opts: HandleOpts,
}

impl Handle {
    pub fn dir(&self) -> &PathBuf {
        &self.dir
    }

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
        conn.pragma_update(None, "journal_mode", "wal")?;
        conn.pragma_update(None, "synchronous", "off")?;
        if false {
            conn.pragma_update(None, "locking_mode", "exclusive")
                .context("set conn locking mode exclusive")?;
        }
        init_manifest_schema(&conn).context("initing manifest schema")?;
        if let Err(err) = delete_unused_snapshots(&dir) {
            error!("error deleting unused snapshots: {}", err);
        }
        let handle = Self {
            conn: Mutex::new(conn),
            exclusive_files: Default::default(),
            dir,
            clones: Default::default(),
            greedy_holes: match std::env::var("POSSUM_GREEDY_HOLES") {
                Ok(value) => value.parse()?,
                Err(std::env::VarError::NotPresent) => true,
                Err(err) => return Err(err.into()),
            },
            opts: Default::default(),
        };
        Ok(handle)
    }

    pub fn block_size(&self) -> u64 {
        4096
    }

    pub fn new_writer(&self) -> Result<BatchWriter> {
        Ok(BatchWriter {
            exclusive_files: Default::default(),
            pending_writes: Default::default(),
            handle_exclusive_files: Arc::clone(&self.exclusive_files),
            handle_dir: self.dir.to_owned(),
        })
    }

    pub fn conn(&self) -> LockResult<MutexGuard<'_, Connection>> {
        self.conn.lock()
    }

    pub(crate) fn start_immediate_transaction<'h, 't>(
        &'h self,
        conn: &'t mut Connection,
    ) -> rusqlite::Result<Transaction<'h, 't>> {
        Ok(Transaction::new(
            conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?,
            self,
        ))
    }

    /// Starts a deferred transaction (the default). There is no guaranteed read-only transaction
    /// mode. There might be pragmas that can limit to read only statements.
    pub fn start_deferred_transaction_for_read<'a, 't>(
        &'a self,
        conn: &'t mut Connection,
    ) -> rusqlite::Result<Transaction<'a, 't>> {
        self.start_deferred_transaction(conn)
    }

    /// Starts a deferred transaction (the default). This might upgrade to a write transaction if
    /// appropriate. I'm not sure about the semantics of doing that yet. This might be useful for
    /// operations that become writes depending on certain conditions, but could violate some
    /// expectations around locking. TBD.
    pub(crate) fn start_deferred_transaction<'a, 't>(
        &'a self,
        conn: &'t mut Connection,
    ) -> rusqlite::Result<Transaction<'a, 't>> {
        Ok(Transaction::new(conn.transaction()?, self))
    }

    /// Begins a read transaction.
    pub fn read<'h, 't>(&'h self, conn: &'t mut Connection) -> rusqlite::Result<Reader<'h, 't>> {
        let reader = Reader {
            owned_tx: self.start_deferred_transaction_for_read(conn)?,
            handle: self,
            files: Default::default(),
        };
        Ok(reader)
    }

    pub fn read_single(&mut self, key: &[u8]) -> Result<Option<SnapshotValue<Value>>> {
        let mut guard = self.conn().unwrap();
        let mut reader = self.read(&mut guard)?;
        let Some(value) = reader.add(key)? else {
            return Ok(None);
        };
        let snapshot = reader.begin()?.complete(&mut guard)?;
        Ok(Some(snapshot.value(value)))
    }

    pub fn single_write_from(
        &mut self,
        key: Vec<u8>,
        r: impl Read,
    ) -> Result<(u64, WriteCommitResult)> {
        let mut writer = self.new_writer()?;
        let mut value = writer.new_value().begin()?;
        let n = value.copy_from(r)?;
        writer.stage_write(key, value)?;
        let commit = writer.commit(self)?;
        Ok((n, commit))
    }

    pub fn single_delete(
        &self,
        key: &[u8],
    ) -> PubResult<PostCommitWork<Option<c_api::PossumStat>>> {
        let mut guard = self.conn().unwrap();
        let mut tx = self.start_deferred_transaction(guard.deref_mut())?;
        let deleted = tx.delete_key(key)?;
        tx.commit(deleted).map_err(Into::into)
    }

    pub fn clone_from_fd(&mut self, key: Vec<u8>, fd: RawFd) -> Result<u64> {
        let mut writer = self.new_writer()?;
        let mut value = writer.new_value().clone_fd(fd, 0)?;
        let n = value.value_length()?;
        writer.stage_write(key, value)?;
        writer.commit(self)?;
        Ok(n)
    }

    pub fn rename_item(&mut self, from: &[u8], to: &[u8]) -> PubResult<Timestamp> {
        let mut guard = self.conn().unwrap();
        let mut tx = self.start_immediate_transaction(guard.deref_mut())?;
        let last_used = tx.rename_item(from, to)?;
        tx.commit(last_used)?
            .complete(&mut guard)
            .map_err(Into::into)
    }

    /// Walks the underlying files in the possum directory.
    pub fn walk_dir(&self) -> Result<Vec<WalkEntry>> {
        crate::walk::walk_dir(&self.dir)
    }

    pub fn list_items(&self, prefix: &[u8]) -> PubResult<Vec<Item>> {
        self.start_deferred_transaction_for_read(self.conn().unwrap().deref_mut())?
            .list_items(prefix)
    }

    /// Starts a read transaction to determine punch boundaries. Since punching is never expanded to
    /// offsets above the targeted values, ongoing writes should not be affected.
    pub(crate) fn punch_values<V>(&self, values: &[V], conn: &mut Connection) -> PubResult<()>
    where
        V: AsRef<Value>,
    {
        let transaction = self.start_deferred_transaction_for_read(conn)?;
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
                greedy_start: self.greedy_holes,
                check_hole: true,
            })
            .context(msg)?;
        }
        Ok(())
    }
}

use crate::tx::PostCommitWork;
use item::Item;
