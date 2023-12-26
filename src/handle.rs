use super::*;

pub struct Handle {
    pub(crate) conn: Mutex<Connection>,
    pub(crate) exclusive_files: Mutex<HashMap<FileId, ExclusiveFile>>,
    pub(crate) dir: PathBuf,
    pub(crate) clones: Mutex<FileCloneCache>,
    pub(crate) greedy_holes: bool,
}

impl Handle {
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

    pub fn new(dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&dir)?;
        let sqlite_version = rusqlite::version_number();
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
        };
        Ok(handle)
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

    fn start_transaction(
        &self,
        make_tx: impl FnOnce(&mut Connection) -> rusqlite::Result<Transaction<'_>>,
    ) -> rusqlite::Result<OwnedTx> {
        let guard = self.conn.lock().unwrap();
        owned_cell::OwnedCell::try_make(guard, make_tx)
    }

    pub(crate) fn start_immediate_transaction(&self) -> rusqlite::Result<OwnedTx> {
        self.start_transaction(|conn| {
            conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        })
    }

    /// Starts a deferred transaction (the default). There is no guaranteed read-only transaction
    /// mode. There might be pragmas that can limit to read only statements.
    pub(crate) fn start_deferred_transaction_for_read(&self) -> rusqlite::Result<OwnedTx> {
        self.start_transaction(|conn| conn.transaction())
    }

    /// Begins a read transaction.
    pub fn read(&self) -> rusqlite::Result<Reader> {
        let reader = Reader {
            owned_tx: self.start_deferred_transaction_for_read()?,
            handle: self,
            files: Default::default(),
        };
        Ok(reader)
    }

    pub fn read_single(&self, key: Vec<u8>) -> Result<Option<SnapshotValue<Value, Snapshot>>> {
        let mut reader = self.read()?;
        let Some(value) = reader.add(&key)? else {
            return Ok(None);
        };
        let snapshot = reader.begin()?;
        Ok(Some(snapshot.with_value(value)))
    }

    pub fn single_write_from(&self, key: Vec<u8>, r: impl Read) -> Result<(u64, Timestamp)> {
        let mut writer = self.new_writer()?;
        let mut value = writer.new_value().begin()?;
        let n = value.copy_from(r)?;
        writer.stage_write(key, value)?;
        let commit = writer.commit()?;
        Ok((n, commit.last_used().unwrap()))
    }

    pub fn clone_from_fd(&mut self, key: Vec<u8>, fd: RawFd) -> Result<u64> {
        let mut writer = self.new_writer()?;
        let value = writer.new_value().clone_fd(fd, 0)?;
        let n = value.value_length;
        writer.stage_write(key, value)?;
        writer.commit()?;
        Ok(n)
    }

    pub fn rename_item(&mut self, from: &[u8], to: &[u8]) -> PubResult<Timestamp> {
        let tx = self.start_immediate_transaction()?;
        let last_used = match tx.query_row(
            "update keys set key=? where key=? returning last_used",
            [to, from],
            |row| {
                let ts: Timestamp = row.get(0)?;
                Ok(ts)
            },
        ) {
            Err(QueryReturnedNoRows) => Err(Error::NoSuchKey),
            Ok(ok) => Ok(ok),
            Err(err) => Err(err.into()),
        }?;
        assert_eq!(tx.changes(), 1);
        tx.move_dependent(|tx| tx.commit())?;
        Ok(last_used)
    }

    /// Walks the underlying files in the possum directory.
    pub fn walk_dir(&self) -> Result<Vec<WalkEntry>> {
        crate::walk::walk_dir(&self.dir)
    }

    pub fn list_items(&self, prefix: &[u8]) -> PubResult<Vec<Item>> {
        self.start_deferred_transaction_for_read()?
            .prepare_cached(&format!(
                "select {}, key from keys where substr(key, 1, octet_length(?1))=?1",
                value_columns_sql()
            ))?
            .query_map([prefix], |row| {
                Ok(Item {
                    value: Value::from_row(row)?,
                    key: row.get(VALUE_COLUMN_NAMES.len())?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }
}

use item::Item;
