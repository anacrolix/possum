use super::*;

/// This is more work to be done after the Handle conn mutex is released.
#[must_use]
pub(crate) struct PostCommitWork<H> {
    handle: H,
    deleted_values: Vec<NonzeroValueLocation>,
    altered_files: HashSet<FileId>,
}

/// Exposes a rusqlite Transaction to implement ReadTransaction.
pub trait ReadOnlyTransactionAccessor {
    fn readonly_transaction(&self) -> &rusqlite::Transaction;
}

/// Extends rusqlite objects with stuff needed for ReadTransaction.
trait ReadOnlyRusqliteTransaction {
    fn prepare_cached_readonly(&self, sql: &str) -> rusqlite::Result<CachedStatement>;
}

// This could just as easily be implemented for rusqlite::Connection too.
impl ReadOnlyRusqliteTransaction for rusqlite::Transaction<'_> {
    fn prepare_cached_readonly(&self, sql: &str) -> rusqlite::Result<CachedStatement> {
        prepare_cached_readonly(self.borrow(), sql)
    }
}

fn prepare_cached_readonly<'a>(
    conn: &'a Connection,
    sql: &str,
) -> rusqlite::Result<CachedStatement<'a>> {
    let stmt = conn.prepare_cached(sql)?;
    assert!(stmt.readonly());
    Ok(stmt)
}

pub type ReadTransactionRef<'a> = &'a ReadTransactionOwned<'a>;

/// Helper type for wrapping rusqlite::Transaction to only provide ReadTransaction capabilities.
pub struct ReadTransactionOwned<'a>(pub(crate) rusqlite::Transaction<'a>);

impl ReadOnlyTransactionAccessor for ReadTransactionOwned<'_> {
    fn readonly_transaction(&self) -> &rusqlite::Transaction {
        &self.0
    }
}

/// Extra methods for types exposing a rusqlite Transaction that's allowed to do read transaction
/// stuff.
pub trait ReadTransaction: ReadOnlyTransactionAccessor {
    fn file_values(&self, file_id: FileId) -> rusqlite::Result<FileValues<CachedStatement>> {
        let stmt = self
            .readonly_transaction()
            .prepare_cached_readonly(&format!(
                "select {} from keys where file_id=? order by file_offset",
                value_columns_sql()
            ))?;
        let iter = FileValues { stmt, file_id };
        Ok(iter)
    }

    fn sum_value_length(&self) -> rusqlite::Result<u64> {
        self.readonly_transaction()
            .prepare_cached_readonly("select value from sums where key='value_length'")?
            .query_row([], |row| row.get(0))
    }

    /// Returns the end offset of the last active value before offset in the same file.
    fn query_last_end_offset(&self, file_id: &FileId, offset: u64) -> rusqlite::Result<u64> {
        self.readonly_transaction()
            .prepare_cached_readonly(
                "select max(file_offset+value_length) as last_offset \
                from keys \
                where file_id=? and file_offset+value_length <= ?",
            )?
            .query_row(params![file_id, offset], |row| {
                // I don't know why, but this can return null for file_ids that have values but
                // don't fit the other conditions.
                let res: rusqlite::Result<Option<_>> = row.get(0);
                res.map(|v| v.unwrap_or_default())
            })
    }

    /// Returns the next value offset with at least min_offset.
    fn next_value_offset(
        &self,
        file_id: &FileId,
        min_offset: u64,
    ) -> rusqlite::Result<Option<u64>> {
        self.readonly_transaction()
            .prepare_cached_readonly(
                "select min(file_offset) \
                from keys \
                where file_id=? and file_offset >= ?",
            )?
            .query_row(params![file_id, min_offset], |row| row.get(0))
    }

    // TODO: Make this iterate.
    fn list_items(&self, prefix: &[u8]) -> PubResult<Vec<Item>> {
        let range_end = {
            let mut prefix = prefix.to_owned();
            if inc_big_endian_array(&mut prefix) {
                Some(prefix)
            } else {
                None
            }
        };
        match range_end {
            None => list_items_inner(
                self.readonly_transaction(),
                &format!(
                    "select {}, key from keys where key >= ?",
                    value_columns_sql()
                ),
                [prefix],
            ),
            Some(range_end) => list_items_inner(
                self.readonly_transaction(),
                &format!(
                    "select {}, key from keys where key >= ? and key < ?",
                    value_columns_sql()
                ),
                rusqlite::params![prefix, range_end],
            ),
        }
    }
}

fn list_items_inner(
    tx: &rusqlite::Transaction,
    sql: &str,
    params: impl rusqlite::Params,
) -> PubResult<Vec<Item>> {
    tx.prepare_cached_readonly(sql)
        .unwrap()
        .query_map(params, |row| {
            Ok(Item {
                value: Value::from_row(row)?,
                key: row.get(VALUE_COLUMN_NAMES.len())?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

impl<H> PostCommitWork<H>
where
    H: AsRef<Handle>,
{
    pub fn complete(self) {
        // This has to happen after exclusive files are flushed or there's a tendency for hole
        // punches to not persist. It doesn't fix the problem, but it significantly reduces it.
        if !self.handle.as_ref().instance_limits.disable_hole_punching {
            self.handle
                .as_ref()
                .send_values_for_delete(self.deleted_values);
        }
        // Forget any references to clones of files that have changed.
        for file_id in self.altered_files {
            self.handle.as_ref().clones.lock().unwrap().remove(&file_id);
        }
    }
}

// I can't work out how to have a reference to the Connection, and a transaction on it here at the
// same time. TODO: Make this private.
pub struct Transaction<'h, H> {
    tx: rusqlite::Transaction<'h>,
    handle: H,
    deleted_values: Vec<NonzeroValueLocation>,
    altered_files: HashSet<FileId>,
}

// TODO: Try doing this with a read trait that just requires a rusqlite::Transaction be available.

impl<H> ReadOnlyTransactionAccessor for Transaction<'_, H> {
    fn readonly_transaction(&self) -> &rusqlite::Transaction {
        &self.tx
    }
}

impl<T> ReadTransaction for T where T: ReadOnlyTransactionAccessor {}

impl<H> Transaction<'_, H> {
    pub fn touch_for_read(&mut self, key: &[u8]) -> rusqlite::Result<Value> {
        // Avoid modifying the manifest. We had to take a write lock already to ensure our data
        // isn't modified on us, but it still seems to be an improvement. (-67% on read times in
        // fact).
        let (file_id, file_offset, value_length, mut last_used, now) = self
            .tx
            .prepare_cached_readonly(&format!(
                "select {}, cast(unixepoch('subsec')*1e3 as integer) \
                from keys where key=?",
                value_columns_sql()
            ))?
            .query_row([key], |row| row.try_into())?;
        let update_last_used = last_used != now;
        // eprintln!("updating last used: {}", update_last_used);
        if update_last_used {
            let (new_last_used,) = self
                .tx
                .prepare_cached(
                    r"
                    update keys
                    set last_used=cast(unixepoch('subsec')*1e3 as integer)
                    where key=?
                    returning last_used
                    ",
                )?
                .query_row([key], |row| row.try_into())?;
            // This can in fact change between calls. Since we're updating now anyway, we don't
            // really care.
            //assert_eq!(new_last_used, now);
            last_used = new_last_used;
        }
        Value::from_column_values(file_id, file_offset, value_length, last_used)
    }
}

impl<'h, H> Transaction<'h, H>
where
    H: AsRef<Handle>,
{
    pub fn handle(&self) -> &Handle {
        self.handle.as_ref()
    }

    pub(crate) fn commit(mut self) -> Result<PostCommitWork<H>> {
        self.apply_limits()?;
        self.tx.commit()?;
        Ok(PostCommitWork {
            handle: self.handle,
            deleted_values: self.deleted_values,
            altered_files: self.altered_files,
        })
    }

    pub fn apply_limits(&mut self) -> Result<()> {
        if self.tx.transaction_state(None)? != rusqlite::TransactionState::Write {
            return Ok(());
        }
        if let Some(max) = self.handle.as_ref().instance_limits.max_value_length_sum {
            loop {
                let actual = self
                    .sum_value_length()
                    .context("reading value_length sum")?;
                if actual <= max {
                    break;
                }
                self.evict_values(actual - max)?;
            }
        }
        Ok(())
    }
    pub fn new(tx: rusqlite::Transaction<'h>, handle: H) -> Self {
        Self {
            tx,
            handle,
            deleted_values: vec![],
            altered_files: Default::default(),
        }
    }

    // TODO: Add a test for renaming onto itself.
    pub fn rename_value(&mut self, value: &Value, new_key: Vec<u8>) -> PubResult<bool> {
        match self
            .tx
            .prepare_cached(&format!(
                "delete from keys where key=? returning {}",
                value_columns_sql()
            ))?
            .query_row(params![&new_key], Value::from_row)
        {
            Err(QueryReturnedNoRows) => {}
            Err(err) => return Err(err.into()),
            Ok(existing_value) => {
                match existing_value.location {
                    Nonzero(a) => {
                        let b = value;
                        if Some(a.file_offset) == b.file_offset() && Some(&a.file_id) == b.file_id()
                        {
                            assert_eq!(a.length, b.length());
                            // Renamed but the name is the same.
                            return Ok(true);
                        }
                        // Schedule the value that previously had the key to be hole punched.
                        self.deleted_values.push(a);
                    }
                    ZeroLength => {}
                }
            }
        };

        let res: rusqlite::Result<ValueLength> = self
            .tx
            .prepare_cached(
                "update keys set key=? where file_id=? and file_offset=?\
                returning value_length",
            )?
            .query_row(
                params![new_key, value.file_id(), value.file_offset()],
                |row| row.get(0),
            );
        match res {
            Err(QueryReturnedNoRows) => Ok(false),
            Err(err) => Err(err).context("updating value key").map_err(Into::into),
            Ok(value_length) => {
                assert_eq!(value_length, value.length());
                Ok(true)
            }
        }
    }

    // I guess this doesn't handle destination collisions? It should give a unique constraint error
    // from sqlite.
    pub fn rename_item(&mut self, from: &[u8], to: &[u8]) -> PubResult<Timestamp> {
        let row_result = self.tx.query_row(
            "update keys set key=? where key=? returning last_used",
            [to, from],
            |row| {
                let ts: Timestamp = row.get(0)?;
                Ok(ts)
            },
        );
        let last_used = match row_result {
            Err(QueryReturnedNoRows) => Err(Error::NoSuchKey),
            Ok(ok) => Ok(ok),
            Err(err) => Err(err.into()),
        }?;
        assert_eq!(self.tx.changes(), 1);
        Ok(last_used)
    }

    pub(crate) fn insert_key(&mut self, pw: PendingWrite) -> rusqlite::Result<()> {
        let mut file_id = Some(pw.value_file_id);
        let mut file_offset = Some(pw.value_file_offset);
        if pw.value_length == 0 {
            file_id = None;
            file_offset = None;
        }
        let inserted = self
            .tx
            .prepare_cached(
                "insert into keys (key, file_id, file_offset, value_length)\
                values (?, ?, ?, ?)",
            )?
            .execute(rusqlite::params!(
                pw.key,
                file_id,
                file_offset,
                pw.value_length
            ))?;
        assert_eq!(inserted, 1);
        if pw.value_length != 0 {
            self.altered_files.insert(pw.value_file_id);
        }
        Ok(())
    }

    fn push_value_for_deletion(&mut self, value: Value) {
        match value.location {
            Nonzero(location) => self.deleted_values.push(location),
            ZeroLength => {}
        }
    }

    pub fn delete_key(&mut self, key: &[u8]) -> rusqlite::Result<Option<c_api::PossumStat>> {
        let res = self
            .tx
            .prepare_cached(&format!(
                "delete from keys where key=? returning {}",
                value_columns_sql()
            ))?
            .query_row([key], Value::from_row);
        match res {
            Err(QueryReturnedNoRows) => Ok(None),
            Ok(value) => {
                let stat = value.as_ref().into();
                self.push_value_for_deletion(value);
                Ok(Some(stat))
            }
            Err(err) => Err(err),
        }
    }

    pub fn evict_values(&mut self, target_bytes: u64) -> Result<()> {
        let mut stmt = self.tx.prepare_cached(&format!(
            "delete from keys where key_id in (\
                select key_id from keys order by last_used limit 1\
            )\
            returning {}",
            value_columns_sql()
        ))?;
        let mut value_bytes_deleted = 0;
        let mut values_deleted = vec![];
        while value_bytes_deleted < target_bytes {
            let value = stmt.query_row([], Value::from_row)?;
            value_bytes_deleted += value.length();
            info!("evicting {:?}", &value);
            values_deleted.push(value);
        }
        drop(stmt);
        for value in values_deleted {
            self.push_value_for_deletion(value);
        }
        Ok(())
    }
}
