use super::*;
use log::info;
use std::ops::Deref;

/// This is more work to be done after the Handle conn mutex is released.
#[must_use]
pub struct PostCommitWork<'h, T> {
    handle: &'h Handle,
    deleted_values: Vec<Value>,
    altered_files: HashSet<FileId>,
    reward: T,
}

impl<'h, T> PostCommitWork<'h, T> {
    pub fn complete(self, conn: &mut Connection) -> Result<T> {
        // This has to happen after exclusive files are flushed or there's a tendency for hole
        // punches to not persist. It doesn't fix the problem but it significantly reduces it.
        self.handle.punch_values(&self.deleted_values, conn)?;
        // Forget any references to clones of files that have changed.
        for file_id in self.altered_files {
            self.handle.clones.lock().unwrap().remove(&file_id);
        }
        Ok(self.reward)
    }
}

// I can't work out how to have a reference to the Connection, and a transaction on it here at the
// same time.
pub struct Transaction<'h, 'c> {
    tx: rusqlite::Transaction<'c>,
    handle: &'h Handle,
    deleted_values: Vec<Value>,
    altered_files: HashSet<FileId>,
}

impl<'h, 'c> Transaction<'h, 'c> {
    pub fn new(tx: rusqlite::Transaction<'c>, handle: &'h Handle) -> Self {
        Self {
            tx,
            handle,
            deleted_values: vec![],
            altered_files: Default::default(),
        }
    }

    pub fn commit<T>(mut self, reward: T) -> Result<PostCommitWork<'h, T>> {
        self.apply_limits()?;
        self.tx.commit()?;
        Ok(PostCommitWork {
            handle: self.handle,
            deleted_values: self.deleted_values,
            altered_files: self.altered_files,
            reward,
        })
    }

    pub fn touch_for_read(&mut self, key: &[u8]) -> rusqlite::Result<Value> {
        self.tx
            .prepare_cached(&format!(
                "update keys \
                set last_used=cast(unixepoch('subsec')*1e3 as integer) \
                where key=? \
                returning {}",
                value_columns_sql()
            ))?
            .query_row([key], Value::from_row)
    }

    pub fn rename_item(&mut self, from: &[u8], to: &[u8]) -> PubResult<Timestamp> {
        let last_used = match self.tx.query_row(
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
        assert_eq!(self.tx.changes(), 1);
        Ok(last_used)
    }

    pub(crate) fn insert_key(&mut self, pw: PendingWrite) -> rusqlite::Result<()> {
        let inserted = self.tx.execute(
            "insert into keys (key, file_id, file_offset, value_length)\
                values (?, ?, ?, ?)",
            rusqlite::params!(
                pw.key,
                pw.value_file_id.deref(),
                pw.value_file_offset,
                pw.value_length
            ),
        )?;
        assert_eq!(inserted, 1);
        if pw.value_length != 0 {
            self.altered_files.insert(pw.value_file_id);
        }
        Ok(())
    }

    pub fn delete_key(&mut self, key: &[u8]) -> rusqlite::Result<Option<c_api::PossumStat>> {
        match self.tx.query_row(
            &format!(
                "delete from keys where key=? returning {}",
                value_columns_sql()
            ),
            [key],
            Value::from_row,
        ) {
            Err(QueryReturnedNoRows) => Ok(None),
            Ok(value) => {
                let stat = value.as_ref().into();
                self.deleted_values.push(value);
                Ok(Some(stat))
            }
            Err(err) => Err(err),
        }
    }

    pub fn file_values<'a>(
        &'a self,
        file_id: &'a FileIdFancy,
    ) -> rusqlite::Result<FileValues<'a, CachedStatement<'a>>> {
        let stmt = self.tx.prepare_cached(&format!(
            "select {} from keys where file_id=? order by file_offset",
            value_columns_sql()
        ))?;
        let iter = FileValues { stmt, file_id };
        Ok(iter)
    }

    pub fn sum_value_length(&self) -> rusqlite::Result<u64> {
        self.tx
            .query_row_and_then("select sum(value_length) from keys", [], |row| row.get(0))
            .map_err(Into::into)
    }

    pub fn apply_limits(&mut self) -> Result<()> {
        if let Some(max) = self.handle.opts.max_value_length {
            loop {
                let actual = self.sum_value_length()?;
                if actual <= max {
                    break;
                }
                self.evict_values(actual - max)?;
            }
        }
        Ok(())
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
        while value_bytes_deleted < target_bytes {
            let value = stmt.query_row([], Value::from_row)?;
            value_bytes_deleted += value.length();
            info!("evicting {:?}", &value);
            self.deleted_values.push(value);
        }
        Ok(())
    }

    /// Returns the end offset of the last active value before offset in the same file.
    pub fn query_last_end_offset(&self, file_id: &FileId, offset: u64) -> rusqlite::Result<u64> {
        self.tx.query_row(
            "select max(file_offset+value_length) as last_offset \
            from keys \
            where file_id=? and file_offset+value_length <= ?",
            params![file_id.deref(), offset],
            |row| {
                // I don't know why, but this can return null for file_ids that have values but
                // don't fit the other conditions.
                let res: rusqlite::Result<Option<_>> = row.get(0);
                res.map(|v| v.unwrap_or_default())
            },
        )
    }

    pub fn list_items(&self, prefix: &[u8]) -> PubResult<Vec<Item>> {
        let range_end = {
            let mut prefix = prefix.to_owned();
            if inc_big_endian_array(&mut prefix) {
                Some(prefix)
            } else {
                None
            }
        };
        match range_end {
            None => self.list_items_inner(
                &format!(
                    "select {}, key from keys where key >= ?",
                    value_columns_sql()
                ),
                [prefix],
            ),
            Some(range_end) => self.list_items_inner(
                &format!(
                    "select {}, key from keys where key >= ? and key < ?",
                    value_columns_sql()
                ),
                rusqlite::params![prefix, range_end],
            ),
        }
    }

    fn list_items_inner(&self, sql: &str, params: impl rusqlite::Params) -> PubResult<Vec<Item>> {
        self.tx
            .prepare_cached(sql)
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
}
