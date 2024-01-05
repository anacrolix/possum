use super::*;
use std::ops::Deref;

pub struct Transaction<'h> {
    tx: rusqlite::Transaction<'h>,
}

/// This should probably go away when we want to control queries via Transaction, and require a
/// special method to dive deeper for custom stuff.
impl<'h> Deref for Transaction<'h> {
    type Target = rusqlite::Transaction<'h>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl Transaction<'_> {
    pub fn commit(self) -> rusqlite::Result<()> {
        self.tx.commit()
    }

    pub fn delete_key(&self, key: &[u8]) -> rusqlite::Result<Option<Value>> {
        match self.query_row(
            &format!(
                "delete from keys where key=? returning {}",
                value_columns_sql()
            ),
            [key],
            Value::from_row,
        ) {
            Err(QueryReturnedNoRows) => Ok(None),
            Ok(value) => Ok(Some(value)),
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
        let iter = FileValues {
            stmt,
            file_id,
            // init: |stmt: &mut Statement| stmt.query_map(&[file_id], Value::from_row).unwrap(),
        };
        Ok(iter)
    }
}

impl<'a> From<rusqlite::Transaction<'a>> for Transaction<'a> {
    fn from(tx: rusqlite::Transaction<'a>) -> Self {
        Self { tx }
    }
}
