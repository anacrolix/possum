use super::*;

/// A Sqlite Transaction and the mutex guard on the Connection it came from.
// Not in the handle module since it can be owned by types other than Handle.
pub struct OwnedTx<'handle> {
    cell: OwnedTxInner<'handle>,
}

type OwnedTxInner<'handle> =
    owned_cell::OwnedCell<MutexGuard<'handle, Connection>, Transaction<'handle>>;

impl<'a> From<OwnedTxInner<'a>> for OwnedTx<'a> {
    fn from(cell: OwnedTxInner<'a>) -> Self {
        Self { cell }
    }
}

impl<'a> Deref for OwnedTx<'a> {
    type Target = Transaction<'a>;

    fn deref(&self) -> &Self::Target {
        &self.cell
    }
}

impl DerefMut for OwnedTx<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cell
    }
}

impl<'a> OwnedTx<'a> {
    pub fn commit(self) -> Result<()> {
        let post_commit_work = self.cell.move_dependent(|tx| tx.commit())?;
        post_commit_work.complete()
    }
}

// impl AsRef<rusqlite::Connection> for OwnedTx<'_> {
//     fn as_ref(&self) -> &Connection {
//         self.cell
//     }
// }
