use super::*;
use crate::tx::ReadTransactionOwned;

/// A Sqlite Transaction and the mutex guard on the Connection it came from.
// Not in the handle module since it can be owned by types other than Handle.
pub struct OwnedTx<'handle> {
    cell: OwnedTxInner<'handle, Transaction<'handle>>,
}

pub type OwnedTxInner<'h, T> = owned_cell::OwnedCell<MutexGuard<'h, Connection>, T>;

impl<'a> From<OwnedTxInner<'a, Transaction<'a>>> for OwnedTx<'a> {
    fn from(cell: OwnedTxInner<'a, Transaction<'a>>) -> Self {
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
    // Except for this move dependent dance it shouldn't be necessary to wrap the OwnedCell.
    pub fn commit<T>(self, reward: T) -> Result<PostCommitWork<'a, T>> {
        self.cell.move_dependent(|tx| tx.commit(reward))
    }
}

type OwnedReadTxCell<'h> =
    owned_cell::OwnedCell<MutexGuard<'h, Connection>, ReadTransactionOwned<'h>>;

pub struct OwnedReadTx<'h> {
    cell: OwnedReadTxCell<'h>,
}

impl<'h> From<OwnedTxInner<'h, ReadTransactionOwned<'h>>> for OwnedReadTx<'h> {
    fn from(cell: OwnedTxInner<'h, ReadTransactionOwned<'h>>) -> Self {
        Self { cell }
    }
}

impl<'h> Deref for OwnedReadTx<'h> {
    type Target = ReadTransactionOwned<'h>;

    fn deref(&self) -> &Self::Target {
        &self.cell
    }
}
