use super::*;
use crate::tx::ReadTransactionOwned;

/// A Sqlite Transaction and the mutex guard on the Connection it came from.
// Not in the handle module since it can be owned by types other than Handle.
pub struct OwnedTx<'handle> {
    cell: OwnedTxInner<'handle, Transaction<'handle>>,
}

pub(crate) type OwnedTxInner<'h, T> = owned_cell::OwnedCell<MutexGuard<'h, Connection>, T>;

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
    pub fn commit(self) -> Result<PostCommitWork<'a>> {
        self.cell.move_dependent(|tx| tx.commit())
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

pub(crate) trait OwnedTxTrait {
    type Tx;
    fn end_tx<R>(self, take: impl FnOnce(Self::Tx) -> R) -> R;
    fn as_handle(&self) -> &Handle;
    fn mut_transaction(&mut self) -> &mut Self::Tx;
    fn transaction(&self) -> &Self::Tx;
}

// impl<'h, T> OwnedTxTrait for OwnedTxInner<'h, T> {
//     type Tx = T;
//
//     fn end_tx<R>(self, take: impl FnOnce(Self::Tx)->R) ->R{
//         self.move_dependent(take)
//     }
//
//     fn as_handle(&self) -> &Handle {
//         todo!()
//     }
//
//     fn mut_transaction(&mut self) -> &mut Self::Tx {
//         self.deref_mut()
//     }
// }

impl<'h> OwnedTxTrait for OwnedTx<'h> {
    type Tx = Transaction<'h>;

    fn end_tx<R>(self, take: impl FnOnce(Self::Tx) -> R) -> R {
        todo!()
    }

    fn as_handle(&self) -> &Handle {
        todo!()
    }

    fn mut_transaction(&mut self) -> &mut Self::Tx {
        todo!()
    }

    fn transaction(&self) -> &Self::Tx {
        self.cell.deref()
    }
}
