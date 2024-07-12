use std::rc::Rc;
use std::sync::RwLockReadGuard;

use super::*;
use crate::c_api::PossumReaderOwnedTransaction;
use crate::owned_cell::*;
use crate::tx::ReadTransactionOwned;

/// A Sqlite Transaction and the mutex guard on the Connection it came from.
///
// Not in the handle module since it can be owned by types other than Handle. TODO: Make this
// private.
pub struct OwnedTx<'handle> {
    cell: OwnedTxInner<'handle, Transaction<'handle, &'handle Handle>>,
}

pub(crate) type OwnedTxInner<'h, T> = MutOwnedCell<MutexGuard<'h, Connection>, T>;

impl<'a> From<OwnedTxInner<'a, Transaction<'a, &'a Handle>>> for OwnedTx<'a> {
    fn from(cell: OwnedTxInner<'a, Transaction<'a, &'a Handle>>) -> Self {
        Self { cell }
    }
}

impl<'a> Deref for OwnedTx<'a> {
    type Target = Transaction<'a, &'a Handle>;

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
    pub(crate) fn commit(self) -> Result<PostCommitWork<&'a Handle>> {
        self.cell.move_dependent(|tx| tx.commit())
    }
}

type OwnedReadTxCell<'h> =
    MutOwnedCell<MutexGuard<'h, Connection>, ReadTransactionOwned<'h>>;

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

impl<'h> OwnedTxTrait for OwnedTx<'h> {
    type Tx = Transaction<'h, &'h Handle>;

    fn end_tx<R>(self, take: impl FnOnce(Self::Tx) -> R) -> R {
        self.cell.move_dependent(take)
    }

    fn as_handle(&self) -> &Handle {
        self.cell.deref().handle()
    }

    fn mut_transaction(&mut self) -> &mut Self::Tx {
        self.deref_mut()
    }

    fn transaction(&self) -> &Self::Tx {
        self.cell.deref()
    }
}

impl OwnedTxTrait for PossumReaderOwnedTransaction<'static> {
    type Tx = Transaction<'static, Rc<RwLockReadGuard<'static, Handle>>>;

    fn end_tx<R>(self, take: impl FnOnce(Self::Tx) -> R) -> R {
        self.move_dependent(|handle_guard| {
            handle_guard.move_dependent(|conn_guard| conn_guard.move_dependent(take))
        })
    }

    fn as_handle(&self) -> &Handle {
        self.deref().owner()
    }

    fn mut_transaction(&mut self) -> &mut Self::Tx {
        self.deref_mut()
    }

    fn transaction(&self) -> &Self::Tx {
        self.deref()
    }
}
