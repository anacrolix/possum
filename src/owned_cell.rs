//! This is a module to protect the internals of the helper type.

use std::ops::Deref;

use stable_deref_trait::StableDeref;

use super::*;

/// Store a value (the dependent, D) that needs a stable reference to another value (the owner, O)
/// together. This is useful to allow the owner to move around with the dependent to ensure it gets
/// dropped when the dependent is no longer needed. This is only possible if the owner implements
/// StableDeref, a market trait for types that can be moved while there are references to them. A
/// great example is MutexGuard.
pub(crate) struct OwnedCell<O, D> {
    // The order here matters. dep must be dropped before owner.
    dep: D,
    owner: O,
}

impl<O, D> OwnedCell<O, D> {
    pub fn owner(&self) -> &O {
        &self.owner
    }
}

impl<O, D> OwnedCell<O, D>
where
    // There's no StableDerefMut, but if StableDeref exists that might be a sufficient marker.
    O: StableDeref + DerefMut,
{
    /// Create the dependent value using an exclusive reference to the owner's deref type
    /// that's promised to outlive the dependent value.
    pub(crate) fn try_make_mut<'a, E>(
        mut owner: O,
        make_dependent: impl FnOnce(&'a mut O::Target) -> Result<D, E>,
    ) -> Result<Self, E>
    where
        O::Target: 'a,
    {
        // Deref knowing that when guard is moved, the deref will still be valid.
        let stable_deref: *mut O::Target = owner.deref_mut();
        Ok(Self {
            owner,
            dep: make_dependent(unsafe { &mut *stable_deref })?,
        })
    }
}

/// Allows for a dependent value that holds a reference to its owner in the same struct.
impl<O, D> OwnedCell<O, D>
where
    // There's no StableDerefMut, but if StableDeref exists that might be a sufficient marker.
    O: StableDeref,
{
    /// Create the dependent value using an exclusive reference to the owner's deref type
    /// that's promised to outlive the dependent value.
    pub(crate) fn try_make<'a, E>(
        owner: O,
        make_dependent: impl FnOnce(&'a O::Target) -> Result<D, E>,
    ) -> Result<Self, E>
    where
        O::Target: 'a,
    {
        // Deref knowing that when guard is moved, the deref will still be valid.
        let stable_deref: *const O::Target = owner.deref();
        Ok(Self {
            owner,
            dep: make_dependent(unsafe { &*stable_deref })?,
        })
    }
}

impl<O, D> OwnedCell<O, D> {
    /// Move the dependent type out, before destroying the owner.
    // Another way to do this might be to extract the dependent and owner together, with the dependents lifetime bound
    // to the owner in the return scope.
    pub(crate) fn move_dependent<R>(self, f: impl FnOnce(D) -> R) -> R {
        f(self.dep)
    }
}

impl<O, D> Deref for OwnedCell<O, D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.dep
    }
}

impl<O, D> DerefMut for OwnedCell<O, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.dep
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::{test, Mutex, OwnedCell};

    struct Conn {
        count: usize,
    }

    impl Conn {
        fn transaction(&mut self) -> Result<MutTransaction, Infallible> {
            Ok(MutTransaction { conn: self })
        }
        fn new() -> Self {
            Self { count: 0 }
        }
    }

    struct MutTransaction<'a> {
        conn: &'a mut Conn,
    }

    impl<'a> MutTransaction<'a> {
        fn inc(&mut self) {
            self.conn.count += 1
        }
    }

    /// Test the case where we mutate the owner from the dependent, and then try to access the
    /// owner. Intended for use with miri.
    #[test]
    fn test_miri_readonly_transaction() -> anyhow::Result<()> {
        let conn = Mutex::new(Conn::new());
        let mut cell = OwnedCell::try_make_mut(conn.lock().unwrap(), |conn| conn.transaction())?;
        // We need to access before the mutate or miri doesn't notice.
        assert_eq!(0, cell.owner().count);
        cell.inc();
        assert_eq!(1, cell.owner().count);
        Ok(())
    }
}
