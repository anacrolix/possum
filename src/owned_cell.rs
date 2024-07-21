//! This is a module to protect the internals of the helper type.

use std::ops::Deref;

use stable_deref_trait::StableDeref;

use super::*;

/// The shared part of owned cell types.
pub(crate) struct OwnedCellInner<O, D> {
    // The order here matters. dep must be dropped before owner.
    dep: D,
    owner: O,
}

impl<O, D> Deref for OwnedCellInner<O, D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.dep
    }
}

impl<O, D> DerefMut for OwnedCellInner<O, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.dep
    }
}
/// Store a value (the dependent, D) that needs a stable reference to another value (the owner, O)
/// together. This is useful to allow the owner to move around with the dependent to ensure it gets
/// dropped when the dependent is no longer needed. This is only possible if the owner implements
/// StableDeref, a market trait for types that can be moved while there are references to them. A
/// great example is MutexGuard. OwnedCell does not allow mutable references to O from D. See
/// MutOwnedCell for that.
pub(crate) struct OwnedCell<O, D> {
    inner: OwnedCellInner<O, D>,
}

impl<O, D> OwnedCell<O, D> {
    /// This is not available on MutOwnedCell.
    pub fn owner(&self) -> &O {
        &self.inner.owner
    }
}

// Self-referential pair of types where the dependent type may have mutable references to the owner.
pub(crate) struct MutOwnedCell<O, D> {
    inner: OwnedCellInner<O, D>,
}

impl<O, D> MutOwnedCell<O, D>
where
    // There's no StableDerefMut, but if StableDeref exists that might be a sufficient marker.
    O: StableDeref + DerefMut,
{
    /// Create the dependent value using an exclusive reference to the owner's deref type
    /// that's promised to outlive the dependent value.
    pub(crate) fn try_make<'a, E>(
        mut owner: O,
        make_dependent: impl FnOnce(&'a mut O::Target) -> Result<D, E>,
    ) -> Result<Self, E>
    where
        O::Target: 'a,
    {
        // Deref knowing that when guard is moved, the deref will still be valid.
        let stable_deref: *mut O::Target = owner.deref_mut();
        Ok(Self {
            inner: OwnedCellInner {
                owner,
                dep: make_dependent(unsafe { &mut *stable_deref })?,
            },
        })
    }
}

/// Allows for a dependent value that holds a reference to its owner in the same struct.
impl<O, D> OwnedCell<O, D>
where
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
            inner: OwnedCellInner {
                owner,
                dep: make_dependent(unsafe { &*stable_deref })?,
            },
        })
    }
}

pub(crate) trait MoveDependent<D> {
    /// Move the dependent type out, before destroying the owner.
    // TODO: Another way to do this might be to extract the dependent and owner together, with the
    // dependents lifetime bound to the owner in the return scope.
    fn move_dependent<R>(self, f: impl FnOnce(D) -> R) -> R;
}

impl<O, D> MoveDependent<D> for MutOwnedCell<O, D> {
    fn move_dependent<R>(self, f: impl FnOnce(D) -> R) -> R {
        let res = f(self.inner.dep);
        drop(self.inner.owner);
        res
    }
}

impl<O, D> MoveDependent<D> for OwnedCell<O, D> {
    fn move_dependent<R>(self, f: impl FnOnce(D) -> R) -> R {
        let res = f(self.inner.dep);
        drop(self.inner.owner);
        res
    }
}

// TODO: Share this between MutOwnedCell and OwnedCell.
#[allow(dead_code)]
impl<O, D> OwnedCellInner<O, D> {
    /// Move the dependent type out, before destroying the owner.
    // Another way to do this might be to extract the dependent and owner together, with the
    // dependent's lifetime bound to the owner in the return scope.
    fn move_dependent<R>(self, f: impl FnOnce(D) -> R) -> R {
        f(self.dep)
    }
}

impl<O, D> DerefMut for OwnedCell<O, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<O, D> Deref for OwnedCell<O, D> {
    type Target = OwnedCellInner<O, D>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<O, D> Deref for MutOwnedCell<O, D> {
    type Target = OwnedCellInner<O, D>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<O, D> DerefMut for MutOwnedCell<O, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use self::test;
    use super::*;

    struct Owner;

    impl Deref for Owner {
        type Target = ();

        fn deref(&self) -> &Self::Target {
            &()
        }
    }

    unsafe impl StableDeref for Owner {}

    impl Drop for Owner {
        fn drop(&mut self) {
            eprintln!("dropping owner")
        }
    }

    struct Dep;

    impl Drop for Dep {
        fn drop(&mut self) {
            eprintln!("dropping dep")
        }
    }

    #[test]
    fn test_owned_cell_dropped_field_drop_ordering() -> anyhow::Result<()> {
        OwnedCell::try_make(Owner {}, |_| anyhow::Ok(Dep {}))?;
        Ok(())
    }

    #[test]
    fn test_move_dependent_field_drop_ordering() -> anyhow::Result<()> {
        let owned_cell = OwnedCell::try_make(Owner {}, |_| anyhow::Ok(Dep {}))?;
        owned_cell.move_dependent(|_dep| println!("dep moving"));
        Ok(())
    }
}
