//! This is a module to protect the internals of the helper type.

use std::ops::Deref;

use stable_deref_trait::StableDeref;

use super::*;

pub(crate) struct OwnedCell<O, D> {
    _owner: O,
    dep: D,
}

/// Allows for a dependent value that holds a reference to its owner in the same struct.
impl<O, D> OwnedCell<O, D>
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
            _owner: owner,
            dep: make_dependent(unsafe { &mut *stable_deref })?,
        })
    }

    /// Move the dependent type out, before destroying the owner.
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
