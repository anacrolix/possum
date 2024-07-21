use crate::StableDeref;
use std::ops::{Deref, DerefMut};

#[cfg(feature = "shuttle")]
use shuttle::sync;
#[cfg(not(feature = "shuttle"))]
use std::sync;

use sync::Mutex as InnerMutex;
use sync::MutexGuard as InnerMutexGuard;
pub use sync::*;
// These types work in any sync context.
use std::sync::{LockResult, PoisonError};

// We need to wrap the real mutex guard in use, so we can implement StableDeref on it.
pub struct MutexGuard<'a, T>(InnerMutexGuard<'a, T>);

impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<T> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}

// We need our own mutex so when it's locked we get a MutexGuard that implements StableDeref.
#[derive(Debug, Default)]
pub struct Mutex<T>(InnerMutex<T>);

unsafe impl<T> StableDeref for MutexGuard<'_, T> {}

impl<T> Mutex<T> {
    pub fn lock(&self) -> LockResult<MutexGuard<T>> {
        // This is super dumb. There's a map_result in std::sync::poison that I can't get at that
        // does the same thing I think.
        match self.0.lock() {
            Ok(inner_guard) => Ok(self::MutexGuard(inner_guard)),
            Err(err) => Err(PoisonError::new(self::MutexGuard(err.into_inner()))),
        }
    }
    pub fn new(t: T) -> Self {
        Self(InnerMutex::new(t))
    }
}
