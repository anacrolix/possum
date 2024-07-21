pub(crate) mod sync;

#[cfg(not(shuttle))]
pub use std::thread;

// This isn't available in loom or shuttle yet. Unfortunately for shuttle it means threads are
// spawned outside its control, and it doesn't work.
#[cfg(shuttle)]
pub use shuttle::thread;
