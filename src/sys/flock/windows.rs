use super::*;

pub enum FlockArg {
    LockShared,
    LockExclusive,
    Unlock,
    LockSharedNonblock,
    LockExclusiveNonblock,
    UnlockNonblock,
}

pub use FlockArg::*;

pub fn try_lock_file(file: &mut File, arg: FlockArg) -> PubResult<bool> {
    let handle = file.as_raw_handle();
    Ok(true)
}
