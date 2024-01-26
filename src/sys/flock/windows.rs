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
    // We lock and unlock an arbitrary 4 GiB, since Windows doesn't have whole-file locking.
    // Possibly we should use MAXDWORD or 0xffffffff instead.
    let handle = HANDLE(file.as_raw_handle() as isize);
    let lpoverlapped = std::ptr::null_mut();
    // We need to map LOCKFILE_FAIL_IMMEDIATELY causing a FALSE return, and presumably an
    // ERROR_SUCCESS Windows error.
    let convert = |res: ::windows::core::Result<()>| match res {
        Ok(()) => Ok(true),
        Err(err) if err.code().is_ok() => Ok(false),
        Err(err) => Err(err.into()),
    };
    if matches!(arg, Unlock | UnlockNonblock) {
        return convert(unsafe { UnlockFileEx(handle, 0, 0, 1, lpoverlapped) });
    }
    let mut dwflags = LOCK_FILE_FLAGS(0);
    if matches!(arg, LockExclusive | LockExclusiveNonblock) {
        dwflags |= LOCKFILE_EXCLUSIVE_LOCK;
    }
    if matches!(arg, LockSharedNonblock | LockExclusiveNonblock) {
        dwflags |= LOCKFILE_FAIL_IMMEDIATELY;
    }
    convert(unsafe { LockFileEx(handle, dwflags, 0, 0, 1, lpoverlapped) })
}
