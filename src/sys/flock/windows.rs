use super::*;

pub enum FlockArg {
    LockShared,
    LockExclusive,
    Unlock,
    LockSharedNonblock,
    LockExclusiveNonblock,
    UnlockNonblock,
}

use ::windows::Win32::System::Threading::CreateEventA;
pub use FlockArg::*;

pub fn try_lock_file(file: &mut File, arg: FlockArg) -> PubResult<bool> {
    let lock_low = 1;
    let lock_high = 0;
    let event = unsafe { CreateEventA(None, false, false, PCSTR::null()) }?;
    // We lock and unlock an arbitrary 4 GiB, since Windows doesn't have whole-file locking.
    // Possibly we should use MAXDWORD or 0xffffffff instead.
    let handle = HANDLE(file.as_raw_handle() as isize);
    let mut overlapped = OVERLAPPED {
        hEvent: event,
        ..Default::default()
    };
    let lpoverlapped = &mut overlapped as *mut _;
    // We need to map LOCKFILE_FAIL_IMMEDIATELY causing a FALSE return, and presumably an
    // ERROR_SUCCESS Windows error.
    let convert = |res: ::windows::core::Result<()>| match res {
        Ok(()) => Ok(true),
        Err(err) if err.code().is_ok() || err.code()==ERROR_LOCK_VIOLATION.into() => Ok(false),
        Err(err) => Err(err.into()),
    };
    if matches!(arg, Unlock | UnlockNonblock) {
        return convert(unsafe { UnlockFileEx(handle, 0, lock_low, lock_high, lpoverlapped) });
    }
    let mut dwflags = LOCK_FILE_FLAGS(0);
    if matches!(arg, LockExclusive | LockExclusiveNonblock) {
        dwflags |= LOCKFILE_EXCLUSIVE_LOCK;
    }
    if matches!(arg, LockSharedNonblock | LockExclusiveNonblock) {
        dwflags |= LOCKFILE_FAIL_IMMEDIATELY;
    }
    let result = convert(unsafe { LockFileEx(handle, dwflags, 0, lock_low, lock_high, lpoverlapped) });
    unsafe{CloseHandle(event)}.unwrap();
    result
}
