use super::*;
use std::convert::TryInto;
use std::fs::File;
use std::io;
use std::io::SeekFrom;
use std::io::SeekFrom::Start;

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

impl FileLocking for File {
    fn trim_exclusive_lock_left(&self, old: u64, new: u64) -> io::Result<bool> {
        assert!(self.lock_segment(UnlockNonblock, None, old)?);
        self.lock_segment(LockExclusiveNonblock, None, new)
    }

    fn lock_segment(&self, arg: FlockArg, len: Option<u64>, offset: u64) -> io::Result<bool> {
        lock_file_segment(
            self,
            arg,
            len.map(|len| len.try_into().unwrap()),
            Start(offset),
        )
    }
}

pub fn lock_file_segment(
    file: &File,
    arg: FlockArg,
    len: Option<i64>,
    whence: SeekFrom,
) -> io::Result<bool> {
    // I'm not sure if we need an event if we want to do blocking locks and unlocks.
    // let event = unsafe { CreateEventA(None, false, false, PCSTR::null()) }?;
    let handle = HANDLE(file.as_raw_handle() as isize);
    let Start(offset) = whence else {
        unimplemented!("{:?}", whence)
    };
    // The example suggests we might not need an event at all. We just need to point to an
    // OVERLAPPED (which we're now using for the lock start offset).
    let mut overlapped = OVERLAPPED {
        // hEvent: event,
        ..Default::default()
    };
    let offset_parts = HighAndLow::from(offset);
    overlapped.Anonymous.Anonymous.Offset = offset_parts.low;
    overlapped.Anonymous.Anonymous.OffsetHigh = offset_parts.high;
    let lpoverlapped = &mut overlapped as *mut _;
    // We need to map LOCKFILE_FAIL_IMMEDIATELY causing a FALSE return, and presumably an
    // ERROR_SUCCESS Windows error.
    let convert = |res: ::windows::core::Result<()>| match res {
        Ok(()) => Ok(true),
        Err(err)
            if err.code().is_ok()
                || [
                    ERROR_LOCK_VIOLATION.into(),
                    ERROR_IO_PENDING.into(),
                    // ERROR_ACCESS_DENIED.into(),
                ]
                .contains(&err.code()) =>
        {
            Ok(false)
        }
        Err(err) => Err(err.into()),
    };
    let len = match len {
        Some(len) => len.try_into().unwrap(),
        None => MAX_LOCKFILE_OFFSET - offset,
    };
    let num_bytes_to_lock = HighAndLow::from(len);
    if matches!(arg, Unlock | UnlockNonblock) {
        return convert(unsafe {
            UnlockFileEx(
                handle,
                0,
                num_bytes_to_lock.low,
                num_bytes_to_lock.high,
                lpoverlapped,
            )
        });
    }
    let mut dwflags = LOCK_FILE_FLAGS(0);
    if matches!(arg, LockExclusive | LockExclusiveNonblock) {
        dwflags |= LOCKFILE_EXCLUSIVE_LOCK;
    }
    if matches!(arg, LockSharedNonblock | LockExclusiveNonblock) {
        dwflags |= LOCKFILE_FAIL_IMMEDIATELY;
    }
    let result = convert(dbg!(unsafe {
        LockFileEx(
            handle,
            dwflags,
            0,
            num_bytes_to_lock.low,
            num_bytes_to_lock.high,
            lpoverlapped,
        )
    }));
    // unsafe { CloseHandle(event) }.unwrap();
    result
}

const MAX_LOCKFILE_OFFSET: u64 = u64::MAX;

#[derive(Debug)]
struct HighAndLow {
    high: u32,
    low: u32,
}

impl From<u64> for HighAndLow {
    fn from(both: u64) -> Self {
        Self {
            high: (both >> 32) as u32,
            low: both as u32,
        }
    }
}
