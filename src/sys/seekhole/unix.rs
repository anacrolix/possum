// Applications should use fpathconf(_PC_MIN_HOLE_SIZE) or pathconf(_PC_MIN_HOLE_SIZE) to determine
// if a file system supports SEEK_HOLE. See pathconf(2).

use std::ffi::c_int;
use std::io;
use std::io::Error;
use std::os::fd::RawFd;

use libc::{ENXIO, SEEK_DATA, SEEK_HOLE};

use super::*;

type SeekWhence = c_int;

/// Using 64 bit integer type rather than off_t to enforce 64-bit offsets (the libc wrappers all use
/// type aliases anyway). For SEEK_HOLE and SEEK_DATA, I don't think negative offset has any
/// meaning, and Windows uses u64. So use u64 for consistency.
pub fn seek_hole_whence(
    file: &mut File,
    offset: u64,
    whence: impl Into<SeekWhence>,
) -> io::Result<Option<RegionOffset>> {
    // lseek64?
    match lseek(file.as_raw_fd(), offset as i64, whence) {
        Ok(offset) => Ok(Some(offset as RegionOffset)),
        Err(errno) => {
            if errno == ENXIO {
                Ok(None)
            } else {
                Err(Error::from_raw_os_error(errno))
            }
        }
    }
}

/// Using i64 rather than off_t to enforce 64-bit offsets (the libc wrappers all use type aliases
/// anyway).
fn lseek(
    fd: RawFd,
    offset: i64,
    whence: impl Into<SeekWhence>,
) -> anyhow::Result<RegionOffset, i32> {
    // lseek64?
    let new_offset = unsafe { super::lseek(fd, offset, whence.into()) };
    if new_offset == -1 {
        return Err(Errno::last_raw());
    }
    Ok(new_offset as RegionOffset)
}

impl From<RegionType> for SeekWhence {
    fn from(value: RegionType) -> Self {
        match value {
            Hole => SEEK_HOLE,
            Data => SEEK_DATA,
        }
    }
}
