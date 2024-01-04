//! Syscall wrappers for hole punching, system configuration, hole-seeking ( ͡° ͜ʖ ͡°), file cloning
//! etc.

use std::os::fd::RawFd;
use std::path::Path;

use crate::CPathBuf;

/// _PC_MIN_HOLE_SIZE is 27 on Darwin.
/// https://github.com/apple/darwin-xnu/blob/main/bsd/sys/unistd.h. It doesn't seem to be defined
/// in the nix or libc crates for Darwin.
#[cfg(target_os = "macos")]
const _PC_MIN_HOLE_SIZE: i32 = 27;

/// Recommended minimum hole size for sparse files for file descriptor.
/// fpathconf(_PC_MIN_HOLE_SIZE);
pub fn fd_min_hole_size(fd: RawFd) -> std::io::Result<i64> {
    let long = unsafe { libc::fpathconf(fd, _PC_MIN_HOLE_SIZE) };
    if long == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(long)
}

/// Recommended minimum hole size for sparse files for file descriptor.
/// fpathconf(_PC_MIN_HOLE_SIZE). On macOS this returns positive if holes are supported, and returns
/// 1 if holes are supported but the minimum hole size is unspecified.
pub fn path_min_hole_size(path: &Path) -> std::io::Result<u64> {
    let path: CPathBuf = path.try_into()?;
    let long = unsafe { libc::pathconf(path.as_ptr(), _PC_MIN_HOLE_SIZE) };
    if long == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(long as u64)
}
