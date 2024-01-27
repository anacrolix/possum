//! Syscall wrappers for hole punching, system configuration, hole-seeking ( ͡° ͜ʖ ͡°), file cloning
//! etc.

#![allow(unused_imports)]
// There are exports here that aren't yet used (they're hardcoded instead).
#![allow(dead_code)]

use super::*;

/// _PC_MIN_HOLE_SIZE is 27 on Darwin.
/// https://github.com/apple/darwin-xnu/blob/main/bsd/sys/unistd.h. It doesn't seem to be defined
/// in the nix or libc crates for Darwin.
#[cfg(target_os = "macos")]
const _PC_MIN_HOLE_SIZE: i32 = 27;

/// Recommended minimum hole size for sparse files for file descriptor.
/// fpathconf(_PC_MIN_HOLE_SIZE);
pub(crate) fn fd_min_hole_size(file: &File) -> std::io::Result<u64> {
    let fd = file.as_raw_fd();
    let long = unsafe { libc::fpathconf(fd, _PC_MIN_HOLE_SIZE) };
    if long == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(long.try_into().unwrap())
}

/// Recommended minimum hole size for sparse files for file descriptor.
/// fpathconf(_PC_MIN_HOLE_SIZE). On macOS this returns positive if holes are supported, and returns
/// 1 if holes are supported but the minimum hole size is unspecified.
pub(crate) fn path_min_hole_size(path: &Path) -> std::io::Result<u64> {
    let path: crate::cpathbuf::CPathBuf = path.try_into()?;
    let long = unsafe { libc::pathconf(path.as_ptr(), _PC_MIN_HOLE_SIZE) };
    if long == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(long.try_into().unwrap())
}
