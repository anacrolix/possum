//! Syscall wrappers for hole punching, system configuration, hole-seeking ( ͡° ͜ʖ ͡°), file cloning
//! etc.

#![allow(unused_imports)]
// There are exports here that aren't yet used (they're hardcoded instead).
#![allow(dead_code)]

use std::fs::File;
use std::os::fd::{AsRawFd, RawFd};
// Needed for Metadata.st_blksize. I don't think the unix variant works?
#[cfg(target_os = "linux")]
use std::os::linux::fs::MetadataExt;
use std::path::Path;

use crate::CPathBuf;

/// _PC_MIN_HOLE_SIZE is 27 on Darwin.
/// https://github.com/apple/darwin-xnu/blob/main/bsd/sys/unistd.h. It doesn't seem to be defined
/// in the nix or libc crates for Darwin.
#[cfg(target_os = "macos")]
const _PC_MIN_HOLE_SIZE: i32 = 27;

/// Recommended minimum hole size for sparse files for file descriptor.
/// fpathconf(_PC_MIN_HOLE_SIZE);
pub(crate) fn fd_min_hole_size(file: &File) -> std::io::Result<u64> {
    #[cfg(not(target_os = "linux"))]
    {
        let fd = file.as_raw_fd();
        let long = unsafe { libc::fpathconf(fd, _PC_MIN_HOLE_SIZE) };
        if long == -1 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(long.try_into().unwrap())
    }
    #[cfg(target_os = "linux")]
    {
        Ok(file.metadata()?.st_blksize())
    }
}

/// Recommended minimum hole size for sparse files for file descriptor.
/// fpathconf(_PC_MIN_HOLE_SIZE). On macOS this returns positive if holes are supported, and returns
/// 1 if holes are supported but the minimum hole size is unspecified.
pub(crate) fn path_min_hole_size(path: &Path) -> std::io::Result<u64> {
    #[cfg(not(target_os = "linux"))]
    {
        let path: CPathBuf = path.try_into()?;
        let long = unsafe { libc::pathconf(path.as_ptr(), _PC_MIN_HOLE_SIZE) };
        if long == -1 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(long.try_into().unwrap())
    }
    #[cfg(target_os = "linux")]
    {
        Ok(std::fs::metadata(path)?.st_blksize())
    }
}
