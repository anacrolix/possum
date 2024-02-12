//! Syscall wrappers for hole punching, system configuration, hole-seeking ( ͡° ͜ʖ ͡°), file cloning
//! etc.

#![allow(unused_imports)]
// There are exports here that aren't yet used (they're hardcoded instead).
#![allow(dead_code)]

use std::fs::File;
// Needed for Metadata.st_blksize. I don't think the unix variant works?
use std::os::linux::fs::MetadataExt;
use std::path::Path;

use crate::cpathbuf::CPathBuf;

/// Recommended minimum hole size for sparse files for file descriptor.
/// fpathconf(_PC_MIN_HOLE_SIZE);
pub(crate) fn fd_min_hole_size(file: &File) -> std::io::Result<u64> {
    Ok(file.metadata()?.st_blksize())
}

/// Recommended minimum hole size for sparse files for file descriptor.
/// fpathconf(_PC_MIN_HOLE_SIZE). On macOS this returns positive if holes are supported, and returns
/// 1 if holes are supported but the minimum hole size is unspecified.
pub(crate) fn path_min_hole_size(path: &Path) -> std::io::Result<u64> {
    Ok(std::fs::metadata(path)?.st_blksize())
}
