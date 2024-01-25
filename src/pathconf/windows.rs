use super::*;

pub(crate) fn fd_min_hole_size(file: &File) -> std::io::Result<u64> {}

/// Recommended minimum hole size for sparse files for file descriptor.
/// fpathconf(_PC_MIN_HOLE_SIZE). On macOS this returns positive if holes are supported, and returns
/// 1 if holes are supported but the minimum hole size is unspecified.
pub(crate) fn path_min_hole_size(path: &Path) -> std::io::Result<u64> {}
