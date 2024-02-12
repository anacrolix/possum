use super::*;

const MIN_HOLE_SIZE: u64 = 1 << 16;

pub(crate) fn fd_min_hole_size(_file: &File) -> std::io::Result<u64> {
    // I think this is just 64 KiB, there may be a GetVolumeInformation to determine support.
    Ok(MIN_HOLE_SIZE)
}

/// Recommended minimum hole size for sparse files for file descriptor.
/// fpathconf(_PC_MIN_HOLE_SIZE). On macOS this returns positive if holes are supported, and returns
/// 1 if holes are supported but the minimum hole size is unspecified.
pub(crate) fn path_min_hole_size(_path: &Path) -> std::io::Result<u64> {
    Ok(MIN_HOLE_SIZE)
}
