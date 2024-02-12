use super::*;

// I thought this would match the cluster size which can be 4KiB to 64KiB, but maybe Windows
// actually tracks down to the individual bytes.
const ZERO_DATA_OFFSET_ALIGNMENT: u64 = 1;

pub(crate) fn fd_min_hole_size(_file: &File) -> std::io::Result<u64> {
    Ok(ZERO_DATA_OFFSET_ALIGNMENT)
}

pub(crate) fn path_min_hole_size(_path: &Path) -> std::io::Result<u64> {
    Ok(ZERO_DATA_OFFSET_ALIGNMENT)
}
