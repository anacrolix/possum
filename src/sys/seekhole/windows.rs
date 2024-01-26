use super::*;

struct SeekWhence;

pub fn seek_hole_whence(
    fd: RawFileHandle,
    offset: i64,
    whence: impl Into<SeekWhence>,
) -> std::io::Result<Option<RegionOffset>> {
    DeviceIoControl(fd.as_raw_handle())?;
    // lseek64?
}
