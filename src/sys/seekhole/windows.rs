use super::*;

pub fn seek_hole_whence(
    file: &mut File,
    offset: i64,
    whence: RegionType,
) -> std::io::Result<Option<RegionOffset>> {
    unimplemented!();
    // DeviceIoControl(file.as_raw_handle())?;
    // lseek64?
}
