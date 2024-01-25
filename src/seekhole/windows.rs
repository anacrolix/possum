use super::*;

pub fn seek_hole_whence(
    fd: RawFileHandle,
    offset: i64,
    whence: impl Into<SeekWhence>,
) -> io::Result<Option<RegionOffset>> {
    // lseek64?
    match lseek(fd, offset, whence) {
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
