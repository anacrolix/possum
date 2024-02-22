use std::convert::TryInto;
use std::io;
use std::io::Error;

use super::*;

pub fn punchfile(file: &File, offset: u64, length: u64) -> io::Result<()> {
    // TODO: On solaris we want fcntl(F_FREESP);
    let fd = file.as_fd().as_raw_fd();
    let mode = libc::FALLOC_FL_KEEP_SIZE | libc::FALLOC_FL_PUNCH_HOLE;
    if -1
        == unsafe {
            libc::fallocate64(
                fd,
                mode,
                offset.try_into().unwrap(),
                length.try_into().unwrap(),
            )
        }
    {
        return Err(Error::last_os_error());
    }
    Ok(())
}
