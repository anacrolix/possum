use super::*;
use std::io;
use std::io::Error;

pub fn punchfile(file: impl AsFd, offset: off_t, length: off_t) -> io::Result<()> {
    // TODO: On solaris we want fcntl(F_FREESP);
    let fd = file.as_fd().as_raw_fd();
    let mode = libc::FALLOC_FL_KEEP_SIZE | libc::FALLOC_FL_PUNCH_HOLE;
    if -1 == unsafe { libc::fallocate64(fd, mode, offset, length) } {
        return Err(Error::last_os_error());
    }
    Ok(())
}
