use super::*;
use std::io;
use std::io::Error;

pub fn punchfile(file: impl AsFd, offset: i64, length: i64) -> io::Result<()> {
    // TODO: On solaris we want fcntl(F_FREESP);
    let punchhole = libc::fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset: offset,
        fp_length: length,
    };
    let first_arg = &punchhole;
    let fcntl_res = unsafe { libc::fcntl(file.as_fd().as_raw_fd(), libc::F_PUNCHHOLE, first_arg) };
    if fcntl_res == -1 {
        return Err(Error::last_os_error());
    }
    Ok(())
}
