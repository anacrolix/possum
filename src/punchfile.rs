use libc::off_t;
use std::io;
use std::io::Error;
use std::os::fd::AsRawFd;

pub fn punchfile(file: impl AsRawFd, offset: off_t, length: off_t) -> io::Result<()> {
    let punchhole = libc::fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset: offset,
        fp_length: length,
    };
    let fcntl_res = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, punchhole) };
    if fcntl_res == -1 {
        return Err(Error::last_os_error());
    }
    Ok(())
}
