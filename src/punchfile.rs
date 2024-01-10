//! Syscall wrappers for hole punching, system configuration, hole-seeking ( ͡° ͜ʖ ͡°), file cloning
//! etc.

use std::io;
use std::io::Error;
use std::os::fd::AsRawFd;

use libc::off_t;

pub fn punchfile(file: impl AsRawFd, offset: off_t, length: off_t) -> io::Result<()> {
    #[cfg(not(target_os = "linux"))]
    {
        let punchhole = libc::fpunchhole_t {
            fp_flags: 0,
            reserved: 0,
            fp_offset: offset,
            fp_length: length,
        };
        let first_arg = &punchhole;
        let fcntl_res = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, first_arg) };
        if fcntl_res == -1 {
            return Err(Error::last_os_error());
        }
    }
    #[cfg(target_os = "linux")]
    {
        let fd = file.as_raw_fd();
        let mode = libc::FALLOC_FL_KEEP_SIZE | libc::FALLOC_FL_PUNCH_HOLE;
        if -1 == unsafe { libc::fallocate(fd, mode, offset, length) } {
            return Err(Error::last_os_error());
        }
    }
    Ok(())
}
