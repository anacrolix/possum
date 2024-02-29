use std::io::Error;
use std::{io, mem};

use super::*;

pub fn punchfile(file: &File, offset: u64, length: u64) -> io::Result<()> {
    // TODO: On solaris we want fcntl(F_FREESP);
    let rqsr = libc::spacectl_range {
        r_offset: offset.try_into().unwrap(),
        r_len: length.try_into().unwrap(),
    };
    let mut rmsr: libc::spacectl_range = unsafe { mem::zeroed() };
    let rv = unsafe {
        libc::fspacectl(
            file.as_fd().as_raw_fd(),
            libc::SPACECTL_DEALLOC,
            &rqsr,
            0,
            &mut rmsr,
        )
    };
    if rv == -1 {
        return Err(Error::last_os_error());
    }
    if rmsr.r_len != 0 {
        warn!(
            req_off = rqsr.r_offset,
            req_len = rqsr.r_len,
            unproc_off = rmsr.r_offset,
            unproc_len = rmsr.r_len,
            "spacectl dealloc returned unprocessed part"
        );
    }
    Ok(())
}
