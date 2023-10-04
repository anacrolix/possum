use std::ffi::CString;
use std::io;
use std::os::unix::prelude::OsStrExt;
use std::path::Path;

pub fn clonefile(src_path: &Path, dst_path: &Path) -> io::Result<()> {
    let src_buf = CString::new(src_path.as_os_str().as_bytes())?;
    let dst_buf = CString::new(dst_path.as_os_str().as_bytes())?;
    let src = src_buf.as_ptr();
    let dst = dst_buf.as_ptr();
    let val = unsafe { libc::clonefile(src, dst, 0) };
    if val != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[test]
fn test_clonefile() {}
