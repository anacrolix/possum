use super::*;

use std::ffi::CString;
#[cfg(target_os = "linux")]
use std::fs::File;
use std::io;
use std::io::{Error, ErrorKind};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use libc::ENOTSUP;

use crate::cpathbuf::CPathBuf;
use crate::PubResult;
cfg_if! {
    if #[cfg(windows)] {
        use windows::Win32::System::Ioctl::*;
        use windows::Win32::System::IO::DeviceIoControl;
    }
}

// Here and not in crate::Error because ENOTSUP has special meaning for clonefile.
fn last_errno() -> crate::Error {
    let errno = errno();
    if errno == ENOTSUP {
        crate::Error::UnsupportedFilesystem
    } else {
        io::Error::from_raw_os_error(errno).into()
    }
}

// TODO: On Solaris we want to use reflink(3)

pub fn clonefile(src_path: &Path, dst_path: &Path) -> PubResult<()> {
    cfg_if! {
        if #[cfg(windows)] {

        } else if #[cfg(not(target_os = "linux"))] {
            let src_buf = CString::new(src_path.as_os_str().as_bytes()).unwrap();
            let dst_buf = CString::new(dst_path.as_os_str().as_bytes()).unwrap();
            let src = src_buf.as_ptr();
            let dst = dst_buf.as_ptr();
            let val = unsafe { libc::clonefile(src, dst, 0) };
            if val != 0 {
                return Err(last_errno());
            }
        } else {
            let src_file = File::open(src_path)?;
            fclonefile_noflags(src_file.as_raw_fd(), dst_path)?;
        }
    }
    Ok(())
}

// fclonefileat but the dst is probably supposed to be an absolute path.
pub fn fclonefile_noflags(src_file: &mut File, dst_path: &Path) -> PubResult<()> {
    cfg_if! {
        if #[cfg(windows)] {
            let dst_file = File::create(dst_path)?;
            let dst_handle = dst_file.as_raw_handle();
            let ByteCount = src_file.seek(End(0))?;
            let data = DUPLICATE_EXTENTS_DATA {
                FileHandle: src_file.as_raw_handle(),
                SourceFileOffset: 0,
                TargetFileOffset: 0,
                ByteCount,
            };
            DeviceIoControl(
                dst_file.as_raw_handle(),
                FSCTL_DUPLICATE_EXTENTS_TO_FILE,
                &data,
                std::mem::size_of(data),
                None,
                0,
                None,
                None,
            )?;

        } else if #[cfg(target_os = "linux")] {
            let dst_file = File::create(dst_path)?;
            let src_fd = src_fd.as_raw_fd();
            let dst_fd = dst_file.as_raw_fd();
            // Is this because the musl bindings are wrong?
            let request = libc::FICLONE.try_into().unwrap();
            let rv = unsafe { libc::ioctl(dst_fd, request, src_fd) };
            if rv == -1 {
                return Err(last_errno());
            }
        } else {
            // assert!(dst_path.is_absolute());
            let dst_buf = CPathBuf::try_from(dst_path).unwrap();
            let dst = dst_buf.as_ptr();
            let val = unsafe { libc::fclonefileat(src_fd.as_raw_fd(), -1, dst, 0) };
            if val != 0 {
                return Err(last_errno());
            }
        }
    }
    Ok(())
}
