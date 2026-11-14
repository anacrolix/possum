#![allow(unused_imports)]
use std::ffi::CString;
#[cfg(target_os = "linux")]
use std::fs::File;
use std::io;
use std::io::{Error, ErrorKind};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use libc::{ENOTSUP, EOPNOTSUPP};

use super::*;
use crate::cpathbuf::CPathBuf;
use crate::Error::UnsupportedFilesystem;
use crate::PubResult;
cfg_if! {
    if #[cfg(windows)] {
    }
}

pub trait CloneFileError {
    fn is_unsupported(&self) -> bool;
}

impl CloneFileError for NativeIoError {
    #[cfg(unix)]
    fn is_unsupported(&self) -> bool {
        self.raw_os_error()
            .map(|errno| matches!(errno, EOPNOTSUPP | libc::EXDEV))
            .unwrap_or_default()
    }
    #[cfg(windows)]
    fn is_unsupported(&self) -> bool {
        // Where can I find a list of possible error codes? At least on Windows there is a file
        // system flag that might avoid us needing to use this value.
        false
    }
}

#[allow(dead_code)]
#[cfg(unix)]
// Here and not in crate::Error because ENOTSUP has special meaning for clonefile.
fn last_errno() -> NativeIoError {
    io::Error::last_os_error()
    // let errno = errno();
    // // On Linux this is EOPNOTSUP, but on Linux it's also the same value as ENOTSUP.
    // if errno == ENOTSUP || errno == EOPNOTSUPP {
    //     crate::Error::UnsupportedFilesystem
    // } else {
    //     io::Error::from_raw_os_error(errno).into()
    // }
}

// TODO: On Solaris we want to use reflink(3)

pub fn clonefile(src_path: &Path, dst_path: &Path) -> Result<(), std::io::Error> {
    cfg_if! {
        if #[cfg(target_os = "macos")] {
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
            fclonefile_noflags(&src_file, dst_path)?;
        }
    }
    Ok(())
}

// fclonefileat but the dst is probably supposed to be an absolute path.
#[allow(unused_variables)]
pub fn fclonefile_noflags(src_file: &File, dst_path: &Path) -> Result<(), NativeIoError> {
    cfg_if! {
        if #[cfg(windows)] {
            use std::ffi::c_void;
            let dst_file = File::create(dst_path)?;
            let dst_handle = std_handle_to_windows(dst_file.as_raw_handle());
            let src_metadata = src_file.metadata()?;
            let byte_count = src_metadata.len() as i64;
            let data = DUPLICATE_EXTENTS_DATA {
                FileHandle: HANDLE(src_file.as_raw_handle() as isize),
                SourceFileOffset: 0,
                TargetFileOffset: 0,
                ByteCount: byte_count,
            };
            let data_ptr = &data as *const _ as *const c_void;
            unsafe {
                DeviceIoControl
            (
                dst_handle,
                FSCTL_DUPLICATE_EXTENTS_TO_FILE,
                Some(data_ptr),
                std::mem::size_of_val(&data) as u32,
                None,
                0,
                None,
                None,
            )}?;
            Ok(())
        } else if #[cfg(target_os = "linux")] {
            // Should this be exclusive create?
            let dst_file = File::create(dst_path)?;
            let src_fd = src_file.as_raw_fd();
            let dst_fd = dst_file.as_raw_fd();
            // Is this because the musl bindings are wrong?
            #[allow(clippy::useless_conversion)]
            let request = libc::FICLONE.try_into().unwrap();
            let rv = unsafe { libc::ioctl(dst_fd, request, src_fd) };
            if rv == -1 {
                return Err(last_errno());
            }
            Ok(())
        } else if #[cfg(target_os = "freebsd")] {
            freebsd_clone_file(src_file, dst_path)
        } else {
            // assert!(dst_path.is_absolute());
            let dst_buf = CPathBuf::try_from(dst_path).unwrap();
            let dst = dst_buf.as_ptr();
            let val = unsafe { libc::fclonefileat(src_file.as_raw_fd(), -1, dst, 0) };
            if val != 0 {
                return Err(last_errno());
            }
            Ok(())
        }
    }
}

/// `COPY_FILE_RANGE_CLONE` from FreeBSD's sys/unistd.h. It isn't in the libc crate yet. It first
/// shipped in FreeBSD 15.0; older kernels reject any non-zero flags with EINVAL.
/// https://man.freebsd.org/cgi/man.cgi?query=copy_file_range&sektion=2&manpath=FreeBSD+15.0-RELEASE
#[cfg(target_os = "freebsd")]
const COPY_FILE_RANGE_CLONE: libc::c_uint = 0x00800000;

/// Clones the whole of src_file to a new file at dst_path using copy_file_range(2) with
/// COPY_FILE_RANGE_CLONE, which fails with EOPNOTSUPP rather than silently copying data when the
/// filesystem can't block clone (for example ZFS with vfs.zfs.bclone_enabled=0).
#[cfg(target_os = "freebsd")]
fn freebsd_clone_file(src_file: &File, dst_path: &Path) -> Result<(), NativeIoError> {
    let dst_file = File::create(dst_path)?;
    let res = freebsd_clone_file_contents(src_file, &dst_file);
    if res.is_err() {
        // Don't leave an empty or partial file behind, callers may fall back to copying elsewhere.
        drop(dst_file);
        let _ = std::fs::remove_file(dst_path);
    }
    res
}

#[cfg(target_os = "freebsd")]
fn freebsd_clone_file_contents(src_file: &File, dst_file: &File) -> Result<(), NativeIoError> {
    let len = src_file.metadata()?.len();
    let mut done: u64 = 0;
    while done < len {
        // Track offsets ourselves: unpatched 15.0 doesn't update them (FreeBSD-EN-25:16.vfs).
        let mut in_off: libc::off_t = done.try_into().unwrap();
        let mut out_off: libc::off_t = in_off;
        let remaining: libc::size_t = (len - done).try_into().unwrap_or(libc::size_t::MAX);
        let rv = unsafe {
            libc::copy_file_range(
                src_file.as_raw_fd(),
                &mut in_off,
                dst_file.as_raw_fd(),
                &mut out_off,
                remaining,
                COPY_FILE_RANGE_CLONE,
            )
        };
        if rv == -1 {
            let err = last_errno();
            // Kernels before 15.0 don't know the clone flag and return EINVAL. Report that as
            // cloning being unsupported so callers fall back.
            if err.raw_os_error() == Some(libc::EINVAL) {
                return Err(io::Error::from_raw_os_error(EOPNOTSUPP));
            }
            return Err(err);
        }
        if rv == 0 {
            // The source shrank underneath us.
            break;
        }
        done += rv as u64;
    }
    Ok(())
}
