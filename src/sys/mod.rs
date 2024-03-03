//! Exposes required lower-level OS primitives for sparse files, block cloning, and file locking.

use super::*;

mod clonefile;
mod flock;
mod pathconf;
mod punchfile;
pub mod seekhole;

use std::fs::File;

pub use clonefile::*;
pub use flock::*;
pub(crate) use pathconf::*;
pub use punchfile::*;

use crate::env::emulate_freebsd;

cfg_if! {
    if #[cfg(windows)] {
        mod windows;
        pub use windows::*;
        pub use ::windows::Win32::System::Ioctl::*;
        use ::windows::Win32::Foundation::*;
        use std::os::windows::io::AsRawHandle;
        use ::windows::Win32::Storage::FileSystem::*;
        use ::windows::Win32::System::IO::*;
        use ::windows::Win32::System::SystemServices::*;
        pub type NativeIoError = std::io::Error;
    } else if #[cfg(unix)] {
        mod unix;
        pub use unix::*;
        pub(crate) use std::os::unix::prelude::OsStrExt;
        pub(crate) use std::os::fd::AsRawFd;
        pub(crate) use std::os::fd::AsFd;
        pub type NativeIoError = std::io::Error;
    }
}

// These are typedefs for 64bit file syscalls.
cfg_if! {
    if #[cfg(not(target_pointer_width = "64"))] {
        pub use libc::off64_t as off_t;
        #[cfg(unix)]
        pub use nix::libc::lseek64 as lseek;
    } else {
        #[cfg(unix)]
         use nix::libc::lseek;
    }
}

pub trait SparseFile {
    fn set_sparse(&self, set_sparse: bool) -> io::Result<()>;
}

#[cfg(not(windows))]
impl SparseFile for File {
    fn set_sparse(&self, _set_sparse: bool) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(not(windows))]
pub(crate) fn open_dir_as_file<P: AsRef<Path>>(path: P) -> io::Result<File> {
    OpenOptions::new().read(true).open(path)
}

pub trait FileSystemFlags {
    fn supports_sparse_files(&self) -> bool;
    /// Returns Some if we know for certain, and None if there's no indication from the system.
    fn supports_block_cloning(&self) -> Option<bool>;
}

pub trait DirMeta {
    fn file_system_flags(&self) -> io::Result<impl FileSystemFlags>;
}

struct UnixFilesystemFlags {}

impl FileSystemFlags for UnixFilesystemFlags {
    fn supports_sparse_files(&self) -> bool {
        // AFAIK, all unix systems support sparse files on all filesystems.
        true
    }

    fn supports_block_cloning(&self) -> Option<bool> {
        // AFAIK there's no way to check if a filesystem supports block cloning on non-Windows
        // platforms, and even then it depends on where you're copying to/from, sometimes even on
        // the same filesystem.
        if emulate_freebsd() {
            Some(false)
        } else {
            None
        }
    }
}

#[cfg(not(windows))]
impl DirMeta for File {
    fn file_system_flags(&self) -> io::Result<impl FileSystemFlags> {
        Ok(UnixFilesystemFlags {})
    }
}
