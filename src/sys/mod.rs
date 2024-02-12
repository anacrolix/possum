use super::*;

pub mod clonefile;
pub mod flock;
pub mod pathconf;
pub mod punchfile;
pub mod seekhole;

pub use clonefile::*;
pub use flock::*;
pub use punchfile::*;
pub use seekhole::*;

use std::fs::File;

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
    } else if #[cfg(unix)] {
        mod unix;
        pub use unix::*;
        pub use std::os::unix::prelude::OsStrExt;
        pub use std::os::unix::ffi::OsStringExt;
        pub use std::os::fd::AsRawFd;
        pub use nix::errno::Errno;
        pub use std::os::fd::AsFd;
    }
}

// These are typedefs for 64bit file syscalls.
cfg_if! {
    if #[cfg(not(target_pointer_width = "64"))] {
        pub use libc::off64_t as off_t;
        #[cfg(unix)]
        pub use nix::libc::lseek64 as lseek;
    } else {
        pub use libc::off_t;
        #[cfg(unix)]
        pub use nix::libc::lseek;
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
pub fn open_dir_as_file<P: AsRef<Path>>(path: P) -> io::Result<File> {
    OpenOptions::new().read(true).open(path)
}

pub trait FileSystemFlags {
    fn supports_sparse_files(&self) -> bool;
    fn supports_block_cloning(&self) -> bool;
}

pub trait DirMeta {
    fn file_system_flags(&self) -> io::Result<impl FileSystemFlags>;
}

struct SupportsEverythingFilesystemFlags {}
impl FileSystemFlags for SupportsEverythingFilesystemFlags {
    fn supports_sparse_files(&self) -> bool {
        // AFAIK, all unix systems support sparse files on all filesystems.
        true
    }

    fn supports_block_cloning(&self) -> bool {
        // I don't know how to constraint this. AFAIK there's no way to check if a filesystem
        // supports block cloning, and even if it does it depends on where you're copying to/from,
        // sometimes even on the same filesystem.
        true
    }
}

#[cfg(not(windows))]
impl DirMeta for File {
    fn file_system_flags(&self) -> io::Result<impl FileSystemFlags> {
        Ok(SupportsEverythingFilesystemFlags {})
    }
}
