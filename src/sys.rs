use super::*;

pub mod clonefile;
pub mod flock;
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
        use ::windows::core::*;
        use ::windows::Win32::System::IO::*;
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
