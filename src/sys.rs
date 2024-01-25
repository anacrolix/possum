use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(windows)] {
        pub use std::os::windows::io::RawHandle as RawFileHandle;
        pub use std::os::windows::io::AsRawHandle as AsRawFd;
    } else if #[cfg(unix)] {
        pub use std::os::unix::prelude::OsStrExt;
        pub use std::os::unix::ffi::OsStringExt;
        pub use std::os::fd::AsRawFd;
        pub use nix::errno::errno;
        pub use nix::errno::Errno;
        pub use std::os::fd::RawFd as RawFileHandle;
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
