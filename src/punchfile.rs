//! Syscall wrappers for hole punching, system configuration, hole-seeking ( ͡° ͜ʖ ͡°), file cloning
//! etc.

cfg_if! {
    if #[cfg(target_os = "linux")] {
        mod linux;
        pub use linux::*;
    } else if #[cfg(windows)] {
        mod windows;
        pub use windows::*;
    } else {
        mod macos;
        pub use macos::*;
    }
}

use super::*;
