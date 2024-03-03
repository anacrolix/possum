//! Syscall wrappers for hole punching, system configuration, hole-seeking ( ͡° ͜ʖ ͡°), file cloning
//! etc.

#![allow(unused_imports)]
// There are exports here that aren't yet used (they're hardcoded instead).
#![allow(dead_code)]

use super::*;

cfg_if! {
    if #[cfg(target_os = "linux")] {
        mod linux;
        pub use linux::*;
    } else if #[cfg(unix)] {
        mod bsd;
        pub use bsd::*;
    } else if #[cfg(target_os = "windows")] {
        mod windows;
        pub use self::windows::*;
    }
}
