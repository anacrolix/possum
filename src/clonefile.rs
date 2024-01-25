//! Syscall wrappers for hole punching, system configuration, hole-seeking ( ͡° ͜ʖ ͡°), file cloning
//! etc.

use super::*;

mod sys;
pub use sys::*;

#[cfg(test)]
#[super::test]
fn test_clonefile() {}
