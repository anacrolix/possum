#![allow(dead_code)]

use super::*;
use std::convert::TryFrom;
use std::ffi::{c_char, CString, NulError};
use std::path::Path;

/// A PathBuf like implementation for use with C APIs that expect a nul-terminated C string. Should
/// convert easily from common Rust path types, and have methods that pass to C.
pub(crate) struct CPathBuf(CString);

impl TryFrom<&Path> for CPathBuf {
    type Error = NulError;

    fn try_from(value: &Path) -> Result<Self, Self::Error> {
        // I wonder if checking is necessary. If Path must have an inner OS implementation, can we
        // know for sure there are no interior nul bytes?
        Ok(Self(CString::new(value.as_os_str().as_bytes())?))
    }
}

impl CPathBuf {
    pub(crate) fn as_ptr(&self) -> *const c_char {
        self.0.as_ptr()
    }
}
