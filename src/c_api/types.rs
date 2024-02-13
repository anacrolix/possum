mod value;

pub(crate) use value::*;

use crate::{BatchWriter, ValueWriter};
use libc::size_t;
use std::ffi::c_char;

pub(crate) type PossumOffset = u64;

pub type RawFileHandle = libc::intptr_t;
pub type PossumWriter = BatchWriter<'static>;

#[repr(C)]
#[derive(Debug)]
pub struct PossumBuf {
    pub ptr: *const c_char,
    pub size: size_t,
}

#[repr(C)]
pub struct PossumStat {
    pub last_used: PossumTimestamp,
    pub size: u64,
}

#[repr(C)]
pub struct PossumTimestamp {
    pub secs: i64,
    pub nanos: u32,
}

#[repr(C)]
pub struct PossumItem {
    pub key: PossumBuf,
    pub stat: PossumStat,
}

#[repr(C)]
pub enum PossumError {
    NoError,
    NoSuchKey,
    SqliteError,
    IoError,
    AnyhowError,
    UnsupportedFilesystem,
}
// TODO: Merge the C and Rust error types.
// pub use crate::Error as PossumError;

#[repr(C)]
pub(crate) struct PossumLimits {
    pub max_value_length_sum: u64,
    pub disable_hole_punching: bool,
}

pub(crate) type PossumValueWriter = ValueWriter;
