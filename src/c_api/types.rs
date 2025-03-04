mod value;

use std::ffi::c_char;

use libc::size_t;
pub(crate) use value::*;

use super::*;
use crate::{BatchWriter, Handle, ValueWriter};

pub(crate) type PossumOffset = u64;

pub type RawFileHandle = libc::intptr_t;
pub type PossumWriter = BatchWriter<PossumHandle>;

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
impl SafeForGo for ValueWriter {}

// This type is spelled out so we can switch between implementations for PossumHandle.
pub(crate) type PossumHandleRc = Arc<RwLock<Handle>>;

pub(crate) type PossumHandle = PossumHandleRc;
impl SafeForGo for PossumHandle {}

// Need to make these guarantees for handles used from Go over the C boundary. I don't actually know
// if we need Send, it might be inferred sufficiently by Sync.
#[allow(dead_code)]
trait SafeForGo: Send + Sync {}
