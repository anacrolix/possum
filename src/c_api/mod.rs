#![allow(clippy::not_unsafe_ptr_arg_deref)]

pub use types::*;

mod ext_fns;
mod types;

use std::ffi::c_char;
use std::mem::size_of;
use std::pin::Pin;
use std::ptr::copy_nonoverlapping;
use std::slice;

use libc::{calloc, malloc, size_t};

use super::*;

impl AsRef<[u8]> for PossumBuf {
    fn as_ref(&self) -> &[u8] {
        let ptr = self.ptr as _;
        unsafe { slice::from_raw_parts(ptr, self.size) }
    }
}

impl PossumBuf {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        let ptr = self.ptr as *mut u8;
        unsafe { slice::from_raw_parts_mut(ptr, self.size) }
    }
}

struct PossumReader {
    // Removed when converted to a snapshot. Specific to the C API so as to not need to expose
    // Snapshot, and to convert Values automatically when a snapshot starts.
    rust_reader: Option<Reader<'static>>,
    values: Vec<Pin<Box<PossumValue>>>,
}

use crate::c_api::PossumError::{AnyhowError, IoError, SqliteError};

impl<V> From<V> for PossumStat
where
    V: AsRef<Value>,
{
    fn from(value: V) -> Self {
        let value = value.as_ref();
        Self {
            size: value.length(),
            last_used: value.last_used().into(),
        }
    }
}

impl From<Timestamp> for PossumTimestamp {
    fn from(value: Timestamp) -> Self {
        Self {
            secs: value.timestamp(),
            nanos: value.timestamp_subsec_nanos(),
        }
    }
}

/// Converts a sequence of Items to C PossumItems. The caller must free both the keys and the
/// out_list. key_prefix_size is the amount of the key prefix to trim in the output, because the
/// keys may be listed from the same prefix.
fn items_list_to_c(
    key_prefix_size: size_t,
    items: Vec<Item>,
    out_list: *mut *mut PossumItem,
    out_list_len: *mut size_t,
) {
    unsafe {
        *out_list = calloc(size_of::<PossumItem>(), items.len()) as *mut PossumItem;
        *out_list_len = items.len();
    }
    for (index, item) in items.iter().enumerate() {
        let key_size = item.key.len() - key_prefix_size;
        let c_item = PossumItem {
            key: PossumBuf {
                ptr: unsafe { malloc(key_size) } as *const c_char,
                size: key_size,
            },
            stat: PossumStat {
                last_used: item.value.last_used().into(),
                size: item.value.length(),
            },
        };
        unsafe {
            copy_nonoverlapping(
                item.key[key_prefix_size..].as_ptr(),
                c_item.key.ptr as *mut u8,
                key_size,
            )
        };
        let dest = unsafe { (*out_list).add(index) };
        unsafe { *dest = c_item };
    }
}

use PossumError::*;

use crate::item::Item;

impl From<Error> for PossumError {
    fn from(value: Error) -> Self {
        match value {
            Error::NoSuchKey => NoSuchKey,
            Error::Sqlite(_) => SqliteError,
            Error::Io(_) => IoError,
            Error::Anyhow(_) => AnyhowError,
            Error::UnsupportedFilesystem => UnsupportedFilesystem,
        }
    }
}

impl From<rusqlite::Error> for PossumError {
    fn from(_value: rusqlite::Error) -> Self {
        SqliteError
    }
}

impl From<io::Error> for PossumError {
    fn from(_value: io::Error) -> Self {
        IoError
    }
}

impl From<anyhow::Error> for PossumError {
    fn from(_value: anyhow::Error) -> Self {
        AnyhowError
    }
}

fn with_residual(f: impl FnOnce() -> PubResult<()>) -> PossumError {
    match f() {
        Ok(()) => NoError,
        Err(err) => {
            warn!("converting rust error into enum: {:#?}", err);
            err.into()
        }
    }
}

impl From<PossumLimits> for handle::Limits {
    fn from(from: PossumLimits) -> Self {
        handle::Limits {
            max_value_length_sum: match from.max_value_length_sum {
                u64::MAX => None,
                otherwise => Some(otherwise),
            },
            disable_hole_punching: from.disable_hole_punching,
        }
    }
}

/// Converts from types to the RawFileHandle exposed in the Possum C API.
trait AsRawFileHandle {
    fn as_raw_file_handle(&self) -> RawFileHandle;
}

impl AsRawFileHandle for File {
    fn as_raw_file_handle(&self) -> RawFileHandle {
        cfg_if! {
            if #[cfg(windows)] {
                use std::os::windows::io::AsRawHandle;
                self.as_raw_handle() as RawFileHandle
            } else {
                self.as_raw_fd() as isize
            }
        }
    }
}
