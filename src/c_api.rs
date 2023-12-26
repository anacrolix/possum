use super::*;

use std::ffi::{c_char, c_uchar, CStr, OsStr};
use std::mem::size_of;
use std::ptr::{copy_nonoverlapping, null_mut};
use std::slice;

use libc::{calloc, malloc, size_t};
use log::error;

pub type KeyPtr = *const c_char;
pub type KeySize = size_t;

#[no_mangle]
pub extern "C" fn possum_new(path: *const c_char) -> *mut Handle {
    let c_str = unsafe { CStr::from_ptr(path) };
    let path_buf: PathBuf = OsStr::from_bytes(c_str.to_bytes()).into();
    let handle = match Handle::new(path_buf.clone()) {
        Ok(handle) => handle,
        Err(err) => {
            error!("error creating possum handle in {path_buf:?}: {err}");
            return null_mut();
        }
    };
    Box::into_raw(Box::new(handle))
}

// This drops the Handle Box. Instead, if this is hard to use correctly from C, it could drop a top-level reference
// count for the box. i.e. If this one goes, there's no way to work with the Handle, and when all other outstanding
// operations on the Handle complete, it will drop the Handle for real.
#[no_mangle]
pub extern "C" fn possum_drop(handle: *mut Handle) {
    drop(unsafe { Box::from_raw(handle) })
}

#[no_mangle]
pub extern "C" fn possum_single_write_buf(
    handle: *mut Handle,
    key: KeyPtr,
    key_size: KeySize,
    value: *const u8,
    value_size: size_t,
) -> size_t {
    let key_vec = byte_vec_from_ptr_and_size(key, key_size);
    let value_slice = unsafe { slice::from_raw_parts(value, value_size) };
    const ERR_SENTINEL: usize = usize::MAX;
    let handle = unsafe { &*handle };
    match handle.single_write_from(key_vec, value_slice) {
        Err(_) => ERR_SENTINEL,
        Ok((n, _)) => {
            let n = n.try_into().unwrap();
            assert_ne!(n, ERR_SENTINEL);
            n
        }
    }
}

fn byte_vec_from_ptr_and_size(ptr: *const c_char, size: size_t) -> Vec<u8> {
    unsafe { slice::from_raw_parts(ptr as *const c_uchar, size) }.to_vec()
}

#[no_mangle]
pub extern "C" fn possum_new_writer(handle: *mut Handle) -> *mut BatchWriter<'static> {
    let handle = unsafe { &*handle };
    Box::into_raw(Box::new(handle.new_writer().unwrap()))
}

pub use libc::timespec;

#[repr(C)]
pub struct PossumStat {
    last_used: PossumTimestamp,
    size: u64,
}

#[repr(C)]
pub struct PossumTimestamp {
    secs: i64,
    nanos: u32,
}

impl From<&Timestamp> for PossumTimestamp {
    fn from(value: &Timestamp) -> Self {
        Self {
            secs: value.timestamp(),
            nanos: value.timestamp_subsec_nanos(),
        }
    }
}

#[no_mangle]
pub extern "C" fn possum_single_stat(
    handle: *const Handle,
    key: KeyPtr,
    key_size: size_t,
    out_stat: *mut PossumStat,
) -> bool {
    match unsafe { handle.as_ref() }
        .unwrap()
        .read_single(byte_vec_from_ptr_and_size(key, key_size))
        .unwrap()
    {
        Some(value) => {
            let stat_in_rust = PossumStat {
                size: value.length(),
                last_used: value.last_used().into(),
            };
            unsafe { *out_stat = stat_in_rust };
            true
        }
        None => false,
    }
}

#[repr(C)]
pub struct possum_item {
    key: KeyPtr,
    key_size: KeySize,
    stat: PossumStat,
}

#[no_mangle]
pub extern "C" fn possum_list_keys(
    handle: *const Handle,
    prefix: *const c_uchar,
    prefix_size: size_t,
    out_list: *mut *mut possum_item,
    out_list_len: *mut size_t,
) -> PossumError {
    let items = match unsafe { handle.as_ref() }
        .unwrap()
        .list_items(unsafe { slice::from_raw_parts(prefix, prefix_size) })
    {
        Ok(items) => items,
        Err(err) => return err.into(),
    };
    unsafe {
        *out_list = calloc(size_of::<possum_item>(), items.len()) as *mut possum_item;
        *out_list_len = items.len();
    }
    for (index, item) in items.iter().enumerate() {
        let key_size = item.key.len() - prefix_size;
        let c_item = possum_item {
            key: unsafe { malloc(key_size) } as KeyPtr,
            key_size,
            stat: PossumStat {
                last_used: item.value.last_used().into(),
                size: item.value.length(),
            },
        };
        unsafe {
            copy_nonoverlapping(
                item.key[prefix_size..].as_ptr(),
                c_item.key as *mut u8,
                key_size,
            )
        };
        let dest = unsafe { (*out_list).offset(index as isize) };
        unsafe { *dest = c_item };
    }
    PossumError::NoError
}

#[repr(C)]
pub enum PossumError {
    NoError,
    NoSuchKey,
    SqliteError,
}

impl From<Error> for PossumError {
    fn from(value: Error) -> Self {
        match value {
            Error::NoSuchKey => PossumError::NoSuchKey,
            Error::Sqlite(_) => PossumError::SqliteError,
        }
    }
}
