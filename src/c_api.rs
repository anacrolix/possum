use std::ffi::{c_char, c_uchar, CStr, OsStr};
use std::ptr::null_mut;
use std::slice;

use libc::size_t;
use log::error;

use super::*;

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
    key: *const c_uchar,
    key_size: size_t,
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

fn byte_vec_from_ptr_and_size(ptr: *const c_uchar, size: size_t) -> Vec<u8> {
    unsafe { slice::from_raw_parts(ptr, size) }.to_vec()
}

#[no_mangle]
pub extern "C" fn possum_new_writer(handle: *mut Handle) -> *mut BatchWriter<'static> {
    let handle = unsafe { &*handle };
    Box::into_raw(Box::new(handle.new_writer().unwrap()))
}

pub use libc::timespec;

#[repr(C)]
pub struct Stat {
    last_used: libc::timespec,
    size: u64,
}

#[no_mangle]
pub extern "C" fn possum_single_stat(
    handle: *const Handle,
    key: *const c_uchar,
    key_size: size_t,
    out_stat: *mut Stat,
) -> bool {
    match unsafe { handle.as_ref() }
        .unwrap()
        .read_single(byte_vec_from_ptr_and_size(key, key_size))
        .unwrap()
    {
        Some(value) => {
            let stat_in_rust = Stat {
                size: value.length(),
                last_used: libc::timespec {
                    tv_sec: value.last_used().timestamp(),
                    tv_nsec: value.last_used.timestamp_subsec_nanos().try_into().unwrap(),
                },
            };
            unsafe { *out_stat = stat_in_rust };
            true
        }
        None => false,
    }
}
