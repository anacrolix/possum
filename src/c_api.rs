use super::*;
use log::error;
use std::ffi::{c_char, CStr, OsStr};
use std::ptr::null_mut;

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
