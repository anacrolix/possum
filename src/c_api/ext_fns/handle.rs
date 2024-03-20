use libc::size_t;
use positioned_io::ReadAt;

use super::*;
use crate::c_api::PossumError::NoError;
use crate::Handle;

// This drops the Handle Box. Instead, if this is hard to use correctly from C, it could drop a
// top-level reference count for the box. i.e. If this one goes, there's no way to work with the
// Handle, and when all other outstanding operations on the Handle complete, it will drop the Handle
// for real.
#[no_mangle]
pub extern "C" fn possum_drop(handle: *mut Handle) {
    drop(unsafe { Box::from_raw(handle) })
}

#[no_mangle]
pub extern "C" fn possum_set_instance_limits(
    handle: *mut Handle,
    limits: *const PossumLimits,
) -> PossumError {
    let handle = unsafe { &mut *handle };
    let limits = unsafe { limits.read() };
    with_residual(|| {
        handle
            .set_instance_limits(limits.into())
            .map_err(Into::into)
    })
}

#[no_mangle]
pub extern "C" fn possum_cleanup_snapshots(handle: *const Handle) -> PossumError {
    let handle = unsafe { &*handle };
    with_residual(|| handle.cleanup_snapshots())
}

#[no_mangle]
pub extern "C" fn possum_single_write_buf(
    handle: *mut Handle,
    key: PossumBuf,
    value: PossumBuf,
) -> size_t {
    let key_vec = key.as_ref().to_vec();
    let value_slice = value.as_ref();
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

#[no_mangle]
pub extern "C" fn possum_new_writer(handle: *mut Handle) -> *mut PossumWriter {
    let handle = unsafe { &*handle };
    Box::into_raw(Box::new(handle.new_writer().unwrap()))
}

#[no_mangle]
pub extern "C" fn possum_single_stat(
    handle: *const Handle,
    key: PossumBuf,
    out_stat: *mut PossumStat,
) -> bool {
    match unsafe { handle.as_ref() }
        .unwrap()
        .read_single(key.as_ref())
        .unwrap()
    {
        Some(value) => {
            let stat_in_rust = value.as_ref().into();
            unsafe { *out_stat = stat_in_rust };
            true
        }
        None => false,
    }
}

#[no_mangle]
pub extern "C" fn possum_list_items(
    handle: *const Handle,
    prefix: PossumBuf,
    out_list: *mut *mut PossumItem,
    out_list_len: *mut size_t,
) -> PossumError {
    let items = match unsafe { handle.as_ref() }
        .unwrap()
        .list_items(prefix.as_ref())
    {
        Ok(items) => items,
        Err(err) => return err.into(),
    };
    items_list_to_c(prefix.size, items, out_list, out_list_len);
    NoError
}

#[no_mangle]
pub extern "C" fn possum_single_read_at(
    handle: *const Handle,
    key: PossumBuf,
    buf: *mut PossumBuf,
    offset: u64,
) -> PossumError {
    let rust_key = key.as_ref();
    let value = match unsafe { handle.as_ref() }.unwrap().read_single(rust_key) {
        Ok(Some(value)) => value,
        Ok(None) => return PossumError::NoSuchKey,
        Err(err) => return err.into(),
    };
    let buf = unsafe { buf.as_mut() }.unwrap();
    let read_buf = buf.as_mut_slice();
    let r_nbyte = match value.read_at(offset, read_buf) {
        Err(err) => return err.into(),
        Ok(ok) => ok,
    };
    buf.size = r_nbyte;
    NoError
}

/// stat is filled if non-null and a delete occurs. NoSuchKey is returned if the key does not exist.
#[no_mangle]
pub extern "C" fn possum_single_delete(
    handle: *const Handle,
    key: PossumBuf,
    stat: *mut PossumStat,
) -> PossumError {
    with_residual(|| {
        let handle = unsafe { &*handle };
        let value = match handle.single_delete(key.as_ref()) {
            Ok(None) => return Err(crate::Error::NoSuchKey),
            Err(err) => return Err(err),
            Ok(Some(value)) => value,
        };
        if let Some(stat) = unsafe { stat.as_mut() } {
            *stat = value;
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn possum_reader_new(
    handle: *const Handle,
    reader: *mut *mut PossumReader,
) -> PossumError {
    let handle = unsafe { handle.as_ref() }.unwrap();
    let reader = unsafe { reader.as_mut() }.unwrap();
    let rust_reader = match handle.read() {
        Ok(ok) => ok,
        Err(err) => return err.into(),
    };
    *reader = Box::into_raw(Box::new(PossumReader {
        rust_reader: Some(rust_reader),
        values: Default::default(),
    }));
    NoError
}

#[no_mangle]
pub extern "C" fn possum_handle_move_prefix(
    handle: *mut Handle,
    from: PossumBuf,
    to: PossumBuf,
) -> PossumError {
    let handle = unsafe { &mut *handle };
    with_residual(|| {
        handle
            .move_prefix(from.as_ref(), to.as_ref())
            .map_err(Into::into)
    })
}

#[no_mangle]
pub extern "C" fn possum_handle_delete_prefix(
    handle: *mut Handle,
    prefix: PossumBuf,
) -> PossumError {
    let handle = unsafe { &mut *handle };
    with_residual(|| handle.delete_prefix(prefix.as_ref()).map_err(Into::into))
}
