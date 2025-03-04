use libc::size_t;
use positioned_io::ReadAt;

use super::*;
use crate::c_api::PossumError::NoError;

// This drops the PossumHandle Box. Instead, if this is hard to use correctly from C, it could drop
// a top-level reference count for the box. i.e. If this one goes, there's no way to work with the
// PossumHandle, and when all other outstanding operations on the PossumHandle complete, it will
// drop the PossumHandle for real.
#[no_mangle]
pub extern "C" fn possum_drop(handle: *mut PossumHandle) {
    drop(unsafe { Box::from_raw(handle) })
}

#[no_mangle]
pub extern "C" fn possum_set_instance_limits(
    handle: *mut PossumHandle,
    limits: *const PossumLimits,
) -> PossumError {
    let handle = unsafe { &mut *handle };
    let limits = unsafe { limits.read() };
    with_residual(|| {
        handle
            .write()
            .unwrap()
            .set_instance_limits(limits.into())
            .map_err(Into::into)
    })
}

#[no_mangle]
pub extern "C" fn possum_cleanup_snapshots(handle: *const PossumHandle) -> PossumError {
    let handle = unwrap_possum_handle(handle);
    with_residual(|| handle.read().unwrap().cleanup_snapshots())
}

#[no_mangle]
pub extern "C" fn possum_single_write_buf(
    handle: *mut PossumHandle,
    key: PossumBuf,
    value: PossumBuf,
) -> size_t {
    let key_vec = key.as_ref().to_vec();
    let value_slice = value.as_ref();
    const ERR_SENTINEL: usize = usize::MAX;
    let handle = unsafe { &*handle };
    match handle
        .read()
        .unwrap()
        .single_write_from(key_vec, value_slice)
    {
        Err(_) => ERR_SENTINEL,
        Ok((n, _)) => {
            let n = n.try_into().unwrap();
            assert_ne!(n, ERR_SENTINEL);
            n
        }
    }
}

#[no_mangle]
pub extern "C" fn possum_new_writer(handle: *mut PossumHandle) -> *mut PossumWriter {
    let handle = unwrap_possum_handle(handle);
    let writer = BatchWriter::new(handle.clone());
    Box::into_raw(Box::new(writer))
}

#[no_mangle]
pub extern "C" fn possum_single_stat(
    handle: *const PossumHandle,
    key: PossumBuf,
    out_stat: *mut PossumStat,
) -> bool {
    match unsafe { handle.as_ref() }
        .unwrap()
        .read()
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
    handle: *const PossumHandle,
    prefix: PossumBuf,
    out_list: *mut *mut PossumItem,
    out_list_len: *mut size_t,
) -> PossumError {
    let items = match unsafe { handle.as_ref() }
        .unwrap()
        .read()
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
    handle: *const PossumHandle,
    key: PossumBuf,
    buf: *mut PossumBuf,
    offset: u64,
) -> PossumError {
    let rust_key = key.as_ref();
    let value = match unsafe { handle.as_ref() }
        .unwrap()
        .read()
        .unwrap()
        .read_single(rust_key)
    {
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
    // eprintln!(
    //     "reading single {} bytes at {} from {}, read {}: {}",
    //     read_buf.len(),
    //     offset,
    //     value.length(),
    //     r_nbyte,
    //     rust_key.escape_ascii(),
    // );
    buf.size = r_nbyte;
    NoError
}

/// stat is filled if non-null and a delete occurs. NoSuchKey is returned if the key does not exist.
#[no_mangle]
pub extern "C" fn possum_single_delete(
    handle: *const PossumHandle,
    key: PossumBuf,
    stat: *mut PossumStat,
) -> PossumError {
    with_residual(|| {
        let handle = unsafe { &*handle };
        let value = match handle.read().unwrap().single_delete(key.as_ref()) {
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
    handle: *const PossumHandle,
    reader: *mut *mut PossumReader,
) -> PossumError {
    let handle = unwrap_possum_handle(handle).clone();
    let reader = unsafe { reader.as_mut() }.unwrap();
    let owned_tx_res = handle.start_transaction(
        // This is copied from Handle::start_writable_transaction_with_behaviour and Handle::read
        // until I make proper abstractions.
        |conn, handle| {
            let rtx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
            Ok(Transaction::new(rtx, handle))
        },
    );
    let owned_tx = match owned_tx_res {
        Ok(ok) => ok,
        Err(err) => return err.into(),
    };
    let rust_reader = Reader {
        owned_tx,
        reads: Default::default(),
    };
    *reader = Box::into_raw(Box::new(PossumReader {
        rust_reader: Some(rust_reader),
        values: Default::default(),
    }));
    NoError
}

#[no_mangle]
pub extern "C" fn possum_handle_move_prefix(
    handle: *mut PossumHandle,
    from: PossumBuf,
    to: PossumBuf,
) -> PossumError {
    let handle = unsafe { &mut *handle };
    with_residual(|| {
        handle
            .read()
            .unwrap()
            .move_prefix(from.as_ref(), to.as_ref())
            .map_err(Into::into)
    })
}

#[no_mangle]
pub extern "C" fn possum_handle_delete_prefix(
    handle: *mut PossumHandle,
    prefix: PossumBuf,
) -> PossumError {
    let handle = unsafe { &mut *handle };
    with_residual(|| {
        handle
            .read()
            .unwrap()
            .delete_prefix(prefix.as_ref())
    })
}
