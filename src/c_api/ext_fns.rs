use std::ffi::{c_char, CStr};
use std::path::PathBuf;
use std::ptr::null_mut;

use libc::size_t;
use positioned_io::ReadAt;
use tracing::{error, warn};

use super::*;
use crate::c_api::PossumError::{NoError, NoSuchKey};
use crate::Handle;

#[no_mangle]
pub extern "C" fn possum_new(path: *const c_char) -> *mut Handle {
    if let Err(err) = env_logger::try_init() {
        warn!("error initing env_logger: {}", err);
    }
    let c_str = unsafe { CStr::from_ptr(path) };
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            let str = ::std::str::from_utf8(c_str.to_bytes()).expect("keep your surrogates paired");
            let path_buf = PathBuf::from(str);
        } else {
            let path_buf: PathBuf = OsStr::from_bytes(c_str.to_bytes()).into();
        }
    }
    let handle = match Handle::new(path_buf.clone()) {
        Ok(handle) => handle,
        Err(err) => {
            error!("error creating possum handle in {path_buf:?}: {err}");
            return null_mut();
        }
    };
    Box::into_raw(Box::new(handle))
}

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
pub extern "C" fn possum_start_new_value(
    writer: *mut PossumWriter,
    value: *mut *mut PossumValueWriter,
) -> PossumError {
    let writer = unsafe { &mut *writer };
    with_residual(|| {
        let v = Box::into_raw(Box::new(writer.new_value().begin()?));
        unsafe { *value = v };
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn possum_value_writer_fd(value: *mut PossumValueWriter) -> RawFileHandle {
    unsafe { &mut *value }
        .get_file()
        .unwrap()
        .as_raw_file_handle()
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
pub extern "C" fn possum_reader_add(
    reader: *mut PossumReader,
    key: PossumBuf,
    value: *mut *const PossumValue,
) -> PossumError {
    let reader = unsafe { reader.as_mut() }.unwrap();
    let rust_value = match reader.rust_reader.as_mut().unwrap().add(key.as_ref()) {
        Ok(None) => return NoSuchKey,
        Ok(Some(value)) => value,
        Err(err) => return err.into(),
    };
    let new_value = PossumValue::ReaderValue(rust_value);
    reader.values.push(Box::pin(new_value));
    let out_value: *const PossumValue = &*reader.values.last().unwrap().as_ref().as_ref();
    unsafe { *value = out_value };
    NoError
}

/// Takes a snapshot so the reader values can be used.
#[no_mangle]
pub extern "C" fn possum_reader_begin(reader: *mut PossumReader) -> PossumError {
    let reader = unsafe { &mut *reader };
    let snapshot = match reader.rust_reader.take().unwrap().begin() {
        Ok(snapshot) => snapshot,
        Err(err) => return err.into(),
    };
    for value in &mut reader.values {
        // Modify the enum in place using values it contains.
        take_mut::take(&mut *value.as_mut(), |value| {
            if let PossumValue::ReaderValue(reader_value) = value {
                PossumValue::SnapshotValue(snapshot.value(reader_value.clone()))
            } else {
                panic!("expected reader value");
            }
        });
    }
    NoError
}

/// Consumes the reader, invalidating all values produced from it.
#[no_mangle]
pub extern "C" fn possum_reader_end(reader: *mut PossumReader) {
    drop(unsafe { Box::from_raw(reader) });
}

#[no_mangle]
pub extern "C" fn possum_value_read_at(
    value: *const PossumValue,
    buf: *mut PossumBuf,
    offset: PossumOffset,
) -> PossumError {
    let value = unsafe { &*value };
    let PossumValue::SnapshotValue(value) = value else {
        panic!("reader snapshot must be taken");
    };
    let buf = unsafe { &mut *buf };
    match value.read_at(offset, buf.as_mut_slice()) {
        Err(err) => return err.into(),
        Ok(ok) => {
            buf.size = ok;
        }
    }
    NoError
}

#[no_mangle]
pub extern "C" fn possum_value_stat(value: *const PossumValue, out_stat: *mut PossumStat) {
    let value = unsafe { &*value };
    let out_stat = unsafe { &mut *out_stat };
    *out_stat = value.into();
}

#[no_mangle]
pub extern "C" fn possum_reader_list_items(
    reader: *const PossumReader,
    prefix: PossumBuf,
    out_items: *mut *mut PossumItem,
    out_len: *mut size_t,
) -> PossumError {
    let reader = unsafe { &*reader };
    with_residual(|| {
        items_list_to_c(
            prefix.size,
            reader
                .rust_reader
                .as_ref()
                .unwrap()
                .list_items(prefix.as_ref())?,
            out_items,
            out_len,
        );
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn possum_writer_commit(writer: *mut PossumWriter) -> PossumError {
    let writer = unsafe { Box::from_raw(writer) };
    with_residual(|| {
        writer.commit()?;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn possum_writer_stage(
    writer: *mut PossumWriter,
    key: PossumBuf,
    value: *mut PossumValueWriter,
) -> PossumError {
    let writer = unsafe { &mut *writer };
    let value = unsafe { Box::from_raw(value) };
    with_residual(|| {
        writer
            .stage_write(key.as_ref().to_vec(), *value)
            .map_err(Into::into)
    })
}
