mod handle;

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
pub extern "C" fn possum_new(path: *const c_char) -> *mut PossumHandle {
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
    Box::into_raw(Box::new(Arc::new(RwLock::new(handle))))
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
pub extern "C" fn possum_writer_rename(
    writer: *mut PossumWriter,
    value: *const PossumValue,
    new_key: PossumBuf,
) -> PossumError {
    let writer = unsafe { &mut *writer };
    let value: Value = unsafe { &*value }.deref().to_owned();
    with_residual(|| {
        writer.rename_value(value, new_key.as_ref().to_vec());
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn possum_reader_add(
    reader: *mut PossumReader,
    key: PossumBuf,
    value: *mut *const PossumValue,
) -> PossumError {
    let reader = unsafe { reader.as_mut() }.unwrap();
    let mut_rust_reader = reader.rust_reader.as_mut().unwrap();
    let rust_value = match mut_rust_reader.add(key.as_ref()) {
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
                PossumValue::SnapshotValue(snapshot.value(reader_value))
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
