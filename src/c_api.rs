#![allow(clippy::not_unsafe_ptr_arg_deref)]
use std::ffi::{c_char, CStr, OsStr};
use std::mem::size_of;
use std::pin::Pin;
use std::ptr::{copy_nonoverlapping, null_mut};
use std::slice;

use libc::{calloc, malloc, size_t};
use log::{error, warn};

use super::*;

pub type PossumWriter = *mut BatchWriter<'static>;
pub type PossumOffset = u64;

#[repr(C)]
#[derive(Debug)]
pub struct PossumBuf {
    ptr: *const c_char,
    size: size_t,
}

impl AsRef<[u8]> for PossumBuf {
    fn as_ref(&self) -> &[u8] {
        let ptr = self.ptr as *const u8;
        unsafe { slice::from_raw_parts(ptr, self.size) }
    }
}

impl PossumBuf {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        let ptr = self.ptr as *mut u8;
        unsafe { slice::from_raw_parts_mut(ptr, self.size) }
    }
}

pub enum PossumValue {
    ReaderValue(Value),
    SnapshotValue(SnapshotValue<Value>),
}

pub struct PossumReader {
    // Removed when converted to a snapshot. Specific to the C API so as to not need to expose
    // Snapshot, and to convert Values automatically when a snapshot starts.
    rust_reader: Option<Reader<'static>>,
    values: Vec<Pin<Box<PossumValue>>>,
}

#[no_mangle]
pub extern "C" fn possum_new(path: *const c_char) -> *mut Handle {
    if let Err(err) = env_logger::try_init() {
        warn!("error initing env_logger: {}", err);
    }
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

// This drops the Handle Box. Instead, if this is hard to use correctly from C, it could drop a
// top-level reference count for the box. i.e. If this one goes, there's no way to work with the
// Handle, and when all other outstanding operations on the Handle complete, it will drop the Handle
// for real.
#[no_mangle]
pub extern "C" fn possum_drop(handle: *mut Handle) {
    drop(unsafe { Box::from_raw(handle) })
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
pub extern "C" fn possum_new_writer(handle: *mut Handle) -> PossumWriter {
    let handle = unsafe { &*handle };
    Box::into_raw(Box::new(handle.new_writer().unwrap()))
}

pub type PossumValueWriter = *mut ValueWriter;

#[no_mangle]
pub extern "C" fn possum_start_new_value(
    writer: PossumWriter,
    value: *mut PossumValueWriter,
) -> PossumError {
    let v = match unsafe { writer.as_mut() }.unwrap().new_value().begin() {
        Err(err) => return err.into(),
        Ok(ok) => Box::into_raw(Box::new(ok)),
    };
    unsafe { *value = v };
    NoError
}

#[no_mangle]
pub extern "C" fn possum_value_writer_fd(value: PossumValueWriter) -> RawFd {
    unsafe { value.as_mut() }
        .unwrap()
        .get_file()
        .unwrap()
        .as_raw_fd()
}

pub use libc::timespec;

use crate::c_api::PossumError::{AnyhowError, IoError, NoError, SqliteError};

#[repr(C)]
pub struct PossumStat {
    last_used: PossumTimestamp,
    size: u64,
}

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

#[repr(C)]
pub struct PossumTimestamp {
    secs: i64,
    nanos: u32,
}

impl From<Timestamp> for PossumTimestamp {
    fn from(value: Timestamp) -> Self {
        Self {
            secs: value.timestamp(),
            nanos: value.timestamp_subsec_nanos(),
        }
    }
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

#[repr(C)]
pub struct PossumItem {
    key: PossumBuf,
    stat: PossumStat,
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

#[repr(C)]
pub enum PossumError {
    NoError,
    NoSuchKey,
    SqliteError,
    IoError,
    AnyhowError,
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
    let handle = unsafe { &*handle };
    let value = match handle.single_delete(key.as_ref()) {
        Ok(None) => return NoSuchKey,
        Err(err) => return err.into(),
        Ok(Some(value)) => value,
    };
    if let Some(stat) = unsafe { stat.as_mut() } {
        *stat = value.into();
    }
    NoError
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
pub extern "C" fn possum_reader_end(reader: *mut PossumReader) -> PossumError {
    drop(unsafe { Box::from_raw(reader) });
    NoError
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
pub extern "C" fn possum_reader_list_items(
    reader: *const PossumReader,
    prefix: PossumBuf,
    out_items: *mut *mut PossumItem,
    out_len: *mut size_t,
) -> PossumError {
    let reader = unsafe { &*reader };
    items_list_to_c(
        prefix.size,
        reader
            .rust_reader
            .as_ref()
            .unwrap()
            .list_items(prefix.as_ref())
            .unwrap(),
        out_items,
        out_len,
    );
    NoError
}
