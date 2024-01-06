pub mod c_api;
pub mod clonefile;
mod cpathbuf;
mod error;
mod exclusive_file;
pub mod flock;
mod handle;
mod item;
mod owned_cell;
pub mod pathconf;
pub mod punchfile;
pub mod seekhole;
pub mod testing;
#[cfg(test)]
mod tests;
pub mod walk;

use std::cmp::{max, min};
use std::collections::{hash_map, HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fmt::{Debug, Display, Formatter};
use std::fs::{read_dir, remove_dir, remove_file, File, OpenOptions};
use std::io::SeekFrom::{End, Start};
use std::io::{ErrorKind, Read, Seek, Write};
use std::num::TryFromIntError;
use std::ops::{Deref, DerefMut};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::str;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;
use std::{fs, io};

use anyhow::Result;
use anyhow::{bail, Context};
use chrono::NaiveDateTime;
use clonefile::clonefile;
use cpathbuf::CPathBuf;
pub use error::Error;
use exclusive_file::ExclusiveFile;
pub use handle::Handle;
use log::{debug, warn};
use memmap2::Mmap;
use nix::fcntl::FlockArg::{LockExclusiveNonblock, LockSharedNonblock};
use num::Integer;
use positioned_io::ReadAt;
use rand::Rng;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef};
use rusqlite::Error::QueryReturnedNoRows;
use rusqlite::{params, CachedStatement, Connection, Statement};
use tempfile::TempDir;
#[cfg(test)]
pub use test_log::test;
pub use walk::Entry as WalkEntry;
use ErrorKind::InvalidInput;

use crate::clonefile::fclonefile;
use crate::flock::*;
use crate::item::Item;
use crate::punchfile::punchfile;
use crate::seekhole::seek_hole_whence;
use crate::walk::walk_dir;

/// Type to be exposed eventually from the lib instead of anyhow. Should be useful for the C API.
pub type PubResult<T> = Result<T, Error>;

#[derive(Debug)]
struct FileClone {
    file: File,
    /// This exists to destroy clone tempdirs after they become empty.
    #[allow(unused)]
    tempdir: Arc<TempDir>,
    mmap: Option<Mmap>,
    len: u64,
}

type FileCloneCache = HashMap<FileId, Arc<Mutex<FileClone>>>;

impl FileClone {
    fn get_mmap(&mut self) -> io::Result<&Mmap> {
        let mmap_opt = &mut self.mmap;
        if let Some(mmap) = mmap_opt {
            return Ok(mmap);
        }
        let mmap = unsafe { Mmap::map(self.file.as_raw_fd()) }?;
        Ok(mmap_opt.insert(mmap))
    }
}

struct PendingWrite {
    key: Vec<u8>,
    value_file_offset: u64,
    value_length: u64,
    value_file_id: FileId,
}

const MANIFEST_SCHEMA_SQL: &str = include_str!("../manifest.sql");

fn init_manifest_schema(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    // We could do something smarter here like check a schema version and test for the existence of
    // tables.
    conn.execute_batch(MANIFEST_SCHEMA_SQL)
}

pub struct BeginWriteValue<'writer> {
    batch: &'writer mut BatchWriter,
}

impl BeginWriteValue<'_> {
    pub fn clone_fd(self, fd: RawFd, _flags: u32) -> Result<ValueWriter> {
        let dst_path = loop {
            let dst_path = random_file_name_in_dir(&self.batch.handle_dir);
            match fclonefile(fd, &dst_path, 0) {
                Err(err) if err.kind() == ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err.into()),
                Ok(()) => break dst_path,
            }
        };
        // TODO: Delete the file if this errors?
        let exclusive_file = ExclusiveFile::open(dst_path)?;
        Ok(ValueWriter {
            exclusive_file,
            value_file_offset: 0,
        })
    }

    pub fn begin(self) -> Result<ValueWriter> {
        let mut exclusive_file = self.batch.get_exclusive_file()?;
        Ok(ValueWriter {
            value_file_offset: exclusive_file.next_write_offset()?,
            exclusive_file,
        })
    }
}

// TODO: Implement Drop for ValueWriter?
#[derive(Debug)]
pub struct ValueWriter {
    exclusive_file: ExclusiveFile,
    value_file_offset: u64,
}

impl ValueWriter {
    pub fn get_file(&mut self) -> Result<&mut File> {
        Ok(&mut self.exclusive_file.inner)
    }

    pub fn copy_from(&mut self, mut value: impl Read) -> Result<u64> {
        let value_file_offset = self.exclusive_file.next_write_offset()?;
        let value_length = match std::io::copy(&mut value, &mut self.exclusive_file.inner) {
            Ok(ok) => ok,
            Err(err) => {
                self.exclusive_file
                    .inner
                    .seek(Start(value_file_offset))
                    .expect("should rewind failed copy");
                return Err(err.into());
            }
        };
        Ok(value_length)
    }

    pub fn value_length(&mut self) -> io::Result<u64> {
        Ok(self.exclusive_file.next_write_offset()? - self.value_file_offset)
    }
}

impl Write for ValueWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.exclusive_file.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        // This makes no sense until commit.
        Ok(())
    }
}

pub struct BatchWriter {
    exclusive_files: Vec<ExclusiveFile>,
    pending_writes: Vec<PendingWrite>,
    handle_exclusive_files: HandleExclusiveFiles,
    handle_dir: PathBuf,
}

pub type TimestampInner = NaiveDateTime;

#[derive(Debug, PartialEq, Copy, Clone, PartialOrd)]
pub struct Timestamp(TimestampInner);

impl FromSql for Timestamp {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let int_time = value.as_i64()?;
        Ok(Self(
            TimestampInner::from_timestamp_millis(int_time)
                .ok_or(FromSqlError::OutOfRange(int_time))?,
        ))
    }
}

pub const LAST_USED_RESOLUTION: Duration = Duration::from_millis(1);

impl Deref for Timestamp {
    type Target = TimestampInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct WriteCommitResult {
    count: usize,
}

impl WriteCommitResult {
    pub fn count(&self) -> usize {
        self.count
    }
}

const VALUE_COLUMN_NAMES: &[&str] = &["file_id", "file_offset", "value_length", "last_used"];

fn value_columns_sql() -> &'static str {
    static ONCE: OnceLock<String> = OnceLock::new();
    ONCE.get_or_init(|| VALUE_COLUMN_NAMES.join(", ")).as_str()
}

impl BatchWriter {
    fn get_exclusive_file(&mut self) -> Result<ExclusiveFile> {
        if let Some(ef) = self.exclusive_files.pop() {
            debug!("reusing exclusive file from writer");
            return Ok(ef);
        }
        self.get_handle_exclusive_file()
    }

    fn open_existing_exclusive_file(&self) -> Result<Option<ExclusiveFile>> {
        for res in read_dir(&self.handle_dir)? {
            let entry = res?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            if !valid_file_name(entry.file_name().to_str().unwrap()) {
                continue;
            }
            if let Ok(ef) = ExclusiveFile::open(entry.path()) {
                return Ok(Some(ef));
            }
        }
        Ok(None)
    }

    pub(crate) fn get_handle_exclusive_file(&self) -> Result<ExclusiveFile> {
        let mut files = self.handle_exclusive_files.lock().unwrap();
        // How do we avoid cloning the key and skipping the unnecessary remove check? Do we need a
        // pop method on HashMap?
        if let Some(id) = files.keys().next().cloned() {
            let file = files.remove(&id).unwrap();
            debug_assert_eq!(id, file.id);
            debug!("using exclusive file {} from handle", &file.id);
            return Ok(file);
        }
        if let Some(file) = self.open_existing_exclusive_file()? {
            debug!("opened existing values file {}", file.id);
            return Ok(file);
        }
        let ret = ExclusiveFile::new(&self.handle_dir);
        if let Ok(file) = &ret {
            debug!("created new exclusive file {}", file.id);
        }
        ret
    }

    pub fn stage_write(&mut self, key: Vec<u8>, mut value: ValueWriter) -> anyhow::Result<()> {
        self.pending_writes.push(PendingWrite {
            key,
            value_file_offset: value.value_file_offset,
            value_length: value.value_length()?,
            value_file_id: value.exclusive_file.id.clone(),
        });
        debug!(
            "pushing exclusive file {} into writer",
            value.exclusive_file.id
        );
        self.exclusive_files.push(value.exclusive_file);
        Ok(())
    }

    pub fn new_value(&mut self) -> BeginWriteValue {
        BeginWriteValue { batch: self }
    }

    pub fn commit(self, handle: &mut Handle) -> Result<WriteCommitResult> {
        self.commit_inner(|| {}, handle)
    }

    fn commit_inner(
        mut self,
        before_write: impl Fn(),
        // mut because we need to enforce that there's only one write connection available to a
        // Handle.
        handle: &mut Handle,
    ) -> Result<WriteCommitResult> {
        let mut conn_guard = handle.conn().unwrap();
        let conn: &mut Connection = conn_guard.deref_mut();
        let mut transaction = handle.start_immediate_transaction(conn)?;
        let mut write_commit_res = WriteCommitResult { count: 0 };
        for pw in self.pending_writes.drain(..) {
            before_write();
            transaction.delete_key(&pw.key)?;
            transaction.insert_key(pw)?;
            write_commit_res.count += 1;
        }
        // TODO: On error here, rewind the exclusive to undo any writes that just occurred.
        let post_commit = transaction
            .commit(write_commit_res)
            .context("commit transaction")?;
        self.flush_exclusive_files();
        post_commit.complete(conn)
    }

    /// Flush Writer's exclusive files and return them to the Handle pool.
    fn flush_exclusive_files(&mut self) {
        let mut handle_exclusive_files = self.handle_exclusive_files.lock().unwrap();
        // dbg!(
        //     "adding {} exclusive files to handle",
        //     self.exclusive_files.len()
        // );
        for mut ef in self.exclusive_files.drain(..) {
            ef.committed().unwrap();
            debug!("returning exclusive file {} to handle", ef.id);
            assert!(handle_exclusive_files.insert(ef.id.clone(), ef).is_none());
        }
        // dbg!(
        //     "handle has {} exclusive files",
        //     handle_exclusive_files.len()
        // );
    }
}

impl Drop for BatchWriter {
    fn drop(&mut self) {
        let mut handle_exclusive_files = self.handle_exclusive_files.lock().unwrap();
        for ef in self.exclusive_files.drain(..) {
            assert!(handle_exclusive_files.insert(ef.id.clone(), ef).is_none());
        }
    }
}

#[derive(Debug, Clone)]
pub struct Value {
    file_id: FileId,
    file_offset: u64,
    length: u64,
    last_used: Timestamp,
}

impl Value {
    fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self> {
        let file_id: FileId = row.get(0)?;
        Ok(Value {
            file_id,
            file_offset: row.get(1)?,
            length: row.get(2)?,
            last_used: row.get(3)?,
        })
    }

    pub fn file_offset(&self) -> u64 {
        self.file_offset
    }

    pub fn length(&self) -> u64 {
        self.length
    }

    pub fn last_used(&self) -> Timestamp {
        self.last_used
    }
}

impl AsRef<Value> for Value {
    fn as_ref(&self) -> &Value {
        self
    }
}

impl AsMut<Snapshot> for Snapshot {
    fn as_mut(&mut self) -> &mut Snapshot {
        self
    }
}

impl AsRef<Snapshot> for Snapshot {
    fn as_ref(&self) -> &Self {
        self
    }
}

#[derive(Debug)]
pub struct Snapshot {
    file_clones: HashMap<FileId, Arc<Mutex<FileClone>>>,
}

#[derive(Debug)]
pub struct SnapshotValue<V> {
    value: V,
    cloned_file: Arc<Mutex<FileClone>>,
}
//
// impl<V> AsRef<Value> for SnapshotValue<V> {
//     fn as_ref(&self) -> &Value {
//         &self.value
//     }
// }

impl<V> Deref for SnapshotValue<V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl Snapshot {
    pub fn value<V>(&self, value: V) -> SnapshotValue<V>
    where
        V: AsRef<Value>,
    {
        SnapshotValue {
            cloned_file: Arc::clone(self.file_clones.get(&value.as_ref().file_id).unwrap()),
            value,
        }
    }
}

impl<V> ReadAt for SnapshotValue<V>
where
    V: AsRef<Value>,
{
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> io::Result<usize> {
        // TODO: Create a thiserror or io::Error for non-usize pos.
        // let pos = usize::try_from(pos).expect("pos should be usize");
        let n = self.view(|view| {
            let r = view;
            r.read_at(pos, buf)
        })??;
        // dbg!(buf.split_at(n).0);
        Ok(n)
    }
}

impl<V> SnapshotValue<V>
where
    V: AsRef<Value>,
{
    fn file_clone(&self) -> &Arc<Mutex<FileClone>> {
        &self.cloned_file
    }

    pub fn view<R>(&self, f: impl FnOnce(&[u8]) -> R) -> io::Result<R> {
        let value = self.value.as_ref();
        let file_clone = self.file_clone();
        let start = to_usize_io(value.file_offset)?;
        let usize_length = to_usize_io(value.length)?;
        let end = usize::checked_add(start, usize_length).ok_or_else(make_to_usize_io_error)?;
        let mut mutex_guard = file_clone.lock().unwrap();
        let mmap = mutex_guard.get_mmap()?;
        Ok(f(&mmap[start..end]))
    }

    pub fn read(&self, mut buf: &mut [u8]) -> Result<usize> {
        let value = self.value.as_ref();
        buf = buf
            .split_at_mut(min(buf.len() as u64, value.length) as usize)
            .0;
        let mut file_clone = self.file_clone().lock().unwrap();
        let file = &mut file_clone.file;
        file.seek(Start(value.file_offset))?;
        file.read(buf).map_err(Into::into)
    }

    pub fn new_reader(&self) -> impl Read + '_ {
        positioned_io::Cursor::new(self)
    }

    /// For testing: Leak a reference to the snapshot tempdir so it's not cleaned up when all
    /// references are forgotten. This could possibly be used from internal tests instead.
    pub fn leak_snapshot_dir(&self) {
        std::mem::forget(Arc::clone(&self.file_clone().lock().unwrap().tempdir))
    }
}

mod ownedtx;

pub struct FileValues<'a, S> {
    stmt: S,
    file_id: &'a FileIdFancy,
}

impl<'a, S> FileValues<'a, S>
where
    S: Deref<Target = Statement<'a>> + DerefMut + 'a,
{
    pub fn begin(
        &mut self,
    ) -> rusqlite::Result<impl Iterator<Item = rusqlite::Result<Value>> + '_> {
        self.stmt.query_map([self.file_id], Value::from_row)
    }
}

pub struct Reader<'h, 't> {
    owned_tx: Transaction<'h, 't>,
    handle: &'h Handle,
    files: HashMap<FileId, u64>,
}

impl<'h, 't> Reader<'h, 't> {
    pub fn add(&mut self, key: &[u8]) -> rusqlite::Result<Option<Value>> {
        let res = self.owned_tx.touch_for_read(key);
        match res {
            Ok(value) => {
                let Value {
                    file_offset,
                    length,
                    ..
                } = value;
                let file = self.files.entry(value.file_id.clone());
                let value_end = file_offset + length;
                use hash_map::Entry::*;
                match file {
                    Occupied(mut entry) => {
                        let value = entry.get_mut();
                        *value = max(*value, value_end);
                    }
                    Vacant(entry) => {
                        entry.insert(value_end);
                    }
                };
                Ok(Some(value))
            }
            Err(QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Takes a snapshot and commits the read transaction.
    pub fn begin(self) -> Result<PostCommitWork<'h, Snapshot>> {
        let mut tempdir = None;
        let mut file_clones: FileCloneCache = Default::default();
        let mut handle_clone_guard = self.handle.clones.lock().unwrap();
        let handle_clones = handle_clone_guard.deref_mut();
        for (file_id, min_len) in self.files {
            file_clones.insert(
                file_id.clone(),
                Self::get_file_clone(
                    file_id,
                    &mut tempdir,
                    handle_clones,
                    &self.handle.dir,
                    min_len,
                )
                .context("getting file clone")?,
            );
        }
        self.owned_tx
            .commit(Snapshot { file_clones })
            .context("committing transaction")
    }

    pub fn list_items(&self, prefix: &[u8]) -> PubResult<Vec<Item>> {
        self.owned_tx.list_items(prefix)
    }

    fn get_file_clone(
        file_id: FileId,
        tempdir: &mut Option<Arc<TempDir>>,
        cache: &mut FileCloneCache,
        src_dir: &Path,
        min_len: u64,
    ) -> Result<Arc<Mutex<FileClone>>> {
        if let Some(ret) = cache.get(&file_id) {
            let file_clone_guard = ret.lock().unwrap();
            if file_clone_guard.len >= min_len {
                return Ok(ret.clone());
            }
        }
        let tempdir: &Arc<TempDir> = match tempdir {
            Some(tempdir) => tempdir,
            None => {
                let mut builder = tempfile::Builder::new();
                builder.prefix(SNAPSHOT_DIR_NAME_PREFIX);
                let new = Arc::new(builder.tempdir_in(src_dir)?);
                *tempdir = Some(new);
                tempdir.as_ref().unwrap()
            }
        };
        let tempdir_path = tempdir.path();
        clonefile(
            &file_path(src_dir, &file_id),
            &file_path(tempdir_path, &file_id),
        )?;
        let mut file = open_file_id(OpenOptions::new().read(true), tempdir_path, &file_id)?;
        let len = file.seek(End(0))?;
        try_lock_file(&mut file, LockSharedNonblock)?;
        let file_clone = Arc::new(Mutex::new(FileClone {
            file,
            tempdir: tempdir.clone(),
            mmap: None,
            len,
        }));

        cache.insert(file_id, file_clone.clone());
        Ok(file_clone)
    }
}

#[allow(dead_code)]
fn floored_multiple<T>(value: T, multiple: T) -> T
where
    T: Integer + Copy,
{
    multiple * (value / multiple)
}

pub fn ceil_multiple<T>(value: T, multiple: T) -> T
where
    T: Integer + Copy,
{
    (value + multiple - T::one()) / multiple * multiple
}

fn open_file_id(options: &OpenOptions, dir: &Path, file_id: &FileId) -> io::Result<File> {
    options.open(file_path(dir, file_id))
}

fn file_path(dir: &Path, file_id: impl AsRef<FileId>) -> PathBuf {
    dir.join(file_id.as_ref())
}

fn random_file_name_in_dir(dir: &Path) -> PathBuf {
    let base = random_file_name();
    dir.join(base)
}

const FILE_NAME_RAND_LENGTH: usize = 8;
const VALUES_FILE_NAME_PREFIX: &str = "values-";
const SNAPSHOT_DIR_NAME_PREFIX: &str = "snapshot-";

fn random_file_name() -> OsString {
    let mut begin = VALUES_FILE_NAME_PREFIX.as_bytes().to_vec();
    begin.extend(
        rand::thread_rng()
            .sample_iter(rand::distributions::Alphanumeric)
            .take(FILE_NAME_RAND_LENGTH),
    );
    OsString::from_vec(begin)
}

pub const MANIFEST_DB_FILE_NAME: &str = "manifest.db";

fn valid_file_name(file_name: &str) -> bool {
    if file_name.starts_with(MANIFEST_DB_FILE_NAME) {
        return false;
    }
    file_name.starts_with(VALUES_FILE_NAME_PREFIX)
}

mod file_id;
mod tx;

use crate::handle::HandleExclusiveFiles;
use crate::tx::PostCommitWork;
pub use crate::tx::Transaction;
use file_id::{FileId, FileIdFancy};

struct PunchValueOptions<'a, 't> {
    dir: &'a Path,
    file_id: &'a FileId,
    offset: u64,
    length: u64,
    tx: &'a Transaction<'a, 't>,
    block_size: u64,
    greedy_start: bool,
    check_hole: bool,
}

// Can't do this as &mut self for dumb Rust reasons.
fn punch_value(opts: PunchValueOptions) -> Result<()> {
    let PunchValueOptions {
        dir,
        file_id,
        offset,
        length,
        tx,
        block_size,
        greedy_start,
        check_hole: check_holes,
    } = opts;
    let mut offset = offset as i64;
    let mut length = length as i64;
    let orig_offset = offset;
    let orig_length = length;
    let block_size = block_size as i64;
    // Find out how far back we can punch and start there, correcting for block boundaries as we go.
    if offset % block_size != 0 || greedy_start {
        let new_offset = ceil_multiple(
            tx.query_last_end_offset(file_id, offset as u64)?,
            block_size as u64,
        ) as i64;
        // Because these are u64 we can't deal with overflow into negatives.
        length += offset - new_offset;
        offset = new_offset;
    }
    let mut file = open_file_id(OpenOptions::new().append(true), dir, file_id)?;
    // append doesn't mean our file position is at the end to begin with.
    let file_end = file.seek(End(0))? as i64;
    let end_offset = offset + length;
    // Does this handle ongoing writes to the same file?
    if end_offset < file_end {
        // What if this is below the offset or negative?
        length -= end_offset % block_size;
    }
    // We should never write past a known value, someone might be writing there.
    assert!(offset <= orig_offset + orig_length);
    assert!(offset + length <= orig_offset + orig_length);
    debug!(target: "punching", "punching {} {} for {}", file_id, offset, length);
    punchfile(file.as_raw_fd(), offset, length).with_context(|| format!("length {}", length))?;
    // fcntl(file.as_raw_fd(), nix::fcntl::F_FULLFSYNC)?;
    // file.flush()?;
    if check_holes {
        if let Err(err) = check_hole(&mut file, offset as u64, length as u64) {
            warn!("checking hole: {}", err);
        }
        match seek_hole_whence(file.as_raw_fd(), offset, seekhole::Data).unwrap() {
            // Data starts after the hole we just punched.
            Some(seek_offset) if seek_offset >= (offset + length).try_into().unwrap() => {}
            // There's no data after the hole we just punched.
            None => {}
            otherwise => {
                warn!("punched hole didn't appear: {:?}", otherwise)
            }
        };
    }
    Ok(())
}

pub fn check_hole(file: &mut File, offset: u64, length: u64) -> Result<()> {
    match seek_hole_whence(file.as_raw_fd(), offset as i64, seekhole::Data)? {
        // Data starts after the hole we just punched.
        Some(seek_offset) if seek_offset >= offset + length => Ok(()),
        // There's no data after the hole we just punched.
        None => Ok(()),
        otherwise => {
            bail!("punched hole didn't appear: {:?}", otherwise)
        }
    }
}

fn delete_unused_snapshots(dir: &Path) -> Result<()> {
    use walk::EntryType::*;
    for entry in walk_dir(dir).context("walking dir")? {
        match entry.entry_type {
            SnapshotDir => {
                // If the dir is not empty, it will be attempted after each snapshot value inside
                // anyway.
                let res = remove_dir(&entry.path);
                debug!("removing snapshot dir {:?}: {:?}", &entry.path, res);
            }
            SnapshotValue => {
                match std::fs::File::open(&entry.path) {
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) => {
                        return Err(err)
                            .with_context(|| format!("opening snapshot value {:?}", &entry.path))
                    }
                    Ok(mut file) => {
                        if try_lock_file(&mut file, LockExclusiveNonblock)
                            .context("locking snapshot value")?
                        {
                            let res = remove_file(&entry.path);
                            debug!("removing snapshot value file {:?}: {:?}", &entry.path, res);
                            // Try to delete the parent directory, if it's empty it will succeed.
                            let _ = remove_dir(
                                entry
                                    .path
                                    .parent()
                                    .expect("snapshot values must have a parent dir"),
                            );
                        } else {
                            debug!("not deleting {:?}, still in use", &entry.path);
                        }
                    }
                };
            }
            _ => {}
        }
    }
    Ok(())
}

fn to_usize_io<F>(from: F) -> io::Result<usize>
where
    usize: TryFrom<F, Error = TryFromIntError>,
{
    convert_int_io(from)
}

fn convert_int_io<F, T>(from: F) -> io::Result<T>
where
    T: TryFrom<F, Error = TryFromIntError>,
{
    from.try_into()
        .map_err(|_: TryFromIntError| make_to_usize_io_error())
}

fn make_to_usize_io_error() -> io::Error {
    io::Error::new(TO_USIZE_IO_ERROR_KIND, TO_USIZE_IO_ERR_PAYLOAD)
}

const TO_USIZE_IO_ERROR_KIND: ErrorKind = InvalidInput;
const TO_USIZE_IO_ERR_PAYLOAD: &str = "can't convert to usize";

/// Increments the right most byte, overflowing leftwards. Returns false if incrementing the array
/// overflows the available bytes.
fn inc_big_endian_array(arr: &mut [u8]) -> bool {
    for e in arr.iter_mut().rev() {
        if *e == u8::MAX {
            *e = 0
        } else {
            *e += 1;
            return true;
        }
    }
    false
}
