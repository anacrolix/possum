pub mod c_api;
pub mod clonefile;
mod cpathbuf;
mod error;
mod exclusive_file;
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
use std::ffi::OsString;
use std::fmt::{Debug, Display, Formatter};
use std::fs::{read_dir, remove_dir, remove_dir_all, remove_file, File, OpenOptions};
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
use rusqlite::types::ValueRef::{Null, Real};
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef};
use rusqlite::Error::QueryReturnedNoRows;
use rusqlite::{params, Connection, ToSql};
use tempfile::TempDir;
#[cfg(test)]
pub use test_log::test;
pub use walk::Entry as WalkEntry;
use ErrorKind::InvalidInput;

use crate::clonefile::fclonefile;
use crate::exclusive_file::try_lock_file;
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

pub struct BeginWriteValue<'writer, 'handle> {
    batch: &'writer mut BatchWriter<'handle>,
}

impl BeginWriteValue<'_, '_> {
    pub fn clone_fd(self, fd: RawFd, _flags: u32) -> Result<ValueWriter> {
        let dst_path = loop {
            let dst_path = random_file_name_in_dir(&self.batch.handle.dir);
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

pub struct BatchWriter<'a> {
    handle: &'a Handle,
    exclusive_files: Vec<ExclusiveFile>,
    pending_writes: Vec<PendingWrite>,
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

impl<'handle> BatchWriter<'handle> {
    fn get_exclusive_file(&mut self) -> Result<ExclusiveFile> {
        if let Some(ef) = self.exclusive_files.pop() {
            debug!("reusing exclusive file from writer");
            return Ok(ef);
        }
        self.handle.get_exclusive_file()
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

    pub fn new_value<'writer>(&'writer mut self) -> BeginWriteValue<'writer, 'handle> {
        BeginWriteValue { batch: self }
    }

    pub fn commit(self) -> Result<WriteCommitResult> {
        self.commit_inner(|| {})
    }

    fn commit_inner(mut self, before_write: impl Fn()) -> Result<WriteCommitResult> {
        let mut punch_values = vec![];
        let transaction: OwnedTx = self.handle.start_immediate_transaction()?;
        let mut altered_files = HashSet::new();
        let mut write_commit_res = WriteCommitResult { count: 0 };
        for pw in self.pending_writes.drain(..) {
            before_write();
            let existing = transaction.delete_key(&pw.key);
            match existing {
                Ok(Some(value)) => {
                    let value_length = value.length;
                    if value_length != 0 {
                        altered_files.insert(value.file_id.clone());
                    }
                    punch_values.push(value);
                }
                Ok(None) => (),
                Err(err) => return Err(err.into()),
            }
            let inserted = transaction.execute(
                "insert into keys (key, file_id, file_offset, value_length)\
                values (?, ?, ?, ?)",
                rusqlite::params!(
                    pw.key,
                    pw.value_file_id,
                    pw.value_file_offset,
                    pw.value_length
                ),
            )?;
            assert_eq!(inserted, 1);
            if pw.value_length != 0 {
                altered_files.insert(pw.value_file_id);
            }
            write_commit_res.count += 1;
        }
        transaction.commit().context("commit transaction")?;

        self.flush_exclusive_files();
        // This has to happen after exclusive files are flushed or there's a tendency for hole
        // punches to not persist. It doesn't fix the problem but it significantly reduces it.
        self.handle.punch_values(&punch_values)?;
        // Forget any references to clones of files that have changed.
        for file_id in altered_files {
            self.handle.clones.lock().unwrap().remove(&file_id);
        }
        Ok(write_commit_res)
    }

    /// Flush Writer's exclusive files and return them to the Handle pool.
    fn flush_exclusive_files(&mut self) {
        let mut handle_exclusive_files = self.handle.exclusive_files.lock().unwrap();
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

impl Drop for BatchWriter<'_> {
    fn drop(&mut self) {
        // dbg!(
        //     "adding exclusive files to handle",
        //     self.exclusive_files.len()
        // );
        let mut handle_exclusive_files = self.handle.exclusive_files.lock().unwrap();
        for ef in self.exclusive_files.drain(..) {
            assert!(handle_exclusive_files.insert(ef.id.clone(), ef).is_none());
        }
        // dbg!("handle exclusive files", handle_exclusive_files.len());
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

impl Value {
    pub fn length(&self) -> u64 {
        self.length
    }

    pub fn last_used(&self) -> Timestamp {
        self.last_used
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

/// A Sqlite Transaction and the mutex guard on the Connection it came from.
// Not in the handle module since it can be owned by types other than Handle.
pub struct OwnedTx<'handle> {
    cell: OwnedTxInner<'handle>,
}

pub struct Transaction<'h> {
    tx: rusqlite::Transaction<'h>,
}

/// This should probably go away when we want to control queries via Transaction, and require a
/// special method to dive deeper for custom stuff.
impl<'h> Deref for Transaction<'h> {
    type Target = rusqlite::Transaction<'h>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl Transaction<'_> {
    fn delete_key(&self, key: &[u8]) -> rusqlite::Result<Option<Value>> {
        match self.query_row(
            &format!(
                "delete from keys where key=? returning {}",
                value_columns_sql()
            ),
            [key],
            Value::from_row,
        ) {
            Err(QueryReturnedNoRows) => Ok(None),
            Ok(value) => Ok(Some(value)),
            Err(err) => Err(err),
        }
    }
}

impl<'a> From<rusqlite::Transaction<'a>> for Transaction<'a> {
    fn from(tx: rusqlite::Transaction<'a>) -> Self {
        Self { tx }
    }
}

type OwnedTxInner<'handle> =
    owned_cell::OwnedCell<MutexGuard<'handle, Connection>, Transaction<'handle>>;

impl<'a> From<OwnedTxInner<'a>> for OwnedTx<'a> {
    fn from(cell: OwnedTxInner<'a>) -> Self {
        Self { cell }
    }
}

impl AsRef<Connection> for OwnedTx<'_> {
    fn as_ref(&self) -> &Connection {
        &self.cell
    }
}

impl<'a> Deref for OwnedTx<'a> {
    type Target = Transaction<'a>;

    fn deref(&self) -> &Self::Target {
        &self.cell
    }
}

impl<'a> OwnedTx<'a> {
    fn commit(self) -> rusqlite::Result<()> {
        self.cell.move_dependent(|tx| tx.tx.commit())
    }
}

// impl AsRef<rusqlite::Connection> for OwnedTx<'_> {
//     fn as_ref(&self) -> &Connection {
//         self.cell
//     }
// }

pub struct Reader<'handle> {
    owned_tx: OwnedTx<'handle>,
    handle: &'handle Handle,
    files: HashMap<FileId, u64>,
}

impl<'a> Reader<'a> {
    pub fn add(&mut self, key: &[u8]) -> rusqlite::Result<Option<Value>> {
        let res = self.owned_tx.query_row(
            "update keys \
            set last_used=cast(unixepoch('subsec')*1e3 as integer) \
            where key=? \
            returning file_id, file_offset, value_length, last_used",
            [key],
            |row| {
                let file_id: FileId = row.get(0)?;
                Ok((file_id, row.get(1)?, row.get(2)?, row.get(3)?))
            },
        );
        match res {
            Ok((file_id, file_offset, value_length, last_used)) => {
                let file = self.files.entry(file_id.clone());
                let value_end = file_offset + value_length;
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
                Ok(Some(Value {
                    file_id,
                    file_offset,
                    length: value_length,
                    last_used,
                }))
            }
            Err(QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Takes a snapshot and commits the read transaction.
    pub fn begin(self) -> Result<Snapshot> {
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
        self.owned_tx.commit().context("committing transaction")?;
        Ok(Snapshot { file_clones })
    }

    pub fn list_items(&self, prefix: &[u8]) -> PubResult<Vec<Item>> {
        list_items(self.owned_tx.deref(), prefix)
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

fn ceil_multiple<T>(value: T, multiple: T) -> T
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

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct FileId(OsString);

impl From<OsString> for FileId {
    fn from(value: OsString) -> Self {
        Self(value)
    }
}

// impl Deref for FileId {
//     type Target = OsString;
//
//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

impl AsRef<Path> for FileId {
    fn as_ref(&self) -> &Path {
        Path::new(&self.0)
    }
}

impl Debug for FileId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<String> for FileId {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<Vec<u8>> for FileId {
    fn from(value: Vec<u8>) -> Self {
        OsString::from_vec(value).into()
    }
}

impl FileId {
    fn as_str(&self) -> &str {
        self.0.to_str().unwrap()
    }
}

impl ToSql for FileId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Blob(self.0.as_bytes())))
    }
}

impl FromSql for FileId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(match value {
            Null | Real(..) => Err(FromSqlError::InvalidType),
            ValueRef::Text(text) => Ok(text.to_owned()),
            ValueRef::Blob(blob) => Ok(blob.to_owned()),
            ValueRef::Integer(int) => Ok(int.to_string().into_bytes()),
        }?
        .into())
    }
}

impl Display for FileId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.as_str(), f)
    }
}

impl AsRef<FileId> for FileId {
    fn as_ref(&self) -> &FileId {
        self
    }
}

struct PunchValueOptions<'a> {
    dir: &'a Path,
    file_id: &'a FileId,
    offset: u64,
    length: u64,
    tx: &'a Transaction<'a>,
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
        check_hole,
    } = opts;
    let mut offset = offset as i64;
    let mut length = length as i64;
    let orig_offset = offset;
    let orig_length = length;
    let block_size = block_size as i64;
    // Find out how far back we can punch and start there, correcting for block boundaries as we go.
    if offset % block_size != 0 || greedy_start {
        let new_offset = ceil_multiple(
            query_last_end_offset(tx, file_id, offset as u64)?,
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
    if check_hole {
        match seek_hole_whence(file.as_raw_fd(), offset, seekhole::Data).unwrap() {
            // Data starts after the hole we just punched.
            Some(seek_offset) if seek_offset >= offset + length => {}
            // There's no data after the hole we just punched.
            None => {}
            otherwise => {
                warn!("punched hole didn't appear: {:?}", otherwise)
            }
        };
    }
    Ok(())
}

fn delete_unused_snapshots(dir: &Path) -> Result<()> {
    use walk::EntryType::*;
    for entry in walk_dir(dir)? {
        match entry.entry_type {
            SnapshotDir => {
                let res = remove_dir(&entry.path);
                debug!("removing snapshot dir {:?}: {:?}", &entry.path, res);
            }
            SnapshotValue => {
                let mut file = std::fs::File::open(&entry.path)?;
                if try_lock_file(&mut file, LockExclusiveNonblock)? {
                    let res = remove_file(&entry.path);
                    debug!("removing snapshot value file {:?}: {:?}", &entry.path, res);
                    let _ = remove_dir_all(
                        entry
                            .path
                            .parent()
                            .expect("snapshot values must have a parent dir"),
                    );
                } else {
                    debug!("not deleting {:?}, still in use", &entry.path);
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Returns the end offset of the last active value before offset in the same file.
pub fn query_last_end_offset(
    tx: &rusqlite::Transaction,
    file_id: &FileId,
    offset: u64,
) -> rusqlite::Result<u64> {
    tx.query_row(
        "select max(file_offset+value_length) as last_offset \
            from keys \
            where file_id=? and file_offset+value_length <= ?",
        params![file_id, offset],
        |row| {
            // I don't know why, but this can return null for file_ids that have values but
            // don't fit the other conditions.
            let res: rusqlite::Result<Option<_>> = row.get(0);
            res.map(|v| v.unwrap_or_default())
        },
    )
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

fn list_items(tx: &Transaction, prefix: &[u8]) -> PubResult<Vec<Item>> {
    let range_end = {
        let mut prefix = prefix.to_owned();
        if inc_big_endian_array(&mut prefix) {
            Some(prefix)
        } else {
            None
        }
    };
    match range_end {
        None => list_items_inner(
            tx,
            &format!(
                "select {}, key from keys where key >= ?",
                value_columns_sql()
            ),
            [prefix],
        ),
        Some(range_end) => list_items_inner(
            tx,
            &format!(
                "select {}, key from keys where key >= ? and key < ?",
                value_columns_sql()
            ),
            rusqlite::params![prefix, range_end],
        ),
    }
}

fn list_items_inner(
    tx: &Transaction,
    sql: &str,
    params: impl rusqlite::Params,
) -> PubResult<Vec<Item>> {
    tx.prepare_cached(sql)
        .unwrap()
        .query_map(params, |row| {
            Ok(Item {
                value: Value::from_row(row)?,
                key: row.get(VALUE_COLUMN_NAMES.len())?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}
