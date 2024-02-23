#![allow(clippy::unused_unit)]

use std::borrow::Borrow;
use std::cmp::min;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fmt::{Debug, Display, Formatter};
use std::fs::{read_dir, remove_dir, remove_file, File, OpenOptions};
use std::io::SeekFrom::{End, Start};
use std::io::{ErrorKind, Read, Seek, Write};
use std::num::TryFromIntError;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::process::abort;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;
use std::{fs, io, str};

use anyhow::{bail, Context, Result};
use cfg_if::cfg_if;
use chrono::NaiveDateTime;
pub use error::Error;
use exclusive_file::ExclusiveFile;
use file_id::{FileId, FileIdFancy};
pub use handle::Handle;
use memmap2::Mmap;
use num::Integer;
use ownedtx::OwnedTx;
use positioned_io::ReadAt;
use rand::Rng;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef};
use rusqlite::Error::QueryReturnedNoRows;
use rusqlite::{params, CachedStatement, Connection, Statement};
use sys::*;
use tempfile::TempDir;
#[cfg(test)]
pub use test_log::test;
use tracing::*;
use ErrorKind::InvalidInput;

use crate::item::Item;
use crate::walk::walk_dir;
use crate::ValueLocation::{Nonzero, ZeroLength};

mod c_api;
mod cpathbuf;
mod dir;
mod error;
mod exclusive_file;
mod file_id;
pub(crate) mod handle;
mod item;
mod owned_cell;
pub mod sys;
#[cfg(feature = "testing")]
pub mod testing;
#[cfg(test)]
mod tests;
mod tx;
pub use tx::*;
mod ownedtx;
pub mod walk;

/// Type to be exposed eventually from the lib instead of anyhow. Should be useful for the C API.
pub type PubResult<T> = Result<T, Error>;

#[derive(Debug)]
struct FileClone {
    file: File,
    /// This exists to destroy clone tempdirs after they become empty.
    #[allow(unused)]
    tempdir: Option<Arc<TempDir>>,
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
        let mmap = unsafe {
            memmap2::MmapOptions::new()
                .len(self.len.try_into().unwrap())
                .map_copy_read_only(&self.file)
        }?;
        assert_eq!(mmap.len() as u64, self.len);
        Ok(mmap_opt.insert(mmap))
    }
}

#[derive(Debug)]
struct PendingWrite {
    key: Vec<u8>,
    value_file_offset: u64,
    value_length: u64,
    value_file_id: FileId,
}

const MANIFEST_SCHEMA_SQL: &str = include_str!("../manifest.sql");

fn init_manifest_schema(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    conn.execute_batch(MANIFEST_SCHEMA_SQL)
}

/// The start of a write, before an exclusive file has been allocated. This allows for bringing your
/// own file, such as by rename, or file clone.
pub struct BeginWriteValue<'writer, 'handle> {
    batch: &'writer mut BatchWriter<'handle>,
}

impl BeginWriteValue<'_, '_> {
    // TODO: On Linux and Windows, this should be possible without creating a new file. I'm not sure
    // if it's worth it however, since cloned blocks have to be a of a minimum size and alignment.
    // See also
    // https://stackoverflow.com/questions/65505765/difference-of-ficlone-vs-ficlonerange-vs-copy-file-range-for-copy-on-write-supp
    // for a discussion on efficient ways to copy values that could be supported.
    /// Clone an entire file in. If cloning fails, this will fall back to copying the provided file.
    /// Its file position may be altered.
    pub fn clone_file(self, file: &mut File) -> PubResult<ValueWriter> {
        if !self.batch.handle.dir_supports_file_cloning() {
            return self.copy_file(file);
        }
        let dst_path = loop {
            let dst_path = random_file_name_in_dir(self.batch.handle.dir.path());
            match fclonefile_noflags(file, &dst_path) {
                Err(err) if err.root_cause_is_unsupported_filesystem() => {
                    return self.copy_file(file);
                }
                Err(err) if err.is_file_already_exists() => continue,
                Err(err) => return Err(err),
                Ok(()) => break dst_path,
            }
        };
        // Should we delete this file if we fail to open it exclusively? I think it is possible that
        // someone else could open it before us. In that case we probably want to punch out the part
        // we cloned and move on.
        let exclusive_file = ExclusiveFile::open(dst_path)?.unwrap();
        Ok(ValueWriter {
            exclusive_file,
            value_file_offset: 0,
        })
    }

    /// Assigns an exclusive file for writing, and copies the entire source file.
    fn copy_file(self, file: &mut File) -> PubResult<ValueWriter> {
        let mut value_writer = self.begin()?;
        // Need to rewind the file since we're cloning the whole thing.
        file.seek(Start(0))?;
        value_writer.copy_from(file)?;
        Ok(value_writer)
    }

    /// Assign an exclusive file for writing a value.
    pub fn begin(self) -> PubResult<ValueWriter> {
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

    pub fn copy_from(&mut self, mut value: impl Read) -> PubResult<u64> {
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
        let file = &mut self.exclusive_file.inner;
        file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        // This makes no sense until commit.
        Ok(())
    }
}

#[derive(Debug)]
struct ValueRename {
    value: Value,
    new_key: Vec<u8>,
}

/// Manages uncommitted writes
#[derive(Debug)]
pub struct BatchWriter<'a> {
    handle: &'a Handle,
    exclusive_files: Vec<ExclusiveFile>,
    pending_writes: Vec<PendingWrite>,
    value_renames: Vec<ValueRename>,
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

// This may only be public for external tests.
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
        let value_length = match value.value_length() {
            Ok(ok) => ok,
            Err(err) => {
                if let Err(err) = value
                    .exclusive_file
                    .revert_to_offset(value.value_file_offset)
                {
                    error!("error reverting value write: {:#?}", err);
                }
                // The ExclusiveFile is probably broken in some way if we couldn't seek on it. Don't
                // return it to the BatchWriter.
                return Err(err.into());
            }
        };
        let exclusive_file = value.exclusive_file;
        let value_file_id = exclusive_file.id.clone();
        self.exclusive_files.push(exclusive_file);
        self.pending_writes.push(PendingWrite {
            key,
            value_file_offset: value.value_file_offset,
            value_length,
            value_file_id,
        });
        Ok(())
    }

    pub fn new_value<'writer>(&'writer mut self) -> BeginWriteValue<'writer, 'handle> {
        BeginWriteValue { batch: self }
    }

    pub fn rename_value(&mut self, value: Value, key: Vec<u8>) {
        self.value_renames.push(ValueRename {
            value,
            new_key: key,
        });
    }

    pub fn commit(self) -> Result<WriteCommitResult> {
        self.commit_inner(|| {})
    }

    fn commit_inner(mut self, before_write: impl Fn()) -> Result<WriteCommitResult> {
        let mut transaction: OwnedTx = self.handle.start_immediate_transaction()?;
        let mut write_commit_res = WriteCommitResult { count: 0 };
        for pw in self.pending_writes.drain(..) {
            before_write();
            transaction.delete_key(&pw.key)?;
            transaction.insert_key(pw)?;
            write_commit_res.count += 1;
        }
        for vr in self.value_renames.drain(..) {
            transaction.rename_value(&vr.value, vr.new_key)?;
        }
        // TODO: On error here, rewind the exclusive to undo any writes that just occurred.
        let work = transaction
            .commit(write_commit_res)
            .context("commit transaction")?;

        self.flush_exclusive_files();
        work.complete()
    }

    /// Flush Writer's exclusive files and return them to the Handle pool.
    fn flush_exclusive_files(&mut self) {
        let mut handle_exclusive_files = self.handle.exclusive_files.lock().unwrap();
        for mut ef in self.exclusive_files.drain(..) {
            ef.committed().unwrap();
            debug!("returning exclusive file {} to handle", ef.id);
            assert!(handle_exclusive_files.insert(ef.id.clone(), ef).is_none());
        }
    }
}

impl Drop for BatchWriter<'_> {
    fn drop(&mut self) {
        let mut handle_exclusive_files = self.handle.exclusive_files.lock().unwrap();
        for ef in self.exclusive_files.drain(..) {
            assert!(handle_exclusive_files.insert(ef.id.clone(), ef).is_none());
        }
    }
}

type ValueLength = u64;

#[derive(Debug, Clone, PartialEq)]
pub struct Value {
    pub location: ValueLocation,
    last_used: Timestamp,
}

/// Storage location info for a non-zero-length value.
#[derive(Debug, Clone, PartialEq)]
pub struct NonzeroValueLocation {
    pub file_id: FileId,
    pub file_offset: u64,
    pub length: ValueLength,
}

/// Location data for a value. Includes the case where the value is empty and so doesn't require
/// allocation.
#[derive(Debug, Clone, PartialEq)]
pub enum ValueLocation {
    ZeroLength,
    Nonzero(NonzeroValueLocation),
}

impl ValueLocation {
    pub fn into_non_zero(self) -> Option<NonzeroValueLocation> {
        match self {
            ZeroLength => None,
            Nonzero(a) => Some(a),
        }
    }

    pub fn file_offset(&self) -> Option<u64> {
        match self {
            ZeroLength => None,
            Nonzero(NonzeroValueLocation { file_offset, .. }) => Some(*file_offset),
        }
    }

    pub fn file_id(&self) -> Option<&FileIdFancy> {
        match self {
            ZeroLength => None,
            Nonzero(NonzeroValueLocation { file_id, .. }) => Some(file_id),
        }
    }

    pub fn length(&self) -> u64 {
        match self {
            ZeroLength => 0,
            Nonzero(NonzeroValueLocation { length, .. }) => *length,
        }
    }
}

impl Deref for Value {
    type Target = ValueLocation;

    fn deref(&self) -> &Self::Target {
        &self.location
    }
}

impl Value {
    fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self> {
        let file_id: Option<FileId> = row.get(0)?;
        let file_offset: Option<u64> = row.get(1)?;
        let length = row.get(2)?;
        let last_used = row.get(3)?;
        let location = if length == 0 {
            assert_eq!(file_id, None);
            assert_eq!(file_offset, None);
            ZeroLength
        } else {
            Nonzero(NonzeroValueLocation {
                file_id: file_id.unwrap(),
                file_offset: file_offset.unwrap(),
                length,
            })
        };
        Ok(Value {
            location,
            last_used,
        })
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
    // This is Some if value is Nonzero.
    cloned_file: Option<Arc<Mutex<FileClone>>>,
}

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
            cloned_file: value
                .as_ref()
                .file_id()
                .map(|file_id| Arc::clone(self.file_clones.get(file_id).unwrap())),
            value,
        }
    }
}

impl<V> ReadAt for SnapshotValue<V>
where
    V: AsRef<Value>,
{
    fn read_at(&self, pos: u64, mut buf: &mut [u8]) -> io::Result<usize> {
        if true {
            // TODO: Create a thiserror or io::Error for non-usize pos.
            // let pos = usize::try_from(pos).expect("pos should be usize");
            let n = self.view(|view| {
                let r = view;
                r.read_at(pos, buf)
            })??;
            // dbg!(buf.split_at(n).0);
            Ok(n)
        } else {
            match self.value.as_ref().location {
                ValueLocation::ZeroLength => Ok(0),
                Nonzero(NonzeroValueLocation {
                    file_offset,
                    length,
                    ..
                }) => {
                    let available = length - pos;
                    #[allow(clippy::absurd_extreme_comparisons)]
                    if available <= 0 {
                        return Ok(0);
                    }
                    buf = buf
                        .split_at_mut(min(buf.len() as u64, available) as usize)
                        .0;
                    let mut file_clone = self.file_clone().unwrap().lock().unwrap();
                    let file = &mut file_clone.file;
                    dbg!(file.seek(Start(file_offset + pos))?);
                    dbg!(file.read(buf).map_err(Into::into))
                }
            }
        }
    }
}

impl<V> SnapshotValue<V>
where
    V: AsRef<Value>,
{
    fn file_clone(&self) -> Option<&Arc<Mutex<FileClone>>> {
        self.cloned_file.as_ref()
    }

    pub fn view<R>(&self, f: impl FnOnce(&[u8]) -> R) -> io::Result<R> {
        let value = self.value.as_ref();
        match value.location {
            Nonzero(NonzeroValueLocation {
                file_offset,
                length,
                ..
            }) => {
                let file_clone = self.file_clone().unwrap();
                let start = to_usize_io(file_offset)?;
                let usize_length = to_usize_io(length)?;
                let end =
                    usize::checked_add(start, usize_length).ok_or_else(make_to_usize_io_error)?;
                let mut mutex_guard = file_clone.lock().unwrap();
                let mmap = mutex_guard.get_mmap()?;
                Ok(f(&mmap[start..end]))
            }
            ZeroLength => Ok(f(&[])),
        }
    }

    pub fn read(&self, mut buf: &mut [u8]) -> Result<usize> {
        match self.value.as_ref().location {
            ValueLocation::ZeroLength => Ok(0),
            Nonzero(NonzeroValueLocation {
                file_offset,
                length,
                ..
            }) => {
                let _value = self.value.as_ref();
                buf = buf.split_at_mut(min(buf.len() as u64, length) as usize).0;
                let mut file_clone = self.file_clone().unwrap().lock().unwrap();
                let file = &mut file_clone.file;
                file.seek(Start(file_offset))?;
                file.read(buf).map_err(Into::into)
            }
        }
    }

    pub fn new_reader(&self) -> impl Read + '_ {
        positioned_io::Cursor::new(self)
    }

    /// For testing: Leak a reference to the snapshot tempdir so it's not cleaned up when all
    /// references are forgotten. This could possibly be used from internal tests instead.
    pub fn leak_snapshot_dir(&self) {
        if let Some(file_clone) = self.file_clone() {
            if let Some(tempdir) = &file_clone.lock().unwrap().tempdir {
                std::mem::forget(Arc::clone(tempdir));
            }
        }
    }
}

/// A value holding a statement temporary for starting an iterator over the Values of a value file.
/// I couldn't seem to get the code to work without this.
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

#[derive(Ord, PartialOrd, Eq, PartialEq)]
struct ReadExtent {
    pub offset: u64,
    pub len: u64,
}

// BTree possibly so we can merge extents in the future.
type Reads = HashMap<FileId, BTreeSet<ReadExtent>>;

pub struct Reader<'handle> {
    owned_tx: OwnedTx<'handle>,
    handle: &'handle Handle,
    reads: Reads,
}

impl<'a> Reader<'a> {
    pub fn add(&mut self, key: &[u8]) -> rusqlite::Result<Option<Value>> {
        let res = self.owned_tx.touch_for_read(key);
        match res {
            Ok(value) => {
                if let Nonzero(NonzeroValueLocation {
                    file_offset,
                    length,
                    file_id,
                }) = value.location.clone()
                {
                    let file = self.reads.entry(file_id);
                    file.or_default().insert(ReadExtent {
                        offset: file_offset,
                        len: length,
                    });
                }
                Ok(Some(value))
            }
            Err(QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Takes a snapshot and commits the read transaction.
    pub fn begin(self) -> Result<Snapshot> {
        let file_clones = self.clone_files().context("cloning files")?;
        self.owned_tx
            .commit(())
            .context("committing transaction")?
            .complete()?;
        Ok(Snapshot { file_clones })
    }

    fn clone_files(&self) -> Result<FileCloneCache> {
        let handle = self.handle;
        let reads = &self.reads;
        let mut tempdir = None;
        let mut file_clones: FileCloneCache = Default::default();
        // This isn't needed if file cloning is disabled...
        let mut handle_clone_guard = handle.clones.lock().unwrap();
        let handle_clones = handle_clone_guard.deref_mut();
        for (file_id, extents) in reads {
            file_clones.insert(
                file_id.clone(),
                self.get_file_clone(
                    file_id,
                    &mut tempdir,
                    handle_clones,
                    handle.dir.path(),
                    extents,
                )
                .context("getting file clone")?,
            );
        }
        Ok(file_clones)
    }

    pub fn list_items(&self, prefix: &[u8]) -> PubResult<Vec<Item>> {
        self.owned_tx.read().list_items(prefix)
    }

    fn get_file_clone(
        &self,
        file_id: &FileId,
        tempdir: &mut Option<Arc<TempDir>>,
        cache: &mut FileCloneCache,
        src_dir: &Path,
        read_extents: &BTreeSet<ReadExtent>,
    ) -> PubResult<Arc<Mutex<FileClone>>> {
        if let Some(ret) = cache.get(file_id) {
            let min_len = read_extents
                .iter()
                .map(|re| re.offset + re.len)
                .max()
                .unwrap();
            let file_clone_guard = ret.lock().unwrap();
            if file_clone_guard.len >= min_len {
                return Ok(ret.clone());
            }
        }
        if self.handle.dir_supports_file_cloning() {
            match self.clone_file(file_id, tempdir, cache, src_dir) {
                Err(err) if err.root_cause_is_unsupported_filesystem() => (),
                default => return default,
            }
        }
        self.get_file_for_read_by_segment_locking(file_id, read_extents)
    }

    fn clone_file(
        &self,
        file_id: &FileId,
        tempdir: &mut Option<Arc<TempDir>>,
        cache: &mut FileCloneCache,
        src_dir: &Path,
    ) -> PubResult<Arc<Mutex<FileClone>>> {
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
        let src_path = file_path(src_dir, file_id);
        // TODO: In order for value files to support truncation, a shared or exclusive lock would
        // need to be taken before cloning. I don't think this is possible, we would have to wait
        // for anyone holding an exclusive lock to release it. Handles already cache these, plus
        // Writers could hold them for a long time while writing. Then we need a separate cloning
        // lock. Also distinct Handles, even across processes can own each exclusive file.
        if false {
            let src_file = OpenOptions::new().read(true).open(&src_path)?;
            assert!(src_file.lock_max_segment(LockSharedNonblock)?);
        }
        let tempdir_path = tempdir.path();
        let dst_path = file_path(tempdir_path, file_id);
        clonefile(&src_path, &dst_path).context("cloning file")?;
        let mut file = open_file_id(OpenOptions::new().read(true), tempdir_path, file_id)
            .context("opening value file")?;
        // This prevents the snapshot file from being cleaned up. There's probably a race between
        // here and when it was cloned above. I wonder if the snapshot dir can be locked, or if we
        // can retry the cloning until we are able to lock it.
        let locked = file.lock_max_segment(LockSharedNonblock)?;
        assert!(locked);
        let len = file.seek(End(0))?;
        let file_clone = Arc::new(Mutex::new(FileClone {
            file,
            tempdir: Some(tempdir.clone()),
            mmap: None,
            len,
        }));

        cache.insert(file_id.to_owned(), file_clone.clone());
        Ok(file_clone)
    }

    fn get_file_for_read_by_segment_locking(
        &self,
        file_id: &FileId,
        read_extents: &BTreeSet<ReadExtent>,
    ) -> PubResult<Arc<Mutex<FileClone>>> {
        let mut file = open_file_id(OpenOptions::new().read(true), self.handle.dir(), file_id)?;
        for extent in read_extents {
            if !file.lock_segment(LockSharedNonblock, Some(extent.len), extent.offset)? {
                abort();
            }
        }
        let len = file.seek(std::io::SeekFrom::End(0))?;
        let file_clone = FileClone {
            file,
            tempdir: None,
            mmap: None,
            len,
        };
        // file_clone.get_mmap()?;
        Ok(Arc::new(Mutex::new(file_clone)))
    }
}

#[allow(dead_code)]
fn floored_multiple<T>(value: T, multiple: T) -> T
where
    T: Integer + Copy,
{
    // What about value - value % multiple?
    multiple * (value / multiple)
}

/// Divides value by multiple, always rounding up. Very common operation for allocators.
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
    unsafe { OsString::from_encoded_bytes_unchecked(begin) }
}

pub const MANIFEST_DB_FILE_NAME: &str = "manifest.db";

fn valid_file_name(file_name: &str) -> bool {
    if file_name.starts_with(MANIFEST_DB_FILE_NAME) {
        return false;
    }
    file_name.starts_with(VALUES_FILE_NAME_PREFIX)
}

struct PunchValueConstraints {
    greedy_start: bool,
    check_hole: bool,
    greedy_end: bool,
    allow_truncate: bool,
    allow_remove: bool,
}

impl Default for PunchValueConstraints {
    fn default() -> Self {
        Self {
            greedy_start: true,
            check_hole: true,
            greedy_end: true,
            allow_truncate: true,
            allow_remove: true,
        }
    }
}

struct PunchValueOptions<'a> {
    dir: &'a Path,
    file_id: &'a FileId,
    offset: u64,
    length: u64,
    tx: &'a ReadTransactionOwned<'a>,
    block_size: u64,
    constraints: PunchValueConstraints,
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
        constraints:
            PunchValueConstraints {
                greedy_start,
                check_hole: check_holes,
                allow_truncate,
                allow_remove,
                greedy_end,
            },
    } = opts;
    let cloning_lock_aware = false;
    // Make signed for easier arithmetic.
    let mut offset = offset as i64;
    let mut length = length as i64;
    let block_size = block_size as i64;
    let file_path = file_path(dir, file_id);
    // Punching values probably requires write permission.
    let mut file = match OpenOptions::new().write(true).open(&file_path) {
        // The file could have already been deleted by a previous punch.
        Err(err) if err.kind() == ErrorKind::NotFound && allow_remove => return Ok(()),
        Err(err) => return Err(err).context("opening value file"),
        Ok(ok) => ok,
    };
    // Find out how far back we can punch and start there, correcting for block boundaries as we go.
    if offset % block_size != 0 || greedy_start {
        let last_end_offset = tx.query_last_end_offset(file_id, offset as u64)?;
        // Round up the end of the last value.
        let new_offset = ceil_multiple(last_end_offset, block_size as u64) as i64;
        // Because these are u64 we can't deal with overflow into negatives.
        length += offset - new_offset;
        offset = new_offset;
    }
    assert_eq!(offset % block_size, 0);
    if greedy_end {
        let next_offset = tx.next_value_offset(file_id, (offset + length).try_into().unwrap())?;
        let end_offset = match next_offset {
            None => {
                // Lock the file to see if we can expand to the end of the file.
                let locked_file = file
                    .lock_max_segment(LockExclusiveNonblock)
                    .context("locking value file")?;
                // Get the file length after we have tried locking the file.
                let file_end = file.seek(End(0))? as i64;
                if locked_file {
                    // I think it's okay to remove and truncate files if cloning doesn't use locks,
                    // because there are no values in this file to clone.
                    if offset == 0 && allow_remove {
                        remove_file(file_path).context("removing value file")?;
                        return Ok(());
                    } else if allow_truncate {
                        file.set_len(offset as u64)?;
                        return Ok(());
                    }
                    file_end
                } else if cloning_lock_aware {
                    // Round the punch region down the the beginning of the last block. We aren't sure
                    // if someone is writing to the file.
                    floored_multiple(file_end, block_size)
                } else {
                    floored_multiple(offset + length, block_size)
                }
            }
            Some(next_offset) => floored_multiple(next_offset as i64, block_size),
        };
        let new_length = end_offset - offset;
        length = new_length;
    } else {
        let end_offset = floored_multiple(offset + length, block_size);
        length = end_offset - offset;
    }
    debug!(target: "punching", "punching {} {} for {}", file_id, offset, length);
    // If a punch is rounded up, and the end is rounded down, they cross each other by exactly a
    // full block.
    assert!(length >= -block_size);
    if length <= 0 {
        return Ok(());
    }
    assert_eq!(offset % block_size, 0);
    if !file.lock_segment(LockExclusiveNonblock, Some(length as u64), offset as u64)? {
        // TODO: If we can't delete immediately, we should schedule to try again later. Maybe
        // spinning up a thread, or putting in a slow queue.
        warn!(%file_id, %offset, %length, "can't punch, file segment locked");
        return Ok(());
    }
    debug!(?file, %offset, %length, "punching");
    punchfile(
        &file,
        offset.try_into().unwrap(),
        length.try_into().unwrap(),
    )
    .with_context(|| format!("length {}", length))?;
    // fcntl(file.as_raw_fd(), nix::fcntl::F_FULLFSYNC)?;
    // file.flush()?;
    if check_holes {
        if let Err(err) = check_hole(&mut file, offset as u64, length as u64) {
            warn!("checking hole: {}", err);
        }
    }
    Ok(())
}

/// Checks that there's no data allocated in the region provided.
pub fn check_hole(file: &mut File, offset: u64, length: u64) -> Result<()> {
    match seekhole::seek_hole_whence(file, offset, seekhole::RegionType::Data)? {
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
                match std::fs::OpenOptions::new().write(true).open(&entry.path) {
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) => {
                        return Err(err)
                            .with_context(|| format!("opening snapshot value {:?}", &entry.path))
                    }
                    Ok(file) => {
                        if file
                            .lock_max_segment(LockExclusiveNonblock)
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
