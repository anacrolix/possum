use crate::punchfile::punchfile;
use anyhow::Result;
use anyhow::{bail, Context};
use clonefile::clonefile;
use log::debug;
use memmap2::Mmap;
use num::Integer;
use positioned_io::ReadAt;
use rand::Rng;
use rusqlite::types::FromSqlError::InvalidType;
use rusqlite::types::ValueRef::{Null, Real};
use rusqlite::types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef};
use rusqlite::Error::QueryReturnedNoRows;
use rusqlite::{params, Connection, ToSql, Transaction};
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fmt::{Debug, Display, Formatter};
use std::fs::{read_dir, File, OpenOptions};
use std::io::SeekFrom::{End, Start};
use std::io::{ErrorKind, Read, Seek, Write};
use std::ops::DerefMut;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::{fs, io};
use tempfile::{tempdir_in, TempDir};

pub mod clonefile;
pub mod punchfile;

mod exclusive_file;
pub mod testing;

use crate::clonefile::fclonefile;
use exclusive_file::ExclusiveFile;

#[derive(Debug)]
struct FileClone {
    file: File,
    #[allow(dead_code)]
    tempdir: Arc<TempDir>,
    mmap: Option<Mmap>,
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
            value_length: exclusive_file.next_write_offset,
            exclusive_file,
            value_file_offset: 0,
        })
    }

    pub fn begin(self) -> Result<ValueWriter> {
        let exclusive_file = self.batch.get_exclusive_file()?;
        Ok(ValueWriter {
            value_file_offset: exclusive_file.next_write_offset,
            value_length: 0,
            exclusive_file,
        })
    }
}

#[derive(Debug)]
pub struct ValueWriter {
    exclusive_file: ExclusiveFile,
    value_file_offset: u64,
    value_length: u64,
}

impl ValueWriter {
    pub fn copy_from(&mut self, mut value: impl Read) -> Result<u64> {
        let value_file_offset = self.exclusive_file.next_write_offset;
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
        self.value_length += value_length;
        self.exclusive_file.next_write_offset += value_length;
        Ok(value_length)
    }
}

impl Write for ValueWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.exclusive_file.inner.write(buf)?;
        self.exclusive_file.next_write_offset += n as u64;
        self.value_length += n as u64;
        Ok(n)
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

impl<'handle> BatchWriter<'handle> {
    fn get_exclusive_file(&mut self) -> Result<ExclusiveFile> {
        if let Some(ef) = self.exclusive_files.pop() {
            return Ok(ef);
        }
        self.handle.get_exclusive_file()
    }

    pub fn stage_write(&mut self, key: Vec<u8>, value: ValueWriter) -> anyhow::Result<()> {
        self.pending_writes.push(PendingWrite {
            key,
            value_file_offset: value.value_file_offset,
            value_length: value.value_length,
            value_file_id: value.exclusive_file.id.clone(),
        });
        self.exclusive_files.push(value.exclusive_file);
        Ok(())
    }

    pub fn new_value<'writer>(&'writer mut self) -> BeginWriteValue<'writer, 'handle> {
        BeginWriteValue { batch: self }
    }

    pub fn commit(mut self) -> Result<()> {
        let mut tx_guard = self.handle.conn.lock().unwrap();
        let mut transaction = tx_guard
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .context("begin immediate")?;
        let mut altered_files = HashSet::new();
        for pw in self.pending_writes.drain(..) {
            let existing = transaction.query_row(
                "delete from keys where key=? returning file_id, file_offset, value_length",
                [&pw.key],
                |row| {
                    let file_id: FileId = row.get(0)?;
                    Ok((file_id, row.get(1)?, row.get(2)?))
                },
            );
            match existing {
                Ok((file_id, file_offset, value_length)) => {
                    let msg = format!(
                        "deleting value at {:?} {} {}",
                        file_id, file_offset, value_length
                    );
                    debug!("{}", msg);
                    punch_value(
                        &self.handle.dir,
                        &file_id,
                        file_offset,
                        value_length,
                        &mut transaction,
                        Handle::block_size(),
                    )
                    .context(msg)?;
                    if value_length != 0 {
                        altered_files.insert(file_id);
                    }
                }
                Err(QueryReturnedNoRows) => (),
                Err(err) => return Err(err.into()),
            }
            transaction.execute(
                "insert into keys (key, file_id, file_offset, value_length)\
                values (?, ?, ?, ?)",
                rusqlite::params!(
                    pw.key,
                    pw.value_file_id.as_str(),
                    pw.value_file_offset,
                    pw.value_length
                ),
            )?;
            if pw.value_length != 0 {
                altered_files.insert(pw.value_file_id);
            }
        }
        transaction.commit().context("commit transaction")?;
        {
            let mut handle_exclusive_files = self.handle.exclusive_files.lock().unwrap();
            // dbg!(
            //     "adding {} exclusive files to handle",
            //     self.exclusive_files.len()
            // );
            for mut ef in self.exclusive_files.drain(..) {
                ef.committed().unwrap();
                assert!(handle_exclusive_files.insert(ef.id.clone(), ef).is_none());
            }
            // dbg!(
            //     "handle has {} exclusive files",
            //     handle_exclusive_files.len()
            // );
        }
        // Forget any references to clones of files that have changed.
        for file_id in altered_files {
            self.handle.clones.lock().unwrap().remove(&file_id);
        }
        Ok(())
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

pub struct Handle {
    conn: Mutex<Connection>,
    exclusive_files: Mutex<HashMap<FileId, ExclusiveFile>>,
    dir: PathBuf,
    // Cache clones until we know the original files have changed. This means new snapshots can
    // reuse clones. TODO: If other processes modify the database, our clones will
    // be out of date, so there needs to be another check involved.
    clones: Mutex<FileCloneCache>,
}

impl Handle {
    fn get_exclusive_file(&self) -> Result<ExclusiveFile> {
        let mut files = self.exclusive_files.lock().unwrap();
        if let Some((_, file)) = files.drain().next() {
            return Ok(file);
        }
        if let Some(file) = self.open_existing_exclusive_file()? {
            return Ok(file);
        }
        ExclusiveFile::new(&self.dir)
    }

    fn open_existing_exclusive_file(&self) -> Result<Option<ExclusiveFile>> {
        for res in read_dir(&self.dir)? {
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

    pub fn new_from_dir(dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&dir)?;
        let sqlite_version = rusqlite::version_number();
        if sqlite_version < 3042000 {
            bail!(
                "sqlite version {} below minimum {}",
                rusqlite::version(),
                "3.42"
            );
        }
        let conn = Connection::open(dir.join(MANIFEST_DB_FILE_NAME))?;
        conn.pragma_update(None, "journal_mode", "wal")?;
        conn.pragma_update(None, "synchronous", "off")?;
        if false {
            conn.pragma_update(None, "locking_mode", "exclusive")
                .context("set conn locking mode exclusive")?;
        }
        init_manifest_schema(&conn).context("initing manifest schema")?;
        let handle = Self {
            conn: Mutex::new(conn),
            exclusive_files: Default::default(),
            dir,
            clones: Default::default(),
        };
        Ok(handle)
    }

    fn block_size() -> u64 {
        4096
    }

    pub fn new_writer(&self) -> Result<BatchWriter> {
        Ok(BatchWriter {
            handle: self,
            exclusive_files: Default::default(),
            pending_writes: Default::default(),
        })
    }

    pub fn read(&self) -> rusqlite::Result<Reader> {
        let mut guard = self.conn.lock().unwrap();
        let tx = unsafe { std::mem::transmute(guard.transaction()?) };
        let reader = Reader {
            _guard: guard,
            tx,
            handle: self,
            files: Default::default(),
        };
        Ok(reader)
    }

    pub fn read_single(&self, key: Vec<u8>) -> Result<Option<SnapshotValue<Value, Snapshot>>> {
        let mut reader = self.read()?;
        let Some(value) = reader.add(&key)? else {
            return Ok(None);
        };
        let snapshot = reader.begin()?;
        Ok(Some(snapshot.with_value(value)))
    }

    pub fn single_write_from(&self, key: Vec<u8>, r: impl Read) -> Result<u64> {
        let mut writer = self.new_writer()?;
        let mut value = writer.new_value().begin()?;
        let n = value.copy_from(r)?;
        writer.stage_write(key, value)?;
        writer.commit()?;
        Ok(n)
    }

    pub fn clone_from_fd(&mut self, key: Vec<u8>, fd: RawFd) -> Result<u64> {
        let mut writer = self.new_writer()?;
        let value = writer.new_value().clone_fd(fd, 0)?;
        let n = value.value_length;
        writer.stage_write(key, value)?;
        writer.commit()?;
        Ok(n)
    }
}

#[derive(Debug)]
pub struct Value {
    file_id: FileId,
    file_offset: u64,
    length: u64,
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
}

#[derive(Debug)]
pub struct Snapshot {
    file_clones: HashMap<FileId, Arc<Mutex<FileClone>>>,
}

pub trait ReadSnapshot {
    fn view(&mut self, f: impl FnOnce(&[u8])) -> Result<()>;
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;
}

#[derive(Debug)]
pub struct SnapshotValue<V, S>
where
    V: AsRef<Value>,
    S: AsRef<Snapshot>,
{
    value: V,
    snapshot: S,
}

impl Snapshot {
    pub fn value<V>(self: &mut Self, value: V) -> SnapshotValue<V, &mut Snapshot>
    where
        V: AsRef<Value>,
    {
        SnapshotValue {
            value,
            snapshot: self,
        }
    }

    pub fn with_value<V>(self: Self, value: V) -> SnapshotValue<V, Self>
    where
        V: AsRef<Value>,
    {
        SnapshotValue {
            value,
            snapshot: self,
        }
    }
}

pub struct SnapshotWithValue<V, S>
where
    V: AsRef<Value>,
    S: AsRef<Snapshot>,
{
    snapshot: Snapshot,
    value: SnapshotValue<V, S>,
}

impl<V, S> ReadAt for SnapshotValue<V, S>
where
    V: AsRef<Value>,
    S: AsRef<Snapshot>,
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

impl<V, S> SnapshotValue<V, S>
where
    V: AsRef<Value>,
    S: AsRef<Snapshot>,
{
    pub fn view<R>(&self, f: impl FnOnce(&[u8]) -> R) -> io::Result<R> {
        let value = self.value.as_ref();
        let file_id = &value.file_id;
        let file_clone = self.snapshot.as_ref().file_clones.get(&file_id).unwrap();
        let start = value
            .file_offset
            .try_into()
            .expect("file offset should be usize");
        let end = start + usize::try_from(value.length).expect("length should be usize");
        let mut mutex_guard = file_clone.lock().unwrap();
        let mmap = mutex_guard.get_mmap()?;
        Ok(f(&mmap[start..end]))
    }

    pub fn read(&self, mut buf: &mut [u8]) -> Result<usize> {
        let value = self.value.as_ref();
        buf = buf
            .split_at_mut(min(buf.len() as u64, value.length) as usize)
            .0;
        let mut file_clone = self
            .snapshot
            .as_ref()
            .file_clones
            .get(&value.file_id)
            .unwrap()
            .lock()
            .unwrap();
        let file = &mut file_clone.file;
        file.seek(Start(value.file_offset))?;
        file.read(buf).map_err(Into::into)
    }

    pub fn new_reader(&self) -> impl Read + '_ {
        positioned_io::Cursor::new(self)
    }
}

pub struct Reader<'handle> {
    _guard: MutexGuard<'handle, Connection>,
    tx: Transaction<'handle>,
    handle: &'handle Handle,
    files: HashSet<FileId>,
}

impl<'a> Reader<'a> {
    pub fn add(&mut self, key: &[u8]) -> rusqlite::Result<Option<Value>> {
        let res = self.tx.query_row(
            "update keys \
            set last_used=cast(unixepoch('subsec')*1e3 as integer) \
            where key=? \
            returning file_id, file_offset, value_length",
            [key],
            |row| {
                let file_id: FileId = row.get(0)?;
                Ok((file_id, row.get(1)?, row.get(2)?))
            },
        );
        match res {
            Ok((file_id, file_offset, value_length)) => {
                self.files.insert(file_id.clone());
                Ok(Some(Value {
                    file_id,
                    file_offset,
                    length: value_length,
                }))
            }
            Err(QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn begin(self) -> Result<Snapshot> {
        let mut tempdir = None;
        let mut file_clones: FileCloneCache = Default::default();
        let mut handle_clone_guard = self.handle.clones.lock().unwrap();
        let handle_clones = handle_clone_guard.deref_mut();
        for file_id in self.files {
            file_clones.insert(
                file_id.clone(),
                Self::get_file_clone(file_id, &mut tempdir, handle_clones, &self.handle.dir)?,
            );
        }
        self.tx.commit()?;
        Ok(Snapshot { file_clones })
    }

    fn get_file_clone(
        file_id: FileId,
        tempdir: &mut Option<Arc<TempDir>>,
        cache: &mut FileCloneCache,
        src_dir: &Path,
    ) -> Result<Arc<Mutex<FileClone>>> {
        if let Some(ret) = cache.get(&file_id) {
            return Ok(ret.clone());
        }
        let tempdir: &Arc<TempDir> = match tempdir {
            Some(tempdir) => tempdir,
            None => {
                let new = Arc::new(tempdir_in(src_dir)?);
                *tempdir = Some(new);
                tempdir.as_ref().unwrap()
            }
        };
        let tempdir_path = tempdir.path();
        clonefile(
            &file_path(src_dir, &file_id),
            &file_path(tempdir_path, &file_id),
        )?;
        let file_clone = Arc::new(Mutex::new(FileClone {
            file: open_file_id(OpenOptions::new().read(true), tempdir_path, &file_id)?,
            tempdir: tempdir.clone(),
            mmap: None,
        }));
        assert!(cache.insert(file_id, file_clone.clone()).is_none());
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

fn random_file_name() -> OsString {
    let mut begin = VALUES_FILE_NAME_PREFIX.as_bytes().to_vec();
    begin.extend(
        rand::thread_rng()
            .sample_iter(rand::distributions::Alphanumeric)
            .take(FILE_NAME_RAND_LENGTH),
    );
    OsString::from_vec(begin)
}

const MANIFEST_DB_FILE_NAME: &str = "manifest.db";

fn valid_file_name(file_name: &str) -> bool {
    if file_name.starts_with(MANIFEST_DB_FILE_NAME) {
        return false;
    }
    file_name.starts_with(VALUES_FILE_NAME_PREFIX)
}

#[derive(Clone, Eq, PartialEq, Hash)]
struct FileId(OsString);

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
            Null | Real(..) => Err(InvalidType),
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

// Can't do this as &mut self for dumb Rust reasons.
fn punch_value(
    dir: &Path,
    file_id: &FileId,
    mut offset: u64,
    mut length: u64,
    tx: &mut Transaction,
    block_size: u64,
) -> anyhow::Result<()> {
    // If we're not at a block boundary to begin with, find out how far back we can punch and
    // start there.
    if offset % block_size != 0 {
        let new_offset = ceil_multiple(query_last_end_offset(tx, file_id, offset)?, block_size);
        length += offset - new_offset;
        offset = new_offset;
    }
    let mut file = open_file_id(OpenOptions::new().append(true), dir, file_id)?;
    // append doesn't mean our file position is at the end to begin with.
    let file_end = file.seek(End(0))?;
    let end_offset = offset + length;
    // Does this handle ongoing writes to the same file?
    if end_offset < file_end {
        // What if this is below the offset or negative?
        length -= end_offset % block_size;
    }
    debug!("punching {} at {} for {}", file_id, offset, length);
    punchfile(file, offset.try_into()?, length.try_into()?)
        .with_context(|| format!("length {}", length))?;
    Ok(())
}

// Returns the end offset of the last active value before offset in the same file.
fn query_last_end_offset(
    tx: &mut Transaction,
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
    .or_else(|err| {
        if matches!(err, rusqlite::Error::QueryReturnedNoRows) {
            Ok(0)
        } else {
            Err(err)
        }
    })
}
