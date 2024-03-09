use std::fs::{File, OpenOptions};
use std::io::Seek;
use std::io::SeekFrom::End;
use std::path::{Path, PathBuf};

use super::*;
use crate::FileId;

#[derive(Debug)]
enum LockLevel {
    // On systems that don't implement flock, downgrades never occur.
    #[allow(dead_code)]
    Shared,
    Exclusive,
}
use LockLevel::*;

#[derive(Debug)]
pub(crate) struct ExclusiveFile {
    pub(crate) inner: File,
    pub(crate) id: FileId,
    last_committed_offset: u64,
    lock_level: LockLevel,
}

impl ExclusiveFile {
    pub(crate) fn open(path: PathBuf) -> Result<Option<Self>> {
        let file = Self::new_open_options().open(&path)?;
        Self::from_file(file, path.file_name().context("file name")?.try_into()?)
    }

    fn new_open_options() -> OpenOptions {
        let mut ret = OpenOptions::new();
        ret.append(true);
        // On Windows, we require GENERIC_READ or GENERIC_WRITE
        // https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex to
        // create exclusive file locks. But .append(true) strips FILE_WRITE_DATA which makes us not
        // FILE_GENERIC_WRITE
        // https://learn.microsoft.com/en-us/windows/win32/fileio/file-security-and-access-rights. I
        // think it's more important to ensure we're only appending than it is to prevent reads.

        #[cfg(windows)]
        ret.read(true);
        ret
    }

    pub(crate) fn revert_to_offset(&mut self, offset: u64) -> io::Result<()> {
        // Shouldn't revert prior to the last commit.
        assert!(offset >= self.last_committed_offset);
        self.inner.seek(Start(offset))?;
        self.inner.set_len(offset)
    }

    pub(crate) fn new(dir: impl AsRef<Path>) -> anyhow::Result<ExclusiveFile> {
        for _ in 0..10 {
            let id = FileId::random();
            let path = dir.as_ref().join(id.values_file_path());
            debug!(?path, "opening new exclusive file");
            let file = Self::new_open_options().create(true).open(path);
            let file = match file {
                Ok(file) => file,
                Err(err) => return Err(err.into()),
            };
            if let Some(exclusive_file) = Self::from_file(file, id)? {
                return Ok(exclusive_file);
            }
        }
        bail!("gave up trying to create exclusive file")
    }

    pub(crate) fn from_file(mut file: File, id: FileId) -> anyhow::Result<Option<ExclusiveFile>> {
        let end = file.seek(End(0))?;
        if !file.lock_segment(LockExclusiveNonblock, None, end)? {
            return Ok(None);
        }
        file.set_sparse(true)?;
        Ok(Some(ExclusiveFile {
            inner: file,
            id,
            last_committed_offset: end,
            lock_level: Exclusive,
        }))
    }

    pub(crate) fn committed(&mut self) -> io::Result<bool> {
        let new_committed_offset = self.inner.stream_position()?;
        // Remove the exclusive lock on the part we just committed.
        if !self
            .inner
            .trim_exclusive_lock_left(self.last_committed_offset, new_committed_offset)?
        {
            return Ok(false);
        }
        self.last_committed_offset = new_committed_offset;
        Ok(true)
    }

    /// The exclusive file offset that writing should occur at. Maybe it shouldn't need to be
    /// mutable since it shouldn't actually shift the file position, however it may decide to cache
    /// it in the future.
    pub(crate) fn next_write_offset(&mut self) -> io::Result<u64> {
        self.inner.stream_position()
    }

    pub(crate) fn downgrade_lock(&mut self) -> io::Result<bool> {
        assert!(flocking());
        assert!(matches!(self.lock_level, Exclusive));
        cfg_if! {
            if #[cfg(unix)] {
                if !self.inner.flock(LockSharedNonblock)? {
                    return Ok(false);
                }
                self.lock_level = Shared;
                Ok(true)
            } else {
                unimplemented!()
            }
        }
    }

    pub fn valid_file_name(file_name: &str) -> bool {
        (|| {
            file_name
                .strip_prefix(VALUES_FILE_NAME_PREFIX)?
                .parse()
                .ok()
        })()
        .unwrap_or(false)
    }
}

impl Drop for ExclusiveFile {
    fn drop(&mut self) {
        debug!("dropping exclusive file {}", self.id.deref());
    }
}
