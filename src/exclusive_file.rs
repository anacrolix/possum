use std::fs::{File, OpenOptions};
use std::io::Seek;
use std::io::SeekFrom::End;
use std::path::{Path, PathBuf};

use super::*;
use crate::FileId;

#[derive(Debug)]
pub(crate) struct ExclusiveFile {
    pub(crate) inner: File,
    pub(crate) id: FileId,
    last_committed_offset: u64,
}

impl ExclusiveFile {
    pub(crate) fn open(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new().write(true).open(&path)?;
        Self::from_file(file, path.file_name().expect("file name").to_owned().into())
    }

    pub(crate) fn revert_to_offset(&mut self, offset: u64) -> io::Result<()> {
        // Shouldn't revert prior to the last commit.
        assert!(offset >= self.last_committed_offset);
        self.inner.seek(Start(offset))?;
        self.inner.set_len(offset)
    }

    pub(crate) fn new(dir: impl AsRef<Path>) -> anyhow::Result<ExclusiveFile> {
        for _ in 0..10000 {
            let id = random_file_name().into();
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(dir.as_ref().join(&id));
            let mut file = match file {
                Ok(file) => file,
                Err(err) => return Err(err.into()),
            };
            if try_lock_file_exclusive(&mut file)? {
                return Self::from_file(file, id);
            }
        }
        bail!("gave up trying to create exclusive file")
    }

    pub(crate) fn from_file(mut file: File, id: FileId) -> anyhow::Result<ExclusiveFile> {
        if !try_lock_file_exclusive(&mut file)? {
            bail!("file is locked");
        }
        let end = file.seek(End(0))?;
        Ok(ExclusiveFile {
            inner: file,
            id,
            last_committed_offset: end,
        })
    }

    pub(crate) fn committed(&mut self) -> io::Result<()> {
        self.last_committed_offset = self.inner.stream_position()?;
        if false {
            self.inner.flush()
        } else {
            Ok(())
        }
    }

    /// The exclusive file offset that writing should occur at. Maybe it shouldn't need to be
    /// mutable since it shouldn't actually shift the file position, however it may decide to cache
    /// it in the future.
    pub(crate) fn next_write_offset(&mut self) -> io::Result<u64> {
        self.inner.stream_position()
    }
}

impl Drop for ExclusiveFile {
    fn drop(&mut self) {
        debug!("dropping exclusive file {}", self.id.deref());
    }
}
