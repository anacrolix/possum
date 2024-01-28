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
    pub(crate) fn open(path: PathBuf) -> Result<Option<Self>> {
        let file = OpenOptions::new().append(true).open(&path)?;
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
        if !lock_file_segment(&mut file, LockExclusiveNonblock, None, Start(end))? {
            return Ok(None);
        }
        Ok(Some(ExclusiveFile {
            inner: file,
            id,
            last_committed_offset: end,
        }))
    }

    pub(crate) fn committed(&mut self) -> io::Result<()> {
        let new_committed_offset = self.inner.stream_position()?;
        // Unlocking should never block, at least according to the change in FlockArg in nix.
        assert!(lock_file_segment(
            &self.inner,
            Unlock,
            Some((new_committed_offset - self.last_committed_offset) as i64),
            Start(self.last_committed_offset),
        )?);
        self.last_committed_offset = new_committed_offset;
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
