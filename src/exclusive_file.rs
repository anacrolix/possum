use std::fs::{File, OpenOptions};
use std::io::Seek;
use std::io::SeekFrom::End;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use anyhow::anyhow;

use nix::errno::Errno;
use nix::fcntl::FlockArg::LockExclusiveNonblock;

const EWOULDBLOCK: Errno = Errno::EWOULDBLOCK;

use super::*;
use crate::FileId;

#[derive(Debug)]
pub(crate) struct ExclusiveFile {
    pub(crate) inner: File,
    pub(crate) id: FileId,
    pub(crate) next_write_offset: u64,
    last_committed_offset: u64,
}

// impl Deref for ExclusiveFile {
//     type Target = File;
//
//     fn deref(&self) -> &Self::Target {
//         &self.inner
//     }
// }

// impl DerefMut for ExclusiveFile {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.inner
//     }
// }

impl ExclusiveFile {
    pub(crate) fn open(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new().write(true).open(&path)?;
        Self::from_file(file, path.file_name().expect("file name").to_owned().into())
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
            if try_lock_file(&mut file)? {
                let end = file.seek(End(0))?;
                return Ok(ExclusiveFile {
                    inner: file,
                    id,
                    next_write_offset: end,
                    last_committed_offset: end,
                });
            }
        }
        bail!("gave up trying to create exclusive file")
    }

    pub(crate) fn from_file(mut file: File, id: FileId) -> anyhow::Result<ExclusiveFile> {
        if !try_lock_file(&mut file)? {
            bail!("file is locked");
        }
        let end = file.seek(End(0))?;
        Ok(ExclusiveFile {
            inner: file,
            id,
            next_write_offset: end,
            last_committed_offset: end,
        })
    }

    pub(crate) fn committed(&mut self) -> io::Result<()> {
        self.last_committed_offset = self.next_write_offset;
        if false {
            self.inner.flush()
        } else {
            Ok(())
        }
    }
}

impl Drop for ExclusiveFile {
    fn drop(&mut self) {
        // dbg!(self);
    }
}

fn try_lock_file(file: &mut File) -> nix::Result<bool> {
    match nix::fcntl::flock(file.as_raw_fd(), LockExclusiveNonblock) {
        Ok(()) => Ok(true),
        Err(errno) => {
            if errno == EWOULDBLOCK {
                Ok(false)
            } else {
                Err(errno)
            }
        }
    }
}
