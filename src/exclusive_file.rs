use std::fs::{File, OpenOptions};
use std::io::Seek;
use std::io::SeekFrom::End;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use anyhow::Context;
use log::info;
use nix::fcntl::FlockArg::LockExclusiveNonblock;

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
        let mut last_err = None;
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
            if let Err(err) = nix::fcntl::flock(file.as_raw_fd(), LockExclusiveNonblock) {
                last_err = Some(err)
            } else {
                info!("opened with exclusive file id {}", id);
                let end = file.seek(End(0))?;
                return Ok(ExclusiveFile {
                    inner: file,
                    id,
                    next_write_offset: end,
                    last_committed_offset: end,
                });
            }
        }
        Err(last_err.unwrap()).context("gave up trying to create exclusive file")
    }

    pub(crate) fn from_file(mut file: File, id: FileId) -> anyhow::Result<ExclusiveFile> {
        nix::fcntl::flock(file.as_raw_fd(), LockExclusiveNonblock)?;
        info!("opened with exclusive file id {:?}", id);
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
