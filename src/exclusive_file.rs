use std::fs::{File, OpenOptions};
use std::io::Seek;
use std::io::SeekFrom::End;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use nix::errno::Errno;
use nix::fcntl::FlockArg;

const EWOULDBLOCK: Errno = Errno::EWOULDBLOCK;

use super::*;
use crate::FileId;

#[derive(Debug)]
pub(crate) struct ExclusiveFile {
    pub(crate) inner: File,
    pub(crate) id: FileId,
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
        debug!("dropping exclusive file {}", self.id);
    }
}

fn try_lock_file_exclusive(file: &mut File) -> nix::Result<bool> {
    try_lock_file(file, LockExclusiveNonblock)
}

pub(crate) use nix::fcntl::FlockArg::*;

pub(crate) fn try_lock_file(file: &mut File, arg: FlockArg) -> nix::Result<bool> {
    let flock_res = nix::fcntl::flock(file.as_raw_fd(), arg);
    match flock_res {
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

#[cfg(test)]
mod tests {
    use std::os::fd::FromRawFd;

    use self::test;
    use super::*;

    #[test]
    fn flock_behaviour() -> Result<()> {
        let mut file = tempfile::NamedTempFile::new()?;
        assert!(try_lock_file_exclusive(file.as_file_mut())?);
        // Taking an existing lock for the same underlying file succeeds.
        assert!(try_lock_file_exclusive(file.as_file_mut())?);
        let mut second_handle = File::open(file.path())?;
        // You can't take the lock from another file instance.
        assert!(!try_lock_file_exclusive(&mut second_handle)?);
        let mut file_dup = unsafe { File::from_raw_fd(libc::dup(file.as_raw_fd())) };
        assert!(!try_lock_file_exclusive(&mut second_handle)?);
        // You can take the existing lock from a file descriptor to the same file.
        assert!(try_lock_file_exclusive(&mut file_dup)?);
        drop(file);
        assert!(!try_lock_file_exclusive(&mut second_handle)?);
        // Still holding the lock because the original file still exist.
        assert!(try_lock_file_exclusive(&mut file_dup)?);
        drop(file_dup);
        assert!(try_lock_file_exclusive(&mut second_handle)?);
        assert!(try_lock_file_exclusive(&mut second_handle)?);
        Ok(())
    }
}
