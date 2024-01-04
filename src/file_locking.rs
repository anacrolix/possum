use std::fs::File;

use std::os::fd::AsRawFd;

use nix::errno::Errno;
use nix::fcntl::FlockArg;

const EWOULDBLOCK: Errno = Errno::EWOULDBLOCK;

pub(crate) fn try_lock_file_exclusive(file: &mut File) -> nix::Result<bool> {
    try_lock_file(file, LockExclusiveNonblock)
}

pub use nix::fcntl::FlockArg::*;

pub fn try_lock_file(file: &mut File, arg: FlockArg) -> nix::Result<bool> {
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

    use super::*;
    use crate::test;

    #[test]
    fn flock_behaviour() -> anyhow::Result<()> {
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
