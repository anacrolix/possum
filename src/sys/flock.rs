use super::*;

cfg_if! {
    if #[cfg(unix)] {
        mod unix;
        pub use self::unix::*;
    } else if #[cfg(windows)] {
        mod windows;
        pub use self::windows::*;
    }
}

use std::fs::File;

pub(crate) fn try_lock_file_exclusive(file: &mut File) -> anyhow::Result<bool> {
    try_lock_file(file, LockExclusiveNonblock).map_err(Into::into)
}

#[cfg(test)]
mod tests {
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
        let mut file_dup = file.as_file().try_clone()?;
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
