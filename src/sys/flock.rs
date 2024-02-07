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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test;
    use tempfile::NamedTempFile;

    fn lock_entire_file_exclusive(file: &mut File) -> anyhow::Result<bool> {
        try_lock_file(file, LockExclusiveNonblock).map_err(Into::into)
    }

    #[test]
    fn flock_behaviour() -> anyhow::Result<()> {
        let mut file = tempfile::NamedTempFile::new()?;
        assert!(lock_entire_file_exclusive(file.as_file_mut())?);
        // Taking an existing lock for the same underlying file succeeds.
        assert!(lock_entire_file_exclusive(file.as_file_mut())?);
        let mut second_handle = OpenOptions::new().write(true).open(file.path())?;
        // You can't take the lock from another file instance.
        assert!(!lock_entire_file_exclusive(&mut second_handle)?);
        let mut file_dup = file.as_file().try_clone()?;
        assert!(!lock_entire_file_exclusive(&mut second_handle)?);
        // You can take the existing lock from a file descriptor to the same file.
        assert!(lock_entire_file_exclusive(&mut file_dup)?);
        drop(file);
        assert!(!lock_entire_file_exclusive(&mut second_handle)?);
        // Still holding the lock because the original file still exist.
        assert!(lock_entire_file_exclusive(&mut file_dup)?);
        drop(file_dup);
        assert!(lock_entire_file_exclusive(&mut second_handle)?);
        assert!(lock_entire_file_exclusive(&mut second_handle)?);
        Ok(())
    }

    #[test]
    fn open_locked_file() -> anyhow::Result<()> {
        let file1_named = NamedTempFile::new()?;
        let file1_ref = file1_named.as_file();
        assert!(lock_file_segment(
            file1_ref,
            LockExclusiveNonblock,
            None,
            Start(0)
        )?);
        assert!(lock_file_segment(file1_ref, Unlock, Some(4096), Start(0))?);
        let file_reader = OpenOptions::new().read(true).open(file1_named.path())?;
        assert!(lock_file_segment(
            &file_reader,
            LockSharedNonblock,
            Some(4096),
            Start(0)
        )?);
        Ok(())
    }

    #[test]
    fn segment_locking() -> anyhow::Result<()> {
        let file1_named = NamedTempFile::new()?;
        let file1_ref = file1_named.as_file();
        assert!(lock_file_segment(
            file1_ref,
            LockExclusiveNonblock,
            None,
            Start(0)
        )?);
        let file1_reopen = file1_named.reopen()?;
        assert!(!lock_file_segment(
            &file1_reopen,
            LockExclusiveNonblock,
            None,
            Start(0)
        )?);
        assert!(!lock_file_segment(
            &file1_reopen,
            LockExclusiveNonblock,
            Some(69),
            Start(42)
        )?);
        assert!(!lock_file_segment(
            &file1_reopen,
            LockExclusiveNonblock,
            None,
            Start(42)
        )?);
        // Reentrant with overlapping segments
        assert!(lock_file_segment(
            file1_ref,
            LockExclusiveNonblock,
            None,
            Start(42)
        )?);
        // Reentrant with overlapping segments
        assert!(lock_file_segment(
            file1_ref,
            LockExclusiveNonblock,
            Some(69),
            Start(42)
        )?);
        // Can take shared locks through an exclusive lock for the same file.
        assert!(lock_file_segment(
            file1_ref,
            LockSharedNonblock,
            Some(69),
            Start(42)
        )?);
        // Can take a second exclusive lock over a shared lock for the same file.
        assert!(lock_file_segment(
            file1_ref,
            LockExclusiveNonblock,
            Some(69),
            Start(42)
        )?);
        assert!(lock_file_segment(file1_ref, Unlock, Some(5), Start(0))?);
        assert!(lock_file_segment(
            &file1_reopen,
            LockExclusiveNonblock,
            Some(5),
            Start(0)
        )?);
        assert!(!lock_file_segment(
            &file1_reopen,
            LockExclusiveNonblock,
            None,
            Start(0)
        )?);
        assert!(!lock_file_segment(
            &file1_reopen,
            LockExclusiveNonblock,
            None,
            Start(5)
        )?);
        assert!(!lock_file_segment(
            file1_ref,
            LockSharedNonblock,
            Some(5),
            Start(0)
        )?);
        assert!(lock_file_segment(
            &file1_reopen,
            LockSharedNonblock,
            Some(5),
            Start(0)
        )?);
        drop(file1_reopen);
        assert!(lock_file_segment(
            file1_ref,
            LockSharedNonblock,
            Some(5),
            Start(0)
        )?);
        Ok(())
    }
}
