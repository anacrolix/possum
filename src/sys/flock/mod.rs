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

pub trait FileLocking {
    /// Returns Ok(false) if the lock could not be moved (you should assume the file is busted at
    /// this point).
    fn trim_exclusive_lock_left(&self, old_left: u64, new_left: u64) -> io::Result<bool>;
    fn lock_segment(&self, arg: FlockArg, len: Option<u64>, offset: u64) -> io::Result<bool>;
    /// Locks a segment that spans the maximum possible range of offsets.
    fn lock_max_segment(&self, arg: FlockArg) -> io::Result<bool> {
        self.lock_segment(arg, None, 0)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;
    use crate::test;

    #[test]
    fn open_locked_file() -> anyhow::Result<()> {
        let file1_named = NamedTempFile::new()?;
        let file1_ref = file1_named.as_file();
        let file_reopen = file1_named.reopen()?;
        assert!(file1_ref.lock_segment(LockExclusiveNonblock, None, 1)?);
        // Trying to exclusively lock from another file handle fails immediately.
        assert!(!file_reopen.lock_segment(LockExclusiveNonblock, None, 2)?);
        let file_reader = OpenOptions::new().read(true).open(file1_named.path())?;
        assert!(file_reader.lock_segment(LockSharedNonblock, Some(1), 0,)?);
        Ok(())
    }
}
