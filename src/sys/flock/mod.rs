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
}

pub fn try_lock_file(file: &mut File, arg: FlockArg) -> anyhow::Result<bool> {
    file.lock_segment(arg, None, 0).map_err(Into::into)
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
