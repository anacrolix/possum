//! Syscall wrappers for hole punching, system configuration, hole-seeking ( ͡° ͜ʖ ͡°), file cloning
//! etc.

cfg_if! {
    if #[cfg(target_os = "linux")] {
        mod linux;
        pub use linux::*;
    } else if #[cfg(windows)] {
        mod windows;
        pub use self::windows::*;
    } else {
        mod macos;
        pub use macos::*;
    }
}

use super::*;

#[cfg(test)]
mod tests {
    use self::test;
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn hole_punching() -> anyhow::Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        let file = temp_file.as_file_mut();
        file.set_sparse(true)?;
        file.set_len(2)?;
        punchfile(file, 0, 1)?;
        check_hole(file, 0, 1)?;
        Ok(())
    }
}
