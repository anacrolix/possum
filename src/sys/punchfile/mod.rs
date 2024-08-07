//! File hole punching support

cfg_if! {
    if #[cfg(target_os = "linux")] {
        mod linux;
        pub use linux::*;
    } else if #[cfg(windows)] {
        mod windows;
        pub use self::windows::*;
    } else if #[cfg(target_os = "freebsd")] {
        mod freebsd;
        pub use freebsd::*;
    } else {
        // Looks like FreeBSD has fspacectl()
        mod macos;
        pub use macos::*;
    }
}

use super::*;

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use self::test;
    use super::*;
    use crate::sys::pathconf::fd_min_hole_size;

    #[test]
    #[allow(clippy::identity_op)]
    #[cfg(not(miri))]
    fn hole_punching() -> anyhow::Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        let file = temp_file.as_file_mut();
        file.set_sparse(true)?;
        let hole_alignment = fd_min_hole_size(file)?;
        file.set_len(2 * hole_alignment)?;
        punchfile(file, 0, 1 * hole_alignment)?;
        check_hole(file, 0, 1 * hole_alignment)?;
        Ok(())
    }
}
