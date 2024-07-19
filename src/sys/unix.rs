use std::fs::File;
use std::io;
use std::path::Path;

pub(crate) use nix::errno::errno;
use crate::sys::{DirMeta, FileSystemFlags};

use crate::env::{emulate_freebsd};

pub fn path_disk_allocation(path: &Path) -> std::io::Result<u64> {
    let metadata = std::fs::metadata(path)?;
    use std::os::unix::fs::MetadataExt;
    Ok(metadata.blocks() * 512)
}

struct UnixFilesystemFlags {}

impl FileSystemFlags for UnixFilesystemFlags {
    fn supports_sparse_files(&self) -> bool {
        // AFAIK, all unix systems support sparse files on all filesystems.
        true
    }

    fn supports_block_cloning(&self) -> Option<bool> {
        // AFAIK there's no way to check if a filesystem supports block cloning on non-Windows
        // platforms, and even then it depends on where you're copying to/from, sometimes even on
        // the same filesystem.
        if emulate_freebsd() {
            Some(false)
        } else {
            None
        }
    }
}

impl DirMeta for File {
    fn file_system_flags(&self) -> io::Result<impl FileSystemFlags> {
        Ok(UnixFilesystemFlags {})
    }
}
