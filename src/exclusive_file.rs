use anyhow::Context;
use log::info;
use nix::fcntl::FlockArg::LockExclusiveNonblock;
use std::fs;
use std::fs::File;
use std::io::Seek;
use std::io::SeekFrom::End;
use std::os::fd::AsRawFd;
use std::path::Path;

pub(crate) struct ExclusiveFile {
    pub(crate) inner: File,
    pub(crate) id: u64,
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
    pub(crate) fn new(dir: impl AsRef<Path>) -> anyhow::Result<ExclusiveFile> {
        let mut last_err = None;
        for i in 0..10000 {
            let file = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(dir.as_ref().join(&i.to_string()));
            let mut file = match file {
                Ok(file) => file,
                Err(err) => return Err(err.into()),
            };
            if let Err(err) = nix::fcntl::flock(file.as_raw_fd(), LockExclusiveNonblock) {
                last_err = Some(err)
            } else {
                info!("opened with exclusive file id {}", i);
                let end = file.seek(End(0))?;
                return Ok(ExclusiveFile {
                    inner: file,
                    id: i,
                    next_write_offset: end,
                    last_committed_offset: end,
                });
            }
        }
        Err(last_err.unwrap()).context("gave up trying to create exclusive file")
    }

    pub(crate) fn committed(&mut self) {
        self.last_committed_offset = self.next_write_offset;
    }
}
