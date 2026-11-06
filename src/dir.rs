use std::borrow::Borrow;

use super::*;

#[derive(Clone, Debug)]
pub struct Dir {
    path_buf: PathBuf,
    block_size: u64,
    supports_file_cloning: bool,
}

impl AsRef<Path> for Dir {
    fn as_ref(&self) -> &Path {
        &self.path_buf
    }
}

impl Borrow<Path> for Dir {
    fn borrow(&self) -> &Path {
        &self.path_buf
    }
}

impl Dir {
    pub fn new(path_buf: PathBuf) -> Result<Self> {
        fs::create_dir_all(&path_buf)?;
        let block_size = path_min_hole_size(&path_buf)?;
        let file = open_dir_as_file(&path_buf)?;
        let supports_file_cloning_flag = file.file_system_flags()?.supports_block_cloning();
        let supports_file_cloning = match supports_file_cloning_flag {
            Some(some) => some,
            None => {
                #[allow(unused_mut)]
                let mut src = tempfile::NamedTempFile::new_in(&path_buf)?;
                // FreeBSD's copy_file_range(2) succeeds trivially for empty files, so give it a
                // real block to clone. Random data so compression can't turn it into a hole or an
                // embedded block.
                #[cfg(target_os = "freebsd")]
                {
                    use rand::RngCore;
                    use std::io::Write;
                    let mut buf = vec![0; block_size as usize];
                    rand::thread_rng().fill_bytes(&mut buf);
                    src.write_all(&buf)?;
                    src.flush()?;
                }
                let dst_path = random_file_name_in_dir(&path_buf, ".clone_test-");
                assert!(!dst_path.exists());
                let clone_res = clonefile(src.path(), &dst_path);
                let _ = std::fs::remove_file(&dst_path);
                match clone_res {
                    Ok(()) => true,
                    Err(err) if CloneFileError::is_unsupported(&err) => {
                        warn!(?err, "clonefile unsupported");
                        false
                    }
                    Err(err) => {
                        error!(?err);
                        return Err(err).context("testing clonefile");
                    }
                }
            }
        };
        Ok(Self {
            path_buf,
            block_size,
            supports_file_cloning,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path_buf
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    pub fn supports_file_cloning(&self) -> bool {
        self.supports_file_cloning
    }

    /// Walks the underlying files in the possum directory.
    pub fn walk_dir(&self) -> Result<Vec<walk::Entry>> {
        crate::walk::walk_dir(self)
    }
}
