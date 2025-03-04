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
                let src = tempfile::NamedTempFile::new_in(&path_buf)?;
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
