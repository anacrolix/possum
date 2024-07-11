use super::*;
use crate::ownedtx::OwnedTxTrait;

// BTree possibly so we can merge extents in the future.
type Reads = HashMap<FileId, BTreeSet<ReadExtent>>;

pub struct Reader<T> {
    pub(crate) owned_tx: T,
    pub(crate) reads: Reads,
}

impl<'a, T, H> Reader<T>
where
    T: OwnedTxTrait<Tx = Transaction<'a, H>>,
    H: AsRef<Handle>,
{
    pub fn add(&mut self, key: &[u8]) -> rusqlite::Result<Option<Value>> {
        let res = self.owned_tx.mut_transaction().touch_for_read(key);
        match res {
            Ok(value) => {
                if let Nonzero(NonzeroValueLocation {
                    file_offset,
                    length,
                    file_id,
                }) = value.location
                {
                    let file = self.reads.entry(file_id);
                    file.or_default().insert(ReadExtent {
                        offset: file_offset,
                        len: length,
                    });
                }
                Ok(Some(value))
            }
            Err(QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Takes a snapshot and commits the read transaction.
    pub fn begin(self) -> Result<Snapshot> {
        let file_clones = self.clone_files().context("cloning files")?;
        self.owned_tx
            .end_tx(|tx| tx.commit())
            .context("committing transaction")?
            .complete();
        Ok(Snapshot { file_clones })
    }

    fn clone_files(&self) -> Result<FileCloneCache> {
        let handle = self.owned_tx.as_handle();
        let reads = &self.reads;
        let mut tempdir = None;
        let mut file_clones: FileCloneCache = Default::default();
        // This isn't needed if file cloning is disabled...
        let mut handle_clone_guard = handle.clones.lock().unwrap();
        let handle_clones = handle_clone_guard.deref_mut();
        for (file_id, extents) in reads {
            file_clones.insert(
                *file_id,
                self.get_file_clone(
                    file_id,
                    &mut tempdir,
                    handle_clones,
                    handle.dir.path(),
                    extents,
                )
                .context("getting file clone")?,
            );
        }
        Ok(file_clones)
    }

    pub fn list_items(&self, prefix: &[u8]) -> PubResult<Vec<Item>> {
        self.owned_tx.transaction().list_items(prefix)
    }

    fn get_file_clone(
        &self,
        file_id: &FileId,
        tempdir: &mut Option<Arc<TempDir>>,
        cache: &mut FileCloneCache,
        src_dir: &Path,
        read_extents: &BTreeSet<ReadExtent>,
    ) -> PubResult<Arc<Mutex<FileClone>>> {
        if let Some(ret) = cache.get(file_id) {
            let min_len = read_extents
                .iter()
                .map(|re| re.offset + re.len)
                .max()
                .unwrap();
            let file_clone_guard = ret.lock().unwrap();
            if file_clone_guard.len >= min_len {
                return Ok(ret.clone());
            }
        }
        if self.owned_tx.as_handle().dir_supports_file_cloning() {
            match self.clone_file(file_id, tempdir, cache, src_dir) {
                Err(err) if err.root_cause_is_unsupported_filesystem() => (),
                default => return default,
            }
        }
        self.get_file_for_read_by_segment_locking(file_id, read_extents)
    }

    fn clone_file(
        &self,
        file_id: &FileId,
        tempdir: &mut Option<Arc<TempDir>>,
        cache: &mut FileCloneCache,
        src_dir: &Path,
    ) -> PubResult<Arc<Mutex<FileClone>>> {
        let tempdir: &Arc<TempDir> = match tempdir {
            Some(tempdir) => tempdir,
            None => {
                let mut builder = tempfile::Builder::new();
                builder.prefix(SNAPSHOT_DIR_NAME_PREFIX);
                let new = Arc::new(builder.tempdir_in(src_dir)?);
                *tempdir = Some(new);
                tempdir.as_ref().unwrap()
            }
        };
        let src_path = file_path(src_dir, file_id);
        // TODO: In order for value files to support truncation, a shared or exclusive lock would
        // need to be taken before cloning. I don't think this is possible, we would have to wait
        // for anyone holding an exclusive lock to release it. Handles already cache these, plus
        // Writers could hold them for a long time while writing. Then we need a separate cloning
        // lock. Also distinct Handles, even across processes can own each exclusive file.
        if false {
            let src_file = OpenOptions::new().read(true).open(&src_path)?;
            assert!(src_file.lock_max_segment(LockSharedNonblock)?);
        }
        let tempdir_path = tempdir.path();
        let dst_path = file_path(tempdir_path, file_id);
        clonefile(&src_path, &dst_path).context("cloning file")?;
        let mut file = open_file_id(OpenOptions::new().read(true), tempdir_path, file_id)
            .context("opening value file")?;
        // This prevents the snapshot file from being cleaned up. There's probably a race between
        // here and when it was cloned above. I wonder if the snapshot dir can be locked, or if we
        // can retry the cloning until we are able to lock it.
        let locked = file.lock_max_segment(LockSharedNonblock)?;
        assert!(locked);
        let len = file.seek(End(0))?;
        let file_clone = Arc::new(Mutex::new(FileClone {
            file,
            tempdir: Some(tempdir.clone()),
            mmap: None,
            len,
        }));

        cache.insert(file_id.to_owned(), file_clone.clone());
        Ok(file_clone)
    }

    fn lock_read_extents<'b>(
        file: &File,
        read_extents: impl Iterator<Item = &'b ReadExtent>,
    ) -> io::Result<()> {
        // This might require a conditional var if it's used everywhere
        #[cfg(not(windows))]
        if flocking() {
            // Possibly we want to block if we're flocking.
            assert!(file.flock(LockShared)?);
            return Ok(());
        }
        for extent in read_extents {
            assert!(file.lock_segment(LockSharedNonblock, Some(extent.len), extent.offset)?);
        }
        Ok(())
    }

    fn get_file_for_read_by_segment_locking(
        &self,
        file_id: &FileId,
        read_extents: &BTreeSet<ReadExtent>,
    ) -> PubResult<Arc<Mutex<FileClone>>> {
        let mut file = open_file_id(
            OpenOptions::new().read(true),
            self.owned_tx.as_handle().dir(),
            file_id,
        )?;

        Self::lock_read_extents(&file, read_extents.iter())?;
        let len = file.seek(std::io::SeekFrom::End(0))?;
        let file_clone = FileClone {
            file,
            tempdir: None,
            mmap: None,
            len,
        };
        // file_clone.get_mmap()?;
        Ok(Arc::new(Mutex::new(file_clone)))
    }
}
