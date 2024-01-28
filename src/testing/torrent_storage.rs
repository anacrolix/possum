use std::hash::Hasher;
use std::io::Write;

use anyhow::anyhow;
use fdlimit::raise_fd_limit;

use super::*;
use crate::testing::{test_tempdir, Hash};
use crate::Handle;

#[derive(Clone, Copy)]
pub struct TorrentStorageOpts {
    pub piece_size: usize,
    pub chunk_size: usize,
    pub num_pieces: usize,
    pub static_tempdir_name: &'static str,
    pub disable_hole_punching: bool,
    pub rename_values: bool,
    pub num_threads: Option<usize>,
}

pub const BENCHMARK_OPTS: TorrentStorageOpts = TorrentStorageOpts {
    piece_size: 2 << 20,
    static_tempdir_name: "benchmark_torrent_storage_default",
    num_pieces: 8,
    chunk_size: 1 << 14,
    disable_hole_punching: false,
    rename_values: true,
    num_threads: None,
};

impl TorrentStorageOpts {
    pub fn build(self) -> Result<TorrentStorageInner> {
        let opts = self;
        // Need to set this globally when testing, but you can't know what test will run first. At least
        // if this is the only test to run, it will be guaranteed to run first.
        let _ = env_logger::builder()
            .is_test(true)
            .format_timestamp_micros()
            .try_init();
        let _ = raise_fd_limit();
        // Running in the same directory messes with the disk analysis at the end of the test.
        let _tempdir = test_tempdir(opts.static_tempdir_name)?;
        let new_handle = || -> anyhow::Result<Handle> {
            let mut handle = Handle::new(_tempdir.path.clone())?;
            handle.set_instance_limits(handle::Limits {
                disable_hole_punching: opts.disable_hole_punching,
                max_value_length_sum: Some(opts.piece_size as u64 * opts.num_pieces as u64 / 2),
            })?;
            Ok(handle)
        };
        let handle = new_handle()?;
        let thread_pool = if let Some(num_threads) = opts.num_threads {
            Some(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(num_threads)
                    .build()?,
            )
        } else {
            None
        };
        Ok(TorrentStorageInner {
            opts,
            handle,
            thread_pool,
            _tempdir,
        })
    }
}

pub struct TorrentStorageInner {
    opts: TorrentStorageOpts,
    handle: Handle,
    _tempdir: TestTempDir,
    thread_pool: Option<rayon::ThreadPool>,
}

impl TorrentStorageInner {
    pub fn run(&self) -> Result<()> {
        torrent_storage_inner_run(self)
    }
}

// TODO: Rename. Maybe make this a method on Opts. Mirror the squirrel benchmark closer by not
// creating a completed key, but by renaming chunks when they're verified.
fn torrent_storage_inner_run(inner: &TorrentStorageInner) -> anyhow::Result<()> {
    let TorrentStorageInner {
        opts,
        handle,
        thread_pool,
        ..
    } = inner;
    let TorrentStorageOpts {
        piece_size,
        chunk_size,
        num_pieces,
        ..
    } = opts;
    let num_pieces = *num_pieces;
    let piece_size = *piece_size;
    let chunk_size = *chunk_size;
    for _piece_index in 0..num_pieces {
        let byte = rand::thread_rng().gen_range(1..u8::MAX);
        let mut piece_data = io::repeat(byte).take(piece_size as u64);
        // Hi alec
        let piece_data_hash = {
            let mut hash = Hash::default();
            assert_eq!(
                io::copy(&mut piece_data, &mut HashWriter(&mut hash))?,
                piece_size as u64,
            );
            hash.finish()
        };
        let chunk_offset_iter = (0..piece_size).step_by(chunk_size);
        let unverified_key = |offset| format!("unverified/{piece_data_hash:x}/{offset}");
        let write_unverified_chunk = |handle: &Handle, offset| -> anyhow::Result<()> {
            let key = unverified_key(offset);
            let (written, _) = handle
                .single_write_from(key.into_bytes(), io::repeat(byte).take(chunk_size as u64))?;
            assert_eq!(written, chunk_size as u64);
            Ok(())
        };
        if let Some(thread_pool) = thread_pool {
            thread_pool.scope(|scope| {
                for offset in chunk_offset_iter.clone() {
                    // let handle = Handle::new(tempdir.path.clone())?;
                    let handle = &handle;
                    scope.spawn(move |_scope| write_unverified_chunk(handle, offset).unwrap());
                }
                anyhow::Ok(())
            })?;
        } else {
            for offset in chunk_offset_iter.clone() {
                write_unverified_chunk(&handle, offset)?;
            }
        }
        debug!("starting piece");
        let mut reader = handle.read()?;
        let values = chunk_offset_iter
            .clone()
            .map(|offset| {
                anyhow::Ok((
                    offset,
                    reader
                        .add(unverified_key(offset).as_ref())?
                        .ok_or(anyhow!("missing value"))?,
                ))
            })
            .collect::<anyhow::Result<Vec<_>, _>>()?;
        let snapshot = reader.begin()?;
        let mut stored_hash = Hash::default();
        let mut writer = handle.new_writer()?;
        let make_verified_key =
            |offset| format!("verified/{piece_data_hash:016x}/{offset}").into_bytes();
        if opts.rename_values {
            for (offset, value) in values {
                snapshot.value(value.clone()).view(|bytes| {
                    stored_hash.write(bytes);
                    compare_reads(bytes, io::repeat(byte).take(chunk_size as u64)).unwrap();
                    writer.rename_value(value, make_verified_key(offset))
                })?;
            }
            assert_eq!(stored_hash.finish(), piece_data_hash);
            writer.commit()?;
            let mut reader = handle.read()?;
            let mut verified_hash = Hash::default();
            let mut values = vec![];
            for offset in chunk_offset_iter.clone() {
                values.push(reader.add(&make_verified_key(offset))?.unwrap());
            }
            let snapshot = reader.begin()?;
            for value in values {
                snapshot.value(value).view(|bytes| {
                    assert_eq!(bytes.len(), chunk_size);
                    verified_hash.write(bytes)
                })?;
            }
            assert_eq!(verified_hash.finish(), piece_data_hash);
        } else {
            let mut completed = writer.new_value().begin()?;
            for (_offset, value) in values {
                snapshot.value(value).view(|bytes| {
                    stored_hash.write(bytes);
                    completed.write_all(bytes)
                })??;
            }
            assert_eq!(stored_hash.finish(), piece_data_hash);
            let completed_key = format!("completed/{:016x}", piece_data_hash).into_bytes();
            writer.stage_write(completed_key.clone(), completed)?;
            writer.commit()?;
            // let completed_value = handle
            //     .read_single(&completed_key)?
            //     .expect("completed item should exist");
            // let completed_reader = completed_value.new_reader();
            // let completed_hash = hash_reader(completed_reader)?;
            // assert_eq!(completed_hash, piece_data_hash);
        }
    }
    Ok(())
}
