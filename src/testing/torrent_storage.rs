use std::hash::Hasher;
use std::io::Write;

use anyhow::anyhow;
use fdlimit::raise_fd_limit;
use log::debug;
use rand::{RngCore, SeedableRng};

use super::*;
use crate::testing::{hash_reader, test_tempdir, Hash};

use crate::Handle;

#[derive(Clone, Copy)]
pub struct TorrentStorageOpts {
    pub piece_size: usize,
    pub block_size: usize,
    pub num_pieces: usize,
    pub static_tempdir_name: &'static str,
}

pub const BENCHMARK_OPTS: TorrentStorageOpts = TorrentStorageOpts {
    piece_size: 2 << 20,
    static_tempdir_name: "benchmark_torrent_storage_default",
    num_pieces: 8,
    block_size: 4096,
};

// TODO: Rename. Maybe make this a method on Opts. Mirror the squirrel benchmark closer by not
// creating a completed key, but by renaming chunks when they're verified.
pub fn torrent_storage_inner(opts: TorrentStorageOpts) -> anyhow::Result<()> {
    let TorrentStorageOpts {
        piece_size,
        block_size,
        num_pieces,
        ..
    } = opts;
    // Need to set this globally when testing, but you can't know what test will run first. At least
    // if this is the only test to run, it will be guaranteed to run first.
    let _ = env_logger::builder()
        .is_test(true)
        .format_timestamp_micros()
        .try_init();
    let _ = raise_fd_limit();
    // Running in the same directory messes with the disk analysis at the end of the test.
    let tempdir = test_tempdir(opts.static_tempdir_name)?;
    let handle = Handle::new(tempdir.path.clone())?;
    for _ in 0..num_pieces {
        let mut piece_data = vec![0; piece_size];
        // Hi alec
        rand::rngs::SmallRng::seed_from_u64(420).fill_bytes(&mut piece_data);
        let piece_data_hash = {
            let mut hash = Hash::default();
            hash.write(&piece_data);
            hash.finish()
        };
        let block_offset_iter = (0..piece_size).step_by(block_size);
        let offset_key = |offset| format!("piece/{}", offset);
        std::thread::scope(|scope| {
            let mut join_handles = vec![];
            for offset in block_offset_iter.clone() {
                let piece_data = &piece_data;
                let handle = Handle::new(tempdir.path.clone())?;
                join_handles.push(scope.spawn(move || -> anyhow::Result<()> {
                    let key = offset_key(offset);
                    handle.single_write_from(
                        key.into_bytes(),
                        &piece_data[offset..offset + block_size],
                    )?;
                    Ok(())
                }));
            }
            for jh in join_handles {
                jh.join().unwrap()?;
            }
            anyhow::Ok(())
        })?;
        debug!("starting piece");
        let mut reader = handle.read()?;
        let values = block_offset_iter
            .clone()
            .map(|offset| {
                anyhow::Ok(
                    reader
                        .add(offset_key(offset).as_ref())?
                        .ok_or(anyhow!("missing value"))?,
                )
            })
            .collect::<anyhow::Result<Vec<_>, _>>()?;
        let snapshot = reader.begin()?;
        let mut stored_hash = Hash::default();
        let mut writer = handle.new_writer()?;
        let mut completed = writer.new_value().begin()?;
        for value in values {
            snapshot.value(value).view(|bytes| {
                stored_hash.write(bytes);
                completed.write_all(bytes)
            })??;
        }
        assert_eq!(stored_hash.finish(), piece_data_hash);
        let completed_key = format!("completed/{:x}", piece_data_hash).into_bytes();
        writer.stage_write(completed_key.clone(), completed)?;
        writer.commit()?;
        let completed_value = handle
            .read_single(&completed_key)?
            .expect("completed item should exist");
        let completed_reader = completed_value.new_reader();
        let completed_hash = hash_reader(completed_reader)?;
        assert_eq!(completed_hash, piece_data_hash);
    }
    Ok(())
}
