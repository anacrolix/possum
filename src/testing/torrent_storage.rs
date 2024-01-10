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
    pub disable_hole_punching: bool,
    pub rename_values: bool,
}

pub const BENCHMARK_OPTS: TorrentStorageOpts = TorrentStorageOpts {
    piece_size: 2 << 20,
    static_tempdir_name: "benchmark_torrent_storage_default",
    num_pieces: 8,
    block_size: 4096,
    disable_hole_punching: false,
    rename_values: true,
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
    let new_handle = || -> anyhow::Result<Handle> {
        let mut handle = Handle::new(tempdir.path.clone())?;
        handle.set_instance_limits(handle::Limits {
            disable_hole_punching: opts.disable_hole_punching,
            max_value_length_sum: None,
        })?;
        Ok(handle)
    };
    let handle = new_handle()?;
    let thread_pool = rayon::ThreadPoolBuilder::new().build()?;
    for piece_index in 0..num_pieces {
        let mut piece_data = vec![0; piece_size];
        // Hi alec
        rand::rngs::SmallRng::seed_from_u64(piece_index as u64).fill_bytes(&mut piece_data);
        let piece_data_hash = {
            let mut hash = Hash::default();
            hash.write(&piece_data);
            hash.finish()
        };
        let block_offset_iter = (0..piece_size).step_by(block_size);
        let unverified_key = |offset| format!("unverified/{piece_data_hash:x}/{offset}");
        thread_pool.scope(|scope| {
            for offset in block_offset_iter.clone() {
                let piece_data = &piece_data;
                // let handle = Handle::new(tempdir.path.clone())?;
                let handle = &handle;
                scope.spawn(move |_scope| {
                    (|| -> anyhow::Result<()> {
                        let key = unverified_key(offset);
                        handle.single_write_from(
                            key.into_bytes(),
                            &piece_data[offset..offset + block_size],
                        )?;
                        Ok(())
                    })()
                    .unwrap()
                });
            }
            anyhow::Ok(())
        })?;
        debug!("starting piece");
        let mut reader = handle.read()?;
        let values = block_offset_iter
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
            |offset| format!("verified/{piece_data_hash:x}/{offset}").into_bytes();
        if opts.rename_values {
            for (offset, value) in values {
                snapshot.value(value.clone()).view(|bytes| {
                    stored_hash.write(bytes);
                    writer.rename_value(value, make_verified_key(offset))
                })?;
            }
            assert_eq!(stored_hash.finish(), piece_data_hash);
            writer.commit()?;
            let mut reader = handle.read()?;
            let mut verified_hash = Hash::default();
            let mut values = vec![];
            for offset in block_offset_iter.clone() {
                values.push(reader.add(&make_verified_key(offset))?.unwrap());
            }
            let snapshot = reader.begin()?;
            for value in values {
                snapshot
                    .value(value)
                    .view(|bytes| verified_hash.write(bytes))?;
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
            let completed_key = format!("completed/{:x}", piece_data_hash).into_bytes();
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
