use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Debug, Display};
use std::fs::OpenOptions;
use std::hash::Hasher;
use std::io::SeekFrom::Start;
use std::io::{Read, Seek, Write};
use std::ops::Bound::Included;
use std::ops::{RangeBounds, RangeInclusive};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use fdlimit::raise_fd_limit;
use itertools::Itertools;
use possum::concurrency::thread;
use possum::testing::*;
use possum::walk::{walk_dir, EntryType};
use possum::Error::NoSuchKey;
use possum::*;
use rand::distributions::uniform::{UniformDuration, UniformSampler};
use rand::{thread_rng, RngCore, SeedableRng};
use tempfile::tempdir;
use test_log::test;
use thread::sleep;
use tracing::*;
use walk::Entry as WalkEntry;

#[test]
fn rename_key() -> Result<()> {
    check_concurrency(
        || {
            let tempdir = tempdir()?;
            let mut handle = Handle::new(tempdir.path().to_owned())?;
            let value_bytes = "world".as_bytes();
            let rename_res = handle
                .rename_item("noexist".as_bytes(), "borat".as_bytes())
                .map(|_| ());
            assert!(matches!(rename_res, Err(NoSuchKey)));
            assert_eq!(
                handle
                    .single_write_from("hello".as_bytes().to_vec(), value_bytes)?
                    .0,
                5
            );
            handle
                .read_single("hello".as_bytes())?
                .ok_or(NoSuchKey)?
                .view(|value| assert_eq!(value, value_bytes))?;
            handle.rename_item("hello".as_ref(), "borat".as_ref())?;
            handle
                .read_single("borat".as_bytes())?
                .expect("key should be renamed to borat")
                .view(|value| assert_eq!(value, value_bytes))?;
            assert!(handle.read_single("hello".as_bytes())?.is_none());
            let handle_entries = handle_relative_walk_entries_hashset(&handle);
            let counts = count_by_entry_types(&handle_entries);
            assert_eq!(counts[&ValuesFile], 1);
            Ok(())
        },
        100,
    )
}

use crate::walk::EntryType::*;

fn count_by_entry_types(
    entries: impl IntoIterator<Item = impl Borrow<WalkEntry>>,
) -> HashMap<EntryType, usize> {
    entries
        .into_iter()
        .group_by(|entry| entry.borrow().entry_type)
        .into_iter()
        .map(|(key, group)| (key, group.count()))
        .collect()
}

fn handle_relative_walk_entries_hashset(handle: &Handle) -> HashSet<WalkEntry> {
    handle
        .walk_dir()
        .expect("should be able to walk handle dir")
        .into_iter()
        .map(|mut entry: WalkEntry| {
            let suffix = entry
                .path
                .strip_prefix(handle.dir())
                .expect("walk entry should have handle dir path prefix")
                .to_owned();
            suffix.clone_into(&mut entry.path);
            entry
        })
        .collect()
}

#[test]
fn set_get() -> Result<()> {
    check_concurrency(
        || {
            let tempdir = tempdir()?;
            let handle = Handle::new(tempdir.path().to_owned())?;
            let value_bytes = "world".as_bytes();
            let mut writer = handle.new_writer()?;
            let mut value = writer.new_value().begin()?;
            value.write_all(value_bytes)?;
            writer.stage_write("hello".as_bytes().to_owned(), value)?;
            let mut reader = handle.read()?;
            assert!(reader.add("hello".as_bytes()).is_ok_and(|ok| ok.is_none()));
            drop(reader);
            writer.commit()?;
            let mut reader = handle.read()?;
            let value = reader.add("hello".as_bytes())?.expect("key should exist");
            let snapshot = reader.begin()?;
            snapshot
                .value(&value)
                .view(|read_value_bytes| assert_eq!(read_value_bytes, value_bytes))?;
            Ok(())
        },
        100,
    )
}

#[test]
fn set_get_reader() -> Result<()> {
    check_concurrency(
        || {
            let tempdir = tempdir()?;
            let handle = Handle::new(tempdir.path().to_owned())?;
            let mut value_bytes = vec![0; 1 << 16];
            thread_rng().fill_bytes(&mut value_bytes);
            let mut value_bytes_reader: &[u8] = &value_bytes;
            dbg!(hash_reader(&mut value_bytes_reader)?);
            let mut writer = handle.new_writer()?;
            let mut value = writer.new_value().begin()?;
            dbg!(value_bytes.len());
            value.write_all(&value_bytes)?;
            writer.stage_write("hello".as_bytes().to_owned(), value)?;
            let mut reader = handle.read()?;
            assert!(reader.add("hello".as_bytes()).is_ok_and(|ok| ok.is_none()));
            drop(reader);
            writer.commit()?;
            let mut reader = handle.read()?;
            let value = reader.add("hello".as_bytes())?.expect("key should exist");
            let snapshot = reader.begin()?;
            let mut reader_bytes = vec![];
            snapshot
                .value(&value)
                .new_reader()
                .read_to_end(&mut reader_bytes)?;
            assert_eq!(reader_bytes, value_bytes);
            Ok(())
        },
        100,
    )
}

#[test]
fn clone_in_file() -> Result<()> {
    check_concurrency(
        || {
            let tempdir = tempdir()?;
            let mut handle = Handle::new(tempdir.path().to_owned())?;
            let mut file = write_random_tempfile(42069)?;
            let key = "hi\x00elon".as_bytes();
            assert_eq!(
                handle.clone_from_file(key.to_owned(), file.as_file_mut())?,
                42069
            );
            file.seek(Start(0))?;
            compare_reads(
                handle
                    .read_single(key)?
                    .context("item should exist")?
                    .new_reader(),
                file,
            )?;
            Ok(())
        },
        100,
    )
}

use std::prelude::rust_2021::test as std_test;

struct TorrentStorageOpts {
    piece_size: usize,
    block_size: usize,
    static_tempdir_name: &'static str,
    view_snapshot_values: bool,
}

#[std_test]
#[ignore]
fn torrent_storage_kernel_bug_min_repro() -> Result<()> {
    check_concurrency(
        || {
            let block_size = 4096;
            let stop = Instant::now() + Duration::from_secs(1);
            while Instant::now() < stop {
                torrent_storage_inner(TorrentStorageOpts {
                    block_size,
                    piece_size: block_size,
                    static_tempdir_name: "torrent_storage_kernel_bug",
                    view_snapshot_values: true,
                })?;
            }
            Ok(())
        },
        100,
    )
}

#[std_test]
fn torrent_storage_small() -> Result<()> {
    check_concurrency(
        || {
            let block_size = 4096;
            let stop = Instant::now() + Duration::from_secs(1);
            while Instant::now() < stop {
                torrent_storage_inner(TorrentStorageOpts {
                    block_size,
                    piece_size: 4 * block_size,
                    static_tempdir_name: "torrent_storage_small",
                    view_snapshot_values: true,
                })?;
            }
            Ok(())
        },
        1,
    )
}

#[std_test]
fn torrent_storage_big() -> Result<()> {
    check_concurrency(
        || {
            let block_size = 4096;
            torrent_storage_inner(TorrentStorageOpts {
                block_size,
                piece_size: 2 << 20,
                static_tempdir_name: "torrent_storage_big",
                view_snapshot_values: true,
            })
        },
        1,
    )
}

fn torrent_storage_inner(opts: TorrentStorageOpts) -> Result<()> {
    let TorrentStorageOpts {
        piece_size,
        block_size,
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
    let handle = Handle::new(tempdir.path)?;
    // handle.set_instance_limits(possum::handle::Limits {
    //     max_value_length_sum: None,
    //     disable_hole_punching: true,
    // })?;
    let handle = Arc::new(handle);
    let mut piece_data = vec![0; piece_size];
    // Hi alec
    rand::rngs::SmallRng::seed_from_u64(420).fill_bytes(&mut piece_data);
    let piece_data_hash = {
        let mut hash = Hash::default();
        hash.write(&piece_data);
        hash.finish()
    };
    dbg!(format!("{:x}", piece_data_hash));
    let block_offset_iter = (0..piece_size).step_by(block_size);
    let offset_key = |offset| format!("piece/{}", offset);
    use std::sync::Arc;
    let piece_data = Arc::new(piece_data);

    let mut join_handles = vec![];
    for (index, offset) in block_offset_iter.clone().enumerate() {
        let piece_data = Arc::clone(&piece_data);
        let start_delay = Duration::from_micros(1000 * (index / 2) as u64);
        let handle = Arc::clone(&handle);
        join_handles.push(thread::spawn(move || -> Result<()> {
            let key = offset_key(offset);
            sleep(start_delay);
            debug!("starting block write");
            handle.single_write_from(key.into_bytes(), &piece_data[offset..offset + block_size])?;
            Ok(())
        }));
    }
    for jh in join_handles {
        jh.join().unwrap()?;
    }
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
        .collect::<Result<Vec<_>, _>>()?;
    let snapshot = reader.begin()?;
    let mut stored_hash = Hash::default();
    // let starting_stored_hash = stored_hash.finish();
    // debug!(%starting_stored_hash, "starting stored hash");
    let mut writer = handle.new_writer()?;
    let mut completed = writer.new_value().begin()?;
    for value in values {
        let snapshot_value = snapshot.value(value);
        if opts.view_snapshot_values {
            snapshot_value.view(|bytes| {
                stored_hash.write(bytes);
                completed.write_all(bytes)
            })??;
        } else {
            assert_eq!(
                std::io::copy(
                    &mut snapshot_value.new_reader(),
                    &mut HashWriter(&mut stored_hash)
                )?,
                snapshot_value.length()
            );
            assert_eq!(
                std::io::copy(&mut snapshot_value.new_reader(), &mut completed,)?,
                snapshot_value.length()
            );
        }
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
    let handle_walk_entries = handle.walk_dir()?;
    let _counts = count_by_entry_types(handle_walk_entries);
    assert_eq!(handle.list_items("a".as_bytes())?.len(), 0);
    assert_eq!(handle.list_items("c".as_bytes())?.len(), 1);
    let offsets_starting_with_1 = offsets_starting_with(block_offset_iter, "1").count();
    assert_ne!(offsets_starting_with_1, 0);
    assert_eq!(
        handle.list_items("piece/1".as_bytes())?.len(),
        offsets_starting_with_1
    );
    Ok(())
}

fn offsets_starting_with<'a>(
    offsets: impl Iterator<Item = usize> + 'a,
    prefix: &'a str,
) -> impl Iterator<Item = usize> + 'a {
    offsets.filter(move |offset| offset.to_string().starts_with(prefix))
}

#[test]
fn big_set_get() -> Result<()> {
    check_concurrency(
        || {
            let tempdir = test_tempdir("big_set_get")?;
            let handle = Handle::new(tempdir.path)?;
            let piece_size = 2 << 20;
            let mut piece_data = vec![0; piece_size];
            thread_rng().fill_bytes(&mut piece_data);
            let piece_data_hash = {
                let mut hash = Hash::default();
                hash.write(&piece_data);
                hash.finish()
            };
            let completed_key = format!("completed/{:x}", piece_data_hash).into_bytes();
            handle.single_write_from(completed_key.clone(), &*piece_data)?;
            let completed_value = handle
                .read_single(&completed_key)?
                .expect("completed item should exist");
            let mut piece_data_actual_single_read = vec![0; piece_size * 2];
            let n = completed_value.read(&mut piece_data_actual_single_read)?;
            assert_eq!(n, piece_size);
            assert_eq!(
                {
                    let mut hash = Hash::default();
                    hash.write(&piece_data_actual_single_read[0..n]);
                    hash.finish()
                },
                piece_data_hash
            );
            assert_eq!(
                { hash_reader(&*completed_value.view(|bytes| bytes.to_owned())?)? },
                piece_data_hash
            );
            {
                let mut all_completed = vec![];
                completed_value
                    .new_reader()
                    .read_to_end(&mut all_completed)?;
                assert_eq!(hash_reader(&*all_completed)?, piece_data_hash);
            }
            let completed_reader = completed_value.new_reader();
            let completed_hash = hash_reader(completed_reader)?;
            assert_eq!(completed_hash, piece_data_hash);
            Ok(())
        },
        100,
    )
}

#[test]
fn cleanup_snapshots() -> Result<()> {
    check_concurrency(
        || {
            let tempdir = test_tempdir("cleanup_snapshots")?;
            let count_snapshot_dirs = || {
                walk_dir(tempdir.path.clone())
                    .unwrap()
                    .iter()
                    .filter(|entry| entry.entry_type == SnapshotDir)
                    .count()
            };
            let handle = Handle::new(tempdir.path.clone())?;
            if !handle.dir_supports_file_cloning() {
                return Ok(());
            }
            handle.cleanup_snapshots()?;
            handle.single_write_from("hello".as_bytes().to_vec(), "world".as_bytes())?;
            let value = handle.read_single("hello".as_bytes())?.unwrap();
            assert_eq!(count_snapshot_dirs(), 1);
            drop(handle);
            // Value holds on to a snapshot dir.
            assert_eq!(count_snapshot_dirs(), 1);
            let _handle = Handle::new(tempdir.path.clone())?;
            // Another handle does not clean up the snapshot dir because there's a lock on a value file
            // inside it.
            assert_eq!(count_snapshot_dirs(), 1);
            drop(value);
            assert_eq!(count_snapshot_dirs(), 0);
            let handle = Handle::new(tempdir.path.clone())?;
            assert_eq!(count_snapshot_dirs(), 0);
            let value = handle.read_single("hello".as_bytes())?.unwrap();
            assert_eq!(count_snapshot_dirs(), 1);
            // This time leak the snapshot dir so it's still around after we drop everything.
            value.leak_snapshot_dir();
            drop(value);
            drop(handle);
            assert_eq!(count_snapshot_dirs(), 1);
            // This will clean up the unused snapshot dir that was leaked earlier.
            let handle = Handle::new(tempdir.path.clone())?;
            handle.cleanup_snapshots()?;
            assert_eq!(count_snapshot_dirs(), 0);
            Ok(())
        },
        100,
    )
}

#[test]
fn reads_update_last_used() -> Result<()> {
    check_concurrency(
        || {
            let tempdir = tempdir()?;
            let handle = Handle::new(tempdir.as_ref().to_owned())?;
            let key = Vec::from("hello");
            let value = "mundo".as_bytes();
            let (n, _) = handle.single_write_from(key.clone(), value)?;
            assert_eq!(n, 5);
            let read_ts = handle.read_single(&key)?.unwrap().last_used();
            let mut rng = thread_rng();
            let uniform = UniformDuration::new(Duration::from_nanos(0), LAST_USED_RESOLUTION);
            for _ in 0..100 {
                let dither = uniform.sample(&mut rng);
                // This needs to be a real sleep or the timestamps sqlite generates don't progress.
                std::thread::sleep(LAST_USED_RESOLUTION + dither);
                let new_read_ts = handle.read_single(&key)?.unwrap().last_used();
                assert!(new_read_ts > read_ts);
            }
            Ok(())
        },
        10,
    )
}

#[test]
fn read_and_writes_different_handles() -> Result<()> {
    check_concurrency(
        || {
            let range = 0..=10;
            let tempdir = test_tempdir("read_and_writes_different_handles")?;
            let dir = tempdir.path;
            let key = "incr".as_bytes();
            // Create a single handle before creating them in threads to get a clean initialization.
            let handle = Handle::new(dir.clone())?;
            // I think this gets stuck if there's a panic in the writer.
            if false {
                ctx_thread::scope(|ctx| {
                    ctx.spawn(|ctx| {
                        read_consecutive_integers(Some(ctx), dir.clone(), key, &range).unwrap()
                    });
                    let range = range.clone();
                    // Writing doesn't wait, so we can just let it run out.
                    ctx.spawn(|_| write_consecutive_integers(dir.clone(), key, range).unwrap());
                })
                .unwrap();
            } else {
                let reader = thread::spawn({
                    let dir = dir.clone();
                    let range = range.clone();
                    move || read_consecutive_integers(None, dir, key, &range).unwrap()
                });
                let writer =
                    thread::spawn(move || write_consecutive_integers(dir, key, range).unwrap());
                writer.join().unwrap();
                reader.join().unwrap();
            }
            let keys: Vec<_> = handle
                .list_items("".as_bytes())?
                .into_iter()
                .map(|item| item.key)
                .collect();
            assert_eq!(keys, vec!["incr".as_bytes()]);
            Ok(())
        },
        100,
    )
}

const RACE_SLEEP_DURATION: Duration = Duration::from_millis(1);

fn write_consecutive_integers<I, R>(dir: PathBuf, key: &[u8], values: R) -> Result<()>
where
    I: Display + Debug,
    R: Iterator<Item = I>,
{
    let handle = Handle::new(dir)?;
    for i in values {
        println!("writing {}", i);
        handle.single_write_from(key.to_owned(), i.to_string().as_bytes())?;
        sleep(RACE_SLEEP_DURATION);
    }
    Ok(())
}

fn read_consecutive_integers<I>(
    ctx: Option<&ctx_thread::Context>,
    dir: PathBuf,
    key: &[u8],
    range: &RangeInclusive<I>,
) -> Result<()>
where
    I: FromStr + Debug + PartialOrd + Display,
    <I as FromStr>::Err: Error + Send + Sync + 'static,
{
    let handle = Handle::new(dir)?;
    let Included(end_i) = range.end_bound() else {
        panic!("expected inclusive range: {:?}", range);
    };
    let mut last_i = None;
    loop {
        let Some(value) = handle.read_single(key)? else {
            if let Some(ctx) = ctx {
                if !ctx.active() {
                    bail!("cancelled");
                }
            }
            sleep(RACE_SLEEP_DURATION);
            continue;
        };
        let mut s = String::new();
        value.new_reader().read_to_string(&mut s)?;
        let i: I = s.parse()?;
        println!("read {}", &i);
        assert!(Some(&i) >= last_i.as_ref());
        if &i >= end_i {
            break;
        }
        let new_i = Some(i);
        if last_i == new_i {
            sleep(RACE_SLEEP_DURATION);
        } else {
            last_i = new_i;
        }
    }
    Ok(())
}

#[test]
fn test_writeback_mmap() -> anyhow::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        // Don't truncate because we're about to it anyway.
        .truncate(false)
        .open("writeback")?;
    file.set_len(0x1000)?;
    let read_file = OpenOptions::new().read(true).open("writeback")?;
    let mmap = unsafe {
        memmap2::MmapOptions::new()
            .len(0x1000)
            .map_copy_read_only(&read_file)
    }?;
    file.write_all(&mmap)?;
    Ok(())
}
