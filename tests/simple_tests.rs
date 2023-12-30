use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Debug, Display};
use std::hash::Hasher;
use std::io::Read;
use std::io::SeekFrom::Start;
use std::io::{Seek, Write};
use std::ops::Bound::Included;
use std::ops::{RangeBounds, RangeInclusive};
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::str::FromStr;
use std::thread::sleep;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use fdlimit::raise_fd_limit;
use itertools::Itertools;
use log::debug;
use possum::testing::*;
use possum::walk::{walk_dir, EntryType};
use possum::Error::NoSuchKey;
use possum::*;
use rand::distributions::uniform::{UniformDuration, UniformSampler};
use rand::{thread_rng, RngCore, SeedableRng};
use tempfile::{tempdir, TempDir};
use test_log::test;

#[test]
fn rename_key() -> Result<()> {
    let tempdir = tempdir()?;
    let mut handle = Handle::new(tempdir.path().to_owned())?;
    let value_bytes = "world".as_bytes();
    let rename_res = handle
        .rename_item("noexist".as_bytes(), "borat".as_bytes())
        .map(|_| ());
    assert_eq!(rename_res, Err(NoSuchKey));
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
        .map(|mut entry: possum::WalkEntry| {
            entry.path = entry
                .path
                .strip_prefix(handle.dir())
                .expect("walk entry should have handle dir path prefix")
                .to_owned();
            entry
        })
        .collect()
}

#[test]
fn set_get() -> Result<()> {
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
}

#[test]
fn set_get_reader() -> Result<()> {
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
}

#[test]
fn clone_in_file() -> Result<()> {
    let tempdir = tempdir()?;
    let mut handle = Handle::new(tempdir.path().to_owned())?;
    let mut file = write_random_tempfile(42069)?;
    let key = "hi\x00elon".as_bytes();
    handle.clone_from_fd(key.to_owned(), file.as_raw_fd())?;
    file.seek(Start(0))?;
    compare_reads(
        handle
            .read_single(key)?
            .context("item should exist")?
            .new_reader(),
        file,
    )?;
    Ok(())
}

/// Keep this in scope so the tempdir isn't deleted right while the path is still in use.
struct TestTempDir {
    _tempdir: Option<TempDir>,
    path: PathBuf,
}

fn test_tempdir(name: &'static str) -> Result<TestTempDir> {
    let (tempdir, path) = if true {
        (None, PathBuf::from(name))
    } else {
        let tempdir = tempdir()?;
        let path = tempdir.path().to_owned();
        (Some(tempdir), path)
    };
    dbg!(&path);
    Ok(TestTempDir {
        _tempdir: tempdir,
        path,
    })
}

use std::prelude::rust_2021::test as std_test;

struct TorrentStorageOpts {
    piece_size: usize,
    block_size: usize,
    static_tempdir_name: &'static str,
}

#[std_test]
fn torrent_storage_small() -> Result<()> {
    let block_size = 4096;
    torrent_storage_inner(TorrentStorageOpts {
        block_size,
        piece_size: 4 * block_size,
        static_tempdir_name: "torrent_storage_small",
    })
}

#[std_test]
fn torrent_storage_big() -> Result<()> {
    let block_size = 4096;
    torrent_storage_inner(TorrentStorageOpts {
        block_size,
        piece_size: 2 << 20,
        static_tempdir_name: "torrent_storage_big",
    })
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
    std::thread::scope(|scope| {
        let mut join_handles = vec![];
        for (index, offset) in block_offset_iter.clone().enumerate() {
            let piece_data = &piece_data;
            let start_delay = Duration::from_micros(1000 * (index / 2) as u64);
            let handle = &handle;
            join_handles.push(scope.spawn(move || -> Result<()> {
                let key = offset_key(offset);
                sleep(start_delay);
                debug!("starting block write");
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
        .collect::<Result<Vec<_>, _>>()?;
    let snapshot = reader.begin()?;
    let mut stored_hash = Hash::default();
    let mut writer = handle.new_writer()?;
    let mut completed = writer.new_value().begin()?;
    dbg!(&completed);
    for value in values {
        snapshot.value(value).view(|bytes| {
            stored_hash.write(bytes);
            completed.write_all(bytes)
        })??;
    }
    dbg!(&completed);
    assert_eq!(stored_hash.finish(), piece_data_hash);
    let completed_key = format!("completed/{:x}", piece_data_hash).into_bytes();
    writer.stage_write(completed_key.clone(), completed)?;
    writer.commit()?;
    let completed_value = handle
        .read_single(&completed_key)?
        .expect("completed item should exist");
    dbg!(&completed_value);
    let completed_reader = completed_value.new_reader();
    let completed_hash = hash_reader(completed_reader)?;
    assert_eq!(completed_hash, piece_data_hash);
    let handle_walk_entries = handle.walk_dir()?;
    let _counts = count_by_entry_types(&handle_walk_entries);
    // ValuesFile count calculation might need changing if this doesn't hold.
    assert_eq!(block_size as u64 % handle.block_size(), 0);
    // This might all be reusable as a Handle current disk usage calculation.
    let mut values_file_total_len = 0;
    for entry in &handle_walk_entries {
        if entry.entry_type != ValuesFile {
            continue;
        }
        let metadata = std::fs::metadata(&entry.path)?;
        //dbg!(metadata.blocks(), metadata.blksize());
        values_file_total_len += if false {
            metadata.len()
        } else {
            metadata.blocks() * 512
        };
    }
    assert_eq!(values_file_total_len, 2 * piece_size as u64);
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
) -> impl Iterator<Item = usize> + '_ {
    offsets.filter(move |offset| offset.to_string().starts_with(prefix))
}

#[test]
fn big_set_get() -> Result<()> {
    let tempdir = PathBuf::from("big_set_get");
    dbg!(&tempdir);
    let handle = Handle::new(tempdir)?;
    let piece_size = 2 << 20;
    let mut piece_data = vec![0; piece_size];
    thread_rng().fill_bytes(&mut piece_data);
    let piece_data_hash = {
        let mut hash = Hash::default();
        hash.write(&piece_data);
        hash.finish()
    };
    dbg!(piece_data_hash);
    let completed_key = format!("completed/{:x}", piece_data_hash).into_bytes();
    handle.single_write_from(completed_key.clone(), &*piece_data)?;
    let completed_value = handle
        .read_single(&completed_key)?
        .expect("completed item should exist");
    dbg!(&completed_value);
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
}

#[test]
fn cleanup_snapshots() -> Result<()> {
    let tempdir = test_tempdir("cleanup_snapshots")?;
    let count_snapshot_dirs = || {
        walk_dir(tempdir.path.clone())
            .unwrap()
            .iter()
            .filter(|entry| entry.entry_type == SnapshotDir)
            .count()
    };
    let handle = Handle::new(tempdir.path.clone())?;
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
    let _handle = Handle::new(tempdir.path.clone());
    assert_eq!(count_snapshot_dirs(), 0);
    Ok(())
}

#[test]
fn reads_update_last_used() -> Result<()> {
    let handle = Handle::new(tempdir()?.into_path())?;
    let key = Vec::from("hello");
    let value = "mundo".as_bytes();
    let (n, write_ts) = handle.single_write_from(key.clone(), value)?;
    assert_eq!(n, 5);
    let read_ts = *handle.read_single(&key)?.unwrap().last_used();
    assert!(read_ts >= write_ts);
    let mut rng = thread_rng();
    let uniform = UniformDuration::new(Duration::from_nanos(0), LAST_USED_RESOLUTION);
    for _ in 0..100 {
        let dither = uniform.sample(&mut rng);
        sleep(LAST_USED_RESOLUTION + dither);
        let new_read_ts = *handle.read_single(&key)?.unwrap().last_used();
        assert!(new_read_ts > read_ts);
    }
    Ok(())
}

#[test]
fn read_and_writes_different_handles() -> Result<()> {
    let range = 0..=10;
    let tempdir = test_tempdir("read_and_writes_different_handles")?;
    let dir = tempdir.path;
    let key = "incr".as_bytes();
    std::thread::scope(|scope| {
        let reader = scope.spawn(|| read_consecutive_integers(dir.clone(), key, &range));
        let range = range.clone();
        let writer = scope.spawn(|| write_consecutive_integers(dir.clone(), key, range));
        reader.join().unwrap()?;
        writer.join().unwrap()?;
        anyhow::Ok(())
    })?;
    let handle = Handle::new(dir)?;
    let keys: Vec<_> = handle
        .list_items("".as_bytes())?
        .into_iter()
        .map(|item| item.key)
        .collect();
    assert_eq!(keys, vec!["incr".as_bytes()]);
    Ok(())
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

fn read_consecutive_integers<I>(dir: PathBuf, key: &[u8], range: &RangeInclusive<I>) -> Result<()>
where
    I: FromStr + Debug + PartialOrd + Display,
    <I as FromStr>::Err: Error + Send + Sync + 'static,
{
    let handle = Handle::new(dir)?;
    let Included(end_i) = range.end_bound() else {
        panic!("expected inclusive range: {:?}", range);
    };
    sleep(RACE_SLEEP_DURATION);
    let mut last_i = None;
    loop {
        let Some(value) = handle.read_single(key)? else {
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
