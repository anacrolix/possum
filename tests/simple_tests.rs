use anyhow::{anyhow, Context, Result};
use possum::testing::*;
use possum::*;
use rand::distributions::uniform::{UniformDuration, UniformSampler};
use rand::{thread_rng, RngCore};
use std::error::Error;
use std::fmt::{Debug, Display};
use std::hash::Hasher;
use std::io::Read;
use std::io::SeekFrom::Start;
use std::io::{Seek, Write};
use std::ops::Bound::Included;
use std::ops::{RangeBounds, RangeInclusive};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::str::FromStr;
use std::thread::{scope, sleep};
use std::time::Duration;
use tempfile::tempdir;

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
    let mut snapshot = reader.begin()?;
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
    let mut snapshot = reader.begin()?;
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
            .read_single(key.to_owned())?
            .context("item should exist")?
            .new_reader(),
        file,
    )?;
    Ok(())
}

#[test]
fn torrent_storage() -> Result<()> {
    let tempdir = PathBuf::from("torrent_storage");
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
    let block_size = 1 << 14;
    let block_offset_iter = (0..piece_size).step_by(block_size);
    let offset_key = |offset| format!("piece/{}", offset);
    std::thread::scope(|scope| {
        let mut join_handles = vec![];
        for offset in block_offset_iter.clone() {
            let handle = &handle;
            let piece_data = &piece_data;
            join_handles.push(scope.spawn(move || -> Result<()> {
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
    let mut reader = handle.read()?;
    let values = block_offset_iter
        .map(|offset| {
            anyhow::Ok(
                reader
                    .add(offset_key(offset).as_ref())?
                    .ok_or(anyhow!("missing value"))?,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut snapshot = reader.begin()?;
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
        .read_single(completed_key.clone())?
        .expect("completed item should exist");
    dbg!(&completed_value);
    let completed_reader = completed_value.new_reader();
    let completed_hash = hash_reader(completed_reader)?;
    assert_eq!(completed_hash, piece_data_hash);
    Ok(())
}

#[test]
fn big_set_get() -> Result<()> {
    let tempdir = PathBuf::from("torrent_storage");
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
        .read_single(completed_key.clone())?
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
fn reads_update_last_used() -> Result<()> {
    let handle = Handle::new(tempdir()?.into_path())?;
    let key = Vec::from("hello");
    let value = "mundo".as_bytes();
    let (n, write_ts) = handle.single_write_from(key.clone(), value)?;
    assert_eq!(n, 5);
    let read_ts = *handle.read_single(key.clone())?.unwrap().last_used();
    assert!(read_ts >= write_ts);
    let mut rng = thread_rng();
    let uniform = UniformDuration::new(Duration::from_nanos(0), LAST_USED_RESOLUTION);
    for _ in 0..100 {
        let dither = uniform.sample(&mut rng);
        sleep(LAST_USED_RESOLUTION + dither);
        let new_read_ts = *handle.read_single(key.clone())?.unwrap().last_used();
        assert!(new_read_ts > read_ts);
    }
    Ok(())
}

#[test]
fn read_and_writes_different_handles() -> Result<()> {
    let range = 0..=10;
    // let tempdir = tempdir()?;
    // let dir = tempdir.path().to_owned();
    let dir = PathBuf::from("herp");
    let key = "incr".as_bytes();
    scope(|scope| {
        let reader = scope.spawn(|| read_consecutive_integers(dir.clone(), key, &range));
        let range = range.clone();
        let writer = scope.spawn(|| write_consecutive_integers(dir.clone(), key, range));
        reader.join().unwrap()?;
        writer.join().unwrap()?;
        anyhow::Ok(())
    })
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
        let Some(value) = handle.read_single(key.to_owned())? else {
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
