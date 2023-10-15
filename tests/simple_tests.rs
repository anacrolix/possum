use anyhow::{anyhow, Context, Result};
use possum::testing::{compare_reads, hash_reader, write_random_tempfile, Hash};
use possum::Handle;
use rand::{thread_rng, RngCore};
use std::hash::Hasher;
use std::io::Read;
use std::io::SeekFrom::Start;
use std::io::{Seek, Write};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use tempfile::tempdir;

#[test]
fn set_get() -> Result<()> {
    let tempdir = tempdir()?;
    let handle = Handle::new_from_dir(tempdir.path().to_owned())?;
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
    let handle = Handle::new_from_dir(tempdir.path().to_owned())?;
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
    let mut handle = Handle::new_from_dir(tempdir.path().to_owned())?;
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
    let handle = Handle::new_from_dir(tempdir)?;
    let piece_size = 2 << 20;
    let mut piece_data = vec![0; piece_size];
    thread_rng().fill_bytes(&mut piece_data);
    let piece_data_hash = {
        let mut hash = Hash::default();
        hash.write(&*piece_data);
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
    let handle = Handle::new_from_dir(tempdir)?;
    let piece_size = 2 << 20;
    let mut piece_data = vec![0; piece_size];
    thread_rng().fill_bytes(&mut piece_data);
    let piece_data_hash = {
        let mut hash = Hash::default();
        hash.write(&*piece_data);
        hash.finish()
    };
    dbg!(piece_data_hash);
    let completed_key = format!("completed/{:x}", piece_data_hash).into_bytes();
    handle.single_write_from(completed_key.clone(), &*piece_data)?;
    let mut completed_value = handle
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
