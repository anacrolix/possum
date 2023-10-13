use anyhow::{Context, Result};
use possum::testing::{compare_reads, write_random_tempfile};
use possum::Handle;
use std::io::SeekFrom::Start;
use std::io::{Seek, Write};
use std::os::fd::AsRawFd;
use tempfile::tempdir;

#[test]
fn set_get() -> Result<()> {
    let tempdir = tempdir()?;
    let mut handle = Handle::new_from_dir(tempdir.path().to_owned())?;
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
