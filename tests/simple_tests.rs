use anyhow::Result;
use possum::Handle;
use tempfile::tempdir;

#[test]
fn set_get() -> Result<()> {
    let tempdir = tempdir()?;
    let mut handle = Handle::new_from_dir(tempdir.path().to_owned())?;
    let value_bytes = "world".as_bytes();
    let mut writer = handle.new_writer()?;
    writer.stage_write("hello".as_bytes().to_owned(), value_bytes)?;
    let mut reader = handle.read()?;
    assert!(reader.add("hello".as_bytes()).is_ok_and(|ok| ok.is_none()));
    drop(reader);
    handle.commit(writer)?;
    let mut reader = handle.read()?;
    let value = reader.add("hello".as_bytes())?.expect("key should exist");
    let mut snapshot = reader.begin()?;
    snapshot.view(&value, |read_value_bytes| {
        assert_eq!(read_value_bytes, value_bytes)
    })?;
    Ok(())
}

#[test]
fn clone_in_file() -> Result<()> {
    let tempdir = tempdir()?;
    let mut handle = Handle::new_from_dir(tempdir.path().to_owned())?;
    let value_bytes = "world".as_bytes();
    let mut writer = handle.new_writer()?;
    writer.stage_write("hello".as_bytes().to_owned(), value_bytes)?;
    let mut reader = handle.read()?;
    assert!(reader.add("hello".as_bytes()).is_ok_and(|ok| ok.is_none()));
    drop(reader);
    handle.commit(writer)?;
    let mut reader = handle.read()?;
    let value = reader.add("hello".as_bytes())?.expect("key should exist");
    let mut snapshot = reader.begin()?;
    snapshot.view(&value, |read_value_bytes| {
        assert_eq!(read_value_bytes, value_bytes)
    })?;
    Ok(())
}
