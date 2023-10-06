use possum::Handle;
use tempfile::tempdir;

#[test]
fn set_get() -> anyhow::Result<()> {
    let tempdir = tempdir()?;
    let mut handle = Handle::new_from_dir(tempdir.path().to_owned())?;
    let value_bytes = "world".as_bytes();
    handle.stage_write("hello".as_bytes().to_owned(), value_bytes)?;
    let mut reader = handle.read()?;
    assert!(reader.add("hello".as_bytes()).is_ok_and(|ok| ok.is_none()));
    drop(reader);
    handle.flush_writes()?;
    let mut reader = handle.read()?;
    let value = reader.add("hello".as_bytes())?.expect("key should exist");
    let mut snapshot = reader.begin()?;
    snapshot.view(&value, |read_value_bytes| {
        assert_eq!(read_value_bytes, value_bytes)
    })?;
    Ok(())
}
