use self::test;
use super::*;
use std::thread::sleep;
use tempfile::tempdir;

#[test]
fn test_to_usize_io() -> Result<()> {
    // Check u32 MAX converts to u32 (usize on 32 bit system) without error.
    assert_eq!(convert_int_io::<_, u32>(u32::MAX as u64)?, u32::MAX);
    // Check that u64 out of u32 bounds fails.
    if let Err(err) = convert_int_io::<_, u32>(u32::MAX as u64 + 1) {
        assert_eq!(err.kind(), TO_USIZE_IO_ERROR_KIND);
        // Check that TryFromIntError isn't leaked.
        assert!(err
            .get_ref()
            .unwrap()
            .downcast_ref::<TryFromIntError>()
            .is_none());
        assert_eq!(err.get_ref().unwrap().to_string(), TO_USIZE_IO_ERR_PAYLOAD);
    } else {
        panic!("expected failure")
    }
    // This checks that usize always converts to u64 (hope you don't have a 128 bit system). We
    // can't test u32 to u64 because it's infallible convert_int_io expects TryFromIntError.
    assert_eq!(
        convert_int_io::<_, u64>(u32::MAX as usize)?,
        u32::MAX as u64
    );
    Ok(())
}

#[test]
fn last_used_consistent_between_pending_writes() -> Result<()> {
    let handle = Handle::new(tempdir()?.into_path())?;
    let key1 = Vec::from("hello");
    let key2 = "hola".as_bytes().to_vec();
    let value = "mundo".as_bytes();
    let mut writer = handle.new_writer()?;
    let mut value_writer_1 = writer.new_value().begin()?;
    assert_eq!(
        value.len(),
        value_writer_1.copy_from(value)?.try_into().unwrap()
    );
    let mut value_writer_2 = writer.new_value().begin()?;
    assert_eq!(
        value.len(),
        value_writer_2.copy_from(value)?.try_into().unwrap()
    );
    writer.stage_write(key1.clone(), value_writer_1)?;
    writer.stage_write(key2.clone(), value_writer_2)?;
    writer.commit_inner(|| sleep(LAST_USED_RESOLUTION))?;
    let mut reader = handle.read()?;
    let first_ts = reader.add(&key1)?.unwrap().last_used();
    let second_ts = reader.add(&key2)?.unwrap().last_used();
    assert_eq!(first_ts, second_ts);
    Ok(())
}

#[test]
fn test_inc_array() {
    let inc_and_ret = |arr: &[u8]| {
        let mut arr = arr.to_vec();
        if inc_big_endian_array(&mut arr[..]) {
            Some(arr)
        } else {
            None
        }
    };
    assert_eq!(inc_and_ret(&[0]), Some(vec![1]));
    assert_eq!(inc_and_ret(&[]), None);
    assert_eq!(inc_and_ret(&[0xff]), None);
    assert_eq!(inc_and_ret(&[0xfe, 0xff]), Some(vec![0xff, 0]));
}
