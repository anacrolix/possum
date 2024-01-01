use self::test;
use super::*;
use crate::testing::test_tempdir;

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

#[test]
fn test_replace_keys() -> Result<()> {
    let tempdir = test_tempdir("test_replace_keys")?;
    let handle = Handle::new(tempdir.path)?;
    let value_for_key = |key| itertools::repeat_n(key as u8, key).collect::<Vec<u8>>();
    let key_range = 0..=1000;
    for _ in 0..2 {
        let mut written = 0;
        for key in key_range.clone() {
            let value = value_for_key(key);
            written += value.len();
            handle.single_write_from(key.to_string().into_bytes(), &*value)?;
        }
        assert!(written >= 4096);
    }
    for key in key_range.clone() {
        let mut value = Default::default();
        handle
            .read_single(key.to_string().as_bytes())?
            .unwrap()
            .new_reader()
            .read_to_end(&mut value)?;
        assert_eq!(value, value_for_key(key));
    }
    Ok(())
}
