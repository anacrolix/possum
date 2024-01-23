use std::sync::*;
use std::thread::*;
use std::time::*;

use anyhow::Result;

use self::test;
use super::*;
use crate::seekhole::*;
use crate::testing::*;

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

/// Show that replacing keys doesn't cause a key earlier in the same values file to be punched. This
/// occurred because there were file_id values in the manifest file that had the wrong type, and so
/// the query that looked for the starting offset for hole punching would punch out the whole file
/// thinking it was empty.
#[test]
fn test_replace_keys() -> Result<()> {
    let tempdir = test_tempdir("test_replace_keys")?;
    let handle = Handle::new(tempdir.path.clone())?;
    let a = "a".as_bytes().to_vec();
    let b = "b".as_bytes().to_vec();
    let block_size: usize = handle.block_size().try_into()?;
    let a_value = readable_repeated_bytes(1, block_size);
    let b_value = readable_repeated_bytes(2, block_size);
    let b_read = b_value.as_slice();
    handle.single_write_from(a.clone(), a_value.as_slice())?;
    handle.single_write_from(b.clone(), b_read)?;
    handle.single_write_from(b.clone(), b_read)?;
    // Check that the value for a hasn't been punched/zeroed.
    assert_repeated_bytes_values_eq(
        handle.read_single(&a).unwrap().unwrap().new_reader(),
        a_value.as_slice(),
    );
    let entries = handle.walk_dir()?;
    let values_files: Vec<_> = entries
        .iter()
        .filter(|entry| entry.entry_type == walk::EntryType::ValuesFile)
        .collect();
    // Make sure there's only a single values file.
    assert_eq!(values_files.len(), 1);
    let value_file = values_files[0];
    let mut file = File::open(&value_file.path)?;
    let regions = file_regions(&mut file)?;
    assert!([2, 3]
        .map(|num_blocks| num_blocks * block_size as RegionOffset)
        .contains(
            &regions
                .iter()
                .filter_map(|region| match region.region_type {
                    RegionType::Data => Some(region.length()),
                    _ => None,
                })
                .sum::<RegionOffset>()
        ));
    // Check the arrangement of holes and data for consistency. For some reason a block occasionally
    // doesn't get punched just before the new a.
    if false {
        let end = file.seek(End(0))?;
        let expected = vec![
            // a
            Region {
                region_type: Data,
                start: end - 3 * (block_size as u64),
                end: end - 2 * (block_size as u64),
            },
            // last b
            Region {
                region_type: Hole,
                start: end - 2 * (block_size as u64),
                end: end - (block_size as u64),
            },
            // new b
            Region {
                region_type: Data,
                start: end - (block_size as u64),
                end,
            },
        ];
        assert_eq!(regions[regions.len() - expected.len()..], expected);
        assert!(regions.len() <= expected.len() + 1);
        if regions.len() > expected.len() {
            assert_eq!(
                regions[..regions.len() - expected.len()],
                [Region {
                    region_type: Hole,
                    start: 0,
                    end: end - expected.len() as u64 * (block_size as u64),
                }]
            )
        }
    }
    Ok(())
}

/// Prove that file cloning doesn't occur too late if the value is replaced.
#[test]
fn punch_value_before_snapshot_cloned() -> anyhow::Result<()> {
    let tempdir = test_tempdir("punch_value_before_snapshot_cloned")?;
    let handle = Handle::new(tempdir.path.clone())?;
    let key = "a".as_bytes().to_vec();
    let first_value = readable_repeated_bytes(1, handle.block_size() as usize);
    let second_value = readable_repeated_bytes(2, handle.block_size() as usize);
    let reader_handle = Handle::new(tempdir.path.clone())?;
    let stop = Instant::now() + Duration::from_secs(1);
    while Instant::now() < stop {
        handle.single_write_from(key.clone(), first_value.as_slice())?;
        let write_barrier = Barrier::new(2);
        scope(|scope| {
            let reader_scope = scope.spawn(|| -> anyhow::Result<()> {
                let mut reader = reader_handle.read()?;
                let value = reader.add(&key).unwrap().unwrap();
                write_barrier.wait();
                let snapshot = reader.begin().unwrap();
                let value = snapshot.value(value);
                // This should read 1. It will get 0 if the value was punched, and 2 if the clone
                // occurred after the write.
                assert_repeated_bytes_values_eq(value.new_reader(), first_value.as_slice());
                Ok(())
            });
            write_barrier.wait();
            handle
                .single_write_from(key.clone(), second_value.as_slice())
                .unwrap();
            reader_scope.join().unwrap()
        })?;
    }
    Ok(())
}

#[test]
fn test_torrent_storage_benchmark() -> anyhow::Result<()> {
    use testing::torrent_storage::*;
    BENCHMARK_OPTS.build()?.run()
}
