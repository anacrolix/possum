use std::time::*;

use anyhow::Result;
use rusqlite::{OpenFlags, TransactionBehavior, TransactionState};

use self::test;
use super::*;
use crate::concurrency::sync::Barrier;
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
/// thinking it was empty. Note sometimes this test fails and there's extra values files floating
/// around. I haven't figured out why.
#[test]
#[cfg(not(miri))]
fn test_replace_keys() -> Result<()> {
    check_concurrency(
        || {
            let tempdir = test_tempdir("test_replace_keys")?;
            let handle = Handle::new(tempdir.path.clone())?;
            handle.delete_prefix("")?;
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

            let dir = handle.dir.clone();
            let values_punched = handle.get_value_puncher_done();
            drop(handle);
            // Wait for it to recv, which should be a disconnect when the value_puncher hangs up.
            values_punched.wait();

            let entries = dir.walk_dir()?;
            let values_files: Vec<_> = entries
                .iter()
                .filter(|entry| entry.entry_type == walk::EntryType::ValuesFile)
                .collect();

            let mut allocated_space = 0;
            // There can be multiple value files if the value puncher is holding onto a file when another
            // write occurs.
            for value_file in values_files {
                let path = &value_file.path;
                eprintln!("{:?}", path);
                let mut file = File::open(path)?;
                // file.sync_all()?;
                for region in seekhole::Iter::new(&mut file) {
                    let region = region?;
                    eprintln!("{:?}", region);
                    if matches!(region.region_type, seekhole::RegionType::Data) {
                        allocated_space += region.length();
                    }
                }
            }
            assert!(
                [2].map(|num_blocks| num_blocks * block_size as seekhole::RegionOffset)
                    .contains(&allocated_space),
                "block_size={}, allocated_space={}",
                block_size,
                allocated_space
            );
            Ok(())
        },
        100,
    )
}

/// Prove that file cloning doesn't occur too late if the value is replaced.
#[test]
#[cfg(not(miri))]
fn punch_value_before_snapshot_cloned() -> anyhow::Result<()> {
    check_concurrency(
        || {
            let tempdir = test_tempdir("punch_value_before_snapshot_cloned")?;
            let handle = Handle::new(tempdir.path.clone())?;
            let key = "a".as_bytes().to_vec();
            let first_value = readable_repeated_bytes(1, handle.block_size() as usize);
            let second_value = readable_repeated_bytes(2, handle.block_size() as usize);
            let reader_handle = Arc::new(Handle::new(tempdir.path.clone())?);
            let stop = Instant::now() + Duration::from_secs(1);
            while Instant::now() < stop {
                handle.single_write_from(key.clone(), first_value.as_slice())?;
                let write_barrier = Arc::new(Barrier::new(2));
                let reader_handle = reader_handle.clone();
                let first_value = first_value.clone();
                let reader_scope = {
                    let key = key.clone();
                    let write_barrier = write_barrier.clone();
                    thread::spawn(move || -> () {
                        let mut reader = reader_handle.read().unwrap();
                        let value = reader.add(&key).unwrap().unwrap();
                        write_barrier.wait();
                        let snapshot = reader.begin().unwrap();
                        let value = snapshot.value(value);
                        // This should read 1. It will get 0 if the value was punched, and 2 if the clone
                        // occurred after the write.
                        assert_repeated_bytes_values_eq(value.new_reader(), first_value.as_slice());
                    })
                };
                write_barrier.wait();
                handle.single_write_from(key.clone(), second_value.as_slice())?;
                reader_scope.join().unwrap();
            }
            Ok(())
        },
        10,
    )
}

#[test]
#[cfg(not(miri))]
fn test_torrent_storage_benchmark() -> anyhow::Result<()> {
    use testing::torrent_storage::*;
    check_concurrency(|| BENCHMARK_OPTS.build()?.run(), 10)
}

/// Show that update moves a transaction to write, even if nothing is changed. This was an
/// investigation on how to optimize touch_for_read if last_used doesn't change.
#[test]
fn test_sqlite_update_same_value_txn_state() -> Result<()> {
    let mut conn = rusqlite::Connection::open_in_memory()?;
    conn.execute_batch(
        r"
        create table a(b);
        --insert into a values (1);
        ",
    )?;
    let tx = conn.transaction()?;
    assert_eq!(tx.transaction_state(None)?, TransactionState::None);
    let changed = tx.execute("update a set b=1", [])?;
    // No rows were changed.
    assert_eq!(changed, 0);
    // But now we're a write transaction anyway.
    assert_eq!(tx.transaction_state(None)?, TransactionState::Write);
    Ok(())
}

/// Verify that punch_value does not remove a values file when newer data has been written to it
/// after the read transaction snapshot was taken. The race: the puncher's snapshot was taken before
/// the new write was committed, so it sees no live values in the file, acquires the exclusive lock
/// (which is available because the writer has already released it), and would incorrectly remove
/// the file — destroying the new data.
#[test]
#[cfg(not(miri))]
fn test_punch_stale_snapshot_no_remove_if_new_data() -> anyhow::Result<()> {
    let tempdir = test_tempdir("test_punch_stale_snapshot_no_remove_if_new_data")?;
    let handle = Handle::new(tempdir.path.clone())?;
    let block_size = handle.block_size() as usize;

    // Write A (one block).
    let a_value = readable_repeated_bytes(1, block_size);
    handle.single_write_from(b"a".to_vec(), a_value.as_slice())?;

    // Record A's storage location before deleting it.
    let items = handle.list_items(b"")?;
    let a_loc = items
        .iter()
        .find(|i| i.key == b"a")
        .unwrap()
        .value
        .location
        .into_non_zero()
        .unwrap();

    handle.single_delete(b"a")?;

    // Open a separate read-only connection and begin a deferred transaction, then force a read to
    // anchor the WAL snapshot. This snapshot sees no live values — A is deleted and B is not yet
    // written.
    let manifest_path = handle.dir.path().join(MANIFEST_DB_FILE_NAME);
    let mut stale_conn = rusqlite::Connection::open_with_flags(
        manifest_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    let stale_tx_inner = stale_conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    let stale_tx = ReadTransactionOwned(stale_tx_inner);
    // Force the snapshot to be taken now (deferred transactions snapshot on first read).
    let _ = stale_tx.sum_value_length()?;

    // Write B — this reuses the same ExclusiveFile from the pool, placing B in the same file.
    let b_value = readable_repeated_bytes(2, block_size);
    handle.single_write_from(b"b".to_vec(), b_value.as_slice())?;

    // Confirm B landed in the same file as A.
    let items = handle.list_items(b"")?;
    let b_loc = items
        .iter()
        .find(|i| i.key == b"b")
        .unwrap()
        .value
        .location
        .into_non_zero()
        .unwrap();
    assert_eq!(
        b_loc.file_id, a_loc.file_id,
        "B should reuse A's values file"
    );

    // Release the exclusive-file lock so that lock_max_segment can succeed in punch_value.
    handle.exclusive_files.lock().unwrap().clear();

    // Punch A using the stale snapshot. punch_value will see no live values in the file and
    // acquire the whole-file lock — it must NOT remove the file because B is committed there.
    Handle::punch_values(&handle.dir, None, vec![a_loc], &stale_tx)?;

    // The file must still exist.
    let vfile_path = file_path(handle.dir.path(), &a_loc.file_id);
    assert!(vfile_path.exists(), "values file was incorrectly removed");

    // B must still be readable with the correct content.
    assert_repeated_bytes_values_eq(
        handle.read_single(b"b").unwrap().unwrap().new_reader(),
        b_value.as_slice(),
    );

    Ok(())
}

/// Verify that the FileClone cache is cleaned up when punch_value removes a values file. Stale
/// cache entries would cause readers to mmap a deleted (and potentially reused) file.
#[test]
#[cfg(not(miri))]
fn test_clone_cache_cleaned_on_file_removal() -> anyhow::Result<()> {
    let tempdir = test_tempdir("test_clone_cache_cleaned_on_file_removal")?;
    let handle = Handle::new(tempdir.path.clone())?;
    let block_size = handle.block_size() as usize;

    // Write A (one block).
    let a_value = readable_repeated_bytes(1, block_size);
    handle.single_write_from(b"a".to_vec(), a_value.as_slice())?;

    // Record A's storage location before deleting it.
    let items = handle.list_items(b"")?;
    let a_loc = items
        .iter()
        .find(|i| i.key == b"a")
        .unwrap()
        .value
        .location
        .into_non_zero()
        .unwrap();

    // Insert a fake FileClone entry so we can check it gets cleaned up when the file is removed.
    let vfile_path = file_path(handle.dir.path(), &a_loc.file_id);
    let file = File::open(&vfile_path)?;
    handle.clones.lock().unwrap().insert(
        a_loc.file_id,
        Arc::new(Mutex::new(FileClone {
            file,
            tempdir: None,
            mmap: None,
            len: a_loc.length,
        })),
    );

    // Delete A from the manifest. Use a manual transaction and drop PostCommitWork without
    // calling complete() so the value_puncher is not notified and cannot race with us.
    {
        let mut tx = handle.start_deferred_transaction()?;
        tx.delete_key(b"a")?;
        drop(tx.commit()?);
    }

    // Drain the exclusive-file pool so lock_max_segment can succeed.
    handle.exclusive_files.lock().unwrap().clear();

    // Open a fresh (non-stale) read-only connection so punch_value sees no live values.
    let manifest_path = handle.dir.path().join(MANIFEST_DB_FILE_NAME);
    let mut conn = rusqlite::Connection::open_with_flags(
        manifest_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    let tx_inner = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    let tx = ReadTransactionOwned(tx_inner);

    Handle::punch_values(&handle.dir, Some(&handle.clones), vec![a_loc], &tx)?;

    // The file must have been removed.
    assert!(!vfile_path.exists(), "values file should have been removed");

    // The clone cache entry for this file must have been cleaned up.
    assert!(
        !handle.clones.lock().unwrap().contains_key(&a_loc.file_id),
        "FileClone cache entry was not removed when the values file was deleted"
    );

    Ok(())
}

/// The value puncher thread must not keep the clone cache alive after the Handle is dropped.
/// Otherwise the cache's FileClones keep snapshot dirs around after every Value using them is
/// dropped, which made tests/simple_tests.rs cleanup_snapshots flaky.
#[test]
#[cfg(not(miri))]
fn test_dropped_handle_releases_clone_cache() -> anyhow::Result<()> {
    let tempdir = test_tempdir("test_dropped_handle_releases_clone_cache")?;
    for _ in 0..100 {
        let handle = Handle::new(tempdir.path.clone())?;
        let clones = Arc::downgrade(&handle.clones);
        drop(handle);
        assert!(
            clones.upgrade().is_none(),
            "clone cache outlived its Handle"
        );
    }
    Ok(())
}
