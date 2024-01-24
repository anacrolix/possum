#![allow(clippy::unused_unit)]

use std::io;
use std::io::Read;

use anyhow::Result;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use possum::flock::try_lock_file;
use possum::testing::test_tempdir;
use possum::{flock, Handle};

pub fn benchmark_read_fallible(c: &mut Criterion) -> anyhow::Result<()> {
    let tempdir = test_tempdir("benchmark_get_exists")?;
    let handle = Handle::new(tempdir.path)?;
    let value_bytes = "world".as_bytes();
    handle.single_write_from("hello".as_bytes().to_owned(), value_bytes)?;
    let mut buf = vec![0; value_bytes.len() + 1];
    c.bench_function("read", |b| {
        b.iter(|| {
            (|| -> anyhow::Result<()> {
                let mut reader = handle.read()?;
                let value = reader.add("hello".as_bytes())?.expect("key should exist");
                let snapshot = reader.begin()?;
                let read_len = snapshot.value(&value).read(&mut buf)?;
                assert_eq!(read_len, value_bytes.len());
                Ok(())
            })()
            .unwrap()
        })
    });
    Ok(())
}

pub fn benchmark_view_fallible(c: &mut Criterion) -> anyhow::Result<()> {
    let tempdir = test_tempdir("benchmark_get_exists")?;
    let handle = Handle::new(tempdir.path)?;
    let value_bytes = "world".as_bytes();
    let key = "hello".as_bytes().to_vec();
    handle.single_write_from(key.clone(), value_bytes)?;
    c.bench_function("view", |b| {
        b.iter(|| {
            (|| -> anyhow::Result<()> {
                handle
                    .read_single(&key)?
                    .expect("key should exist")
                    .view(|view_bytes| assert_eq!(view_bytes, value_bytes))?;
                Ok(())
            })()
            .unwrap()
        })
    });
    Ok(())
}

pub fn benchmark_list_keys_fallible(c: &mut Criterion) -> Result<()> {
    c.bench_function("list", |b| -> () {
        (|| -> Result<()> {
            let tempdir = test_tempdir("benchmark_list_keys")?;
            let handle = Handle::new(tempdir.path)?;
            let prefix_range = 0..=100;
            let keys = |prefix| {
                (0..100)
                    .map(|suffix| {
                        format!("{}/{}", prefix, prefix_range.end() * prefix + suffix).into_bytes()
                    })
                    .collect::<Vec<_>>()
            };
            let mut writer = handle.new_writer()?;
            for prefix in prefix_range.clone() {
                for key in keys(prefix) {
                    let value = writer.new_value().begin()?;
                    writer.stage_write(key, value)?;
                }
            }
            writer.commit()?;
            let exists_key = 1;
            let exists_keys: Vec<String> = keys(exists_key)
                .into_iter()
                .map(|key| unsafe { String::from_utf8_unchecked(key) })
                .collect();
            let prefix = format!("{}/", exists_key).into_bytes();
            b.iter(|| {
                let items = handle.list_items(&prefix).unwrap();
                for item in &items {
                    assert_eq!(item.value.length(), 0);
                }
                assert_eq!(
                    items
                        .iter()
                        .map(|item| std::str::from_utf8(&item.key).unwrap())
                        .collect::<Vec<_>>(),
                    exists_keys
                );
            });
            Ok(())
        })()
        .unwrap()
    });
    Ok(())
}

// This is several benchmarks grouped together since I discovered that it doesn't make much
// difference anyway. One problem is that the initialization outside a bench/iter routine for any
// benchmark is always run, even if the benchmark itself isn't.
pub fn multiple_benchmarks_fallible(c: &mut Criterion) -> Result<()> {
    {
        let mut group = c.benchmark_group("batch_read");
        group.bench_function("existing", |b| {
            (|| -> Result<()> {
                let tempdir = test_tempdir("benchmark_read_multiple_keys")?;
                let handle = Handle::new(tempdir.path)?;
                let batch_size = 10;
                let keys = {
                    (0..batch_size)
                        .map(|suffix: i32| suffix.to_ne_bytes())
                        .collect::<Vec<_>>()
                };
                let mut writer = handle.new_writer()?;
                for key in &keys {
                    let mut value = writer.new_value().begin()?;
                    value.copy_from(&key[..])?;
                    writer.stage_write(key.to_vec(), value)?;
                }
                writer.commit()?;
                b.iter(|| -> () {
                    (|| -> Result<()> {
                        let mut reader = handle.read()?;
                        for key in &keys {
                            assert!(reader.add(key.as_slice())?.is_some());
                        }
                        reader.begin()?;
                        Ok(())
                    })()
                    .unwrap()
                });
                Ok(())
            })()
            .unwrap()
        });
        group.bench_function("missing", |b| {
            (|| -> Result<()> {
                let tempdir = test_tempdir("benchmark_read_multiple_keys")?;
                let handle = Handle::new(tempdir.path)?;
                let batch_size = 10;
                let keys = {
                    (0..batch_size)
                        .map(|suffix: i32| suffix.to_ne_bytes())
                        .collect::<Vec<_>>()
                };
                let mut writer = handle.new_writer()?;
                for key in &keys {
                    let mut value = writer.new_value().begin()?;
                    value.copy_from(&key[..])?;
                    writer.stage_write(key.to_vec(), value)?;
                }
                writer.commit()?;
                b.iter(|| -> () {
                    (|| -> Result<()> {
                        let mut reader = handle.read()?;
                        for key in (batch_size..batch_size * 2).map(i32::to_ne_bytes) {
                            assert!(reader.add(key.as_slice())?.is_none());
                        }
                        reader.begin()?;
                        Ok(())
                    })()
                    .unwrap()
                });
                Ok(())
            })()
            .unwrap()
        });
    }
    {
        let test_tempdir = test_tempdir("benchmark_transactions")?;
        let handle = Handle::new(test_tempdir.path)?;
        let mut group = c.benchmark_group("transactions");
        group.bench_function("read", |b| b.iter(|| handle.read().unwrap()));
        group.bench_function("writer", |b| {
            b.iter(|| handle.new_writer().unwrap().commit().unwrap())
        });
    }
    {
        let mut file = tempfile::tempfile().unwrap();
        c.benchmark_group("flock")
            .bench_function("exclusive nonblock", |b| {
                b.iter(|| -> () {
                    assert!(try_lock_file(&mut file, flock::LockExclusiveNonblock).unwrap());
                    // assert!(try_lock_file(&mut file, flock::UnlockNonblock).unwrap());
                })
            })
            .bench_function("shared nonblock", |b| {
                b.iter(|| -> () {
                    assert!(try_lock_file(&mut file, flock::LockSharedNonblock).unwrap());
                    // assert!(try_lock_file(&mut file, flock::UnlockNonblock).unwrap());
                })
            });
    }
    {
        let size = 64 << 20;
        c.benchmark_group("write_value")
            .throughput(Throughput::Bytes(size))
            .bench_function("write_very_large_value", |b| {
                (|| -> anyhow::Result<()> {
                    let tempdir = tempdir()?;
                    let handle = Handle::new(tempdir.path().to_path_buf())?;
                    b.iter(|| -> () {
                        (|| -> anyhow::Result<()> {
                            let mut writer = handle.new_writer()?;
                            let value = writer.new_value();
                            let mut value = value.begin()?;
                            let written = io::copy(&mut io::repeat(1).take(size), &mut value)?;
                            assert_eq!(written, size);
                            writer.stage_write("hello".as_bytes().to_vec(), value)?;
                            writer.commit()?;
                            Ok(())
                        })()
                        .unwrap()
                    });
                    Ok(())
                })()
                .unwrap()
            });
    }
    Ok(())
}

// This might be made generic using the Try trait.
fn unwrap_fallible(
    f: impl FnOnce(&mut Criterion) -> anyhow::Result<()>,
) -> impl FnOnce(&mut Criterion) {
    move |c| f(c).unwrap()
}

fn benchmark_read(c: &mut Criterion) {
    unwrap_fallible(benchmark_read_fallible)(c)
}

fn benchmark_view(c: &mut Criterion) {
    unwrap_fallible(benchmark_view_fallible)(c)
}

fn benchmark_list_keys(c: &mut Criterion) {
    unwrap_fallible(benchmark_list_keys_fallible)(c)
}

fn multiple_benchmarks(c: &mut Criterion) {
    unwrap_fallible(multiple_benchmarks_fallible)(c)
}

mod clonefile;
mod torrent_storage;
use std::time::Duration;

use tempfile::tempdir;
criterion_group!(
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = benchmark_read,
    benchmark_view,
    benchmark_list_keys,
    multiple_benchmarks,
    clonefile::clonefile_benchmark,
    torrent_storage::benchmark,
);
criterion_main!(benches);
