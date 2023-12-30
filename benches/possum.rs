use std::path::PathBuf;

use anyhow::Result;
use criterion::{criterion_group, criterion_main, Criterion};
use possum::testing::test_tempdir;
use possum::Handle;

mod clonefile;

pub fn benchmark_read_fallible(c: &mut Criterion) -> anyhow::Result<()> {
    let tempdir = PathBuf::from("benchmark_get_exists");
    let handle = Handle::new(tempdir)?;
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
    let tempdir = PathBuf::from("benchmark_get_exists");
    let handle = Handle::new(tempdir)?;
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
            // handle.single_write_from(key, "".as_bytes())?;
        }
    }
    writer.commit()?;
    let exists_key = 1;
    let exists_keys: Vec<String> = keys(exists_key)
        .into_iter()
        .map(|key| unsafe { String::from_utf8_unchecked(key) })
        .collect();
    let prefix = format!("{}/", exists_key).into_bytes();
    c.bench_function("list", |b| {
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
        })
    });
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

criterion_group!(
    benches,
    benchmark_read,
    benchmark_view,
    benchmark_list_keys,
    clonefile::clonefile_benchmark
);
criterion_main!(benches);
