use criterion::{criterion_group, criterion_main, Criterion};
use possum::Handle;
use std::path::PathBuf;

mod clonefile;

pub fn benchmark_read_fallible(c: &mut Criterion) -> anyhow::Result<()> {
    let tempdir = PathBuf::from("benchmark_get_exists");
    let handle = Handle::new_from_dir(tempdir)?;
    let value_bytes = "world".as_bytes();
    handle.single_write_from("hello".as_bytes().to_owned(), value_bytes)?;
    let mut buf = vec![0; value_bytes.len() + 1];
    c.bench_function("read", |b| {
        b.iter(|| {
            (|| -> anyhow::Result<()> {
                let mut reader = handle.read()?;
                let value = reader.add("hello".as_bytes())?.expect("key should exist");
                let mut snapshot = reader.begin()?;
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
    let handle = Handle::new_from_dir(tempdir)?;
    let value_bytes = "world".as_bytes();
    handle.single_write_from("hello".as_bytes().to_owned(), value_bytes)?;
    c.bench_function("view", |b| {
        b.iter(|| {
            (|| -> anyhow::Result<()> {
                let mut reader = handle.read()?;
                let value = reader.add("hello".as_bytes())?.expect("key should exist");
                let mut snapshot = reader.begin()?;
                snapshot
                    .value(&value)
                    .view(|read_value_bytes| assert_eq!(read_value_bytes, value_bytes))?;
                Ok(())
            })()
            .unwrap()
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

criterion_group!(
    benches,
    benchmark_read,
    benchmark_view,
    clonefile::clonefile_benchmark
);
criterion_main!(benches);
