use criterion::{criterion_group, criterion_main, Criterion};
use possum::Handle;
use std::path::PathBuf;
use tempfile::tempdir;

pub fn benchmark_get_exists_fallible(c: &mut Criterion) -> anyhow::Result<()> {
    let tempdir = PathBuf::from("benchmark_get_exists");
    let mut handle = Handle::new_from_dir(tempdir)?;
    let value_bytes = "world".as_bytes();
    handle.stage_write("hello".as_bytes().to_owned(), value_bytes)?;
    handle.flush_writes()?;
    c.bench_function("get_exists", |b| {
        b.iter(|| {
            (|| -> anyhow::Result<()> {
                let mut reader = handle.read()?;
                let value = reader.add("hello".as_bytes())?.expect("key should exist");
                let mut snapshot = reader.begin()?;
                let read_value_bytes = snapshot.view(&value)?;
                assert_eq!(read_value_bytes, value_bytes);
                Ok(())
            })()
            .unwrap()
        })
    });
    Ok(())
}

pub fn benchmark_get_exists(c: &mut Criterion) {
    benchmark_get_exists_fallible(c).unwrap();
}

criterion_group!(benches, benchmark_get_exists);
criterion_main!(benches);
