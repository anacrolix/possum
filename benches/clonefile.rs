use anyhow::ensure;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use possum::clonefile::clonefile;
use rand::Rng;
use std::fs::remove_file;
use std::io::{Seek, SeekFrom, Write};
use tempfile::NamedTempFile;

pub fn clonefile_benchmark_fallible(c: &mut Criterion) -> anyhow::Result<()> {
    for size_power in [12, 16, 20, 24, 28] {
        let mut file = NamedTempFile::new()?;
        let len: u64 = 1 << size_power;
        let mut rng = rand::thread_rng();
        let mut buf = [0; 4096];
        let mut remaining_size = len;
        while remaining_size > 0 {
            let n1 = std::cmp::min(remaining_size, buf.len() as u64).try_into()?;
            let buf1 = &mut buf[..n1];
            rng.fill(buf1);
            file.write(buf1)?;
            remaining_size -= n1 as u64;
        }
        ensure!(file.as_file().seek(SeekFrom::End(0))? == len);
        let dst_path = "hello";
        c.bench_with_input(
            BenchmarkId::new("hello", bytesize::ByteSize(len).to_string_as(true)),
            &file,
            |b, file| {
                b.iter(|| {
                    (|| -> anyhow::Result<()> {
                        remove_file(dst_path)?;
                        clonefile(file.path(), dst_path.as_ref())?;
                        Ok(())
                    })()
                    .unwrap()
                })
            },
        );
    }
    Ok(())
}

fn clonefile_benchmark(c: &mut Criterion) {
    clonefile_benchmark_fallible(c).unwrap()
}

criterion_group!(benches, clonefile_benchmark);
criterion_main!(benches);
