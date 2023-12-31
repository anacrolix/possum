use std::fs::remove_file;

use criterion::{BenchmarkId, Criterion};
use possum::clonefile::clonefile;
use possum::testing::test_tempdir;

fn clonefile_benchmark_fallible(c: &mut Criterion) -> anyhow::Result<()> {
    let tempdir = test_tempdir("benchmark_clonefile")?;
    for size_power in [12, 20, 28] {
        let len = 1 << size_power;
        let file = possum::testing::write_random_tempfile(len)?;
        let dst_path = tempdir.path.join("hello");
        let mut group = c.benchmark_group("clonefile");
        group.bench_with_input(
            BenchmarkId::new("hello", bytesize::ByteSize(len).to_string_as(true)),
            &file,
            |b, file| {
                b.iter(|| {
                    (|| -> anyhow::Result<()> {
                        let _ = remove_file(&dst_path);
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

pub fn clonefile_benchmark(c: &mut Criterion) {
    clonefile_benchmark_fallible(c).unwrap()
}
