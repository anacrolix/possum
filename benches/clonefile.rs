use std::fs::remove_file;

use criterion::{BenchmarkId, Criterion};
use possum::sys::clonefile::clonefile;
use possum::testing::test_tempdir;

fn clonefile_benchmark_fallible(c: &mut Criterion) -> anyhow::Result<()> {
    let mut group = c.benchmark_group("clonefile");
    for size_power in [12, 20, 28] {
        let len = 1 << size_power;
        group.bench_with_input(
            BenchmarkId::new("hello", bytesize::ByteSize(len).to_string_as(true)),
            &len,
            |b, file_size| {
                (|| -> anyhow::Result<()> {
                    let tempdir = test_tempdir("benchmark_clonefile")?;
                    let file = possum::testing::write_random_tempfile(*file_size).unwrap();
                    let dst_path = tempdir.path.join("hello");
                    b.iter(|| {
                        (|| -> anyhow::Result<()> {
                            let _ = remove_file(&dst_path);
                            clonefile(file.path(), dst_path.as_ref())?;
                            Ok(())
                        })()
                        .unwrap()
                    });
                    Ok(())
                })()
                .unwrap()
            },
        );
    }
    Ok(())
}

pub fn clonefile_benchmark(c: &mut Criterion) {
    clonefile_benchmark_fallible(c).unwrap()
}
