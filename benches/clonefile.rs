use std::fs::remove_file;

use criterion::{BenchmarkId, Criterion};
use possum::sys::clonefile;
use possum::testing::test_tempdir;

fn clonefile_benchmark_fallible(c: &mut Criterion) -> anyhow::Result<()> {
    let tempdir = test_tempdir("benchmark_clonefile")?;
    let possum_dir = possum::Dir::new(tempdir.path.clone())?;
    if !possum_dir.supports_file_cloning() {
        anyhow::bail!("file cloning not supported on fs");
    }
    let mut group = c.benchmark_group("clonefile");
    for size_power in [12, 20, 28] {
        let len = 1 << size_power;
        group.bench_with_input(
            BenchmarkId::new("hello", bytesize::ByteSize(len).to_string_as(true)),
            &len,
            |b, file_size| {
                (|| -> anyhow::Result<()> {
                    // Make sure the source file is in the same directory as the destination so we
                    // don't trip on unexpected cross-device linking. Note that in normal operation,
                    // we have a fallback for cross-linking, but here in the benchmark we're not
                    // interested in that.
                    let mut file = tempfile::NamedTempFile::new_in(&tempdir.path)?;
                    possum::testing::write_random(&mut file, *file_size)?;
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
