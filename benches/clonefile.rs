use anyhow::ensure;
use anyhow::Context;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::Rng;
use std::ffi::CString;
use std::io::{Error, Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use tempfile::NamedTempFile;
pub fn clonefile_benchmark(c: &mut Criterion) -> anyhow::Result<()> {
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
        c.bench_with_input(
            BenchmarkId::new("hello", bytesize::ByteSize(len).to_string_as(true)),
            &file,
            |b, file| {
                b.iter(|| {
                    (|| -> anyhow::Result<()> {
                        let dst_file = NamedTempFile::new()?;
                        let src_path = file.path();
                        let src_buf = CString::new(src_path.as_os_str().as_bytes())?;
                        let dst_path = dst_file.path().to_path_buf();
                        // println!("{:?} -> {:?}", src_path, dst_path);
                        let dst_buf = CString::new(dst_path.as_os_str().as_bytes())?;
                        drop(dst_file);
                        let src = src_buf.as_ptr();
                        let dst = dst_buf.as_ptr();
                        let val = unsafe { libc::clonefile(src, dst, 0) };
                        std::fs::remove_file(&dst_path)?;
                        if val != 0 {
                            return Err(Error::last_os_error())
                                .with_context(|| format!("{:?} -> {:?}", src_path, dst_path));
                        }
                        Ok(())
                    })()
                    .unwrap()
                })
            },
        );
    }
    Ok(())
}

criterion_group!(benches, clonefile_benchmark);
criterion_main!(benches);
