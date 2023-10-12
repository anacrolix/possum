use super::*;
use anyhow::{ensure, Result};
use rand::Rng;
use std::hash::Hasher;
use std::io::Write;
use std::io::{copy, SeekFrom};
use tempfile::NamedTempFile;
use twox_hash::XxHash64;

pub fn write_random_tempfile(len: u64) -> Result<NamedTempFile> {
    let mut file = NamedTempFile::new()?;
    let mut rng = rand::thread_rng();
    let mut buf = [0; 4096];
    let mut remaining_size = len;
    while remaining_size > 0 {
        let n1 = min(remaining_size, buf.len() as u64).try_into()?;
        let buf1 = &mut buf[..n1];
        rng.fill(buf1);
        file.write(buf1)?;
        remaining_size -= n1 as u64;
    }
    ensure!(file.as_file().seek(SeekFrom::End(0))? == len);
    Ok(file)
}

struct HashWriter<T: Hasher>(T);

impl<T: Hasher> Write for HashWriter<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    // fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
    //     self.write(buf).map(|_| ())
    // }
}

fn hash_reader(mut r: impl Read) -> Result<u64> {
    let h = XxHash64::default();
    let mut hw = HashWriter(h);
    copy(&mut r, &mut hw)?;
    Ok(h.finish())
}

pub fn compare_reads(a: impl Read, b: impl Read) -> Result<()> {
    let ah = hash_reader(a)?;
    let bh = hash_reader(b)?;
    ensure!(ah == bh, "hash {} != {}", ah, bh);
    Ok(())
}
