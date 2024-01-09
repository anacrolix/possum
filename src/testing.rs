use std::hash::Hasher;
use std::io::Write;
use std::io::{copy, SeekFrom};

use anyhow::{ensure, Result};
use rand::Rng;
use tempfile::{tempdir, NamedTempFile};
use twox_hash::XxHash64;

use super::*;

pub type Hash = XxHash64;

pub fn write_random_tempfile(len: u64) -> Result<NamedTempFile> {
    let mut file = NamedTempFile::new()?;
    let mut rng = rand::thread_rng();
    let mut buf = [0; 4096];
    let mut remaining_size = len;
    while remaining_size > 0 {
        let n1 = min(remaining_size, buf.len() as u64).try_into()?;
        let buf1 = &mut buf[..n1];
        rng.fill(buf1);
        file.write_all(buf1)?;
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
}

pub fn hash_reader(mut r: impl Read) -> Result<u64> {
    let h = Hash::default();
    let mut hw = HashWriter(h);
    let n = copy(&mut r, &mut hw)?;
    dbg!(n);
    Ok(hw.0.finish())
}

pub fn compare_reads(a: impl Read, b: impl Read) -> Result<()> {
    let ah = hash_reader(a)?;
    let bh = hash_reader(b)?;
    ensure!(ah == bh, "hash {} != {}", ah, bh);
    Ok(())
}

/// Keep this in scope so the tempdir isn't deleted right while the path is still in use.
pub struct TestTempDir {
    _tempdir: Option<TempDir>,
    pub path: PathBuf,
}

pub fn test_tempdir(name: &'static str) -> Result<TestTempDir> {
    let (tempdir, path) = if true {
        let path = PathBuf::from("tmp").join(name);
        std::fs::create_dir_all(&path)?;
        (None, path)
    } else {
        let tempdir = tempdir()?;
        let path = tempdir.path().to_owned();
        (Some(tempdir), path)
    };
    Ok(TestTempDir {
        _tempdir: tempdir,
        path,
    })
}

pub fn readable_repeated_bytes(byte: u8, limit: usize) -> Vec<u8> {
    std::iter::repeat(byte).take(limit).collect()
}

pub fn condense_repeated_bytes(r: impl Read) -> (Option<u8>, u64) {
    let mut count = 0;
    let mut byte = None;
    for b in r.bytes() {
        let b = b.unwrap();
        match byte {
            None => byte = Some(b),
            Some(byte) => {
                assert_eq!(b, byte);
            }
        }
        count += 1;
    }
    (byte, count)
}

pub fn assert_repeated_bytes_values_eq(a: impl Read, b: impl Read) {
    let a = condense_repeated_bytes(a);
    let b = condense_repeated_bytes(b);
    assert_eq!(a, b);
}
