use super::*;
use anyhow::{ensure, Result};
use rand::Rng;
use std::io::SeekFrom;
use std::io::Write;
use tempfile::NamedTempFile;

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
