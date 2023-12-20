use libc::c_int;
use nix::errno::errno;

use nix::libc::{SEEK_DATA, SEEK_HOLE};

use std::io::Error;
use std::os::fd::RawFd;

/// Using i64 rather than off_t to enforce 64-bit offsets (the libc wrappers all use type aliases
/// anyway).
fn seek_hole(fd: RawFd, offset: i64) -> std::io::Result<Option<i64>> {
    seek_hole_whence(fd, offset, SEEK_HOLE)
}

/// Using i64 rather than off_t to enforce 64-bit offsets (the libc wrappers all use type aliases
/// anyway).
fn seek_hole_whence(fd: RawFd, offset: i64, whence: c_int) -> std::io::Result<Option<i64>> {
    // lseek64?
    let new_offset = unsafe { nix::libc::lseek(fd, offset, whence) };
    if new_offset == -1 {
        let errno = errno();
        if errno == libc::ENXIO {
            return Ok(None);
        }
        return Err(Error::from_raw_os_error(errno));
    }
    Ok(Some(new_offset))
}

/// Using i64 rather than off_t to enforce 64-bit offsets (the libc wrappers all use type aliases
/// anyway).
fn seek_data(fd: RawFd, offset: i64) -> std::io::Result<Option<i64>> {
    seek_hole_whence(fd, offset, SEEK_DATA)
}

#[derive(Debug, Copy, Clone)]
pub enum RegionType {
    Hole,
    Data,
}

impl std::ops::Not for RegionType {
    type Output = RegionType;

    fn not(self) -> Self::Output {
        match self {
            Hole => Data,
            Data => Hole,
        }
    }
}

use RegionType::*;

#[derive(Debug, Clone)]
pub struct Region {
    pub region_type: RegionType,
    pub start: i64,
    pub end: i64,
}

impl Region {
    pub fn length(&self) -> i64 {
        return self.end - self.start;
    }
}

pub struct Iter {
    fd: RawFd,
    last_item: Option<Region>,
}

impl Iter {
    pub fn new_from_fd(fd: RawFd) -> Iter {
        Self {
            fd,
            last_item: Some(Region {
                region_type: Hole,
                start: 0,
                end: 0,
            }),
        }
    }

    fn try_seek_inner(
        &self,
        last_offset: i64,
        whence: RegionType,
    ) -> std::io::Result<Option<Region>> {
        let new_offset = match whence {
            Hole => seek_hole(self.fd, last_offset),
            Data => seek_data(self.fd, last_offset),
        }?;
        let new_offset = match new_offset {
            Some(some) => some,
            None => return Ok(None),
        };
        if new_offset == last_offset {
            return Ok(None);
        }
        Ok(Some(Region {
            region_type: !whence,
            start: last_offset,
            end: new_offset,
        }))
    }

    fn try_seek(&self, last_offset: i64, whence: RegionType) -> std::io::Result<Option<Region>> {
        let first = self.try_seek_inner(last_offset, whence)?;
        if first
            .as_ref()
            .map(|region| region.length() >= 0)
            .unwrap_or(false)
        {
            return Ok(first);
        }
        self.try_seek_inner(last_offset, !whence)
    }
}

impl Iterator for Iter {
    type Item = std::io::Result<Region>;

    fn next(&mut self) -> Option<Self::Item> {
        let last_item = match &self.last_item {
            Some(a) => a,
            None => return None,
        };
        match self.try_seek(last_item.end, last_item.region_type) {
            Err(err) => Some(Err(err)),
            Ok(item) => {
                self.last_item = item.clone();
                item.map(Ok)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pathconf::path_min_hole_size;
    use crate::seekhole;
    use crate::testing::write_random_tempfile;
    use std::env::temp_dir;
    use std::os::fd::AsRawFd;

    #[test]
    fn just_a_hole() -> Result<()> {
        let os_temp_dir = temp_dir();
        let mut min_hole_size = path_min_hole_size(&os_temp_dir)?;
        if min_hole_size <= 0 {
            min_hole_size = 1;
        }
        let file = write_random_tempfile(min_hole_size.try_into()?)?;
        let iter = seekhole::Iter::new_from_fd(file.as_raw_fd());
        for region in iter {
            dbg!(region);
        }
        Ok(())
    }
}
