use super::*;
use libc::{ENXIO, SEEK_END};
use nix::errno::errno;
use nix::libc::{SEEK_DATA, SEEK_HOLE};
use std::ffi::c_int;
use std::io::Error;
use std::os::fd::RawFd;

type SeekWhence = c_int;

/// Using i64 rather than off_t to enforce 64-bit offsets (the libc wrappers all use type aliases
/// anyway).
pub fn seek_hole_whence(
    fd: RawFd,
    offset: i64,
    whence: impl Into<SeekWhence>,
) -> std::io::Result<Option<i64>> {
    // lseek64?
    match lseek(fd, offset, whence) {
        Ok(offset) => Ok(Some(offset)),
        Err(errno) => {
            if errno == ENXIO {
                Ok(None)
            } else {
                Err(Error::from_raw_os_error(errno))
            }
        }
    }
}

/// Using i64 rather than off_t to enforce 64-bit offsets (the libc wrappers all use type aliases
/// anyway).
fn lseek(fd: RawFd, offset: i64, whence: impl Into<SeekWhence>) -> Result<i64, i32> {
    // lseek64?
    let new_offset = unsafe { nix::libc::lseek(fd, offset, whence.into()) };
    if new_offset == -1 {
        return Err(errno());
    }
    Ok(new_offset)
}

type Whence = RegionType;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum RegionType {
    Hole,
    Data,
}

impl From<RegionType> for SeekWhence {
    fn from(value: RegionType) -> Self {
        match value {
            Hole => SEEK_HOLE,
            Data => SEEK_DATA,
        }
    }
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

pub use RegionType::*;

#[derive(Debug, Clone, PartialEq)]
pub struct Region {
    pub region_type: RegionType,
    pub start: i64,
    pub end: i64,
}

impl Region {
    pub fn length(&self) -> i64 {
        self.end - self.start
    }
}

/// Mutable because the File offset may be changed.
pub fn file_regions(file: &mut File) -> Result<Vec<Region>> {
    let fd = file.as_raw_fd();
    let mut offsets = vec![];
    {
        let mut offset = 0;
        let mut whence = Data;
        loop {
            let new_offset = match seek_hole_whence(fd, offset, whence)? {
                Some(a) => a,
                None => match whence {
                    Hole => break,
                    Data => file.seek(End(0))?.try_into()?,
                },
            };
            offsets.push((new_offset, whence));
            whence = !whence;
            offset = new_offset;
        }
    }
    offsets.sort_by_key(|tuple| tuple.0);
    let mut last_offset = 0;
    let mut last_type = Hole;
    let mut output = vec![];
    for (offset, region_type) in offsets {
        let region = Region {
            region_type: last_type,
            start: last_offset,
            end: offset,
        };
        last_type = region_type;
        if region.length() == 0 {
            continue;
        }
        last_offset = offset;
        output.push(region);
    }
    assert_eq!(output, regions_iter_to_vec(file).unwrap());
    Ok(output)
}

pub struct Iter {
    last_whence: RegionType,
    offset: i64,
    fd: RawFd,
}

impl Iter {
    fn new(fd: RawFd) -> Self {
        Self {
            // We want to start with whatever will most likely result in a positive seek on the
            // first next. Most files start with Data. This might not be the case for long-term
            // values files, but let's find out.
            last_whence: Data,
            offset: 0,
            fd,
        }
    }
}

impl Iterator for Iter {
    type Item = Result<Region>;

    // We don't enter a final state, I think fused iterators are for that purpose. Plus it's valid
    // for an iterator to start working again if the file changes.
    fn next(&mut self) -> Option<Self::Item> {
        let first_whence = !self.last_whence;
        let mut whence = first_whence;
        // This only runs twice. Once with each whence, starting with the one we didn't try last.
        loop {
            // dbg!(self.offset, whence);
            match seek_hole_whence(self.fd, self.offset, whence) {
                Ok(Some(offset)) if offset != self.offset => {
                    let region = Region {
                        region_type: !whence,
                        start: self.offset,
                        end: offset,
                    };
                    self.last_whence = whence;
                    self.offset = offset;
                    return Some(Ok(region));
                }
                Err(err) => return Some(Err(err.into())),
                Ok(None | Some(_)) => {}
            }
            whence = !whence;
            if whence == first_whence {
                break;
            }
        }
        // We do this when both SEEK_DATA and SEEK_HOLE fail to move the offset. If a file ends in
        // data, SEEK_HOLE will always get to the end, but if it ends in a hole, SEEK_HOLE will get
        // stuck. Therefore, SEEK_END will progress past a final hole.
        match lseek(self.fd, 0, SEEK_END) {
            Err(errno) => Some(Err(Error::from_raw_os_error(errno).into())),
            Ok(offset) => {
                if offset == self.offset {
                    None
                } else {
                    let region = Region {
                        region_type: Hole,
                        start: self.offset,
                        end: offset,
                    };
                    // Now that we're at the end of the file, the most likely reason for further
                    // successful seeks would be new data being written. Therefore we want to ensure
                    // SEEK_DATA is tried first.
                    self.last_whence = Hole;
                    self.offset = offset;
                    Some(Ok(region))
                }
            }
        }
    }
}

fn regions_iter_to_vec(file: &mut File) -> Result<Vec<Region>> {
    let fd = file.as_raw_fd();
    let itered: Vec<_> = Iter::new(fd).collect::<Result<Vec<_>>>()?;
    Ok(itered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pathconf::path_min_hole_size;
    
    use crate::testing::write_random_tempfile;
    
    use std::env::temp_dir;
    use std::os::fd::{AsRawFd};

    fn get_regions(file: &mut File) -> Result<Vec<Region>> {
        let fd = file.as_raw_fd();
        let itered: Vec<_> = Iter::new(fd).collect::<Result<Vec<_>>>()?;
        let vec = file_regions(file)?;
        assert_eq!(itered, vec);
        Ok(vec)
    }

    #[self::test]
    fn just_a_hole() -> Result<()> {
        let os_temp_dir = temp_dir();
        let mut min_hole_size = path_min_hole_size(&os_temp_dir)?;
        if min_hole_size <= 1 {
            min_hole_size = 2;
        }
        let mut file = write_random_tempfile(min_hole_size.try_into()?)?;
        let regions = get_regions(file.as_file_mut())?;
        assert_eq!(
            regions,
            vec![Region {
                region_type: Data,
                start: 0,
                end: min_hole_size
            }]
        );
        punchfile(file.as_raw_fd(), 0, min_hole_size)?;
        file.seek(Start(0))?;
        let regions: Vec<_> = get_regions(file.as_file_mut())?;
        assert_eq!(
            regions,
            vec![Region {
                region_type: Hole,
                start: 0,
                end: min_hole_size
            }]
        );
        Ok(())
    }
}
