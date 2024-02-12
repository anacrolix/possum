//! Syscall wrappers for hole punching, system configuration, hole-seeking ( ͡° ͜ʖ ͡°), file cloning
//! etc.

pub use RegionType::*;

use super::*;

cfg_if! {
    if #[cfg(unix)] {
        mod unix;
        pub use self::unix::*;
    } else if #[cfg(windows)] {
        mod windows;
        pub use self::windows::*;
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
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

pub type RegionOffset = u64;

#[derive(Debug, Clone, PartialEq)]
pub struct Region {
    pub region_type: RegionType,
    pub start: RegionOffset,
    pub end: RegionOffset,
}

impl Region {
    pub fn length(&self) -> RegionOffset {
        self.end - self.start
    }
}

pub fn file_regions(file: &mut File) -> anyhow::Result<Vec<Region>> {
    let itered: Vec<_> = Iter::new(file).collect::<io::Result<Vec<_>>>()?;
    Ok(itered)
}

pub struct Iter<'a> {
    last_whence: RegionType,
    offset: RegionOffset,
    file: &'a mut File,
}

impl<'a> Iter<'a> {
    pub fn new(file: &'a mut File) -> Self {
        Self {
            // We want to start with whatever will most likely result in a positive seek on the
            // first next. Most files start with Data. This might not be the case for long-term
            // values files, but let's find out.
            last_whence: Data,
            offset: 0,
            file,
        }
    }
}

impl Iterator for Iter<'_> {
    type Item = std::io::Result<Region>;

    // We don't enter a final state, I think fused iterators are for that purpose. Plus it's valid
    // for an iterator to start working again if the file changes.
    fn next(&mut self) -> Option<Self::Item> {
        let first_whence = !self.last_whence;
        let mut whence = first_whence;
        // This only runs twice. Once with each whence, starting with the one we didn't try last.
        loop {
            match seek_hole_whence(self.file, self.offset, whence) {
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
                Err(err) => return Some(Err(err)),
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
        match self.file.seek(End(0)) {
            Err(err) => Some(Err(err)),
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

#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use super::*;
    use crate::pathconf::path_min_hole_size;
    use crate::testing::write_random_tempfile;

    #[self::test]
    fn just_a_hole() -> anyhow::Result<()> {
        let os_temp_dir = temp_dir();
        let mut min_hole_size = path_min_hole_size(&os_temp_dir)?;
        if min_hole_size <= 1 {
            min_hole_size = 2;
        }
        let mut temp_file = write_random_tempfile(2 * min_hole_size)?;
        let file_ref = temp_file.as_file_mut();
        let regions = file_regions(file_ref)?;
        assert_eq!(
            regions,
            vec![Region {
                region_type: Data,
                start: 0,
                end: 2 * min_hole_size
            }]
        );
        file_ref.set_sparse(true)?;
        punchfile(&file_ref, 0, min_hole_size)?;
        file_ref.seek(Start(0))?;
        let regions: Vec<_> = file_regions(file_ref)?;
        assert_eq!(
            regions,
            vec![
                Region {
                    region_type: Hole,
                    start: 0,
                    end: min_hole_size
                },
                Region {
                    region_type: Data,
                    start: min_hole_size,
                    end: 2 * min_hole_size,
                }
            ]
        );
        Ok(())
    }
}
