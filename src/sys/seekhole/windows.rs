use super::*;

pub fn seek_hole_whence(
    file: &File,
    offset: u64,
    whence: RegionType,
) -> io::Result<Option<RegionOffset>> {
    let offset = offset.try_into().unwrap();
    let mut output = Vec::with_capacity(1);
    let file_offset = match whence {
        Hole => offset,
        Data => offset + 1,
    };
    query_allocated_ranges(
        file,
        &[FILE_ALLOCATED_RANGE_BUFFER {
            FileOffset: file_offset,
            // Invalid parameter if FileOffset+Length > i64::MAX.
            Length: i64::MAX - file_offset,
        }],
        &mut output,
    )?;
    dbg!(&output);
    // match returns Result<Some<i64>> because FILE_ALLOCATED_RANGE_BUFFER uses i64.
    match whence {
        Hole => match output[..] {
            [next_range, ..] => {
                assert!(next_range.FileOffset >= offset);
                Ok(Some(if next_range.FileOffset == offset {
                    next_range.FileOffset + next_range.Length
                } else {
                    offset
                }))
            }
            [] => Ok(Some(offset)),
        },
        Data => match output[..] {
            [next_range, ..] => Ok(Some(next_range.FileOffset)),
            [] => Ok(None),
        },
    }
    .map(|ok| ok.map(|some| some.try_into().unwrap()))
}
