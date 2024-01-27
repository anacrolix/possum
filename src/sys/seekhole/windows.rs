use super::*;

pub fn seek_hole_whence(
    file: &mut File,
    offset: i64,
    whence: RegionType,
) -> io::Result<Option<RegionOffset>> {
    let mut output = Vec::with_capacity(1);
    let FileOffset = match whence {
        Hole => offset,
        Data => offset + 1,
    };
    query_allocated_ranges(
        file,
        &[FILE_ALLOCATED_RANGE_BUFFER {
            FileOffset,
            // Invalid parameter if FileOffset+Length > i64::MAX.
            Length: i64::MAX - FileOffset,
        }],
        &mut output,
    )
    .map_err(windows_error_to_io)?;
    match whence {
        Hole => match output[..] {
            [next_range, ..] => Ok(Some(
                (next_range.FileOffset + next_range.Length) as RegionOffset,
            )),
            [] => Ok(Some(offset as RegionOffset)),
        },
        Data => match output[..] {
            [next_range, ..] => Ok(Some(next_range.FileOffset as RegionOffset)),
            [] => Ok(None),
        },
    }
}

fn windows_error_to_io(win_error: ::windows::core::Error) -> io::Error {
    io::Error::from_raw_os_error(win_error.code().0)
}
