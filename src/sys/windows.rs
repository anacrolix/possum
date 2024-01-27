//! Windows specific APIs

use super::*;
use std::mem::{size_of, size_of_val};

type AllocatedRanges = Vec<FILE_ALLOCATED_RANGE_BUFFER>;

pub fn query_allocated_ranges(
    file: &File,
    input: &[FILE_ALLOCATED_RANGE_BUFFER],
    output: &mut AllocatedRanges,
) -> ::windows::core::Result<()> {
    let handle = std_handle_to_windows(file.as_raw_handle());
    let out_buffer =
        unsafe { std::slice::from_raw_parts_mut(output.as_mut_ptr(), output.capacity()) };
    let mut bytes_returned: u32 = 0;
    unsafe {
        DeviceIoControl(
            handle,
            FSCTL_QUERY_ALLOCATED_RANGES,
            Some(input.as_ptr() as _),
            size_of_val(input) as u32,
            Some(out_buffer.as_mut_ptr() as _),
            size_of_val(out_buffer) as u32,
            Some(&mut bytes_returned as *mut _),
            None,
        )
    }?;
    let out_len = bytes_returned as usize / size_of::<FILE_ALLOCATED_RANGE_BUFFER>();
    unsafe { output.set_len(out_len) };
    Ok(())
}

pub fn std_handle_to_windows(std: std::os::windows::io::RawHandle) -> HANDLE {
    HANDLE(std as isize)
}

pub fn file_disk_allocation(file: &File) -> Result<u64> {
    let handle = std_handle_to_windows(file.as_raw_handle());
    let mut stream_info: FILE_STREAM_INFO = Default::default();
    unsafe {
        GetFileInformationByHandleEx(
            handle,
            FileStreamInfo,
            &mut stream_info as *mut _ as _,
            size_of_val(&stream_info) as u32,
        )
    }?;
    assert_eq!(stream_info.NextEntryOffset, 0);
    Ok(stream_info.StreamAllocationSize as u64)
}

pub fn path_disk_allocation(path: &Path) -> Result<u64> {
    file_disk_allocation(&File::open(path)?)
}
