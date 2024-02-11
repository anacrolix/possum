//! Windows specific APIs

use super::*;
use std::mem::{size_of, size_of_val};

type AllocatedRanges = Vec<FILE_ALLOCATED_RANGE_BUFFER>;

pub fn query_allocated_ranges(
    file: &File,
    input: &[FILE_ALLOCATED_RANGE_BUFFER],
    output: &mut AllocatedRanges,
) -> io::Result<()> {
    let out_buffer =
        unsafe { std::slice::from_raw_parts_mut(output.as_mut_ptr(), output.capacity()) };
    let bytes_returned = device_io_control(
        file,
        FSCTL_QUERY_ALLOCATED_RANGES,
        Some(input),
        Some(out_buffer),
        None,
    )?;
    let out_len = bytes_returned as usize / size_of::<FILE_ALLOCATED_RANGE_BUFFER>();
    unsafe { output.set_len(out_len) };
    Ok(())
}

pub fn std_handle_to_windows(std: std::os::windows::io::RawHandle) -> HANDLE {
    HANDLE(std as isize)
}

pub fn file_disk_allocation(file: &File) -> io::Result<u64> {
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

pub fn path_disk_allocation(path: &Path) -> io::Result<u64> {
    file_disk_allocation(&File::open(path)?)
}

pub(crate) fn windows_error_to_io(win_error: ::windows::core::Error) -> io::Error {
    io::Error::from_raw_os_error(win_error.code().0)
}

// Do we need to require that I and O be slices? Does that mean we can do the bytes_returned element
// calculations here rather than force the caller to do it?
pub(crate) fn device_io_control<I: ?Sized, O: ?Sized>(
    file: &File,
    control_code: u32,
    input: Option<&I>,
    output: Option<&mut O>,
    overlapped: Option<&mut OVERLAPPED>,
) -> io::Result<u32> {
    let handle = std_handle_to_windows(file.as_raw_handle());
    let in_buffer_size = input
        .map(|i| size_of_val(i))
        .unwrap_or(0)
        .try_into()
        .unwrap();
    let input = input.map(|input| input as *const _ as _);
    let mut bytes_returned: u32 = 0;
    let out_buffer_size = output
        .as_ref()
        .map(|o| size_of_val(*o))
        .unwrap_or(0)
        .try_into()
        .unwrap();
    let lp_bytes_returned = if output.as_ref().is_some() {
        Some(&mut bytes_returned as *mut _)
    } else {
        None
    };
    let out_buffer = output.map(|o| o as *mut O as _);
    let overlapped = overlapped.map(|some| some as *mut _);
    if let Err(err) = unsafe {
        DeviceIoControl(
            handle,
            control_code,
            input,
            in_buffer_size,
            out_buffer,
            out_buffer_size,
            lp_bytes_returned,
            overlapped,
        )
    } {
        // No need to flag this to the caller unless they are doing an operation that wouldn't
        // otherwise try again with a new starting point.
        if err.code() != ERROR_MORE_DATA.into() {
            return Err(err.into());
        }
    }
    Ok(bytes_returned)
}
