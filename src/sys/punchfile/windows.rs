use super::*;
use std::convert::TryInto;
use std::io;

pub fn punchfile(file: &File, offset: u64, length: u64) -> io::Result<()> {
    let handle = std_handle_to_windows(file.as_raw_handle());
    // FILE_ZERO_DATA_INFORMATION_EX exists but it's for drivers.
    let input = FILE_ZERO_DATA_INFORMATION {
        FileOffset: offset.try_into().unwrap(),
        BeyondFinalZero: (offset + length).try_into().unwrap(),
    };
    let res = unsafe {
        DeviceIoControl(
            handle,
            FSCTL_SET_ZERO_DATA,
            Some(&input as *const _ as _),
            std::mem::size_of_val(&input) as u32,
            None,
            0,
            None,
            None,
        )
    };
    Ok(res?)
}

pub fn set_file_sparse(file: &File, set_sparse: bool) -> io::Result<()> {
    let input = FILE_SET_SPARSE_BUFFER {
        SetSparse: set_sparse.into(),
    };
    device_io_control(file, FSCTL_SET_SPARSE, Some(&input), None::<&mut ()>, None)?;
    Ok(())
}

impl SparseFile for File {
    fn set_sparse(&self, set_sparse: bool) -> io::Result<()> {
        set_file_sparse(self, set_sparse)
    }
}
