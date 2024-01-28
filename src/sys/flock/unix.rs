use super::*;
use libc::*;
use std::io::SeekFrom;

use nix::fcntl::FlockArg;
pub use nix::fcntl::FlockArg::*;

pub fn try_lock_file(file: &mut File, arg: FlockArg) -> nix::Result<bool> {
    lock_file_segment(file, arg, None, SeekFrom::Start(0))
}

fn seek_from_offset(seek_from: SeekFrom) -> off_t {
    use SeekFrom::*;
    match seek_from {
        Start(offset) => offset as off_t,
        End(offset) | Current(offset) => offset as off_t,
    }
}

fn seek_from_whence(seek_from: SeekFrom) -> c_short {
    use libc::*;
    use SeekFrom::*;
    (match seek_from {
        Start(_) => SEEK_SET,
        Current(_) => SEEK_CUR,
        End(_) => SEEK_END,
    }) as c_short
}

pub fn lock_file_segment(
    file: &File,
    arg: FlockArg,
    len: Option<i64>,
    whence: SeekFrom,
) -> nix::Result<bool> {
    debug!(?arg, ?len, ?whence, "locking file segment");
    if let Some(len) = len {
        // This has special meaning on macOS: To the end of the file. Use None instead.
        if len == 0 {
            return Ok(true);
        }
    }
    let flock_arg = nix::libc::flock {
        l_start: seek_from_offset(whence),
        l_len: len.unwrap_or_default(),
        l_pid: 0,
        l_type: match arg {
            LockShared | LockSharedNonblock => libc::F_RDLCK,
            LockExclusive | LockExclusiveNonblock => libc::F_WRLCK,
            Unlock => libc::F_UNLCK,
            // Silly non-exhaustive enum.
            _ => unimplemented!(),
        },
        l_whence: seek_from_whence(whence),
    };
    use nix::fcntl::*;
    let arg = match arg {
        LockShared | LockExclusive => F_OFD_SETLKW(&flock_arg),
        LockSharedNonblock | LockExclusiveNonblock | Unlock => F_OFD_SETLK(&flock_arg),
        _ => unimplemented!(),
    };
    // EWOULDBLOCK is an inherent impl const so can't be glob imported.
    use nix::errno::Errno;
    match fcntl(file.as_raw_fd(), arg) {
        Ok(_) => Ok(true),
        Err(errno) if errno == Errno::EWOULDBLOCK || errno == Errno::EAGAIN => Ok(false),
        Err(err) => Err(err),
    }
}
