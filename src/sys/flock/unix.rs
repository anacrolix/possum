use super::*;
use std::io::SeekFrom;

use nix::fcntl::FlockArg;
pub use nix::fcntl::FlockArg::*;

pub fn try_lock_file(file: &mut File, arg: FlockArg) -> nix::Result<bool> {
    lock_file_segment(file, arg, None, SeekFrom::Start(0))
}

fn seek_from_offset(seek_from: SeekFrom) -> i64 {
    use SeekFrom::*;
    match seek_from {
        Start(offset) => offset as off_t,
        End(offset) | Current(offset) => offset as off_t,
    }
}

fn seek_from_whence(seek_from: SeekFrom) -> libc::c_short {
    use libc::*;
    use SeekFrom::*;
    (match seek_from {
        Start(_) => SEEK_SET,
        Current(_) => SEEK_CUR,
        End(_) => SEEK_END,
    }) as c_short
}

cfg_if! {
    if #[cfg(target_os = "macos")] {
        use libc::flock as flock_struct;
    } else {
        use libc::flock64 as flock_struct;
    }
}

// #[instrument]
pub fn lock_file_segment(
    file: &File,
    arg: FlockArg,
    len: Option<i64>,
    whence: SeekFrom,
) -> nix::Result<bool> {
    debug!(?file, ?arg, ?len, ?whence, "locking file segment");
    if let Some(len) = len {
        // This has special meaning on macOS: To the end of the file. Use None instead.
        if len == 0 {
            return Ok(true);
        }
    }
    let l_type = match arg {
        LockShared | LockSharedNonblock => libc::F_RDLCK,
        LockExclusive | LockExclusiveNonblock => libc::F_WRLCK,
        Unlock => libc::F_UNLCK,
        // Silly non-exhaustive enum.
        _ => unimplemented!(),
    };
    #[allow(clippy::useless_conversion)]
    let l_type = l_type.try_into().unwrap();
    let flock_arg = flock_struct {
        l_start: seek_from_offset(whence),
        l_len: len.unwrap_or(0),
        l_pid: 0,
        l_type,
        l_whence: seek_from_whence(whence),
    };
    use libc::{F_OFD_SETLK,F_OFD_SETLKW};
    let arg = match arg {
        LockShared | LockExclusive => F_OFD_SETLKW,
        LockSharedNonblock | LockExclusiveNonblock | Unlock => F_OFD_SETLK,
        _ => unimplemented!(),
    };
    // EWOULDBLOCK is an inherent impl const so can't be glob imported.
    use nix::errno::Errno;
    // nix doesn't handle 64bit fcntl on 32bit Linux yet, and I can't be bothered to send a PR since
    // my last one got stalled. https://github.com/nix-rust/nix/pull/2032#issuecomment-1931419587.
    // There doesn't seem to be a fcntl64 available for 32bit systems. Hopefully fcntl there still
    // takes a fcntl64 arg.
    match Errno::result(unsafe {libc::fcntl(file.as_raw_fd(), arg, &flock_arg)}) {
        Ok(_) => Ok(true),
        Err(errno) if errno == Errno::EWOULDBLOCK || errno == Errno::EAGAIN => Ok(false),
        Err(err) => {
            error!(?err, "fcntl");
            Err(err)
        }
    }
}
