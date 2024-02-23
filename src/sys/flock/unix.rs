use std::io::SeekFrom;

pub use nix::fcntl::FlockArg;
pub use nix::fcntl::FlockArg::*;

use super::*;

fn seek_from_offset(seek_from: SeekFrom) -> i64 {
    use SeekFrom::*;
    match seek_from {
        Start(offset) => offset as i64,
        End(offset) | Current(offset) => offset,
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
pub(super) fn lock_file_segment(
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
    #[allow(deprecated)]
    let l_type = match arg {
        LockShared | LockSharedNonblock => libc::F_RDLCK,
        LockExclusive | LockExclusiveNonblock => libc::F_WRLCK,
        Unlock | UnlockNonblock => libc::F_UNLCK,
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
    use libc::{F_OFD_SETLK, F_OFD_SETLKW};
    #[allow(deprecated)]
    let arg = match arg {
        LockShared | LockExclusive => F_OFD_SETLKW,
        LockSharedNonblock | LockExclusiveNonblock | Unlock | UnlockNonblock => F_OFD_SETLK,
        _ => unimplemented!(),
    };
    // EWOULDBLOCK is an inherent impl const so can't be glob imported.
    use nix::errno::Errno;
    // nix doesn't handle 64bit fcntl on 32bit Linux yet, and I can't be bothered to send a PR since
    // my last one got stalled. https://github.com/nix-rust/nix/pull/2032#issuecomment-1931419587.
    // There doesn't seem to be a fcntl64 available for 32bit systems. Hopefully fcntl there still
    // takes a fcntl64 arg.
    match Errno::result(unsafe { libc::fcntl(file.as_raw_fd(), arg, &flock_arg) }) {
        Ok(_) => Ok(true),
        Err(errno) if errno == Errno::EWOULDBLOCK || errno == Errno::EAGAIN => Ok(false),
        Err(err) => {
            error!(?err, "fcntl");
            Err(err)
        }
    }
}

impl FileLocking for File {
    fn trim_exclusive_lock_left(&self, old_left: u64, new_left: u64) -> io::Result<bool> {
        #[allow(deprecated)]
        self.lock_segment(UnlockNonblock, Some(new_left - old_left), old_left)
    }

    fn lock_segment(&self, arg: FlockArg, len: Option<u64>, offset: u64) -> io::Result<bool> {
        Ok(lock_file_segment(
            self,
            arg,
            len.map(|some| some.try_into().unwrap()),
            Start(offset),
        )?)
    }
}
