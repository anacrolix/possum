use super::*;

use nix::fcntl::FlockArg;
pub use nix::fcntl::FlockArg::*;

const EWOULDBLOCK: Errno = Errno::EWOULDBLOCK;

pub fn try_lock_file(file: &mut File, arg: FlockArg) -> nix::Result<bool> {
    let flock_res = nix::fcntl::flock(file.as_raw_fd(), arg);
    match flock_res {
        Ok(()) => Ok(true),
        Err(errno) => {
            if errno == EWOULDBLOCK {
                Ok(false)
            } else {
                Err(errno)
            }
        }
    }
}
