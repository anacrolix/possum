//! Implement file locking for systems that lack open file description segment locking but have flock.

pub use nix::fcntl::FlockArg;
pub use nix::fcntl::FlockArg::*;

use super::*;

pub trait Flock {
    fn flock(&self, arg: FlockArg) -> io::Result<bool>;
}

impl Flock for File {
    /// Locks a segment that spans the maximum possible range of offsets.
    fn flock(&self, arg: FlockArg) -> io::Result<bool> {
        match nix::fcntl::flock(self.as_raw_fd(), arg) {
            Ok(()) => Ok(true),
            Err(errno) if errno == nix::Error::EWOULDBLOCK => Ok(false),
            Err(errno) => Err(std::io::Error::from_raw_os_error(errno as i32)),
        }
    }
}
