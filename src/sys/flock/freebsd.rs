use std::fs::File;
use std::io;

use nix::fcntl::FlockArg;

use crate::sys::{FileLocking, Flock};

impl FileLocking for File {
    fn trim_exclusive_lock_left(&self, _old_left: u64, _new_left: u64) -> io::Result<bool> {
        Ok(true)
    }

    fn lock_segment(&self, arg: FlockArg, _len: Option<u64>, _offset: u64) -> io::Result<bool> {
        self.lock_max_segment(arg)
    }

    fn lock_max_segment(&self, arg: FlockArg) -> io::Result<bool> {
        self.flock(arg)
    }
}
