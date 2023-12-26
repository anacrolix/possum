use thiserror::Error;

use super::*;

#[derive(Error, Debug, PartialEq)]
#[repr(C)]
pub enum Error {
    #[error("no such key")]
    NoSuchKey,
    #[error("sqlite error: {0:?}")]
    Sqlite(#[from] rusqlite::Error),
}
