use thiserror::Error;

use super::*;

#[derive(Error, Debug)]
#[repr(C)]
pub enum Error {
    #[error("no such key")]
    NoSuchKey,
    #[error("sqlite error: {0:?}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io error: {0:?}")]
    Io(#[from] std::io::Error),
    #[error("anyhow: {0:?}")]
    Anyhow(#[from] anyhow::Error),
}
