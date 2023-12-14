use super::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("no such key")]
    NoSuchKey,
    #[error("sqlite error: {0:?}")]
    Sqlite(#[from] rusqlite::Error),
}
