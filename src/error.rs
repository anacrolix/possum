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
    #[error("unsupported filesystem")]
    UnsupportedFilesystem,
}

use Error::*;

impl Error {
    // This isn't great, since such an error could already be wrapped up in anyhow, or come from
    // Sqlite for example.
    pub(crate) fn is_file_already_exists(&self) -> bool {
        match self {
            Io(err) => err.kind() == ErrorKind::AlreadyExists,
            _ => false,
        }
    }
}
