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
    // Having the #[from] here kinda sux because everything is stuffed into the anyhow variant
    // automatically, even other instances of Error.
    Anyhow(#[from] anyhow::Error),
    #[error("unsupported filesystem")]
    UnsupportedFilesystem,
}

use Error::*;

pub trait FileAlreadyExistsError {
    fn is_file_already_exists(&self) -> bool;
}

impl FileAlreadyExistsError for std::io::Error {
    fn is_file_already_exists(&self) -> bool {
        self.kind() == ErrorKind::AlreadyExists
    }
}

impl FileAlreadyExistsError for Error {
    // This isn't great, since such an error could already be wrapped up in anyhow, or come from
    // Sqlite for example.
    fn is_file_already_exists(&self) -> bool {
        match self {
            Io(err) => err.is_file_already_exists(),
            _ => false,
        }
    }
}

impl Error {
    pub fn root_cause(&self) -> &(dyn std::error::Error + 'static) {
        match self {
            NoSuchKey | UnsupportedFilesystem => self,
            Sqlite(inner) => inner,
            Anyhow(inner) => inner.root_cause(),
            _ => unimplemented!(),
        }
    }

    pub fn root_cause_is_unsupported_filesystem(&self) -> bool {
        matches!(
            self.root_cause().downcast_ref(),
            Some(Self::UnsupportedFilesystem)
        )
    }
}

#[cfg(windows)]
impl From<windows::core::Error> for Error {
    fn from(from: windows::core::Error) -> Self {
        anyhow::Error::from(from).into()
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context;

    use crate::{Error, PubResult};

    #[test]
    fn test_downcast_double_contexted() {
        let res = Err::<(), _>(Error::UnsupportedFilesystem);
        let res: PubResult<_> = res.context("sup").map_err(Into::into);
        let res: PubResult<()> = res.context("bro").map_err(Into::into);
        let err: Error = res.unwrap_err();
        assert!(matches!(
            err.root_cause().downcast_ref::<Error>(),
            Some(Error::UnsupportedFilesystem)
        ));
        assert!(err.root_cause_is_unsupported_filesystem());
    }
}
