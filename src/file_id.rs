use std::borrow::Borrow;

use super::*;

#[derive(Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct FileIdFancy(OsStr);

impl FileIdFancy {
    // Stolen from std::path::Path::new. What a world.
    pub fn new<S: AsRef<OsStr> + ?Sized>(s: &S) -> &Self {
        unsafe { &*(s.as_ref() as *const OsStr as *const Self) }
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct FileId(OsString);

impl Deref for FileId {
    type Target = FileIdFancy;

    fn deref(&self) -> &Self::Target {
        FileIdFancy::new(&self.0)
    }
}

impl From<OsString> for FileId {
    fn from(value: OsString) -> Self {
        Self(value)
    }
}

impl AsRef<Path> for FileId {
    fn as_ref(&self) -> &Path {
        Path::new(&self.0)
    }
}

impl Debug for FileId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<String> for FileId {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<Vec<u8>> for FileId {
    fn from(value: Vec<u8>) -> Self {
        OsString::from_vec(value).into()
    }
}

impl FileIdFancy {
    fn as_str(&self) -> &str {
        self.0.to_str().unwrap()
    }
}

impl rusqlite::ToSql for FileIdFancy {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Blob(self.0.as_bytes())))
    }
}

impl FromSql for FileId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        use rusqlite::types::ValueRef::*;
        Ok(match value {
            Null | Real(..) => Err(FromSqlError::InvalidType),
            Text(text) => Ok(text.to_owned()),
            Blob(blob) => Ok(blob.to_owned()),
            Integer(int) => Ok(int.to_string().into_bytes()),
        }?
        .into())
    }
}

impl Display for FileIdFancy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.as_str(), f)
    }
}

impl Display for FileId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.as_str(), f)
    }
}

impl AsRef<FileId> for FileId {
    fn as_ref(&self) -> &FileId {
        self
    }
}

impl Borrow<FileIdFancy> for FileId {
    fn borrow(&self) -> &FileIdFancy {
        self
    }
}
