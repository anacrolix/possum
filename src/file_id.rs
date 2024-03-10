use super::*;

// sqlite allows *signed* integers up to 8 bytes. But this constrains us to using only the positive
// half unless we cast the sign back and forth. If we had that many files in a directory we're going
// to run into bigger problems first.
type FileIdInner = u32;

/// Values file identifier
#[derive(Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Copy)]
pub struct FileId(FileIdInner);

impl Debug for FileId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Deref for FileId {
    type Target = FileIdInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::str::FromStr for FileId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self(FileIdInner::from_str_radix(s, 16)?))
    }
}

impl TryFrom<&OsStr> for FileId {
    type Error = anyhow::Error;

    fn try_from(value: &OsStr) -> std::result::Result<Self, Self::Error> {
        let file_id_str = value
            .to_str()
            .ok_or(anyhow!("to_str"))?
            .strip_prefix(VALUES_FILE_NAME_PREFIX)
            .ok_or(anyhow!("missing values file name prefix"))?;
        let file_id = file_id_str.parse()?;
        Ok(file_id)
    }
}

impl FromSql for FileId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let int = FileIdInner::column_result(value)?;
        Ok(Self(int))
    }
}

impl ToSql for FileId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

impl Display for FileId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.values_file_path().to_str().unwrap())
    }
}

impl AsRef<FileId> for FileId {
    fn as_ref(&self) -> &FileId {
        self
    }
}

impl FileId {
    pub fn values_file_path(&self) -> PathBuf {
        format!("{}{:08x}", VALUES_FILE_NAME_PREFIX, self.0).into()
    }

    pub fn random() -> Self {
        Self(rand::random())
    }
}
