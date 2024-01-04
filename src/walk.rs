use std::fs::DirEntry as StdDirEntry;
use std::fs::FileType;

use super::*;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Entry {
    pub path: PathBuf,
    pub entry_type: EntryType,
}

use EntryType::*;

impl Entry {
    pub fn file_id(&self) -> Option<&FileIdFancy> {
        match self.entry_type {
            SnapshotValue | ValuesFile => self.path.file_name().map(FileIdFancy::new),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub enum EntryType {
    ManifestFile,
    SnapshotDir,
    SnapshotValue,
    ValuesFile,
    Unknown,
}

/// A slightly decoded std::fs::DirEntry
struct DirEntry {
    file_name: String,
    file_type: FileType,
}

impl TryFrom<&StdDirEntry> for DirEntry {
    type Error = anyhow::Error;

    fn try_from(value: &StdDirEntry) -> Result<Self, Self::Error> {
        let file_name = value
            .file_name()
            .to_str()
            .context("file name not str")?
            .to_owned();
        Ok(Self {
            file_name,
            file_type: value.file_type()?,
        })
    }
}

pub fn walk_dir<P>(dir: P) -> Result<Vec<Entry>>
where
    P: AsRef<Path>,
{
    let mut ok = vec![];
    for std_entry in read_dir(dir)? {
        let std_entry = std_entry?;
        let entry = DirEntry::try_from(&std_entry)?;
        let file_name = entry.file_name;
        let file_type = entry.file_type;
        use EntryType::*;
        let entry_type = if file_name.starts_with(MANIFEST_DB_FILE_NAME) && file_type.is_file() {
            ManifestFile
        } else if file_name.starts_with(VALUES_FILE_NAME_PREFIX) && file_type.is_file() {
            ValuesFile
        } else if file_name.starts_with(SNAPSHOT_DIR_NAME_PREFIX) && file_type.is_dir() {
            ok.extend(walk_snapshot_dir(std_entry.path())?);
            SnapshotDir
        } else {
            Unknown
        };
        ok.push(Entry {
            entry_type,
            path: std_entry.path(),
        });
    }
    Ok(ok)
}

pub(crate) fn walk_snapshot_dir<P>(path: P) -> Result<Vec<Entry>>
where
    P: AsRef<Path>,
{
    let mut ok = vec![];
    for std_entry in read_dir(path)? {
        let std_entry = std_entry?;
        let entry = DirEntry::try_from(&std_entry)?;
        let file_name = entry.file_name;
        let file_type = entry.file_type;
        use EntryType::*;
        let entry_type = if file_name.starts_with(VALUES_FILE_NAME_PREFIX) && file_type.is_file() {
            SnapshotValue
        } else {
            Unknown
        };
        ok.push(Entry {
            path: std_entry.path(),
            entry_type,
        });
    }
    Ok(ok)
}
