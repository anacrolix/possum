use crate::punchfile::punchfile;
use anyhow::{anyhow, bail, Context};
use log::{debug, info, warn};
use nix::fcntl::FlockArg::LockExclusiveNonblock;
use num::Integer;
use rusqlite::Transaction;
use std::ffi::OsString;
use std::fs::File;
use std::io::SeekFrom::{End, Start};
use std::io::{ErrorKind, Seek, SeekFrom};
use std::ops::{Add, Deref, DerefMut, Div, Mul, Sub};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::{fs, io};

mod clonefile;
mod punchfile;

#[derive(clap::Subcommand)]
enum Commands {
    PunchHole {
        file: String,
        offset: libc::off_t,
        length: libc::off_t,
    },
    Database {
        dir: PathBuf,
        #[command(subcommand)]
        command: DatabaseCommands,
    },
}

#[derive(clap::Subcommand, Clone)]
enum DatabaseCommands {
    WriteFile { file: OsString },
}

#[derive(clap::Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let cli: Cli = clap::Parser::parse();
    use Commands::*;
    match cli.command {
        PunchHole {
            file,
            offset,
            length,
        } => {
            let file = fs::OpenOptions::new().write(true).open(file)?;
            punchfile(file, offset, length)?;
            Ok(())
        }
        Database { dir, command } => {
            info!("sqlite version: {}", rusqlite::version());
            let mut handle = Handle::new_from_dir(dir)?;
            match command {
                DatabaseCommands::WriteFile { file } => {
                    let key = Path::new(&file)
                        .file_name()
                        .ok_or_else(|| anyhow!("can't extract file name"))?;
                    let file = File::open(&file).with_context(|| format!("opening {:?}", key))?;
                    handle.stage_write(key.to_os_string().into_vec(), file)?;
                    handle.flush_writes()?;
                    Ok(())
                }
            }
        }
    }
}

struct Handle {
    conn: rusqlite::Connection,
    exclusive_file: ExclusiveFile,
    pending_writes: Vec<PendingWrite>,
    next_write_offset: u64,
    last_flush_offset: u64,
    dir: PathBuf,
}

struct ExclusiveFile {
    inner: File,
    id: u64,
}

impl Deref for ExclusiveFile {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for ExclusiveFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

struct PendingWrite {
    key: Vec<u8>,
    value_file_offset: u64,
    value_length: u64,
}

fn open_new_exclusive_file(dir: impl AsRef<Path>) -> anyhow::Result<ExclusiveFile> {
    let mut last_err = None;
    for i in 0..10000 {
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.as_ref().join(&i.to_string()));
        let file = match file {
            Ok(file) => file,
            Err(err) => return Err(err.into()),
        };
        if let Err(err) = nix::fcntl::flock(file.as_raw_fd(), LockExclusiveNonblock) {
            last_err = Some(err)
        } else {
            info!("opened with exclusive file id {}", i);
            return Ok(ExclusiveFile { inner: file, id: i });
        }
    }
    Err(last_err.unwrap()).context("gave up trying to create exclusive file")
}

const MANIFEST_SCHEMA_SQL: &str = include_str!("../manifest.sql");

fn init_manifest_schema(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    // We could do something smarter here like check a schema version and test for the existence of
    // tables.
    conn.execute_batch(MANIFEST_SCHEMA_SQL)
}

impl Handle {
    fn new_from_dir(dir: PathBuf) -> anyhow::Result<Self> {
        fs::create_dir_all(&dir)?;
        let sqlite_version = rusqlite::version_number();
        if sqlite_version < 3042000 {
            bail!(
                "sqlite version {} below minimum {}",
                rusqlite::version(),
                "3.42"
            );
        }
        let conn = rusqlite::Connection::open(dir.join("manifest.db"))?;
        init_manifest_schema(&conn).context("initing manifest schema")?;
        let mut exclusive_file = open_new_exclusive_file(&dir)?;
        let last_flush_offset = exclusive_file.seek(End(0))?;
        let next_write_offset = last_flush_offset;
        let handle = Self {
            conn,
            exclusive_file,
            pending_writes: vec![],
            last_flush_offset,
            next_write_offset,
            dir,
        };
        Ok(handle)
    }

    fn block_size() -> u64 {
        4096
    }

    fn stage_write(&mut self, key: Vec<u8>, mut value: impl std::io::Read) -> anyhow::Result<()> {
        let value_file_offset = self.next_write_offset;
        let value_length = std::io::copy(&mut value, &mut self.exclusive_file.deref())?;
        self.pending_writes.push(PendingWrite {
            key,
            value_file_offset,
            value_length,
        });
        self.next_write_offset += value_length;
        Ok(())
    }

    fn rollback_writes(&mut self) -> io::Result<()> {
        self.exclusive_file.seek(Start(self.last_flush_offset))?;
        self.exclusive_file.set_len(self.last_flush_offset)?;
        self.next_write_offset = self.last_flush_offset;
        self.pending_writes.clear();
        Ok(())
    }

    fn flush_writes(&mut self) -> anyhow::Result<()> {
        let mut transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        for pw in &self.pending_writes {
            let existing = transaction.query_row(
                "delete from keys where key=? returning file_id, file_offset, value_length",
                &[&pw.key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            );
            match existing {
                Ok((file_id, file_offset, value_length)) => {
                    let msg = format!(
                        "deleting value at {} {} {}",
                        file_id, file_offset, value_length
                    );
                    debug!("{}", msg);
                    Self::punch_value(
                        &self.dir,
                        file_id,
                        file_offset,
                        value_length,
                        &mut transaction,
                    )
                    .context(msg)?
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => (),
                Err(err) => return Err(err.into()),
            }
            transaction.execute(
                "insert into keys (key, file_id, file_offset, value_length)\
                values (?, ?, ?, ?)",
                rusqlite::params!(
                    pw.key,
                    self.exclusive_file.id,
                    pw.value_file_offset,
                    pw.value_length
                ),
            )?;
        }
        transaction.commit()?;
        self.pending_writes.clear();
        Ok(())
    }

    // Returns the end offset of the last active value before offset in the same file.
    fn query_last_end_offset(
        tx: &mut Transaction,
        file_id: u64,
        offset: u64,
    ) -> rusqlite::Result<u64> {
        tx.query_row(
            "select max(file_offset+value_length) as last_offset \
            from keys \
            where file_id=? and file_offset+value_length <= ?",
            [file_id, offset],
            |row| {
                // I don't know why, but this can return null for file_ids that have values but
                // don't fit the other conditions.
                let res: rusqlite::Result<Option<_>> = row.get(0);
                res.map(|v| v.unwrap_or_default())
            },
        )
        .or_else(|err| {
            if matches!(err, rusqlite::Error::QueryReturnedNoRows) {
                Ok(0)
            } else {
                Err(err)
            }
        })
    }

    // Can't do this as &mut self for dumb Rust reasons.
    fn punch_value(
        dir: &Path,
        file_id: u64,
        mut offset: u64,
        mut length: u64,
        tx: &mut Transaction,
    ) -> anyhow::Result<()> {
        let block_size = Self::block_size();
        // If we're not at a block boundary to begin with, find out how far back we can punch and
        // start there.
        if offset % block_size != 0 {
            let new_offset = ceil_multiple(
                Self::query_last_end_offset(tx, file_id, offset)?,
                block_size,
            );
            length += offset - new_offset;
            offset = new_offset;
        }
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(dir.join(file_id.to_string()))?;
        // append doesn't mean our file position is at the end to begin with.
        let file_end = file.seek(End(0))?;
        let end_offset = offset + length;
        // Does this handle ongoing writes to the same file?
        if end_offset < file_end {
            // What if this is below the offset or negative?
            length -= end_offset % Self::block_size();
        }
        debug!("punching {} at {} for {}", file_id, offset, length);
        punchfile(file, offset.try_into()?, length.try_into()?)
            .with_context(|| format!("length {}", length))?;
        Ok(())
    }
}

fn floored_multiple<T>(value: T, multiple: T) -> T
where
    T: Integer + Copy,
{
    multiple * (value / multiple)
}

fn ceil_multiple<T>(value: T, multiple: T) -> T
where
    T: Integer + Copy,
{
    (value + multiple - T::one()) / multiple * multiple
}
