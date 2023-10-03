use anyhow::{bail, ensure, Context};
use log::{info, warn};
use std::ffi::OsString;
use std::io;
use std::io::SeekFrom::Start;
use std::io::{ErrorKind, Seek, SeekFrom};
use std::ops::{Deref, DerefMut};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStringExt;
use std::path::Path;
// use clap::{Parser, Subcommand};

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
        dir: String,
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
            let file = std::fs::OpenOptions::new().write(true).open(file)?;
            let punchhole = libc::fpunchhole_t {
                fp_flags: 0,
                reserved: 0,
                fp_offset: offset,
                fp_length: length,
            };
            let fcntl_res = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, punchhole) };
            if fcntl_res == -1 {
                eprintln!("{:?}", std::io::Error::last_os_error());
            }
        }
        Database { dir, command } => {
            info!("sqlite version: {}", rusqlite::version());
            let mut handle = Handle::new_from_dir(dir)?;
            match command {
                DatabaseCommands::WriteFile { file } => {
                    let key = &file;
                    let file = std::fs::File::open(&file)?;
                    handle.stage_write(key.clone().into_vec(), file)?;
                    handle.flush_writes()?;
                }
            }
        }
    }
    Ok(())
}

struct Handle {
    conn: rusqlite::Connection,
    exclusive_file: ExclusiveFile,
    pending_writes: Vec<PendingWrite>,
    next_write_offset: u64,
    last_flush_offset: u64,
}

struct ExclusiveFile {
    inner: std::fs::File,
    id: u64,
}

impl Deref for ExclusiveFile {
    type Target = std::fs::File;

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
    let mut err = None;
    for i in 0..10000 {
        err = Some(
            match std::fs::OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(dir.as_ref().join(&i.to_string()))
            {
                Ok(file) => return Ok(ExclusiveFile { inner: file, id: i }),
                Err(err) => {
                    if err.kind() == ErrorKind::AlreadyExists {
                        continue;
                    }
                    err
                }
            },
        )
    }
    Err(err.unwrap()).context("gave up trying to create exclusive file")
}

const MANIFEST_SCHEMA_SQL: &str = include_str!("../manifest.sql");

fn init_manifest_schema(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    // We could do something smarter here like check a schema version and test for the existence of
    // tables.
    conn.execute_batch(MANIFEST_SCHEMA_SQL)
}

impl Handle {
    fn new_from_dir(dir: impl AsRef<Path>) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let sqlite_version = rusqlite::version_number();
        if sqlite_version < 3042000 {
            bail!(
                "sqlite version {} below minimum {}",
                rusqlite::version(),
                "3.42"
            );
        }
        let conn = rusqlite::Connection::open(dir.as_ref().join("manifest.db"))?;
        init_manifest_schema(&conn).context("initing manifest schema")?;
        let mut exclusive_file = open_new_exclusive_file(dir)?;
        let last_flush_offset = exclusive_file.seek(SeekFrom::Current(0))?;
        let next_write_offset = last_flush_offset;
        let mut handle = Self {
            conn,
            exclusive_file,
            pending_writes: vec![],
            last_flush_offset,
            next_write_offset,
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
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        for pw in &self.pending_writes {
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
            // let key_id = transaction.last_insert_rowid();
        }
        transaction.commit()?;
        self.pending_writes.clear();
        Ok(())
    }
}
