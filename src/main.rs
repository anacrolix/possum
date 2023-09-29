use std::io::Error;
use std::os::fd::AsRawFd;
use std::path::Path;
// use clap::{Parser, Subcommand};

mod clonefile;

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
    WriteFile { file: String },
}

#[derive(clap::Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

fn main() -> anyhow::Result<()> {
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
                eprintln!("{:?}", Error::last_os_error());
            }
        }
        Database { dir, command } => {
            let mut handle = Handle::new_from_dir(dir)?;
            match command {
                DatabaseCommands::WriteFile { file } => {
                    let key = &file;
                    let file = std::fs::File::open(&file)?;
                    handle.write_key(key.as_bytes(), file)?;
                }
            }
        }
    }
    Ok(())
}

struct Handle {
    conn: rusqlite::Connection,
    exclusive_file: std::fs::File,
}

fn open_new_exclusive_file(dir: impl AsRef<Path>) -> anyhow::Result<std::fs::File> {
    for i in 0..10000 {
        // TODO: Handle already exists errors etc.
        match std::fs::OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(dir.as_ref().join(&i.to_string()))
        {
            Ok(file) => return Ok(file),
            Err(err) => return Err(err.into()),
        }
    }
    anyhow::bail!("gave up trying to create exclusive file");
}

impl Handle {
    fn new_from_dir(dir: impl AsRef<Path>) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let conn = rusqlite::Connection::open(dir.as_ref().join("manifest.db"))?;
        let exclusive_file = open_new_exclusive_file(dir)?;
        Ok(Self {
            conn,
            exclusive_file,
        })
    }

    fn block_size() -> u64 {
        4096
    }

    fn write_key(&mut self, key: &[u8], mut value: impl std::io::Read) -> anyhow::Result<()> {
        std::io::copy(&mut value, &mut self.exclusive_file)?;
        // TODO: Hash parts
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        // TODO: Unlock exclusive file tail

        Ok(())
    }
}
