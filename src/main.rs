use std::ffi::OsString;
use std::fs::{File, OpenOptions};

use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};

use log::info;

use possum::punchfile::punchfile;
use possum::{seekhole, Handle};

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
    // ( ͡° ͜ʖ ͡°)
    ShowHoles {
        file: PathBuf,
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
            let file = OpenOptions::new().write(true).open(file)?;
            punchfile(file, offset, length)?;
            Ok(())
        }
        Database { dir, command } => {
            info!("sqlite version: {}", rusqlite::version());
            let handle = Handle::new(dir)?;
            match command {
                DatabaseCommands::WriteFile { file } => {
                    let key = Path::new(&file)
                        .file_name()
                        .ok_or_else(|| anyhow!("can't extract file name"))?;
                    let file = File::open(&file).with_context(|| format!("opening {:?}", key))?;
                    handle.single_write_from(key.to_os_string().into_vec(), file)?;
                    Ok(())
                }
            }
        }
        ShowHoles { file: path } => {
            let file = OpenOptions::new()
                .read(true)
                .open(path)
                .context("opening file")?;
            let raw_fd = file.as_raw_fd();
            for region in seekhole::Iter::new_from_fd(raw_fd) {
                match region {
                    Ok(region) => println!("{:?}, length {}", region, region.end - region.start),
                    Err(err) => println!("{:#?}", err),
                }
            }
            Ok(())
        }
    }
}
