use anyhow::{anyhow, Context};
use log::info;
use possum::punchfile::punchfile;
use possum::Handle;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};

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
            let file = OpenOptions::new().write(true).open(file)?;
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
                    handle.single_write_from(key.to_os_string().into_vec(), file)?;
                    Ok(())
                }
            }
        }
    }
}
