use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context};
use log::info;
use possum::punchfile::punchfile;
use possum::seekhole::file_regions;
use possum::{query_last_end_offset, Handle, MANIFEST_DB_FILE_NAME};

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
        #[arg(required=true,num_args=1..)]
        files: Vec<PathBuf>,
    },
    /// This is a debugging command to check how far back a greedy hole punch will go.
    LastEndOffset {
        dir: PathBuf,
        file: String,
        offset: u64,
    },
}

#[derive(clap::Subcommand, Clone)]
enum DatabaseCommands {
    WriteFile { file: OsString },
    ListKeys { prefix: String },
    ReadKey { key: String },
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
            use DatabaseCommands::*;
            match command {
                WriteFile { file } => {
                    let key = Path::new(&file)
                        .file_name()
                        .ok_or_else(|| anyhow!("can't extract file name"))?;
                    let file = File::open(&file).with_context(|| format!("opening {:?}", key))?;
                    handle.single_write_from(key.to_os_string().into_vec(), file)?;
                    Ok(())
                }
                ListKeys { prefix } => {
                    let items = handle.list_items(prefix.as_bytes())?;
                    for item in items {
                        println!("{}", unsafe { std::str::from_utf8_unchecked(&item.key) })
                    }
                    Ok(())
                }
                ReadKey { key } => {
                    let Some(value) = handle.read_single(key.as_bytes())? else {
                        bail!("key not found")
                    };
                    let mut r = value.new_reader();
                    let n = std::io::copy(&mut r, &mut std::io::stdout())?;
                    // dbg!(n, value.length());
                    if n != value.length() {
                        bail!("read {} bytes, expected {}", n, value.length());
                    }
                    Ok(())
                }
            }
        }
        ShowHoles { files: paths } => {
            for path in paths {
                let mut file = OpenOptions::new()
                    .read(true)
                    .open(&path)
                    .context("opening file")?;
                for region in file_regions(&mut file)? {
                    println!(
                        "{}: {:?}, {}-{} (length {})",
                        path.display(),
                        region.region_type,
                        region.start,
                        region.end,
                        region.length()
                    );
                }
            }
            Ok(())
        }
        LastEndOffset { dir, file, offset } => {
            let mut conn = rusqlite::Connection::open(dir.join(MANIFEST_DB_FILE_NAME))?;
            let tx = conn.transaction()?;
            let file_id = file.into();
            let last_end = query_last_end_offset(&tx, &file_id, offset)?;
            println!("{}", last_end);
            Ok(())
        }
    }
}
