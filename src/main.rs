use std::cmp::max;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context};
use log::info;
use possum::punchfile::punchfile;
use possum::seekhole::{file_regions, Region, RegionType};
use possum::{
    ceil_multiple, check_hole, query_last_end_offset, Handle, Transaction, WalkEntry,
    MANIFEST_DB_FILE_NAME,
};

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
    MissingHoles { file_id: Option<PathBuf> },
    PunchMissingHoles { file_id: Option<PathBuf> },
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
                MissingHoles {
                    file_id: values_file_path,
                } => {
                    let tx = handle.start_deferred_transaction_for_read()?;
                    for values_file_entry in handle.walk_dir()?.iter().filter(|entry| {
                        matches!(entry.entry_type, possum::walk::EntryType::ValuesFile)
                    }) {
                        if values_file_path
                            .as_ref()
                            .map(|path| path == &values_file_entry.path)
                            .unwrap_or(true)
                        {
                            print_missing_holes(&tx, values_file_entry)?;
                        }
                    }
                    Ok(())
                }
                PunchMissingHoles {
                    file_id: values_file_path,
                } => {
                    let tx = handle.start_deferred_transaction_for_read()?;
                    for values_file_entry in handle.walk_dir()?.iter().filter(|entry| {
                        matches!(entry.entry_type, possum::walk::EntryType::ValuesFile)
                    }) {
                        if values_file_path
                            .as_ref()
                            .map(|path| path == &values_file_entry.path)
                            .unwrap_or(true)
                        {
                            let mut file = std::fs::OpenOptions::new()
                                .write(true)
                                .open(&values_file_entry.path)?;
                            // Make sure nobody could be writing to the file. It should be possible
                            // to punch holes before the last value despite this (just as greedy
                            // start hole punching occurs during regular key deletes).
                            possum::file_locking::try_lock_file(
                                &mut file,
                                possum::file_locking::LockExclusiveNonblock,
                            )?;
                            for (mut start, mut length) in missing_holes(&tx, values_file_entry)? {
                                let delay = ceil_multiple(start, handle.block_size()) - start;
                                // dbg!(start, length, delay);
                                if delay >= length {
                                    continue;
                                }
                                start += delay;
                                length -= delay;
                                length -= length % handle.block_size();
                                println!(
                                    "punching hole in {}: {}-{} (length {}, block_size mod {})",
                                    &values_file_entry.path.display(),
                                    start,
                                    start + length,
                                    length,
                                    start % handle.block_size(),
                                );
                                check_hole(&mut file, start, length)?;
                                possum::punchfile::punchfile(
                                    file.as_raw_fd(),
                                    start as i64,
                                    length as i64,
                                )?;
                            }
                        }
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

fn missing_holes(
    tx: &Transaction,
    values_file_entry: &WalkEntry,
) -> anyhow::Result<Vec<(u64, u64)>> {
    let file_id = values_file_entry.file_id().unwrap();
    let file = File::open(&values_file_entry.path)?;
    let mut ret = vec![];
    let mut iter =
        possum::seekhole::Iter::new(file.as_raw_fd()).filter_map(|region_res| match region_res {
            Ok(
                reg @ Region {
                    region_type: RegionType::Hole,
                    ..
                },
            ) => Some(Ok(reg)),
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        });
    let mut hole: Option<Region> = None;
    let mut offset = 0;
    for value in tx.file_values(file_id)?.begin()? {
        let value = value?;
        while offset < value.file_offset() {
            while match &hole {
                None => true,
                Some(hole) => hole.end <= offset,
            } {
                hole = iter.next().transpose()?;
                if hole.is_none() {
                    break;
                }
            }
            match &hole {
                Some(some_hole) if some_hole.start <= offset => {
                    offset = max(some_hole.end, offset);
                }
                Some(hole) if hole.start < value.file_offset() => {
                    ret.push((offset, hole.start - offset));
                    offset = hole.end;
                }
                _ => {
                    ret.push((offset, value.file_offset() - offset));
                    break;
                }
            };
        }
        offset = value.file_offset() + value.length();
    }
    Ok(ret)
}

fn print_missing_holes(tx: &Transaction, values_file_entry: &WalkEntry) -> anyhow::Result<()> {
    let file_id = values_file_entry.file_id().unwrap();
    for (start, length) in missing_holes(tx, values_file_entry)? {
        println!(
            "{}: {}-{} (length {})",
            file_id,
            start,
            start + length,
            length,
        );
    }
    Ok(())
}
