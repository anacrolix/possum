use std::cmp::max;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context};
use itertools::Itertools;
use log::info;
use possum::sys::punchfile::punchfile;
use possum::sys::seekhole::{file_regions, Region, RegionType};
use possum::sys::SparseFile;
use possum::tx::ReadTransactionRef;
use possum::{ceil_multiple, check_hole, Handle, NonzeroValueLocation, WalkEntry};

#[derive(clap::Subcommand)]
enum Commands {
    PunchHole {
        file: String,
        offset: i64,
        length: i64,
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
    WriteFile {
        file: OsString,
    },
    ListKeys {
        prefix: String,
    },
    ReadKey {
        key: String,
    },
    PrintMissingHoles {
        file_id: Option<PathBuf>,
        #[arg(short, long)]
        fragments: bool,
    },
    PunchMissingHoles {
        file_id: Option<PathBuf>,
    },
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
            punchfile(&file, offset, length)?;
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
                    handle.single_write_from(key.to_os_string().into_encoded_bytes(), file)?;
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
                PrintMissingHoles {
                    file_id: values_file_path,
                    fragments,
                } => {
                    let tx = handle.start_deferred_transaction_for_read()?;
                    for values_file_entry in handle.walk_dir()?.iter().filter(|entry| {
                        matches!(entry.entry_type, possum::walk::EntryType::ValuesFile)
                    }) {
                        // Require that the values file path matches the file_id given if it's Some,
                        // otherwise show everything.
                        if values_file_path
                            .as_ref()
                            .map(|path| path == &values_file_entry.path)
                            .unwrap_or(true)
                        {
                            print_missing_holes(
                                tx.as_ref(),
                                values_file_entry,
                                handle.block_size(),
                                fragments,
                            )?;
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
                            // Read access might be required to query allocated ranges on Windows.
                            let mut file = std::fs::OpenOptions::new()
                                .write(true)
                                .read(true)
                                .open(&values_file_entry.path)?;
                            file.set_sparse(true)?;
                            // Make sure nobody could be writing to the file. It should be possible
                            // to punch holes before the last value despite this (just as greedy
                            // start hole punching occurs during regular key deletes).
                            possum::sys::flock::try_lock_file(
                                &mut file,
                                possum::sys::flock::LockExclusiveNonblock,
                            )?;
                            for FileRegion {
                                mut start,
                                mut length,
                            } in missing_holes(tx.as_ref(), values_file_entry)?
                            {
                                let delay = ceil_multiple(start, handle.block_size()) - start;
                                // dbg!(start, length, delay);
                                if delay >= length {
                                    continue;
                                }
                                start += delay;
                                length -= delay;
                                length -= length % handle.block_size();
                                if length == 0 {
                                    continue;
                                }
                                println!(
                                    "{}: {}-{} (length {}, block_size mod {})",
                                    &values_file_entry.path.display(),
                                    start,
                                    start + length,
                                    length,
                                    start % handle.block_size(),
                                );
                                possum::sys::punchfile::punchfile(
                                    &file,
                                    start as i64,
                                    length as i64,
                                )?;
                                check_hole(&mut file, start, length)?;
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
            let handle = Handle::new(dir)?;
            let tx = handle.start_deferred_transaction_for_read()?;
            let file_id = file.into();
            let last_end = tx.query_last_end_offset(&file_id, offset)?;
            println!("{}", last_end);
            Ok(())
        }
    }
}

struct FileRegion {
    pub start: u64,
    pub length: u64,
}

impl FileRegion {
    fn end(&self) -> u64 {
        self.start + self.length
    }
}

impl From<Region> for FileRegion {
    fn from(value: Region) -> Self {
        Self {
            start: value.start,
            length: value.length(),
        }
    }
}

impl From<NonzeroValueLocation> for FileRegion {
    fn from(value: NonzeroValueLocation) -> Self {
        Self {
            start: value.file_offset,
            length: value.length,
        }
    }
}

fn missing_holes(
    tx: ReadTransactionRef,
    values_file_entry: &WalkEntry,
) -> anyhow::Result<Vec<FileRegion>> {
    let file_id = values_file_entry.file_id().unwrap();
    let mut file = File::open(&values_file_entry.path)?;
    let iter =
        possum::sys::seekhole::Iter::new(&mut file).filter_map(|region_res| match region_res {
            Ok(walk_reg) if matches!(walk_reg.region_type, RegionType::Hole) => {
                Some(Ok(walk_reg.into()))
            }
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        });
    let mut binding = tx.file_values(file_id)?;
    let values_iter = binding
        .begin()?
        .filter_map_ok(|value| value.location.into_non_zero());
    missing_holes_pure(iter, values_iter)
}

fn missing_holes_pure(
    mut iter: impl Iterator<Item = Result<FileRegion, impl std::error::Error + Send + Sync + 'static>>,
    values: impl Iterator<
        Item = Result<NonzeroValueLocation, impl std::error::Error + Send + Sync + 'static>,
    >,
) -> anyhow::Result<Vec<FileRegion>> {
    let mut ret = vec![];
    let mut hole: Option<FileRegion> = None;
    let mut offset = 0;
    for value in values {
        let value: FileRegion = value?.into();
        while offset < value.start {
            while match &hole {
                None => true,
                Some(hole) => hole.end() <= offset,
            } {
                hole = iter.next().transpose()?;
                if hole.is_none() {
                    break;
                }
            }
            match &hole {
                Some(some_hole) if some_hole.start <= offset => {
                    offset = max(some_hole.end(), offset);
                }
                Some(hole) if hole.start < value.start => {
                    ret.push(FileRegion {
                        start: offset,
                        length: hole.start - offset,
                    });
                    offset = hole.end();
                }
                _ => {
                    ret.push(FileRegion {
                        start: offset,
                        length: value.start - offset,
                    });
                    break;
                }
            };
        }
        offset = value.start + value.length;
    }
    Ok(ret)
}

fn print_missing_holes(
    tx: ReadTransactionRef,
    values_file_entry: &WalkEntry,
    block_size: u64,
    fragments: bool,
) -> anyhow::Result<()> {
    let file_id = values_file_entry.file_id().unwrap();
    let mut last_hole_end = None;
    for FileRegion { start, length } in missing_holes(tx, values_file_entry)? {
        if let Some(last_hole_end) = last_hole_end {
            assert!(start > last_hole_end);
        }
        last_hole_end = Some(start + length);
        if !fragments && length < block_size {
            continue;
        }
        println!(
            "{}: {}-{} (length {}, block_size mod: {})",
            file_id,
            start,
            start + length,
            length,
            start % block_size,
        );
    }
    Ok(())
}
