//! redb's main comparison benchmark, with possum added. The engine setup for redb, LMDB and fjall
//! is copied from
//! https://github.com/cberner/redb/blob/96ce31fe5cafcf735425a02f1781487a4287e2de/crates/redb-bench-compare/benches/lmdb_benchmark.rs
//! (MIT OR Apache-2.0). The workload itself is `redb_bench::benchmark`, used unchanged.

use std::{fs, process};
use tempfile::{NamedTempFile, TempDir};

mod common;
use common::*;
use possum_redb_bench::PossumBenchDatabase;
use redb_bench::*;

fn main() {
    let _ = env_logger::try_init();
    let tmpdir = benchmark_dir().join(".benchmark");
    fs::create_dir(&tmpdir).unwrap();

    let tmpdir2 = tmpdir.clone();
    ctrlc::set_handler(move || {
        fs::remove_dir_all(&tmpdir2).unwrap();
        process::exit(1);
    })
    .unwrap();

    let mut results = vec![];

    if engine_enabled("redb") {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(&tmpdir).unwrap();
        let mut db = redb::Database::builder()
            .set_cache_size(CACHE_SIZE)
            .create(tmpfile.path())
            .unwrap();
        let table = RedbBenchDatabase::new(&mut db);
        results.push(("redb", benchmark(table, tmpfile.path())));
    }

    if engine_enabled("lmdb") {
        let tempdir: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(4096 * 1024 * 1024)
                .open(tempdir.path())
                .unwrap()
        };
        let table = HeedBenchDatabase::new(env);
        results.push(("lmdb", benchmark(table, tempdir.path())));
    }

    if engine_enabled("fjall") {
        let tmpfile: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();
        let mut db = fjall::SingleWriterTxDatabase::builder(tmpfile.path())
            .cache_size(CACHE_SIZE.try_into().unwrap())
            .open()
            .unwrap();
        let table = FjallBenchDatabase::new(&mut db);
        results.push(("fjall", benchmark(table, tmpfile.path())));
    }

    if engine_enabled("possum") {
        let tempdir: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();
        let handle = possum::Handle::new(tempdir.path().to_owned()).unwrap();
        let table = PossumBenchDatabase::new(&handle);
        results.push(("possum", benchmark(table, tempdir.path())));
    }

    fs::remove_dir_all(&tmpdir).unwrap();

    print_results_table(&results);
}
