//! redb's large values benchmark, with possum and fjall added. The workload is copied unchanged
//! from
//! https://github.com/cberner/redb/blob/96ce31fe5cafcf735425a02f1781487a4287e2de/crates/redb-bench-compare/benches/large_values_benchmark.rs
//! (MIT OR Apache-2.0), because upstream keeps it in the bench file rather than in redb-bench.

use tempfile::{NamedTempFile, TempDir};

use redb_bench::benchmark_dir;

mod common;
use common::*;
use possum_redb_bench::PossumBenchDatabase;
use redb_bench::*;

use rand::Rng;
use std::time::Instant;
const ELEMENTS: usize = 1_000_000;

/// Returns pairs of key, value
fn random_data(count: usize, key_size: usize, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut pairs = vec![];

    for _ in 0..count {
        let mut key = vec![0; key_size];
        rand::rng().fill_bytes(&mut key);
        let mut value = vec![0; value_size];
        rand::rng().fill_bytes(&mut value);
        pairs.push((key, value));
    }

    pairs
}

fn benchmark<T: BenchDatabase>(db: T) -> Vec<(String, ResultType)> {
    let mut results = Vec::new();
    let mut pairs = random_data(1_000_000, 24, 150);
    let mut written = 0;

    let mut bigpairs = random_data(100, 24, 2_000_000);
    let bigelements = 4000;

    let start = Instant::now();
    let connection = db.connect();
    let mut txn = connection.write_transaction();
    let mut inserter = txn.get_inserter();
    {
        for _ in 0..bigelements {
            let len = bigpairs.len();
            let (key, value) = &mut bigpairs[written % len];
            key[16..].copy_from_slice(&(written as u64).to_le_bytes());
            inserter.insert(key, value).unwrap();
            written += 1;
        }
        for _ in 0..ELEMENTS {
            let len = pairs.len();
            let (key, value) = &mut pairs[written % len];
            key[16..].copy_from_slice(&(written as u64).to_le_bytes());
            inserter.insert(key, value).unwrap();
            written += 1;
        }
    }
    drop(inserter);
    txn.commit().unwrap();

    let end = Instant::now();
    let duration = end - start;
    let result = ResultType::keys(bigelements + ELEMENTS, duration);
    println!(
        "{}: Bulk loaded {} 2MB items and {} small items in {}ms ({})",
        T::db_type_name(),
        bigelements,
        ELEMENTS,
        duration.as_millis(),
        result.with_unit()
    );
    results.push(("bulk load (2MB values)".to_string(), result));

    results
}

fn main() {
    let _ = env_logger::try_init();

    let mut results = vec![];

    if engine_enabled("redb") {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(benchmark_dir()).unwrap();
        let mut db = redb::Database::builder().create(tmpfile.path()).unwrap();
        let table = RedbBenchDatabase::new(&mut db);
        results.push(("redb", benchmark(table)));
    }

    if engine_enabled("lmdb") {
        let tmpfile: TempDir = tempfile::tempdir_in(benchmark_dir()).unwrap();
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(10 * 4096 * 1024 * 1024)
                .open(tmpfile.path())
                .unwrap()
        };
        let table = HeedBenchDatabase::new(env);
        results.push(("lmdb", benchmark(table)));
    }

    if engine_enabled("fjall") {
        let tmpfile: TempDir = tempfile::tempdir_in(benchmark_dir()).unwrap();
        let mut db = fjall::SingleWriterTxDatabase::builder(tmpfile.path())
            .open()
            .unwrap();
        let table = FjallBenchDatabase::new(&mut db);
        results.push(("fjall", benchmark(table)));
    }

    if engine_enabled("possum") {
        let tempdir: TempDir = tempfile::tempdir_in(benchmark_dir()).unwrap();
        let handle = possum::Handle::new(tempdir.path().to_owned()).unwrap();
        let table = PossumBenchDatabase::new(&handle);
        results.push(("possum", benchmark(table)));
    }

    print_results_table(&results);
}
