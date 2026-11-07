# possum in redb's benchmarks

This crate runs [redb](https://github.com/cberner/redb)'s storage engine benchmarks against possum, alongside redb, LMDB and fjall, for [issue #3](https://github.com/anacrolix/possum/issues/3).

- `src/lib.rs` implements redb's `BenchDatabase` traits ([redb-bench](https://github.com/cberner/redb/blob/96ce31fe5cafcf735425a02f1781487a4287e2de/crates/redb-bench/src/lib.rs)) for possum.
- `benches/lmdb_benchmark.rs` runs `redb_bench::benchmark`, the workload behind the table in redb's README, unchanged: 24 byte keys, 150 byte values, 5M bulk load, individual and batch writes, random reads and range reads, reads on 4 to 32 threads, removals, retain, extract_if, pop, and on-disk size.
- `benches/large_values_benchmark.rs` is redb's large values benchmark, copied because upstream keeps it in the bench file: one transaction inserting 4,000 values of 2 MB and 1M values of 150 bytes.
- `benches/common/upstream.rs` holds redb's LMDB and fjall adapters, copied unchanged. RocksDB is left out because it's a long C++ build. SQLite is left out because redb's adapter needs a rusqlite whose bundled SQLite conflicts with the one possum links.

redb is pinned to commit `96ce31f` in `Cargo.toml` so results stay comparable.

## Running

```sh
cd bench-redb
cargo bench --bench lmdb_benchmark
cargo bench --bench large_values_benchmark
```

Set `BENCH_ENGINES` to a comma separated subset of `redb,lmdb,fjall,possum` to run fewer engines. The databases are created in the current directory, or in `REDB_BENCHMARK_DIR` if set, so point that at the filesystem you want to measure. Possum uses file cloning on Btrfs, XFS, ZFS and APFS, and falls back to file region locking elsewhere (ext4, for example), so record which one was used.

The large values benchmark writes about 8 GB per engine.

## How possum maps onto the harness

redb's harness models a transactional, ordered key-value store. Possum is a cache, so some phases don't map one to one:

- A write transaction is a `BatchWriter`. Inserts and removes are staged and applied in order on commit, using `BatchWriter::stage_delete`, which was added for this.
- Reads inside a write transaction (the pop, retain and extract_if phases) see the last committed state, not the transaction's own staged changes. The pop phase tracks the keys it has popped so successive pops move inwards.
- A read transaction holds no snapshot. Each `get` is `Handle::read_single`, possum's normal read path. That takes the manifest lock briefly and records the access time, so reads from many threads serialize on it.
- Range reads, `len()`, retain, extract_if and pop page through `Handle::list_items_range`, added for this, in key order.
- Possum never fsyncs (the manifest uses `synchronous=off`). It can't switch sync modes, so "nosync writes" shows N/A, and the individual and batch write phases are already unsynced for possum while the other engines sync.
- There's no compaction, so "compacted size" shows N/A. Possum frees space by punching holes in value files. The size rows sum apparent file lengths, so they overstate what possum actually uses on disk.
