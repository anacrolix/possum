use criterion::Criterion;

use possum::testing::torrent_storage::*;

pub(crate) fn benchmark(c: &mut Criterion) {
    let opts = TorrentStorageOpts {
        piece_size: 2 << 20,
        static_tempdir_name: "benchmark_torrent_storage_default",
        num_pieces: 8,
        block_size: 4096,
    };
    c.benchmark_group("torrent_storage")
        .throughput(criterion::Throughput::Bytes(
            opts.num_pieces as u64 * opts.piece_size as u64,
        ))
        .sample_size(10)
        .bench_function("torrent_storage", |b| {
            b.iter(|| torrent_storage_inner(opts))
        });
}
