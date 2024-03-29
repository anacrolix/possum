use criterion::Criterion;
use possum::testing::torrent_storage::*;

pub(crate) fn benchmark(c: &mut Criterion) {
    let opts = TorrentStorageOpts { ..BENCHMARK_OPTS };
    c.benchmark_group("torrent_storage")
        .throughput(criterion::Throughput::Bytes(
            opts.num_pieces as u64 * opts.piece_size as u64,
        ))
        .sample_size(10)
        .bench_function("no_hole_punching", |b| {
            let inner = TorrentStorageOpts {
                disable_hole_punching: true,
                static_tempdir_name: "benchmark_torrent_storage_no_hole_punching",
                ..opts
            }
            .build()
            .unwrap();
            b.iter(|| inner.run().unwrap())
        })
        .bench_function("hole_punching", |b| {
            let inner = TorrentStorageOpts {
                disable_hole_punching: false,
                static_tempdir_name: "benchmark_torrent_storage_hole_punching",
                ..opts
            }
            .build()
            .unwrap();
            b.iter(|| inner.run().unwrap())
        });
}
