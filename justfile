build-supported-targets:
    for a in `cat supported-targets`; do just build-target "$a"; done

build-tests-windows:
    # This outputs the paths of the generated test executables, which can be passed to wine.
    cargo test --no-run --target x86_64-pc-windows-gnu

test-windows:
    cargo test --target x86_64-pc-windows-gnu

test-xfs:
    truncate -s 1G xfsfs
    mkfs -t xfs xfsfs || true
    mkdir xfsmnt || true
    mount xfsfs xfsmnt
    ln -sTf xfsmnt tmp
    TMPDIR=xfsmnt cargo test

sync-repo dest *args:
    rsync ./ {{ dest }} -rit --filter ':- .gitignore' -f '- .git/' {{ args }}

flamegraph-macos bench_filter:
    CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph --root --bench possum -- --bench '{{ bench_filter }}'

build-target target:
    cargo build --release --target {{target}}
