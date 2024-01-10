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
	rsync ./ {{dest}} -rv --filter ':- .gitignore' --exclude '.git*' {{ args}}
