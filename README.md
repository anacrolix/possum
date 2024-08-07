# Possum

![Crates.io Version](https://img.shields.io/crates/v/possum-db)
![docs.rs](https://img.shields.io/docsrs/possum-db)

## What is it?

Possum is a key-value cache stored directly on disk. It supports concurrent access from multiple processes without requiring interprocess communication or sidecar processes. It efficiently evicts data by using hole punching and sparse files. It supports read snapshots by using file cloning, a feature available on [filesystems](https://www.ctrl.blog/entry/file-cloning.html) such as Btrfs, XFS, ZFS, APFS and ReFSv2.

Value reads and writes occur directly on files, allowing memory-mapping, zero-copy/splicing, vector I/O, polling and more.

## Why?

I couldn’t find a cache implementation that supported storing directly on disk, concurrent access from multiple processes without a dedicated process, and limiting disk space usage. Existing solutions seem to be in-memory with snapshots to disk (Redis), or single-process with the disk management (including all keys) maintained in memory too (Bitcask derivatives). There are plenty of single-process (this is commonly implemented within individual applications), and fewer concurrent key-value disk stores, but they have no regard for space limits and don’t do user-level caching well (pretty much any LSM-based database falls into this category, for example RocksDB, Badger, and Pebble). So while in-memory key-value caches are well-supported, there is an unfilled niche for disk-caches, particularly one that can be shared by multiple applications. There are plenty of systems out there that have huge amounts of disk space that could be shared by package managers, HTTP proxies and reverse proxies, thumbnail generation, API caches and more where main memory latency isn’t needed.

## Technical Details

Possum maintains a pool of sparse files and a manifest file to synchronize writes and reads.

Writing is done by starting a batch. Possum records keys and exposes a location to write values concurrently. Value writes can be done using file handles to allow splicing and efficient system calls to offload to disk, and by copying or renaming files directly into the cache. When writes are completed, the batch keys are committed, and the appended and new files are released back into the cache for future reads and writes. Batches can be prepared concurrently. Writing does not block reading.

Reading is done by locking the cache, and recording the keys to be read. When all the keys to be read have been submitted, the regions of files in the cache containing the value data are cloned to temporary files (a very efficient, constant-time system operation) and made available for read for the lifetime of the snapshot.

The committing of keys when a write batch is finalized, and the creation of a read snapshot are the only serial operations. Write batch preparation, and read snapshots can coexist in any arrangement and quantity, including concurrently with the aforementioned serial operations.

Efficiently removing data, and creating read snapshots requires hole punching, and sparse files respectively. Both features are available on Windows, macOS, Solaris, BSDs and Linux, depending on the filesystem in use.

## Supported systems

macOS, Linux and Windows.

FreeBSD 14+. Unfortunately file cloning and open file description locking are missing. The implementation falls back on flock(2) and has to separate write and read files.

Solaris requires a small amount of work to complete the implementation.

On filesystems where block cloning is not supported (ext4 on Linux, and NTFS on Windows are notable examples), the implementation falls back to file region locking unless that is also not available (FreeBSD).

## anacrolix/squirrel

I previously wrote [anacrolix/squirrel](https://github.com/anacrolix/squirrel), which can fit the role of both in-memory or disk caching due to SQLite’s VFS. However, it’s written in Go and so is less accessible from other languages. Using SQLite for value storage means large streaming writes are exclusive, even within a single transaction. Value data is serialized to the blob format, potentially across multiple linked pages, and copied multiple times as items are evicted and moved around. Direct file I/O isn’t available with SQLite, and the size of values must be known before they can be written, which can mean copying streams to temporary files before they can be written into the SQLite file.
