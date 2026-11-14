//! A possum adapter for redb's storage engine benchmark harness (the `redb-bench` crate's
//! `BenchDatabase` trait family), so possum can be measured with the same workloads redb uses to
//! compare itself against LMDB, RocksDB, fjall and SQLite.
//!
//! Mapping, and where it differs from a transactional ordered key-value store:
//!
//! - A write transaction is a [possum::BatchWriter]. Inserts and removes are staged and applied in
//!   order when it commits. Reads inside a write transaction (the pop, retain and extract_if
//!   phases) see the last committed state, not the transaction's own staged changes. The pop
//!   phase tracks which keys it has popped itself so successive pops move inwards.
//! - A read transaction holds no snapshot. Each `get` is a [possum::Handle::read_single], which is
//!   possum's own read path: a short manifest transaction that also records the access time, then
//!   reading from a file clone or a locked file region.
//! - Range reads page through [possum::Handle::list_items_range] in key order.
//! - Possum never fsyncs (the manifest runs with `synchronous=off`), so it can't change its
//!   synchronization mode, and every write phase is effectively "nosync".
//! - There is no compaction. Possum reclaims space by punching holes in value files, so the size
//!   phases, which sum apparent file lengths, overstate the space possum actually uses.

use possum::{BatchWriter, Handle};
use redb_bench::*;
use std::collections::VecDeque;
use std::io::Write;
use std::ops::Bound;

/// How many keys to list per manifest query when paging through key ranges.
const PAGE_SIZE: u64 = 4096;

/// Range reads in the harness visit 10 keys, so fetch a few more than that per page.
const RANGE_PAGE_SIZE: u64 = 16;

type Pair = (Vec<u8>, Vec<u8>);

pub struct PossumBenchDatabase<'a> {
    handle: &'a Handle,
}

impl<'a> PossumBenchDatabase<'a> {
    pub fn new(handle: &'a Handle) -> Self {
        Self { handle }
    }
}

impl BenchDatabase for PossumBenchDatabase<'_> {
    type C<'db>
        = PossumBenchDatabaseConnection<'db>
    where
        Self: 'db;

    fn db_type_name() -> &'static str {
        "possum"
    }

    fn connect(&self) -> Self::C<'_> {
        PossumBenchDatabaseConnection {
            handle: self.handle,
        }
    }
}

pub struct PossumBenchDatabaseConnection<'a> {
    handle: &'a Handle,
}

impl BenchDatabaseConnection for PossumBenchDatabaseConnection<'_> {
    type W<'db>
        = PossumBenchWriteTransaction<'db>
    where
        Self: 'db;
    type R<'db>
        = PossumBenchReadTransaction<'db>
    where
        Self: 'db;

    fn write_transaction(&self) -> Self::W<'_> {
        PossumBenchWriteTransaction {
            handle: self.handle,
            writer: self.handle.new_writer().unwrap(),
        }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        PossumBenchReadTransaction {
            handle: self.handle,
        }
    }
}

pub struct PossumBenchWriteTransaction<'a> {
    handle: &'a Handle,
    writer: BatchWriter<&'a Handle>,
}

impl<'a> BenchWriteTransaction for PossumBenchWriteTransaction<'a> {
    type W<'txn>
        = PossumBenchInserter<'txn, 'a>
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        PossumBenchInserter {
            handle: self.handle,
            writer: &mut self.writer,
            popped_front: None,
            popped_back: None,
        }
    }

    fn commit(self) -> Result<(), ()> {
        self.writer.commit().map(|_| ()).map_err(|_| ())
    }
}

pub struct PossumBenchInserter<'txn, 'a> {
    handle: &'a Handle,
    writer: &'txn mut BatchWriter<&'a Handle>,
    // The last keys popped from each end in this transaction. Pops are staged deletes, so the
    // manifest still holds them until commit.
    popped_front: Option<Vec<u8>>,
    popped_back: Option<Vec<u8>>,
}

fn bound_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(key) => Bound::Included(key.as_slice()),
        Bound::Excluded(key) => Bound::Excluded(key.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn excluded_or_unbounded(key: &Option<Vec<u8>>) -> Bound<&[u8]> {
    match key {
        Some(key) => Bound::Excluded(key.as_slice()),
        None => Bound::Unbounded,
    }
}

/// Lists up to `limit` items in a key range and reads their values, within one possum read.
fn read_range(
    handle: &Handle,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    descending: bool,
    limit: u64,
) -> Vec<Pair> {
    let mut reader = handle.read().unwrap();
    let items = reader
        .list_items_range(start, end, descending, Some(limit))
        .unwrap();
    let values: Vec<_> = items
        .iter()
        .map(|item| reader.add(&item.key).unwrap().unwrap())
        .collect();
    let snapshot = reader.begin().unwrap();
    items
        .into_iter()
        .zip(values)
        .map(|(item, value)| {
            let bytes = snapshot.value(value).view(|bytes| bytes.to_vec()).unwrap();
            (item.key, bytes)
        })
        .collect()
}

/// Calls `f` on every item in a key range, in key order, a page at a time.
fn for_each_in_range(
    handle: &Handle,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    mut f: impl FnMut(Pair),
) {
    let mut start: Bound<Vec<u8>> = start.map(<[u8]>::to_vec);
    loop {
        let page = read_range(handle, bound_ref(&start), end, false, PAGE_SIZE);
        let Some((last_key, _)) = page.last() else {
            return;
        };
        start = Bound::Excluded(last_key.clone());
        let full = page.len() as u64 == PAGE_SIZE;
        page.into_iter().for_each(&mut f);
        if !full {
            return;
        }
    }
}

impl PossumBenchInserter<'_, '_> {
    fn pop(&mut self, descending: bool) -> Option<Pair> {
        let start = excluded_or_unbounded(&self.popped_front);
        let end = excluded_or_unbounded(&self.popped_back);
        let (key, value) = read_range(self.handle, start, end, descending, 1)
            .into_iter()
            .next()?;
        self.writer.stage_delete(key.clone());
        if descending {
            self.popped_back = Some(key.clone());
        } else {
            self.popped_front = Some(key.clone());
        }
        Some((key, value))
    }
}

impl BenchInserter for PossumBenchInserter<'_, '_> {
    type Output<'out>
        = Vec<u8>
    where
        Self: 'out;
    type ExtractIfIterator<'out, F>
        = VecBenchIterator<Vec<u8>>
    where
        Self: 'out,
        F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'out;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        let mut value_writer = self.writer.new_value().begin().map_err(|_| ())?;
        value_writer.write_all(value).map_err(|_| ())?;
        self.writer
            .stage_write(key.to_vec(), value_writer)
            .map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.writer.stage_delete(key.to_vec());
        Ok(())
    }

    fn pop_first(&mut self) -> BenchPopResult<'_, Self> {
        Ok(self.pop(false))
    }

    fn pop_last(&mut self) -> BenchPopResult<'_, Self> {
        Ok(self.pop(true))
    }

    fn retain<F: FnMut(&[u8], &[u8]) -> bool>(&mut self, mut predicate: F) -> Result<u64, ()> {
        // Skip anything this transaction already popped.
        let start = excluded_or_unbounded(&self.popped_front);
        let end = excluded_or_unbounded(&self.popped_back);
        let mut removed = 0;
        let writer = &mut *self.writer;
        for_each_in_range(self.handle, start, end, |(key, value)| {
            if !predicate(&key, &value) {
                writer.stage_delete(key);
                removed += 1;
            }
        });
        Ok(removed)
    }

    fn extract_if<'a, F>(
        &'a mut self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        mut predicate: F,
    ) -> BenchExtractIfResult<'a, Self, F>
    where
        F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'a,
    {
        let mut extracted = vec![];
        let writer = &mut *self.writer;
        for_each_in_range(self.handle, range.0, range.1, |(key, value)| {
            if predicate(&key, &value) {
                writer.stage_delete(key.clone());
                extracted.push((key, value));
            }
        });
        Ok(VecBenchIterator::new(extracted))
    }
}

pub struct PossumBenchReadTransaction<'a> {
    handle: &'a Handle,
}

impl<'a> BenchReadTransaction for PossumBenchReadTransaction<'a> {
    type T<'txn>
        = PossumBenchReader<'a>
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        PossumBenchReader {
            handle: self.handle,
        }
    }
}

pub struct PossumBenchReader<'a> {
    handle: &'a Handle,
}

impl<'a> BenchReader for PossumBenchReader<'a> {
    type Output<'out>
        = Vec<u8>
    where
        Self: 'out;
    type Iterator<'out>
        = PossumBenchIterator<'a>
    where
        Self: 'out;

    fn get<'b>(&'b mut self, key: &[u8]) -> Option<Self::Output<'b>> {
        self.handle
            .read_single(key)
            .unwrap()
            .map(|value| value.view(|bytes| bytes.to_vec()).unwrap())
    }

    fn range_from<'b>(&'b mut self, start: &'b [u8]) -> Self::Iterator<'b> {
        PossumBenchIterator {
            handle: self.handle,
            next_start: Bound::Included(start.to_vec()),
            buffered: Default::default(),
            exhausted: false,
        }
    }

    fn len(&mut self) -> u64 {
        let mut count = 0;
        let mut start: Option<Vec<u8>> = None;
        loop {
            let page = self
                .handle
                .list_items_range(
                    excluded_or_unbounded(&start),
                    Bound::Unbounded,
                    false,
                    Some(PAGE_SIZE * 16),
                )
                .unwrap();
            count += page.len() as u64;
            match page.into_iter().last() {
                Some(item) => start = Some(item.key),
                None => return count,
            }
        }
    }
}

/// Pages forward through keys from a start key, reading values a page at a time.
pub struct PossumBenchIterator<'a> {
    handle: &'a Handle,
    next_start: Bound<Vec<u8>>,
    buffered: VecDeque<Pair>,
    exhausted: bool,
}

impl BenchIterator for PossumBenchIterator<'_> {
    type Output<'out>
        = Vec<u8>
    where
        Self: 'out;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
        if self.buffered.is_empty() && !self.exhausted {
            let page = read_range(
                self.handle,
                bound_ref(&self.next_start),
                Bound::Unbounded,
                false,
                RANGE_PAGE_SIZE,
            );
            self.exhausted = (page.len() as u64) < RANGE_PAGE_SIZE;
            if let Some((last_key, _)) = page.last() {
                self.next_start = Bound::Excluded(last_key.clone());
            }
            self.buffered.extend(page);
        }
        self.buffered.pop_front()
    }
}
