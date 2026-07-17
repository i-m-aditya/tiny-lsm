use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

pub struct MemTable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub(crate) fn map_bound_keybytes(bound: Bound<&[u8]>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x),
            TS_DEFAULT,
        )),
        Bound::Excluded(x) => Bound::Excluded(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x),
            TS_DEFAULT,
        )),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn map_bound_lower(bound: Bound<&[u8]>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x),
            TS_RANGE_BEGIN,
        )),
        Bound::Excluded(x) => Bound::Excluded(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x),
            TS_RANGE_END,
        )),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn map_bound_upper(bound: Bound<&[u8]>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x),
            TS_RANGE_END,
        )),
        Bound::Excluded(x) => Bound::Excluded(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x),
            TS_RANGE_BEGIN,
        )),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    pub fn create(id: usize) -> Self {
        MemTable {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0_usize)),
        }
    }

    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let wal = Wal::create(path.as_ref())?;
        Ok(MemTable {
            map: Arc::new(SkipMap::new()),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let skip_map = Arc::new(SkipMap::new());
        let wal = Wal::recover(path.as_ref(), skip_map.as_ref())?;
        let size = skip_map
            .iter()
            .map(|e| e.key().raw_len() + e.value().len())
            .sum();
        Ok(MemTable {
            map: Arc::clone(&skip_map),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(size)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(lower, upper)
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map
            .get(&KeyBytes::from_bytes_with_ts(
                Bytes::copy_from_slice(key),
                TS_DEFAULT,
            ))
            .map(|entry| entry.value().clone())
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let keybytes =
            KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(key.key_ref()), key.ts());

        let _ = self
            .map
            .insert(keybytes.clone(), Bytes::copy_from_slice(value));

        self.approximate_size
            .fetch_add(keybytes.raw_len() + value.len(), Ordering::Relaxed);

        if let Some(wal) = &self.wal {
            wal.put(keybytes, value)?;
        }
        Ok(())
    }

    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        for (key, value) in data {
            self.put(*key, value)?;
        }
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        let map = self.map.clone();
        let lower = map_bound_lower(lower);
        let upper = map_bound_upper(upper);

        let mut iter = MemTableIteratorBuilder {
            map,
            iter_builder: |map| map.range((lower, upper)),
            item: (KeyBytes::new(), Bytes::new()),
        }
        .build();

        iter.next().unwrap();
        iter
    }

    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        let mut iter = self.scan(Bound::Unbounded, Bound::Unbounded);

        while iter.is_valid() {
            builder.add(iter.key(), iter.value());
            iter.next()?;
        }

        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

#[self_referencing]
pub struct MemTableIterator {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    item: (KeyBytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> KeySlice<'_> {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let mut new_item = (KeyBytes::new(), Bytes::new());
        self.with_iter_mut(|it| {
            if let Some(entry) = it.next() {
                new_item = (entry.key().clone(), entry.value().clone())
            }
        });

        self.with_item_mut(|item| *item = new_item);
        Ok(())
    }
}
