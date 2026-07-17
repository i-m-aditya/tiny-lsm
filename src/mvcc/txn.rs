use std::{
    collections::HashSet,
    ops::Bound,
    sync::{Arc, atomic::AtomicBool},
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::LsmStorageInner,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(key_hashes) = &self.key_hashes {
            let mut guard = key_hashes.lock();
            let key_hash = farmhash::hash32(key);
            let _ = guard.1.insert(key_hash);
        }
        // Check local (uncommitted) writes first.
        if let Some(entry) = self.local_storage.get(key) {
            if entry.value().is_empty() {
                return Ok(None); // local tombstone
            }
            return Ok(Some(entry.value().clone()));
        }

        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let lsm_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        let local_iter = TxnLocalIterator::create(self.local_storage.clone(), lower, upper);
        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(local_iter, lsm_iter)?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if let Some(key_hashes) = &self.key_hashes {
            let mut guard = key_hashes.lock();
            let key_hash = farmhash::hash32(key);
            let _ = guard.1.insert(key_hash);
        }

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        if let Some(key_hashes) = &self.key_hashes {
            let mut guard = key_hashes.lock();
            let key_hash = farmhash::hash32(key);
            let _ = guard.1.insert(key_hash);
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
    }

    pub fn commit(&self) -> Result<()> {
        unimplemented!()
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {}
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    map: Arc<SkipMap<Bytes, Bytes>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    pub fn create(
        map: Arc<SkipMap<Bytes, Bytes>>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Self {
        let lower = map_bound(lower);
        let upper = map_bound(upper);
        let mut iter = TxnLocalIteratorBuilder {
            map,
            iter_builder: |map| map.range((lower, upper)),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        iter.next().unwrap();
        iter
    }
}

fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_ref()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry =
            self.with_iter_mut(|iter| iter.next().map(|e| (e.key().clone(), e.value().clone())));
        self.with_item_mut(|item| {
            *item = entry.unwrap_or_default();
        });
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut txn_iter = Self { txn, iter };
        txn_iter.move_to_non_delete()?;
        txn_iter.record_read_key();
        Ok(txn_iter)
    }

    fn record_read_key(&self) {
        if !self.iter.is_valid() {
            return;
        }
        if let Some(key_hashes) = &self.txn.key_hashes {
            let key_hash = farmhash::hash32(self.iter.key());
            key_hashes.lock().1.insert(key_hash);
        }
    }

    fn skip_key(&mut self) -> Result<()> {
        let key = Bytes::copy_from_slice(self.iter.key());
        while self.iter.is_valid() && self.iter.key() == key.as_ref() {
            self.iter.next()?;
        }
        Ok(())
    }

    fn move_to_non_delete(&mut self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.skip_key()?;
        }
        Ok(())
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.skip_key()?;
        self.move_to_non_delete()?;
        self.record_read_key();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
