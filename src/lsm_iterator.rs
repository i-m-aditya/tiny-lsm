use std::ops::Bound;

use anyhow::Result;
use bytes::Bytes;

use crate::{
    iterators::{
        StorageIterator, concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    read_timestamp: u64,
    prev_key: Option<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>, read_timestamp: u64) -> Result<Self> {
        let mut lsm_iter = Self {
            inner: iter,
            end_bound,
            read_timestamp,
            prev_key: None,
        };

        lsm_iter.skip_tompstone()?;

        if lsm_iter.inner.is_valid() {
            lsm_iter.prev_key = Some(Bytes::copy_from_slice(lsm_iter.inner.key().key_ref()));
        }

        Ok(lsm_iter)
    }

    /// Advance the inner iterator past any entries that should not be visible:
    ///   - versions newer than `read_timestamp`
    ///   - tombstones (the best visible version of a key is a delete marker,
    ///     so skip all remaining versions of that user key and repeat)
    pub(crate) fn skip_tompstone(&mut self) -> Result<()> {
        loop {
            // Skip versions that are too new for our snapshot.
            while self.inner.is_valid() && self.inner.key().ts() > self.read_timestamp {
                self.inner.next()?;
            }

            if !self.inner.is_valid() {
                break;
            }

            // We are now at the desired version for the current user key.
            if !self.inner.value().is_empty() {
                break; // non-tombstone: correct position
            }

            // Tombstone: the key is logically deleted. Skip all remaining versions
            // of this user key before evaluating the next one.
            let tomb_key = Bytes::copy_from_slice(self.inner.key().key_ref());
            while self.inner.is_valid() && self.inner.key().key_ref() == tomb_key.as_ref() {
                self.inner.next()?;
            }
        }
        Ok(())
    }

    fn in_range(&self) -> bool {
        match &self.end_bound {
            Bound::Included(bound) => self.inner.key().key_ref() <= bound.as_ref(),
            Bound::Excluded(bound) => self.inner.key().key_ref() < bound.as_ref(),
            Bound::Unbounded => true,
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid() && self.in_range()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        // Skip all remaining versions of the user key we just returned.
        while self.inner.is_valid() && self.prev_key.as_deref() == Some(self.inner.key().key_ref())
        {
            self.inner.next()?;
        }

        // Advance to the first visible entry for the next user key.
        self.skip_tompstone()?;

        // Update prev_key for the next call.
        if self.inner.is_valid() {
            self.prev_key = Some(Bytes::copy_from_slice(self.inner.key().key_ref()));
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            return Err(anyhow::anyhow!("iterator has previously errored"));
        }
        match self.iter.next() {
            Ok(()) => Ok(()),
            Err(e) => {
                self.has_errored = true;
                Err(e)
            }
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
