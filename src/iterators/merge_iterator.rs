use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::{KeySlice, TS_DEFAULT};

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
    num_iterators: usize,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut num_iterators = 0;
        let mut binary_heap = BinaryHeap::new();
        for (index, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                num_iterators += iter.num_active_iterators();
                binary_heap.push(HeapWrapper(index, iter));
            }
        }

        let current = binary_heap.pop();
        Self {
            iters: binary_heap,
            current,
            num_iterators,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice<'_> {
        if let Some(curr) = &self.current {
            curr.1.key()
        } else {
            KeySlice::from_slice(&[], TS_DEFAULT)
        }
    }

    fn value(&self) -> &[u8] {
        if let Some(curr) = &self.current {
            curr.1.value()
        } else {
            &[]
        }
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        let prev_key = self
            .current
            .as_ref()
            .map(|c| c.1.key().to_key_vec())
            .unwrap_or_default();

        if let Some(mut curr) = self.current.take() {
            curr.1.next()?;
            if curr.1.is_valid() {
                self.iters.push(curr);
            }
        }

        loop {
            let mut w = match self.iters.pop() {
                Some(w) => w,
                None => break,
            };
            if w.1.key() == prev_key.as_key_slice() {
                w.1.next()?;
                if w.1.is_valid() {
                    self.iters.push(w);
                }
            } else if w.1.is_valid() {
                self.current = Some(w);
                return Ok(());
            }
        }
        self.current = None;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.num_iterators
    }
}
