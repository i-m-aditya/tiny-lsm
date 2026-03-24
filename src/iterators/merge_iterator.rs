// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;
use bytes::Bytes;

use crate::key::KeySlice;

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

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
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

        // Pre-position at the smallest key so key()/value()/is_valid() work before first next().
        let current = binary_heap.pop();
        println!("Merge num iter: {}", num_iterators);
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
            KeySlice::from_slice(&[])
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
        // Remember the key we just output (for skipping duplicates).
        let prev_key: Bytes = self
            .current
            .as_ref()
            .map(|c| Bytes::copy_from_slice(c.1.key().raw_ref()))
            .unwrap_or_default();

        // Advance the iterator we were currently showing.
        if let Some(mut curr) = self.current.take() {
            curr.1.next()?;
            if curr.1.is_valid() {
                self.iters.push(curr);
            }
        }

        // Skip any heap top that has the same key we just output (duplicate key).
        loop {
            let mut w = match self.iters.pop() {
                Some(w) => w,
                None => break,
            };
            if w.1.key().raw_ref() == prev_key.as_ref() {
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
