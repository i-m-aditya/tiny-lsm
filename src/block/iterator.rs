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

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let data_entry = if block.offsets.len() > 1 {
            &block.data[block.offsets[0] as usize..block.offsets[1] as usize]
        } else {
            &block.data
        };

        let mut cursor = 0_usize;
        let key_len = u16::from_be_bytes(
            data_entry[cursor..cursor + 2]
                .try_into()
                .expect("Slice len is less than 2"),
        );

        cursor += 2;
        let key = &data_entry[cursor..(cursor + key_len as usize)];
        cursor += key_len as usize;

        let value_len = u16::from_be_bytes(
            data_entry[cursor..cursor + 2]
                .try_into()
                .expect("Slice len less than 2"),
        );

        cursor += 2;
        let value = &data_entry[cursor..(cursor + value_len as usize)];
        BlockIterator {
            block: block.clone(),
            key: KeyVec::from_vec(key.to_vec()),
            value_range: (cursor, cursor + value_len as usize),
            idx: 0,
            first_key: KeyVec::from_vec(key.to_vec()),
        }
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }

    /// Seeks to the idx-th key in the block.
    fn seek_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
        self.idx = idx;
    }

    /// Seek to the specified position and update the current `key` and `value`
    fn seek_to_offset(&mut self, offset: usize) {
        let data_entry = &self.block.data[offset..];
        let mut cursor = 0_usize;

        if !self.first_key.is_empty() {
            let key_overlap_len = u16::from_be_bytes(
                data_entry[cursor..cursor + 2]
                    .try_into()
                    .expect("slice len less than 2"),
            );
            cursor += 2;

            let rest_len = u16::from_be_bytes(
                data_entry[cursor..cursor + 2]
                    .try_into()
                    .expect("slice len less than 2"),
            );
            cursor += 2;

            let rest = &data_entry[cursor..cursor + rest_len as usize];
            let mut key = Vec::new();
            key.extend_from_slice(&self.first_key.raw_ref()[0..key_overlap_len as usize]);
            key.extend_from_slice(rest);

            cursor += rest_len as usize;
            let value_len = u16::from_be_bytes(
                data_entry[cursor..cursor + 2]
                    .try_into()
                    .expect("Slice len less than 2"),
            );
            cursor += 2;

            self.key.clear();
            self.key.append(key.as_ref());

            if self.first_key.is_empty() {
                self.first_key.append(key.as_ref());
            }

            let value_start = offset + cursor;
            self.value_range = (value_start, value_start + value_len as usize);
        } else {
            let key_len = u16::from_be_bytes(
                data_entry[cursor..cursor + 2]
                    .try_into()
                    .expect("Slice len is less than 2"),
            );
            cursor += 2;

            let key = &data_entry[cursor..cursor + key_len as usize];
            cursor += key_len as usize;

            let value_len = u16::from_be_bytes(
                data_entry[cursor..cursor + 2]
                    .try_into()
                    .expect("Slice len less than 2"),
            );
            cursor += 2;

            self.key.clear();
            self.key.append(key);

            if self.first_key.is_empty() {
                self.first_key.append(key);
            }

            let value_start = offset + cursor;
            self.value_range = (value_start, value_start + value_len as usize);
        }
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // use binary search to find the key
        let (mut start, mut end) = (0, self.block.offsets.len());
        let mut mid;
        while start < end {
            mid = (start + end) / 2;
            self.seek_to(mid);
            let mid_key = self.key();

            match mid_key.cmp(&key) {
                std::cmp::Ordering::Less => {
                    start = mid + 1;
                }
                std::cmp::Ordering::Greater => {
                    end = mid;
                }
                std::cmp::Ordering::Equal => {
                    return;
                }
            }
        }

        self.seek_to(start);
    }
}
