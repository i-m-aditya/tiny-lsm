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

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    pub(crate) first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // Calculate entry size: 2 bytes (key_len) + key + 2 bytes (value_len) + value
        let entry_size = 2 + key.len() + 2 + value.len();

        // Calculate offset size: 2 bytes per offset entry
        let offset_size = 2;

        // Calculate total size after adding this entry
        let new_size = self.data.len() + entry_size + (self.offsets.len() + 1) * offset_size + 2;

        // Check if it would exceed block_size (but allow first entry)
        if !self.is_empty() && new_size > self.block_size {
            return false; // Block is full
        }

        if self.first_key.is_empty() {
            self.first_key.append(key.raw_ref());
            self.offsets.push(self.data.len() as u16);

            let key_len = key.len() as u16;

            let value_len = value.len() as u16;

            self.offsets.push(self.data.len() as u16);
            // Then append the parts:
            self.data.extend_from_slice(&key_len.to_be_bytes());
            self.data.extend_from_slice(key.raw_ref());
            self.data.extend_from_slice(&value_len.to_be_bytes());
            self.data.extend_from_slice(value);
        } else {
            self.offsets.push(self.data.len() as u16);
            let key_overlap_len =
                common_prefix(self.first_key.raw_ref(), key.for_testing_key_ref());

            if key_overlap_len != 0 {
                let key_len = key.len() as u16;
                let value_len = value.len() as u16;

                self.data
                    .extend_from_slice(&(key_overlap_len as u16).to_be_bytes());
                self.data.extend_from_slice(
                    &((key.raw_ref().len() - key_overlap_len) as u16).to_be_bytes(),
                );
                self.data
                    .extend_from_slice(&key.raw_ref()[key_overlap_len..]);
                self.data.extend_from_slice(&value_len.to_be_bytes());
                self.data.extend_from_slice(value);
            } else {
                let key_len = key.len() as u16;
                let value_len = value.len() as u16;

                // Then append the parts:
                self.data.extend_from_slice(&key_len.to_be_bytes());
                self.data.extend_from_slice(key.raw_ref());
                self.data.extend_from_slice(&value_len.to_be_bytes());
                self.data.extend_from_slice(value);
            }
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}

pub fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}
