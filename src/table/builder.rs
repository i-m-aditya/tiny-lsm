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
use std::{mem, path::Path};

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, SsTable};
use crate::key::Key;
use crate::table::FileObject;
use crate::table::bloom::Bloom;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // update the key_hashes
        let key_hash = farmhash::fingerprint32(key.raw_ref());
        self.key_hashes.push(key_hash);

        // Track first key in SSTable
        if self.first_key.is_empty() {
            self.first_key = key.raw_ref().to_vec();
        }

        let is_added = self.builder.add(key, value);

        if is_added {
            // Successfully added to current block
            self.last_key = key.raw_ref().to_vec();
        } else {
            // Current block is full, finalize it

            // Replace current builder with a new one, get the old builder
            let old_block_builder =
                mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));

            // Create block metadata using the old builder's first_key and current last_key
            let block_meta = BlockMeta {
                offset: self.data.len(),
                first_key: Key::from_bytes(Bytes::copy_from_slice(
                    old_block_builder.first_key.raw_ref(),
                )),
                last_key: Key::from_bytes(Bytes::copy_from_slice(&self.last_key)),
            };
            self.meta.push(block_meta);

            // Build and encode the block
            let block = old_block_builder.build();
            let encoded_block = block.encode();
            self.data.extend_from_slice(&encoded_block);

            // Add the key-value pair to the new block
            let _ = self.builder.add(key, value);
            self.last_key = key.raw_ref().to_vec();
        }
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        #[allow(unused_mut)] mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // Finalize the last block if it has data
        if !self.builder.is_empty() {
            let block_meta = BlockMeta {
                offset: self.data.len(),
                first_key: Key::from_bytes(Bytes::copy_from_slice(
                    self.builder.first_key.raw_ref(),
                )),
                last_key: Key::from_bytes(Bytes::copy_from_slice(&self.last_key)),
            };
            self.meta.push(block_meta);

            let block = self.builder.build();
            let encoded_block = block.encode();
            self.data.extend_from_slice(&encoded_block);
        }

        // The block_meta_offset is where metadata starts (after all data blocks)
        let block_meta_offset = self.data.len();

        // Encode block metadata
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);

        // Write the block metadata offset (u32)
        self.data
            .extend_from_slice(&(block_meta_offset as u32).to_be_bytes());

        let bloom_bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);

        let bloom = Bloom::build_from_key_hashes(self.key_hashes.as_ref(), bloom_bits_per_key);

        let mut encoded_bloom_buf = Vec::new();
        bloom.encode(&mut encoded_bloom_buf);

        let bloom_len = encoded_bloom_buf.len();
        let bloom_offset = self.data.len();

        self.data
            .extend_from_slice(&(bloom_len as u32).to_be_bytes());
        self.data.extend_from_slice(&encoded_bloom_buf);
        self.data
            .extend_from_slice(&(bloom_offset as u32).to_be_bytes());

        // println!("Path: {:?}", path.as_ref());
        let file_obj = FileObject::create(path.as_ref(), self.data)?;

        Ok(SsTable {
            file: file_obj,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key: Key::from_bytes(Bytes::copy_from_slice(&self.first_key)),
            last_key: Key::from_bytes(Bytes::copy_from_slice(&self.last_key)),
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
