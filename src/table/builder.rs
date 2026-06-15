use std::sync::Arc;
use std::{mem, path::Path};

use anyhow::Result;

use super::{BlockMeta, SsTable};
use crate::key::KeyVec;
use crate::table::FileObject;
use crate::table::bloom::Bloom;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        let key_hash = farmhash::fingerprint32(key.key_ref());
        self.key_hashes.push(key_hash);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        let is_added = self.builder.add(key, value);

        if is_added {
            self.last_key = key.to_key_vec();
        } else {
            let old_block_builder =
                mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));

            let block_meta = BlockMeta {
                offset: self.data.len(),
                first_key: old_block_builder.first_key.clone().into_key_bytes(),
                last_key: self.last_key.clone().into_key_bytes(),
            };
            self.meta.push(block_meta);

            let block = old_block_builder.build();
            let encoded_block = block.encode();
            let checksum = crc32fast::hash(&encoded_block);
            self.data.extend_from_slice(&encoded_block);
            self.data.extend_from_slice(&checksum.to_be_bytes());

            let _ = self.builder.add(key, value);
            self.last_key = key.to_key_vec();
        }
    }

    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    pub fn build(
        #[allow(unused_mut)] mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.builder.is_empty() {
            let block_meta = BlockMeta {
                offset: self.data.len(),
                first_key: self.builder.first_key.clone().into_key_bytes(),
                last_key: self.last_key.clone().into_key_bytes(),
            };
            self.meta.push(block_meta);

            let block = self.builder.build();
            let encoded_block = block.encode();
            let checksum = crc32fast::hash(&encoded_block);
            self.data.extend_from_slice(&encoded_block);
            self.data.extend_from_slice(&checksum.to_be_bytes());
        }

        let block_meta_offset = self.data.len();

        BlockMeta::encode_block_meta(&self.meta, &mut self.data);

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

        let file_obj = FileObject::create(path.as_ref(), self.data)?;

        Ok(SsTable {
            file: file_obj,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key: self.first_key.clone().into_key_bytes(),
            last_key: self.last_key.clone().into_key_bytes(),
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
