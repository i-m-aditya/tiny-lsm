use std::mem::size_of;
use std::sync::Arc;

use bytes::Buf as _;

use crate::{
    block::SIZEOF_U16,
    key::{KeySlice, KeyVec},
};

use super::Block;

pub struct BlockIterator {
    block: Arc<Block>,
    key: KeyVec,
    value_range: (usize, usize),
    idx: usize,
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            first_key: block.get_first_key(),
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    pub fn key(&self) -> KeySlice<'_> {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        self.key.as_key_slice()
    }

    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }

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

    fn seek_to_offset(&mut self, offset: usize) {
        let mut entry = &self.block.data[offset..];
        let overlap_len = entry.get_u16() as usize;
        let key_suffix_len = entry.get_u16() as usize;
        let key_suffix = &entry[..key_suffix_len];
        self.key.clear();
        self.key.append(&self.first_key.key_ref()[..overlap_len]);
        self.key.append(key_suffix);
        entry.advance(key_suffix_len);
        let ts = entry.get_u64();
        self.key.set_ts(ts);

        let value_len = entry.get_u16() as usize;
        let value_offset_begin =
            offset + SIZEOF_U16 + SIZEOF_U16 + key_suffix_len + size_of::<u64>() + SIZEOF_U16;
        let value_offset_end = value_offset_begin + value_len;
        self.value_range = (value_offset_begin, value_offset_end);
    }

    pub fn seek_to_key(&mut self, key: KeySlice) {
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
