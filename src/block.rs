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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut encoded = Vec::with_capacity(self.data.len() + self.offsets.len() + 1);
        encoded.extend_from_slice(self.data.as_ref());

        for offset in &self.offsets {
            encoded.extend_from_slice(&offset.to_be_bytes());
        }
        encoded.extend_from_slice(&(self.offsets.len() as u16).to_be_bytes());

        Bytes::from(encoded)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let data_len = data.len();
        let num_entries = u16::from_be_bytes(
            data[data_len - 2..data_len]
                .try_into()
                .expect("Slice should be 2 bytes"),
        );
        let mut tmp = Vec::with_capacity(num_entries as usize);

        let offset_start = data_len - 2 - 2 * num_entries as usize;
        let offset_end = data_len - 2;

        tmp.extend_from_slice(&data[offset_start..offset_end]);

        let mut offsets = Vec::new();

        for (i, val) in tmp.iter().enumerate().step_by(2) {
            let offset = u16::from_be_bytes([tmp[i], tmp[i + 1]]);
            offsets.push(offset);
        }
        let mut decoded_data = Vec::new();
        decoded_data.extend_from_slice(&data[0..offset_start]);

        Block {
            data: decoded_data,
            offsets,
        }
    }
}
