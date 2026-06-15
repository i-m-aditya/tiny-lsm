mod builder;
mod iterator;

use std::hash::Hasher;

pub use builder::BlockBuilder;
use bytes::{Buf as _, BufMut, Bytes};
use crate::key::KeyVec;
pub use iterator::BlockIterator;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    pub(crate) fn get_first_key(&self) -> KeyVec {
        let mut buf = &self.data[..];
        buf.get_u16();
        let key_len = buf.get_u16() as usize;
        let key = &buf[..key_len];
        buf.advance(key_len);
        let ts = buf.get_u64();
        KeyVec::from_vec_with_ts(key.to_vec(), ts)
    }

    pub fn encode(&self) -> Bytes {
        let mut encoded = Vec::with_capacity(self.data.len() + self.offsets.len() + 1);
        encoded.put(self.data.as_ref());

        let mut hasher = crc32fast::Hasher::new();
        hasher.write(&self.data);
        let checksum = hasher.finalize();
        encoded.put_u32(checksum);

        for offset in &self.offsets {
            encoded.put_u16(*offset);
        }
        encoded.put_u16(self.offsets.len() as u16);

        Bytes::from(encoded)
    }

    pub fn decode(data: &[u8]) -> Self {
        let data_len = data.len();
        let num_entries = u16::from_be_bytes(
            data[(data_len - 2)..data_len]
                .try_into()
                .expect("Slice should be 2 bytes"),
        );
        let mut tmp = Vec::with_capacity(num_entries as usize);

        let offset_start = (data_len - 2) - 2 * num_entries as usize;
        let offset_end = data_len - 2;

        tmp.extend_from_slice(&data[offset_start..offset_end]);

        let mut offsets = Vec::new();
        for (i, _val) in tmp.iter().enumerate().step_by(2) {
            let offset = u16::from_be_bytes([tmp[i], tmp[i + 1]]);
            offsets.push(offset);
        }

        let _checksum = u32::from_be_bytes(
            data[(offset_start - 4)..offset_start]
                .try_into()
                .expect("slice should be 4 bytes"),
        );
        let mut decoded_data = Vec::new();
        decoded_data.extend_from_slice(&data[0..(offset_start - 4)]);

        Block {
            data: decoded_data,
            offsets,
        }
    }
}
