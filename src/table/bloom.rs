use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};

/// Implements a bloom filter
pub struct Bloom {
    /// data of filter in bits
    pub(crate) filter: Bytes,
    /// number of hash functions
    pub(crate) k: u8,
}

pub trait BitSlice {
    fn get_bit(&self, idx: usize) -> bool;
    fn bit_len(&self) -> usize;
}

pub trait BitSliceMut {
    fn set_bit(&mut self, idx: usize, val: bool);
}

impl<T: AsRef<[u8]>> BitSlice for T {
    fn get_bit(&self, idx: usize) -> bool {
        let pos = idx / 8;
        let offset = idx % 8;
        println!("Pos: {}, offset: {}", pos, offset);
        (self.as_ref()[pos] & (1 << offset)) != 0
    }

    fn bit_len(&self) -> usize {
        self.as_ref().len() * 8
    }
}

impl<T: AsMut<[u8]>> BitSliceMut for T {
    fn set_bit(&mut self, idx: usize, val: bool) {
        let pos = idx / 8;
        let offset = idx % 8;
        if val {
            self.as_mut()[pos] |= 1 << offset;
        } else {
            self.as_mut()[pos] &= !(1 << offset);
        }
    }
}

impl Bloom {
    /// Decode a bloom filter
    pub fn decode(buf: &[u8]) -> Result<Self> {
        let data_len = buf.len() - std::mem::size_of::<u32>();
        let expected = crc32fast::hash(&buf[..data_len]);
        let actual = u32::from_be_bytes(buf[data_len..].try_into().unwrap());
        anyhow::ensure!(expected == actual, "bloom filter checksum mismatch");
        let k = buf[data_len - 1];
        let filter = &buf[..data_len - 1];
        Ok(Self {
            filter: filter.to_vec().into(),
            k,
        })
    }

    /// Encode a bloom filter
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let start = buf.len();
        buf.extend_from_slice(&self.filter);
        buf.put_u8(self.k);
        let checksum = crc32fast::hash(&buf[start..]);
        buf.put_u32(checksum);
    }

    /// Get bloom filter bits per key from entries count and FPR
    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        let size = -(entries as f64) * false_positive_rate.ln() / std::f64::consts::LN_2.powi(2);
        let locs = (size / (entries as f64)).ceil();
        locs as usize
    }

    /// Build bloom filter from key hashes
    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Self {
        let k = (bits_per_key as f64 * 0.69) as u32;
        let k = k.clamp(1, 30);
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = nbits.div_ceil(8);
        let nbits = nbytes * 8;
        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);

        for h in keys.iter() {
            let delta = h.rotate_left(15); // h is the key hash

            let mut h = *h;
            for _ in 0..k {
                let bit_position = (h as usize) % nbits;

                filter.set_bit(bit_position, true);

                h = h.wrapping_add(delta);
            }
        }

        Self {
            filter: filter.freeze(),
            k: k as u8,
        }
    }

    /// Check if a bloom filter may contain some data
    pub fn may_contain(&self, h: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short bloom filters
            true
        } else {
            let nbits = self.filter.bit_len();
            let delta = h.rotate_left(15);

            let mut h = h;
            for _ in 0..self.k {
                let bit_pos = (h as usize) % nbits;

                if !self.filter.get_bit(bit_pos) {
                    return false;
                }
                h = h.wrapping_add(delta);
            }

            true
        }
    }

    pub fn key_hash(key: u32, num_bits: u32) {}
}

#[cfg(test)]
mod tt {
    use super::*;

    #[test]
    fn test_get_bit() {
        let val = [1, 7, 3, 4];

        println!("{:?}", val);
        let val = Bytes::copy_from_slice(&val[..]);

        let output = val.get_bit(10);
        println!("Output: {}", output);
    }
}
