use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        let mut buff = Vec::new();
        file.read_to_end(&mut buff)?;
        let mut data = buff.as_slice();

        while !data.is_empty() {
            let key_len = data.get_u16();
            let key = &data[..key_len as usize];
            data.advance(key_len as usize);

            let ts = data.get_u64();

            let value_len = data.get_u16();
            let value = &data[..value_len as usize];
            data.advance(value_len as usize);

            let mut hasher = crc32fast::Hasher::new();
            hasher.write_u16(key_len);
            hasher.write(key);
            hasher.write_u64(ts);
            hasher.write_u16(value_len);
            hasher.write(value);

            assert_eq!(hasher.finalize(), data.get_u32());

            let key_bytes = KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(key), ts);
            skiplist.insert(key_bytes, Bytes::copy_from_slice(value));
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeyBytes, value: &[u8]) -> Result<()> {
        let mut file_guard = self.file.lock();
        let mut buf = Vec::with_capacity(key.raw_len() + value.len() + 2 + 2 + 4);
        let mut hasher = crc32fast::Hasher::new();

        buf.put_u16(key.key_len() as u16);
        hasher.write_u16(key.key_len() as u16);

        buf.put_slice(key.key_ref());
        buf.put_u64(key.ts());
        hasher.write(key.key_ref());
        hasher.write_u64(key.ts());

        buf.put_u16(value.len() as u16);
        hasher.write_u16(value.len() as u16);

        buf.put_slice(value);
        hasher.write(value);

        buf.put_u32(hasher.finalize());

        file_guard.write_all(&buf)?;
        Ok(())
    }

    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file_guard = self.file.lock();
        file_guard.flush()?;
        file_guard.get_mut().sync_all()?;
        Ok(())
    }
}
