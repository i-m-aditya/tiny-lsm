use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::KeySlice;

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

        let buf_writer = BufWriter::new(file);
        Ok(Self {
            file: Arc::new(Mutex::new(buf_writer)),
        })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        unimplemented!()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut data = Vec::new();

        data.extend_from_slice(&key.len().to_be_bytes());
        data.extend_from_slice(key);
        data.extend_from_slice(&value.len().to_be_bytes());
        data.extend_from_slice(value);

        let mut file_guard = self.file.lock();
        file_guard.write_all(data.as_slice())?;
        Ok(())
    }

    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        for (key, value) in data {
            self.put(key.raw_ref(), value)?;
        }
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file_guard = self.file.lock();
        file_guard.flush()?;
        file_guard.get_mut().sync_all()?;
        Ok(())
    }
}
