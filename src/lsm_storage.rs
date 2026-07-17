use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{KeySlice, TS_RANGE_BEGIN};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{self, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::mvcc::txn::Transaction;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

fn range_overlap(
    user_begin: Bound<&[u8]>,
    user_end: Bound<&[u8]>,
    table_begin: KeySlice<'_>,
    table_end: KeySlice<'_>,
) -> bool {
    match user_end {
        Bound::Excluded(key) if key <= table_begin.key_ref() => {
            return false;
        }
        Bound::Included(key) if key < table_begin.key_ref() => {
            return false;
        }
        _ => {}
    }
    match user_begin {
        Bound::Excluded(key) if key >= table_end.key_ref() => {
            return false;
        }
        Bound::Included(key) if key > table_end.key_ref() => {
            return false;
        }
        _ => {}
    }
    true
}

fn key_within(user_key: &[u8], table_begin: KeySlice<'_>, table_end: KeySlice<'_>) -> bool {
    table_begin.key_ref() <= user_key && user_key <= table_end.key_ref()
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(()).ok();
        self.compaction_notifier.send(()).ok();

        self.flush_thread.lock().take().map(|h| h.join());
        self.compaction_thread.lock().take().map(|h| h.join());

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        let snapshot = self.inner.state.read();

        if !snapshot.memtable.is_empty() {
            let state_lock_observer = self.inner.state_lock.lock();
            self.inner.force_freeze_memtable(&state_lock_observer)?;
        }

        while !snapshot.imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.clone().scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        fs::create_dir_all(path)?;

        let mut state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let manifest_path = path.join("MANIFEST");
        let manifest;

        if fs::exists(&manifest_path).unwrap() {
            let records;
            (manifest, records) = Manifest::recover(manifest_path)?;

            let mut next_sst_id = 0;

            let mut flushed_memtables = HashSet::new();
            let mut all_memtables = Vec::new();

            for record in records {
                match record {
                    ManifestRecord::NewMemtable(new_sst_id) => {
                        next_sst_id = max(next_sst_id, new_sst_id + 1);
                        all_memtables.insert(0, new_sst_id);
                    }
                    ManifestRecord::Flush(new_sst_id) => {
                        next_sst_id = max(next_sst_id, new_sst_id + 1);
                        state.l0_sstables.insert(0, new_sst_id);
                        flushed_memtables.insert(new_sst_id);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let (new_state, _removed_ssts) = compaction_controller
                            .apply_compaction_result(&state, &task, output.as_slice(), true);

                        let t = *output.iter().max().unwrap_or(&0);
                        next_sst_id = max(next_sst_id, t + 1);
                        state = new_state;
                    }
                }
            }

            all_memtables.retain_mut(|id| !flushed_memtables.contains(id));

            if options.enable_wal {
                let mut iter = all_memtables.into_iter();
                if let Some(active_id) = iter.next() {
                    state.memtable = Arc::new(MemTable::recover_from_wal(
                        active_id,
                        Self::path_of_wal_static(path, active_id),
                    )?);
                }
                for id in iter {
                    let imm = MemTable::recover_from_wal(id, Self::path_of_wal_static(path, id))?;
                    state.imm_memtables.push(Arc::new(imm));
                }
            }

            let block_cache = Arc::new(BlockCache::new(1024));

            for sst_id in state.l0_sstables.iter() {
                let sst_path = Self::path_of_sst_static(path, *sst_id);
                let file = FileObject::open(&sst_path)?;
                let sst = SsTable::open(*sst_id, Some(block_cache.clone()), file)?;
                state.sstables.insert(*sst_id, Arc::new(sst));
            }

            for (_, sst_ids) in state.levels.iter() {
                for sst_id in sst_ids.iter() {
                    let sst_path = Self::path_of_sst_static(path, *sst_id);
                    let file = FileObject::open(&sst_path)?;
                    let sst = SsTable::open(*sst_id, Some(block_cache.clone()), file)?;
                    state.sstables.insert(*sst_id, Arc::new(sst));
                }
            }

            // Recover the commit timestamp from the max ts seen across all data.
            let mut max_ts = 0u64;
            for sst in state.sstables.values() {
                max_ts = max_ts.max(sst.max_ts());
            }
            for memtable in std::iter::once(&state.memtable).chain(state.imm_memtables.iter()) {
                let mut iter = memtable.scan(Bound::Unbounded, Bound::Unbounded);
                while iter.is_valid() {
                    max_ts = max_ts.max(iter.key().ts());
                    iter.next()?;
                }
            }

            let storage = Self {
                state: Arc::new(RwLock::new(Arc::new(state))),
                state_lock: Mutex::new(()),
                path: path.to_path_buf(),
                block_cache,
                next_sst_id: AtomicUsize::new(next_sst_id),
                compaction_controller,
                manifest: Some(manifest),
                options: options.into(),
                mvcc: Some(LsmMvccInner::new(max_ts)),
                compaction_filters: Arc::new(Mutex::new(Vec::new())),
            };
            Ok(storage)
        } else {
            manifest = Manifest::create(&manifest_path)?;
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    0,
                    Self::path_of_wal_static(path, 0),
                )?);
                manifest.add_record_when_init(ManifestRecord::NewMemtable(0))?;
            }
            let storage = Self {
                state: Arc::new(RwLock::new(Arc::new(state))),
                state_lock: Mutex::new(()),
                path: path.to_path_buf(),
                block_cache: Arc::new(BlockCache::new(1024)),
                next_sst_id: AtomicUsize::new(1),
                compaction_controller,
                manifest: Some(manifest),
                options: options.into(),
                mvcc: Some(LsmMvccInner::new(0)),
                compaction_filters: Arc::new(Mutex::new(Vec::new())),
            };
            Ok(storage)
        }
    }

    pub fn sync(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let read_ts = self.mvcc().latest_commit_ts();
        self.get_with_ts(key, read_ts)
    }

    pub fn get_with_ts(&self, key: &[u8], read_ts: u64) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let memtable_iter = snapshot
            .memtable
            .scan(Bound::Included(key), Bound::Included(key));

        let mut in_memory_table_iters = Vec::new();
        in_memory_table_iters.push(Box::new(memtable_iter));
        for memtable in &snapshot.imm_memtables {
            in_memory_table_iters.push(Box::new(
                memtable.scan(Bound::Included(key), Bound::Included(key)),
            ));
        }
        let merge_1 = MergeIterator::create(in_memory_table_iters);

        let ks = KeySlice::from_slice(key, TS_RANGE_BEGIN);

        let keep_table = |key: &[u8], table: &SsTable| {
            if key_within(
                key,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                if let Some(bloom) = &table.bloom {
                    bloom.may_contain(farmhash::fingerprint32(key))
                } else {
                    true
                }
            } else {
                false
            }
        };

        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table_id].clone();
            if keep_table(key, &table) {
                l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    table, ks,
                )?));
            }
        }
        let l0_merge_iter = MergeIterator::create(l0_iters);
        let mem_l0_merge_iter = TwoMergeIterator::create(merge_1, l0_merge_iter)?;

        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for sst_id in level_sst_ids {
                let sstable = snapshot.sstables[sst_id].clone();
                if keep_table(key, &sstable) {
                    level_ssts.push(sstable);
                }
            }
            let level_iter = SstConcatIterator::create_and_seek_to_key(level_ssts, ks)?;
            level_iters.push(Box::new(level_iter));
        }

        let mut iter =
            TwoMergeIterator::create(mem_l0_merge_iter, MergeIterator::create(level_iters))?;

        // Advance past versions newer than the snapshot.
        while iter.is_valid() && iter.key().key_ref() == key && iter.key().ts() > read_ts {
            iter.next()?;
        }

        if iter.is_valid() && iter.key().key_ref() == key && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }
        Ok(None)
    }

    /// Write a batch of data into the storage.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let _lck = self.mvcc().write_lock.lock();

        let ts = self.mvcc().latest_commit_ts() + 1;

        let mut data = Vec::new();
        for record in batch {
            let (key, value): (&[u8], &[u8]) = match record {
                WriteBatchRecord::Put(k, v) => (k.as_ref(), v.as_ref()),
                WriteBatchRecord::Del(k) => (k.as_ref(), &[]),
            };
            data.push((KeySlice::from_slice(key, ts), value));
        }
        self.state.read().memtable.put_batch(&data)?;
        self.try_freeze()?;

        self.mvcc().update_commit_ts(ts);
        Ok(())
    }

    pub fn try_freeze(&self) -> Result<()> {
        let approx_size = self.state.read().memtable.approximate_size();
        if approx_size >= self.options.target_sst_size {
            let state_lock_observer = self.state_lock.lock();
            self.force_freeze_memtable(&state_lock_observer)?;
        }
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let read_state_guard = self.state.read();

        let old_mem_table = read_state_guard.memtable.clone();
        let mut new_storage_state = read_state_guard.as_ref().clone();

        drop(read_state_guard);
        new_storage_state.imm_memtables.insert(0, old_mem_table);

        if self.options.enable_wal {
            let memtable_id = self.next_sst_id();
            new_storage_state.memtable =
                Arc::new(MemTable::create_with_wal(memtable_id, self.path_of_wal(memtable_id))?);
        } else {
            new_storage_state.memtable = Arc::new(MemTable::create(self.next_sst_id()));
        }

        self.manifest.as_ref().unwrap().add_record(
            state_lock_observer,
            ManifestRecord::NewMemtable(new_storage_state.memtable.id()),
        )?;

        let mut write_state_guard = self.state.write();
        *write_state_guard = Arc::new(new_storage_state);
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let memtable_to_flush = {
            let guard = self.state.read();
            guard.imm_memtables.last().cloned() // Clone the Arc
        };

        if let Some(memtable) = memtable_to_flush {
            let mut ss_table_builder = SsTableBuilder::new(self.options.block_size);

            memtable.flush(&mut ss_table_builder)?;

            let sst_id = memtable.id();
            let sst_path = self.path_of_sst(sst_id);
            let ss_table =
                ss_table_builder.build(sst_id, Some(self.block_cache.clone()), sst_path)?;

            let state_lock_observer = self.state_lock.lock();
            let mut guard = self.state.write();
            let mut new_state = guard.as_ref().clone();

            let mem = new_state.imm_memtables.pop().unwrap();
            assert_eq!(mem.id(), sst_id);

            if self.compaction_controller.flush_to_l0() {
                new_state.l0_sstables.insert(0, sst_id);
            } else {
                new_state.levels.insert(0, (sst_id, vec![sst_id]));
            }

            new_state.sstables.insert(sst_id, Arc::new(ss_table));

            *guard = Arc::new(new_state);

            self.manifest
                .as_ref()
                .unwrap()
                .add_record(&state_lock_observer, ManifestRecord::Flush(sst_id))?;
        }
        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self.mvcc().new_txn(self.clone(), self.options.serializable))
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        self: &Arc<Self>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let read_ts = self.mvcc().latest_commit_ts();
        self.scan_with_ts(lower, upper, read_ts)
    }

    pub fn scan_with_ts(
        self: &Arc<Self>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(lower, upper)));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        let mut table_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table_id].clone();
            if range_overlap(
                lower,
                upper,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                let iter = match lower {
                    Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                        table,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?,
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            table,
                            KeySlice::from_slice(key, TS_RANGE_BEGIN),
                        )?;
                        while iter.is_valid() && iter.key().key_ref() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
                };
                table_iters.push(Box::new(iter));
            }
        }
        let l0_iter = MergeIterator::create(table_iters);

        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for table_id in level_sst_ids {
                let table = snapshot.sstables[table_id].clone();
                if range_overlap(
                    lower,
                    upper,
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice(),
                ) {
                    level_ssts.push(table);
                }
            }

            let level_iter = match lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    level_ssts,
                    KeySlice::from_slice(key, TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        level_ssts,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?;
                    while iter.is_valid() && iter.key().key_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(level_ssts)?,
            };
            level_iters.push(Box::new(level_iter));
        }

        let iter = TwoMergeIterator::create(memtable_iter, l0_iter)?;
        let iter = TwoMergeIterator::create(iter, MergeIterator::create(level_iters))?;

        Ok(FusedIterator::new(LsmIterator::new(iter, mem_table::map_bound(upper), read_ts)?))
    }
}
