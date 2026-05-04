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

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = None;
        let mut new_sst = Vec::new();

        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            let builder_inner = builder.as_mut().unwrap();
            if compact_to_bottom {
                if !iter.value().is_empty() {
                    builder_inner.add(iter.key(), iter.value());
                }
            } else {
                builder_inner.add(iter.key(), iter.value());
            }
            iter.next()?;

            if builder_inner.estimated_size() >= self.options.target_sst_size {
                let sst_id = self.next_sst_id();
                let builder = builder.take().unwrap();
                let sst = Arc::new(builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?);
                new_sst.push(sst);
            }
        }
        if let Some(builder) = builder {
            let sst_id = self.next_sst_id();
            let sst = Arc::new(builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?);
            new_sst.push(sst);
        }
        Ok(new_sst)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            state.as_ref().clone()
        };
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                for id in l0_sstables.iter() {
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables.get(id).unwrap().clone(),
                    )?));
                }
                let mut l1_ssts = Vec::with_capacity(l1_sstables.len());
                for id in l1_sstables.iter() {
                    l1_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                }
                let iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_ssts)?,
                )?;
                self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
            }
            CompactionTask::Simple(simple_leveled_task) => {
                if simple_leveled_task.upper_level.is_none() {
                    // l0 to l1 merge
                    let mut l0_iters =
                        Vec::with_capacity(simple_leveled_task.upper_level_sst_ids.len());

                    for id in &simple_leveled_task.upper_level_sst_ids {
                        l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables.get(&id).unwrap().clone(),
                        )?));
                    }
                    let mut l1_ssts =
                        Vec::with_capacity(simple_leveled_task.lower_level_sst_ids.len());
                    for id in &simple_leveled_task.lower_level_sst_ids {
                        l1_ssts.push(snapshot.sstables.get(&id).unwrap().clone());
                    }
                    let iter = TwoMergeIterator::create(
                        MergeIterator::create(l0_iters),
                        SstConcatIterator::create_and_seek_to_first(l1_ssts)?,
                    )?;
                    self.compact_generate_sst_from_iter(
                        iter,
                        simple_leveled_task.is_lower_level_bottom_level,
                    )
                } else {
                    let upper_level = simple_leveled_task.upper_level.unwrap();
                    let lower_level = simple_leveled_task.lower_level;

                    let mut upper_ssts =
                        Vec::with_capacity(simple_leveled_task.upper_level_sst_ids.len());

                    for id in &simple_leveled_task.upper_level_sst_ids {
                        upper_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }

                    let mut lower_ssts =
                        Vec::with_capacity(simple_leveled_task.lower_level_sst_ids.len());

                    for id in &simple_leveled_task.lower_level_sst_ids {
                        lower_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }

                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;

                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;

                    self.compact_generate_sst_from_iter(
                        iter,
                        simple_leveled_task.is_lower_level_bottom_level,
                    )
                }
            }
            CompactionTask::Tiered(tiered_compaction_task) => {
                let mut iters = Vec::with_capacity(tiered_compaction_task.tiers.len());
                for (_, sst_ids) in &tiered_compaction_task.tiers {
                    let mut level_ssts = Vec::with_capacity(sst_ids.len());
                    for sst_id in sst_ids {
                        level_ssts.push(snapshot.sstables.get(sst_id).unwrap().clone());
                    }
                    let iter = SstConcatIterator::create_and_seek_to_first(level_ssts)?;
                    iters.push(Box::new(iter));
                }
                self.compact_generate_sst_from_iter(
                    MergeIterator::create(iters),
                    task.compact_to_bottom_level(),
                )
            }
            _ => unimplemented!(),
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };
        let ssts_to_compact: Vec<usize> = l0_sstables
            .iter()
            .chain(l1_sstables.iter())
            .copied()
            .collect();

        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        };

        let new_ssts = self.compact(&compaction_task)?;
        let new_sst_ids: Vec<usize> = new_ssts.iter().map(|s| s.sst_id()).collect();

        {
            let mut guard = self.state.write();
            let state = Arc::make_mut(&mut guard);

            state.l0_sstables.retain(|id| !ssts_to_compact.contains(id));

            for id in &ssts_to_compact {
                state.sstables.remove(id);
            }
            for sst in new_ssts {
                state.sstables.insert(sst.sst_id(), sst);
            }

            state.levels[0] = (1, new_sst_ids);
        }

        for compact_sst_id in &ssts_to_compact {
            let path = self.path_of_sst(*compact_sst_id);
            std::fs::remove_file(path)?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let task = {
            let snapshot = self.state.read();
            self.compaction_controller
                .generate_compaction_task(&snapshot)
        };
        let Some(compaction_task) = task else {
            return Ok(());
        };

        let output_sst = self.compact(&compaction_task)?;
        let output: Vec<usize> = output_sst.iter().map(|sst| sst.sst_id()).collect();

        let mut new_state = {
            let snapshot = self.state.read();
            (**snapshot).clone()
        };
        for sst in output_sst {
            new_state.sstables.insert(sst.sst_id(), sst);
        }

        let (mut new_state, removed_ssts) = self.compaction_controller.apply_compaction_result(
            &new_state,
            &compaction_task,
            &output,
            false,
        );

        for id in &removed_ssts {
            new_state.sstables.remove(id);
        }

        *self.state.write() = Arc::new(new_state);

        for id in removed_ssts {
            std::fs::remove_file(self.path_of_sst(id))?;
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let num_memtables = {
            let state = self.state.read();
            1 + state.imm_memtables.len()
        };
        if num_memtables > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
