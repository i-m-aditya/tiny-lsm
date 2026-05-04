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

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        // level0_file_num_compaction_trigger check
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            // trigger compaction
            let task = SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: snapshot.levels.len() == 1,
            };
            return Some(task);
        }

        let max_level = snapshot.levels.len();
        for i in 0..max_level - 1 {
            let upper_l = &snapshot.levels[i].0;
            let upper_ssts = &snapshot.levels[i].1;
            let (lower_l, lower_ssts) = &snapshot.levels[i + 1];

            if self.compaction_required(upper_ssts.len(), lower_ssts.len()) {
                let task = SimpleLeveledCompactionTask {
                    upper_level: Some(*upper_l),
                    upper_level_sst_ids: upper_ssts.clone(),
                    lower_level: *lower_l,
                    lower_level_sst_ids: lower_ssts.clone(),
                    is_lower_level_bottom_level: max_level == *lower_l,
                };
                return Some(task);
            }
        }
        None
    }

    /// Returns true when (lower file count / upper file count) * 100 is below the configured
    /// threshold (simplified from total byte size in the course text).
    fn compaction_required(&self, upper_num_files: usize, lower_num_files: usize) -> bool {
        if upper_num_files == 0 {
            return false;
        }
        let ratio = (lower_num_files * 100) / upper_num_files;
        ratio < self.options.size_ratio_percent
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();

        let mut removed_ssts = Vec::new();

        if task.upper_level.is_none() {
            // l0 to l1 merge
            snapshot
                .l0_sstables
                .retain(|id| !task.upper_level_sst_ids.contains(id));
            snapshot.levels[0] = (1, output.to_vec());
            removed_ssts.extend_from_slice(&task.upper_level_sst_ids);
            removed_ssts.extend_from_slice(&task.lower_level_sst_ids);
        } else {
            let upper_level = task.upper_level.unwrap();
            let lower_level = task.lower_level;

            snapshot.levels[upper_level - 1] = (upper_level, Vec::new());
            snapshot.levels[lower_level - 1] = (lower_level, output.to_vec());
            removed_ssts.extend_from_slice(&task.upper_level_sst_ids);
            removed_ssts.extend_from_slice(&task.lower_level_sst_ids);
        }

        (snapshot, removed_ssts)
    }
}
