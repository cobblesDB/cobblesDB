use serde::{Deserialize, Serialize};

use crate::compact::Path;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::BlockCache;
use crate::lsm_storage::LsmStorageState;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use std::path;
use std::sync::Arc;

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
        // generate L0->L1 compaction
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            // if L0 L1 ratio percnet is equal to size_ratio_percent, then we don't need to do compaction
            let upper_level_size = snapshot.l0_sstables.len();
            let lower_level_size = snapshot.levels[0].1.len();
            if upper_level_size != 0 {
                let cur_size_ratio_percent = (lower_level_size / upper_level_size) * 100;
                if cur_size_ratio_percent == self.options.size_ratio_percent {
                    return None;
                }
            }

            let task = SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: self.options.max_levels == 1,
            };
            return Some(task);
        }
        for level in 0..snapshot.levels.len() - 1 {
            let upper_level_size = snapshot.levels[level].1.len();
            let lower_level_size = snapshot.levels[level + 1].1.len();
            if upper_level_size == 0 {
                continue;
            }
            let cur_size_ratio_percent = (lower_level_size / upper_level_size) * 100;
            if cur_size_ratio_percent < self.options.size_ratio_percent {
                let task = SimpleLeveledCompactionTask {
                    upper_level: Some(level + 1),
                    upper_level_sst_ids: snapshot.levels[level].1.clone(),
                    lower_level: level + 2,
                    lower_level_sst_ids: snapshot.levels[level + 1].1.clone(),
                    is_lower_level_bottom_level: self.options.max_levels == level + 1,
                };
                return Some(task);
            }
        }
        return None;
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
        let mut state = snapshot.clone();
        let mut del = vec![];

        // 1.move sst id form upper&lower level to del
        if task.upper_level.is_none() {
            del.append(&mut state.l0_sstables);
        } else {
            // upper_level means real level number, so we need to -1
            del.append(&mut state.levels[task.upper_level.unwrap() - 1].1);
        }

        let l0_size = state.l0_sstables.len();

        // lower_level means real level number, so we need to -1
        del.append(&mut state.levels[task.lower_level - 1].1);

        // // 2. add new sst id to lower level
        state.levels[task.lower_level - 1].1 = output.to_vec();

        // deal with L0 SST gets flushed while the compactor generates new SSTs
        assert!(l0_size == state.l0_sstables.len());
        (state, del)
    }
}
