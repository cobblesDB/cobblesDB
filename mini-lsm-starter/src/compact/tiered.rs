use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        let level_num = snapshot.levels.len();

        // 1. Triggered by Space Amplification Ratio
        let mut engine_size = 0;
        let mut last_level_size = snapshot.levels.last().unwrap().1.len();

        if level_num > 1 {
            for i in 0..level_num - 1 {
                engine_size += snapshot.levels[i].1.len();
            }

            if engine_size / last_level_size * 100 >= self.options.max_size_amplification_percent {
                let task = TieredCompactionTask {
                    tiers: snapshot.levels.clone(),
                    bottom_tier_included: true,
                };
                return Some(task);
            }
        }

        // 2. Triggered by Size Ratio
        if level_num > self.options.min_merge_width {
            let mut cur_level_idx = self.options.min_merge_width;
            let mut cur_tier: f32 = snapshot.levels[cur_level_idx].1.len() as f32;
            let mut size_of_all_previous_tiers: f32 = 0.0;
            let target_size_ratio: f32 = (100.0 + self.options.size_ratio as f32) / 100.0;

            for i in 0..cur_level_idx {
                size_of_all_previous_tiers += snapshot.levels[i].1.len() as f32;
            }

            loop {
                if cur_tier != 0.0 && size_of_all_previous_tiers / cur_tier >= target_size_ratio {
                    let task = TieredCompactionTask {
                        tiers: snapshot
                            .levels
                            .iter()
                            .cloned()
                            .take(cur_level_idx + 1)
                            .collect(),
                        bottom_tier_included: cur_level_idx == level_num - 1,
                    };
                    return Some(task);
                }
                cur_level_idx += 1;
                if cur_level_idx == level_num {
                    break;
                }
                size_of_all_previous_tiers += cur_tier;
                cur_tier = snapshot.levels[cur_level_idx].1.len() as f32;
            }
        }
        // 3. Reduce Sorted Runs
        if level_num >= self.options.num_tiers {
            let task = TieredCompactionTask {
                tiers: snapshot
                    .levels
                    .iter()
                    .cloned()
                    .take(self.options.num_tiers - 1)
                    .collect(),
                bottom_tier_included: false,
            };
            return Some(task);
        }

        return None;
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut state = snapshot.clone();
        let mut del = vec![];

        // 1. move sst id form tiers to del and remove empty tiers
        let level_num = task.tiers.len();
        let last_level_id = state.levels[0].0 + 1;
        for i in 0..level_num {
            del.extend(&state.levels[0].1);
            state.levels.remove(0);
        }
        state.levels.insert(0, (last_level_id, vec![]));

        // 2. add new sst id to lower level
        state.levels[0].1 = output.to_vec();

        (state, del)
    }
}
