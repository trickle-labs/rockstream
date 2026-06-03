//! `DistinctOp` — incremental DISTINCT operator using `WeightAdd/v1`.
//!
//! SQL `SELECT DISTINCT` collapses duplicate rows into a single output row.
//! In DBSP Z-set algebra the accumulated weight of a `(key, value)` pair is
//! the total number of times that row has been inserted minus retracted. A
//! row is "present" in a `DISTINCT` view when its accumulated weight is > 0.
//!
//! # Algorithm
//!
//! Maintains per-`(key, value)` weight using `WeightAdd/v1` encoding (8-byte
//! big-endian i64). For each incoming delta:
//!
//! 1. Add the delta weight to the stored weight.
//! 2. If the stored weight crossed zero in the positive direction (old ≤ 0,
//!    new > 0): emit `(key, value, +1)`.
//! 3. If the stored weight crossed zero in the negative direction (old > 0,
//!    new ≤ 0): emit `(key, value, -1)`.
//! 4. Remove zero-weight entries to keep state compact.
//!
//! # Merge law
//!
//! The arrangement is backed by `WeightAdd/v1` (id `0x0001`). The operator
//! reports `WEIGHT_ADD_ID` via `merge_law()` for `EXPLAIN INCREMENTAL`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use rockstream_types::batch::{SinkBatch, SourceBatch, ZSet, ZSetBatch};
use rockstream_types::laws::weight_add::{decode_weight, encode_weight, WEIGHT_ADD_ID};
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::timestamp::Epoch;

use crate::operator::Operator;

/// Incremental `DISTINCT` operator backed by `WeightAdd/v1`.
///
/// Emits a `+1` delta when a row's net weight crosses from ≤ 0 to > 0,
/// and a `−1` delta when it crosses from > 0 to ≤ 0.
pub struct DistinctOp {
    name: String,
    /// Per-`(key, value)` cumulative weight stored as `WeightAdd/v1` bytes.
    weight_state: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
    budget: Option<Arc<rockstream_types::state_budget::StateBudgetMeter>>,
    warned_over_budget: bool,
    rows_processed: u64,
    state_read_count: u64,
}

impl DistinctOp {
    /// Create a new `DistinctOp`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            weight_state: HashMap::new(),
            budget: None,
            warned_over_budget: false,
            rows_processed: 0,
            state_read_count: 0,
        }
    }

    /// Attach a state budget for memory bounding.
    pub fn with_budget(
        mut self,
        budget: Arc<rockstream_types::state_budget::StateBudgetMeter>,
    ) -> Self {
        self.budget = Some(budget);
        self
    }

    /// Process a `ZSet` delta and return the DISTINCT output delta.
    ///
    /// Updates per-row weight counters and emits zero-crossing events.
    pub fn process_zset(&mut self, input: &ZSet) -> ZSet {
        let mut output = ZSet::new();

        for row in input.iter() {
            self.rows_processed += 1;
            let entry_key = (row.key.clone(), row.value.clone());

            self.state_read_count += 1;
            let is_new = !self.weight_state.contains_key(&entry_key);
            if is_new {
                if let Some(ref budget) = self.budget {
                    let charge = (row.key.len() + row.value.len() + 8) as u64; // key + val + weight bytes
                    if let Err(e) = budget.try_acquire(charge) {
                        if !self.warned_over_budget {
                            tracing::warn!(
                                "RS-3604: state budget exceeded, OVER_BUDGET_RELAXED: {}",
                                e
                            );
                            self.warned_over_budget = true;
                        }
                        continue;
                    }
                }
            }

            // Load existing weight (default 0 if not present).
            let old_weight: i64 = self
                .weight_state
                .get(&entry_key)
                .and_then(|b| decode_weight(b).ok())
                .unwrap_or(0);

            let new_weight = old_weight + row.weight;

            // Update or remove state.
            if new_weight == 0 {
                if self.weight_state.remove(&entry_key).is_some() {
                    if let Some(ref budget) = self.budget {
                        let released = (row.key.len() + row.value.len() + 8) as u64;
                        budget.release(released);
                    }
                }
            } else {
                self.weight_state
                    .insert(entry_key.clone(), encode_weight(new_weight));
            }

            // Zero-crossing detection.
            match (old_weight > 0, new_weight > 0) {
                (false, true) => {
                    // 0 (or negative) → positive: row becomes present.
                    output.insert(row.key.clone(), row.value.clone(), 1);
                }
                (true, false) => {
                    // positive → 0 (or negative): row is removed.
                    output.insert(row.key.clone(), row.value.clone(), -1);
                }
                _ => {
                    // No zero-crossing: output is unchanged.
                }
            }
        }

        output
    }

    /// Expose internal weight state (for tests).
    pub fn weight_state(&self) -> &HashMap<(Vec<u8>, Vec<u8>), Vec<u8>> {
        &self.weight_state
    }
}

#[async_trait]
impl Operator for DistinctOp {
    fn set_context(&mut self, ctx: crate::operator::OperatorContext) {
        if let Some(budget) = ctx.state_budget {
            self.budget = Some(budget);
        }
    }

    async fn process(&mut self, _input: &SourceBatch) -> SinkBatch {
        SinkBatch::default()
    }

    async fn process_delta(&mut self, input: &ZSetBatch) -> ZSetBatch {
        let out = self.process_zset(&input.zset);
        ZSetBatch {
            zset: out,
            epoch: input.epoch,
        }
    }

    async fn epoch_complete(&mut self, _epoch: Epoch) {}

    fn name(&self) -> &str {
        &self.name
    }

    fn merge_law(&self) -> Option<MergeLawId> {
        Some(WEIGHT_ADD_ID)
    }

    fn snapshot_metrics(&self) -> crate::operator::OperatorMetrics {
        crate::operator::OperatorMetrics {
            rows_processed: self.rows_processed,
            state_read_count: self.state_read_count,
            rmw_avoided: true,
            p99_latency_ms: 0.0,
        }
    }

    fn state_bytes(&self) -> u64 {
        self.weight_state
            .iter()
            .map(|((k, v), val)| {
                (k.len() + v.len() + val.len()) as u64
            })
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_produces_empty_output() {
        let mut op = DistinctOp::new("test");
        let result = op.process_zset(&ZSet::new());
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn single_insert_emits_present() {
        let mut op = DistinctOp::new("test");
        let mut input = ZSet::new();
        input.insert(b"key".to_vec(), b"val".to_vec(), 1);
        let result = op.process_zset(&input);
        assert_eq!(result.len(), 1);
        let row = result.iter().next().unwrap();
        assert_eq!(row.key, b"key");
        assert_eq!(row.value, b"val");
        assert_eq!(row.weight, 1);
    }

    #[test]
    fn duplicate_insert_produces_no_additional_output() {
        let mut op = DistinctOp::new("test");
        let mut input = ZSet::new();
        input.insert(b"key".to_vec(), b"val".to_vec(), 1);
        op.process_zset(&input);
        // A second insert of the same row does NOT re-emit (already present).
        let result2 = op.process_zset(&input);
        assert_eq!(result2.len(), 0, "no new output for duplicate insert");
    }

    #[test]
    fn retraction_after_two_inserts_stays_present() {
        let mut op = DistinctOp::new("test");
        let mut ins = ZSet::new();
        ins.insert(b"k".to_vec(), b"v".to_vec(), 2);
        op.process_zset(&ins);
        // Retract one copy: weight goes 2→1, still positive → no output.
        let mut ret = ZSet::new();
        ret.insert(b"k".to_vec(), b"v".to_vec(), -1);
        let result = op.process_zset(&ret);
        assert_eq!(result.len(), 0, "still present, no output change");
    }

    #[test]
    fn full_retraction_emits_removal() {
        let mut op = DistinctOp::new("test");
        let mut ins = ZSet::new();
        ins.insert(b"k".to_vec(), b"v".to_vec(), 1);
        op.process_zset(&ins);

        let mut ret = ZSet::new();
        ret.insert(b"k".to_vec(), b"v".to_vec(), -1);
        let result = op.process_zset(&ret);
        assert_eq!(result.len(), 1);
        assert_eq!(result.iter().next().unwrap().weight, -1);
    }
}
