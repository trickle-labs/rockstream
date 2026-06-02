//! `AggregateMergeOp` — incremental aggregate operator built on `LawBundle`.
//!
//! Implements `SUM`, `COUNT(*)`, and `AVG` aggregation over a `ZSetBatch`
//! delta stream using the `SumCount/v1` merge law.
//!
//! # Algorithm
//!
//! The operator maintains per-group-key state as `SumCount/v1` bytes. For
//! each incoming delta `(row_key, row_value, weight)`:
//!
//! 1. Extract `group_key = group_fn(row_key, row_value)`.
//! 2. Extract `(sum_contribution, count_contribution) = measure_fn(row_key, row_value)`.
//! 3. Merge `(sum_contribution * weight, count_contribution * weight)` into
//!    the per-group accumulator using `SumCount/v1::merge`.
//!
//! After all deltas in the batch are processed, emit a Z-set delta:
//! - For each modified group: retract the old emitted value (weight -1) and
//!   insert the new value (weight +1). This is the "last-emitted cache" pattern.
//!
//! Groups whose accumulator reaches the identity `(0, 0)` are compacted out.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use rockstream_types::batch::{SinkBatch, SourceBatch, ZSet, ZSetBatch};
use rockstream_types::laws::sum_count::{encode_sum_count, SumCountV1, SUM_COUNT_ID};
use rockstream_types::merge_law::{LawBundle, MergeLawId};
use rockstream_types::timestamp::Epoch;

use crate::operator::Operator;

/// Function type for extracting a group key from a row.
///
/// Takes `(row_key_bytes, row_value_bytes)` and returns the group key bytes.
pub type GroupFn = Arc<dyn Fn(&[u8], &[u8]) -> Vec<u8> + Send + Sync + 'static>;

/// Function type for extracting the measure contribution from a row.
///
/// Returns `(sum_contribution, count_contribution)` for a single row.
/// The weight multiplier is applied by the operator, not the function.
pub type MeasureFn = Arc<dyn Fn(&[u8], &[u8]) -> (i64, i64) + Send + Sync + 'static>;

/// Incremental aggregate operator using `SumCount/v1` as the merge law.
///
/// Re-implements aggregate semantics on top of `LawBundle` so that the merge
/// law drives all accumulation. The operator holds no hand-coded arithmetic —
/// all state transitions go through `SumCountV1::merge`.
pub struct AggregateMergeOp {
    name: String,
    group_fn: GroupFn,
    measure_fn: MeasureFn,
    law: SumCountV1,
    /// Per-group-key accumulator state (SumCount/v1 bytes).
    agg_state: HashMap<Vec<u8>, Vec<u8>>,
    /// Last-emitted value per group key (for computing output deltas).
    last_emitted: HashMap<Vec<u8>, Vec<u8>>,
    budget: Option<Arc<rockstream_types::state_budget::StateBudgetMeter>>,
    warned_over_budget: bool,
}

impl AggregateMergeOp {
    /// Create a new aggregate operator.
    ///
    /// - `name`: diagnostic name used in `EXPLAIN`.
    /// - `group_fn`: extracts the group-by key from each row.
    /// - `measure_fn`: returns `(sum_contribution, count_contribution)` for
    ///   each row *before* weight multiplication.
    pub fn new(name: impl Into<String>, group_fn: GroupFn, measure_fn: MeasureFn) -> Self {
        Self {
            name: name.into(),
            group_fn,
            measure_fn,
            law: SumCountV1,
            agg_state: HashMap::new(),
            last_emitted: HashMap::new(),
            budget: None,
            warned_over_budget: false,
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

    /// Process a `ZSet` delta and return the output Z-set delta.
    ///
    /// This is the core IVM method: applies all incoming deltas incrementally
    /// and emits only the changed aggregate values.
    pub fn process_zset(&mut self, input: &ZSet) -> ZSet {
        let mut modified_groups: HashSet<Vec<u8>> = HashSet::new();

        // Phase 1: accumulate deltas into per-group state.
        for row in input.iter() {
            let group_key = (self.group_fn)(&row.key, &row.value);
            let (sum_contrib, count_contrib) = (self.measure_fn)(&row.key, &row.value);

            // Scale contribution by weight.
            let weighted_sum = sum_contrib.saturating_mul(row.weight);
            let weighted_count = count_contrib.saturating_mul(row.weight);
            let delta_bytes = encode_sum_count(weighted_sum, weighted_count);

            let is_new = !self.agg_state.contains_key(&group_key);
            if is_new {
                if let Some(ref budget) = self.budget {
                    let charge = (group_key.len() + delta_bytes.len()) as u64;
                    if let Err(e) = budget.try_acquire(charge) {
                        if !self.warned_over_budget {
                            tracing::warn!(
                                "RS-3604: state budget exceeded, OVER_BUDGET_RELAXED: {}",
                                e
                            );
                            self.warned_over_budget = true;
                        }
                        continue; // Skip this insertion!
                    }
                }
            }

            let current = self
                .agg_state
                .entry(group_key.clone())
                .or_insert_with(|| self.law.identity().unwrap());

            // Merge via the law (all arithmetic goes through LawBundle).
            *current = self
                .law
                .merge(current, &delta_bytes)
                .expect("SumCount merge");

            modified_groups.insert(group_key);
        }

        // Phase 2: emit deltas for modified groups.
        let mut output = ZSet::new();
        for group_key in modified_groups {
            let new_state = self
                .agg_state
                .get(&group_key)
                .cloned()
                .unwrap_or_else(|| self.law.identity().unwrap());
            let old_emitted = self.last_emitted.get(&group_key).cloned();

            // Retract old emitted value (if any).
            if let Some(ref old) = old_emitted {
                if !self.law.is_identity(old) {
                    output.insert(group_key.clone(), old.clone(), -1);
                }
            }

            // Insert new value (if not identity).
            if !self.law.is_identity(&new_state) {
                output.insert(group_key.clone(), new_state.clone(), 1);
                self.last_emitted
                    .insert(group_key.clone(), new_state.clone());
            } else {
                self.last_emitted.remove(&group_key);
            }

            // Compact identity-valued groups from agg_state.
            if self.law.is_identity(&new_state) && self.agg_state.remove(&group_key).is_some() {
                if let Some(ref budget) = self.budget {
                    let released = (group_key.len() + new_state.len()) as u64;
                    budget.release(released);
                }
            }
        }

        output
    }

    /// Return the current accumulated aggregate state for all groups.
    ///
    /// Used in tests and diagnostics to inspect the operator's internal state.
    pub fn current_state(&self) -> &HashMap<Vec<u8>, Vec<u8>> {
        &self.agg_state
    }

    /// The merge law ID used by this operator.
    pub fn law_id(&self) -> MergeLawId {
        SUM_COUNT_ID
    }
}

#[async_trait]
impl Operator for AggregateMergeOp {
    async fn process(&mut self, _input: &SourceBatch) -> SinkBatch {
        SinkBatch::default()
    }

    async fn process_delta(&mut self, input: &ZSetBatch) -> ZSetBatch {
        let output_zset = self.process_zset(&input.zset);
        ZSetBatch {
            zset: output_zset,
            epoch: input.epoch,
        }
    }

    async fn epoch_complete(&mut self, _epoch: Epoch) {}

    fn name(&self) -> &str {
        &self.name
    }

    fn merge_law(&self) -> Option<MergeLawId> {
        Some(SUM_COUNT_ID)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::laws::sum_count::{decode_sum_count, encode_sum_count};

    /// Build a group_fn that uses the key as the group key.
    fn key_as_group() -> GroupFn {
        Arc::new(|key: &[u8], _value: &[u8]| key.to_vec())
    }

    /// Build a measure_fn that extracts val from the value bytes (8-byte i64).
    fn val_measure() -> MeasureFn {
        Arc::new(|_key: &[u8], value: &[u8]| {
            let val = if value.len() >= 8 {
                i64::from_be_bytes(value[..8].try_into().unwrap_or([0u8; 8]))
            } else {
                0
            };
            (val, 1)
        })
    }

    fn make_row(id: i64, val: i64) -> (Vec<u8>, Vec<u8>) {
        (id.to_be_bytes().to_vec(), val.to_be_bytes().to_vec())
    }

    #[test]
    fn single_insert_produces_sum_and_count() {
        let mut op = AggregateMergeOp::new("test_agg", key_as_group(), val_measure());
        let mut input = ZSet::new();
        let (k, v) = make_row(1, 10);
        input.insert(k, v, 1);

        let output = op.process_zset(&input);

        let rows: Vec<_> = output.iter().collect();
        assert_eq!(rows.len(), 1);
        let (sum, count) = decode_sum_count(&rows[0].value).unwrap();
        assert_eq!(sum, 10);
        assert_eq!(count, 1);
        assert_eq!(rows[0].weight, 1);
    }

    #[test]
    fn second_insert_retracts_old_emits_new() {
        let mut op = AggregateMergeOp::new("test_agg", key_as_group(), val_measure());

        // First delta
        let mut d1 = ZSet::new();
        let (k1, v1) = make_row(1, 10);
        d1.insert(k1, v1, 1);
        let out1 = op.process_zset(&d1);
        // Should emit +1 for (sum=10, count=1)
        assert_eq!(out1.iter().count(), 1);

        // Second delta: insert another row with same group key
        let mut d2 = ZSet::new();
        let (k2, v2) = make_row(1, 20);
        d2.insert(k2, v2, 1);
        let out2 = op.process_zset(&d2);

        let rows: Vec<_> = out2.iter().collect();
        // Should have retraction of old (-1) and insertion of new (+1)
        assert_eq!(rows.len(), 2);
        let retractions: Vec<_> = rows.iter().filter(|r| r.weight == -1).collect();
        let insertions: Vec<_> = rows.iter().filter(|r| r.weight == 1).collect();
        assert_eq!(retractions.len(), 1);
        assert_eq!(insertions.len(), 1);
        let (old_sum, old_count) = decode_sum_count(&retractions[0].value).unwrap();
        assert_eq!(old_sum, 10);
        assert_eq!(old_count, 1);
        let (new_sum, new_count) = decode_sum_count(&insertions[0].value).unwrap();
        assert_eq!(new_sum, 30);
        assert_eq!(new_count, 2);
    }

    #[test]
    fn delete_retracts_all_produces_empty() {
        let mut op = AggregateMergeOp::new("test_agg", key_as_group(), val_measure());

        // Insert
        let mut d1 = ZSet::new();
        let (k, v) = make_row(1, 10);
        d1.insert(k.clone(), v.clone(), 1);
        op.process_zset(&d1);

        // Delete
        let mut d2 = ZSet::new();
        d2.insert(k, v, -1);
        let out = op.process_zset(&d2);

        let rows: Vec<_> = out.iter().collect();
        // Should retract (10, 1), no new insertion (group is now identity)
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].weight, -1);

        // Internal state should be clean
        assert!(op.current_state().is_empty());
    }

    #[test]
    fn multiple_groups_independent() {
        let mut op = AggregateMergeOp::new("test_agg", key_as_group(), val_measure());

        let mut d = ZSet::new();
        let (k1, v1) = make_row(1, 10);
        let (k2, v2) = make_row(2, 20);
        d.insert(k1.clone(), v1, 1);
        d.insert(k2.clone(), v2, 1);

        let out = op.process_zset(&d);
        assert_eq!(out.iter().count(), 2);

        let state = op.current_state();
        assert_eq!(state.len(), 2);
        let (s1, c1) = decode_sum_count(state.get(&k1).unwrap()).unwrap();
        assert_eq!((s1, c1), (10, 1));
        let (s2, c2) = decode_sum_count(state.get(&k2).unwrap()).unwrap();
        assert_eq!((s2, c2), (20, 1));
    }

    #[test]
    fn merge_law_id_is_sum_count() {
        let op = AggregateMergeOp::new("test_agg", key_as_group(), val_measure());
        assert_eq!(op.merge_law(), Some(SUM_COUNT_ID));
    }

    #[test]
    fn encode_decode_round_trip() {
        let bytes = encode_sum_count(42, 7);
        let (s, c) = decode_sum_count(&bytes).unwrap();
        assert_eq!(s, 42);
        assert_eq!(c, 7);
    }

    #[test]
    fn state_budget_property_test_over_budget() {
        use rockstream_types::state_budget::StateBudgetMeter;

        let budget_limit = 50;
        let budget = Arc::new(StateBudgetMeter::new("test_agg_budget", budget_limit));
        let mut op = AggregateMergeOp::new("test_agg", key_as_group(), val_measure())
            .with_budget(Arc::clone(&budget));

        // Let's insert many unique keys to exceed budget by 10x
        let mut input = ZSet::new();
        for i in 0..100 {
            let (k, v) = make_row(i, 10);
            input.insert(k, v, 1);
        }

        let _output = op.process_zset(&input);

        // Usage must not exceed limit + one delta
        let delta_bytes_limit = 8 + 16; // key (8 bytes) + val (16 bytes)
        assert!(budget.current_bytes() <= budget_limit + delta_bytes_limit);
        assert!(op.warned_over_budget);
    }
}
