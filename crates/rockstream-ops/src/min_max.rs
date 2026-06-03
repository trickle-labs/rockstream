//! `MinMaxOp` — retraction-aware MIN/MAX operator using indexed multiset state.
//!
//! Implements `MIN` and `MAX` aggregation over a `ZSetBatch` delta stream.
//! Unlike `AggregateMergeOp`, MIN/MAX cannot use an abelian-group law because
//! they are not invertible: deleting the maximum requires rescanning all
//! remaining values to find the new extremum.
//!
//! # Algorithm
//!
//! The operator maintains per-group-key state as a `BTreeMap<i64, i64>`
//! mapping each observed value to its net weight (insertions minus deletions).
//! This is the **indexed multiset** representation:
//!
//! - On insert of `(group_key, val, weight)`:
//!   `multiset[group_key][val] += weight`
//! - On delete of `(group_key, val, weight)` (weight < 0):
//!   `multiset[group_key][val] += weight` (may go negative during transit)
//! - Extremum: iterate the `BTreeMap` in order to find the first entry
//!   with positive net weight.
//!
//! For MAX: iterate in *descending* order (`.iter().rev()`).
//! For MIN: iterate in *ascending* order (`.iter()`).
//!
//! # Delete-path prefix scan
//!
//! When the stored extremum is deleted, the BTreeMap ordered iteration is
//! O(log n) to find the new extremum — this is the "prefix scan" described
//! in the roadmap. No external storage scan is needed because the operator
//! holds the full multiset in memory.
//!
//! # Cached-slot law
//!
//! The operator reports `MAX_REGISTER_ID` or `MIN_REGISTER_ID` as its
//! `merge_law()`. This surfaces in `EXPLAIN INCREMENTAL` to communicate which
//! law backs the cached extremum slot. The operator itself stays retraction-
//! aware (the law is not used for storage accumulation).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use rockstream_types::batch::{SinkBatch, SourceBatch, ZSet, ZSetBatch};
use rockstream_types::laws::max_register::MAX_REGISTER_ID;
use rockstream_types::laws::min_register::MIN_REGISTER_ID;
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::timestamp::Epoch;

use crate::operator::Operator;

/// Which extremum function to compute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MinMaxKind {
    /// Return the minimum (smallest positive-weight value in the multiset).
    Min,
    /// Return the maximum (largest positive-weight value in the multiset).
    Max,
}

/// Function type for extracting a group key from a row.
///
/// Takes `(row_key_bytes, row_value_bytes)` and returns the group key bytes.
pub type GroupFn = Arc<dyn Fn(&[u8], &[u8]) -> Vec<u8> + Send + Sync + 'static>;

/// Function type for extracting the i64 measure value from a row.
pub type ScalarFn = Arc<dyn Fn(&[u8], &[u8]) -> i64 + Send + Sync + 'static>;

/// Per-group indexed multiset state.
///
/// Maps `value -> net_weight`. Only entries with `net_weight > 0` are
/// semantically "present". Entries with `net_weight <= 0` are candidates
/// for cleanup.
type GroupMultiset = BTreeMap<i64, i64>;

pub struct MinMaxOp {
    name: String,
    kind: MinMaxKind,
    group_fn: GroupFn,
    scalar_fn: ScalarFn,
    /// Per-group-key indexed multiset: value → net_weight.
    multiset_state: HashMap<Vec<u8>, GroupMultiset>,
    /// Last-emitted extremum per group (for computing output deltas).
    last_emitted: HashMap<Vec<u8>, i64>,
    budget: Option<Arc<rockstream_types::state_budget::StateBudgetMeter>>,
    warned_over_budget: bool,
    rows_processed: u64,
    state_read_count: u64,
}

impl MinMaxOp {
    /// Create a new `MinMaxOp`.
    ///
    /// - `name`: diagnostic name used in `EXPLAIN`.
    /// - `kind`: whether to compute `MIN` or `MAX`.
    /// - `group_fn`: extracts the group-by key from each row.
    /// - `scalar_fn`: extracts the i64 value to aggregate.
    pub fn new(
        name: impl Into<String>,
        kind: MinMaxKind,
        group_fn: GroupFn,
        scalar_fn: ScalarFn,
    ) -> Self {
        Self {
            name: name.into(),
            kind,
            group_fn,
            scalar_fn,
            multiset_state: HashMap::new(),
            last_emitted: HashMap::new(),
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

    /// Derive the current extremum for a group from its indexed multiset.
    ///
    /// Returns `None` if the group has no entries with positive net weight.
    fn current_extremum(&self, group_key: &[u8]) -> Option<i64> {
        let btree = self.multiset_state.get(group_key)?;
        match self.kind {
            MinMaxKind::Max => btree.iter().rev().find(|(_, w)| **w > 0).map(|(v, _)| *v),
            MinMaxKind::Min => btree.iter().find(|(_, w)| **w > 0).map(|(v, _)| *v),
        }
    }

    /// Process a `ZSet` delta and return the output Z-set delta.
    ///
    /// Updates the indexed multiset for each (group, value) pair touched by
    /// the delta, then emits retract/insert pairs for changed extrema.
    pub fn process_zset(&mut self, input: &ZSet) -> ZSet {
        let mut modified_groups: HashSet<Vec<u8>> = HashSet::new();

        // Phase 1: update multiset state for all incoming deltas.
        for row in input.iter() {
            self.rows_processed += 1;
            let group_key = (self.group_fn)(&row.key, &row.value);
            let val = (self.scalar_fn)(&row.key, &row.value);

            self.state_read_count += 1;
            let is_new = !self.multiset_state.contains_key(&group_key);
            if is_new {
                if let Some(ref budget) = self.budget {
                    let charge = (group_key.len() + 16) as u64; // key + val bytes approx
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

            let multiset = self.multiset_state.entry(group_key.clone()).or_default();
            *multiset.entry(val).or_insert(0) += row.weight;

            // Prune zero-weight entries to keep the multiset compact.
            if multiset.get(&val).copied().unwrap_or(0) == 0 {
                multiset.remove(&val);
            }

            modified_groups.insert(group_key);
        }

        // Phase 2: emit deltas for modified groups.
        let mut output = ZSet::new();
        for group_key in modified_groups {
            let new_extremum = self.current_extremum(&group_key);
            let old_extremum = self.last_emitted.get(&group_key).copied();

            // Skip groups whose extremum has not changed.
            if new_extremum == old_extremum {
                continue;
            }

            // Retract the old extremum (if any was emitted).
            if let Some(old_val) = old_extremum {
                output.insert(group_key.clone(), old_val.to_be_bytes().to_vec(), -1);
            }

            // Insert the new extremum (if the group is non-empty).
            if let Some(new_val) = new_extremum {
                output.insert(group_key.clone(), new_val.to_be_bytes().to_vec(), 1);
                self.last_emitted.insert(group_key.clone(), new_val);
            } else {
                // Group became empty — remove from last_emitted.
                self.last_emitted.remove(&group_key);
            }

            // Prune empty multisets.
            if self
                .multiset_state
                .get(&group_key)
                .is_some_and(|m| m.is_empty())
            {
                self.multiset_state.remove(&group_key);
                if let Some(ref budget) = self.budget {
                    let released = (group_key.len() + 16) as u64;
                    budget.release(released);
                }
            }
        }

        output
    }

    /// Returns the indexed multiset state for all groups.
    ///
    /// Used in tests to verify internal operator state.
    pub fn current_state(&self) -> &HashMap<Vec<u8>, GroupMultiset> {
        &self.multiset_state
    }

    /// The cached-slot law ID (reported in `EXPLAIN INCREMENTAL`).
    ///
    /// Returns `MAX_REGISTER_ID` for MAX operators and `MIN_REGISTER_ID`
    /// for MIN operators.
    pub fn law_id(&self) -> MergeLawId {
        match self.kind {
            MinMaxKind::Max => MAX_REGISTER_ID,
            MinMaxKind::Min => MIN_REGISTER_ID,
        }
    }
}

#[async_trait]
impl Operator for MinMaxOp {
    fn set_context(&mut self, ctx: crate::operator::OperatorContext) {
        if let Some(budget) = ctx.state_budget {
            self.budget = Some(budget);
        }
    }

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
        Some(self.law_id())
    }

    fn snapshot_metrics(&self) -> crate::operator::OperatorMetrics {
        crate::operator::OperatorMetrics {
            rows_processed: self.rows_processed,
            state_read_count: self.state_read_count,
            rmw_avoided: false,
            p99_latency_ms: 0.0,
        }
    }

    fn state_bytes(&self) -> u64 {
        self.multiset_state
            .iter()
            .map(|(k, v)| (k.len() + v.len() * 16) as u64)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::batch::ZSet;
    use rockstream_types::laws::max_register::MAX_REGISTER_ID;
    use rockstream_types::laws::min_register::MIN_REGISTER_ID;

    /// Group fn: use the key bytes as the group key.
    fn key_as_group() -> GroupFn {
        Arc::new(|key: &[u8], _val: &[u8]| key.to_vec())
    }

    /// Scalar fn: decode the value bytes as a big-endian i64.
    fn be_i64_scalar() -> ScalarFn {
        Arc::new(|_key: &[u8], val: &[u8]| {
            if val.len() >= 8 {
                i64::from_be_bytes(val[..8].try_into().unwrap())
            } else {
                0
            }
        })
    }

    fn group_key(g: u8) -> Vec<u8> {
        vec![g]
    }

    fn val_bytes(v: i64) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }

    #[test]
    fn max_single_insert() {
        let mut op = MinMaxOp::new("max", MinMaxKind::Max, key_as_group(), be_i64_scalar());
        let mut input = ZSet::new();
        input.insert(group_key(1), val_bytes(10), 1);

        let output = op.process_zset(&input);
        assert_eq!(output.len(), 1);

        let row = output.iter().next().unwrap();
        assert_eq!(row.key, group_key(1));
        assert_eq!(
            i64::from_be_bytes(row.value.clone().try_into().unwrap()),
            10
        );
        assert_eq!(row.weight, 1);
    }

    #[test]
    fn min_single_insert() {
        let mut op = MinMaxOp::new("min", MinMaxKind::Min, key_as_group(), be_i64_scalar());
        let mut input = ZSet::new();
        input.insert(group_key(1), val_bytes(10), 1);

        let output = op.process_zset(&input);
        assert_eq!(output.len(), 1);

        let row = output.iter().next().unwrap();
        assert_eq!(
            i64::from_be_bytes(row.value.clone().try_into().unwrap()),
            10
        );
    }

    #[test]
    fn max_higher_value_replaces_current() {
        let mut op = MinMaxOp::new("max", MinMaxKind::Max, key_as_group(), be_i64_scalar());
        let gk = group_key(1);

        let mut input1 = ZSet::new();
        input1.insert(gk.clone(), val_bytes(5), 1);
        op.process_zset(&input1);

        let mut input2 = ZSet::new();
        input2.insert(gk.clone(), val_bytes(10), 1);
        let output = op.process_zset(&input2);

        // Retract 5, insert 10
        let rows: Vec<_> = output.iter().collect();
        assert_eq!(rows.len(), 2);
        let retract = rows.iter().find(|r| r.weight == -1).unwrap();
        assert_eq!(
            i64::from_be_bytes(retract.value.clone().try_into().unwrap()),
            5
        );
        let insert = rows.iter().find(|r| r.weight == 1).unwrap();
        assert_eq!(
            i64::from_be_bytes(insert.value.clone().try_into().unwrap()),
            10
        );
    }

    #[test]
    fn max_lower_value_does_not_change_max() {
        let mut op = MinMaxOp::new("max", MinMaxKind::Max, key_as_group(), be_i64_scalar());
        let gk = group_key(1);

        let mut input1 = ZSet::new();
        input1.insert(gk.clone(), val_bytes(10), 1);
        op.process_zset(&input1);

        let mut input2 = ZSet::new();
        input2.insert(gk.clone(), val_bytes(3), 1);
        let output = op.process_zset(&input2);

        // Inserting 3 when max is 10 => no change to emitted max
        assert!(
            output.is_empty(),
            "lower value insert must not change emitted MAX"
        );
    }

    #[test]
    fn delete_max_rescans_for_new_max() {
        let mut op = MinMaxOp::new("max", MinMaxKind::Max, key_as_group(), be_i64_scalar());
        let gk = group_key(1);

        // Insert 10 and 5
        let mut input1 = ZSet::new();
        input1.insert(gk.clone(), val_bytes(10), 1);
        input1.insert(gk.clone(), val_bytes(5), 1);
        op.process_zset(&input1);

        // Delete 10 — operator must rescan to find new max = 5
        let mut input2 = ZSet::new();
        input2.insert(gk.clone(), val_bytes(10), -1);
        let output = op.process_zset(&input2);

        // Retract 10, insert 5
        let rows: Vec<_> = output.iter().collect();
        assert_eq!(rows.len(), 2);
        let retract = rows.iter().find(|r| r.weight == -1).unwrap();
        assert_eq!(
            i64::from_be_bytes(retract.value.clone().try_into().unwrap()),
            10
        );
        let insert = rows.iter().find(|r| r.weight == 1).unwrap();
        assert_eq!(
            i64::from_be_bytes(insert.value.clone().try_into().unwrap()),
            5
        );
    }

    #[test]
    fn delete_only_element_empties_group() {
        let mut op = MinMaxOp::new("max", MinMaxKind::Max, key_as_group(), be_i64_scalar());
        let gk = group_key(1);

        let mut input1 = ZSet::new();
        input1.insert(gk.clone(), val_bytes(10), 1);
        op.process_zset(&input1);

        let mut input2 = ZSet::new();
        input2.insert(gk.clone(), val_bytes(10), -1);
        let output = op.process_zset(&input2);

        let rows: Vec<_> = output.iter().collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].weight, -1);
        assert_eq!(
            i64::from_be_bytes(rows[0].value.clone().try_into().unwrap()),
            10
        );
    }

    #[test]
    fn multiple_groups_independent() {
        let mut op = MinMaxOp::new("max", MinMaxKind::Max, key_as_group(), be_i64_scalar());

        let mut input = ZSet::new();
        input.insert(group_key(1), val_bytes(10), 1);
        input.insert(group_key(2), val_bytes(20), 1);
        input.insert(group_key(3), val_bytes(5), 1);
        let output = op.process_zset(&input);

        // Each group should emit its own max
        let rows: Vec<_> = output.iter().collect();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn min_delete_rescans_for_new_min() {
        let mut op = MinMaxOp::new("min", MinMaxKind::Min, key_as_group(), be_i64_scalar());
        let gk = group_key(1);

        let mut input1 = ZSet::new();
        input1.insert(gk.clone(), val_bytes(3), 1);
        input1.insert(gk.clone(), val_bytes(7), 1);
        op.process_zset(&input1);

        // Delete the minimum (3); new min should be 7
        let mut input2 = ZSet::new();
        input2.insert(gk.clone(), val_bytes(3), -1);
        let output = op.process_zset(&input2);

        let rows: Vec<_> = output.iter().collect();
        assert_eq!(rows.len(), 2);
        let retract = rows.iter().find(|r| r.weight == -1).unwrap();
        assert_eq!(
            i64::from_be_bytes(retract.value.clone().try_into().unwrap()),
            3
        );
        let insert = rows.iter().find(|r| r.weight == 1).unwrap();
        assert_eq!(
            i64::from_be_bytes(insert.value.clone().try_into().unwrap()),
            7
        );
    }

    #[test]
    fn merge_law_returns_correct_id() {
        let max_op = MinMaxOp::new("max", MinMaxKind::Max, key_as_group(), be_i64_scalar());
        assert_eq!(max_op.merge_law(), Some(MAX_REGISTER_ID));
        assert_eq!(max_op.law_id(), MAX_REGISTER_ID);

        let min_op = MinMaxOp::new("min", MinMaxKind::Min, key_as_group(), be_i64_scalar());
        assert_eq!(min_op.merge_law(), Some(MIN_REGISTER_ID));
        assert_eq!(min_op.law_id(), MIN_REGISTER_ID);
    }
}
