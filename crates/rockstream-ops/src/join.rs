//! `HashJoinOp` — incremental inner-join operator with dual arrangements.
//!
//! Implements DBSP-sound incremental inner join using two Z-set arrangements
//! (one per side). The algorithm follows the bilinear join identity:
//!
//! ```text
//! Δ(L ⊗ R) = ΔL ⊗ R + L ⊗ ΔR
//! ```
//!
//! Each side maintains its own arrangement so that new deltas on one side can
//! be joined against the **pre-change snapshot** of the other side before the
//! arrangement is updated.
//!
//! # Stable row identity
//!
//! Row identity is the `key` bytes of each `ZSetRow`. The `value` bytes carry
//! the payload (including the join-key column). A row update (retract + insert
//! on the same key) is handled correctly: the retraction is joined against the
//! snapshot arrangement, producing retractions of the old join products, and
//! the insertion produces the new join products.
//!
//! # Join metadata
//!
//! `HashJoinOp::metadata()` returns `JoinMeta` for `EXPLAIN INCREMENTAL`.

use std::collections::HashMap;
use std::sync::Arc;

use rockstream_types::batch::{ZSet, ZSetBatch};
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::timestamp::Epoch;

use crate::operator::Operator;

// ─── Public types ─────────────────────────────────────────────────────────────

/// Extract the join key bytes from a `(row_key, row_value)` pair.
///
/// For a SQL `ON l.order_key = r.order_key` join, this function would
/// extract `order_key` from whichever columns it lives in.
pub type JoinKeyFn = Arc<dyn Fn(&[u8], &[u8]) -> Vec<u8> + Send + Sync + 'static>;

/// Combine a matched left + right row pair into an output `(key, value)`.
///
/// The output `key` uniquely identifies the joined row (e.g., concatenation
/// of both primary keys) and `value` carries the combined payload.
pub type CombineFn =
    Arc<dyn Fn(&[u8], &[u8], &[u8], &[u8]) -> (Vec<u8>, Vec<u8>) + Send + Sync + 'static>;

/// Arrangement for one side of the join.
///
/// `join_key → { (row_key, row_value) → cumulative_weight }`
///
/// Zero-weight entries are left in place and filtered during iteration;
/// compaction is deferred to epoch boundaries.
type Arrangement = HashMap<Vec<u8>, HashMap<(Vec<u8>, Vec<u8>), i64>>;

// ─── JoinMeta ─────────────────────────────────────────────────────────────────

/// Runtime metadata for `EXPLAIN INCREMENTAL` output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinMeta {
    /// Operator name.
    pub name: String,
    /// Number of distinct join keys in the left arrangement.
    pub left_join_keys: usize,
    /// Total non-zero rows in the left arrangement.
    pub left_rows: usize,
    /// Number of distinct join keys in the right arrangement.
    pub right_join_keys: usize,
    /// Total non-zero rows in the right arrangement.
    pub right_rows: usize,
}

// ─── HashJoinOp ───────────────────────────────────────────────────────────────

/// Incremental inner-join operator with dual Z-set arrangements.
///
/// Process left and right deltas using the explicit `process_left_delta` and
/// `process_right_delta` methods. For each pair of epochs where both sides
/// receive updates, process both sides before calling `epoch_complete`.
pub struct HashJoinOp {
    name: String,
    left_key_fn: JoinKeyFn,
    right_key_fn: JoinKeyFn,
    combine_fn: CombineFn,
    /// Left arrangement: join_key → (row_key, row_value) → weight.
    left_arr: Arrangement,
    /// Right arrangement: join_key → (row_key, row_value) → weight.
    right_arr: Arrangement,
    budget: Option<Arc<rockstream_types::state_budget::StateBudgetMeter>>,
    warned_over_budget: bool,
}

impl HashJoinOp {
    /// Create a new `HashJoinOp`.
    ///
    /// - `name`: diagnostic name (shown in `EXPLAIN`).
    /// - `left_key_fn`: extracts the join key from a left-side row.
    /// - `right_key_fn`: extracts the join key from a right-side row.
    /// - `combine_fn`: builds an output `(key, value)` from a matched pair.
    pub fn new(
        name: impl Into<String>,
        left_key_fn: JoinKeyFn,
        right_key_fn: JoinKeyFn,
        combine_fn: CombineFn,
    ) -> Self {
        Self {
            name: name.into(),
            left_key_fn,
            right_key_fn,
            combine_fn,
            left_arr: HashMap::new(),
            right_arr: HashMap::new(),
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

    /// Process a left-side delta.
    ///
    /// Joins every row in `left_delta` against the **current** right
    /// arrangement (pre-change snapshot), then updates the left arrangement.
    ///
    /// Returns the output delta.
    pub fn process_left_delta(&mut self, left_delta: &ZSet) -> ZSet {
        let mut output = ZSet::new();

        // Phase 1: join each left delta row against the right-arrangement snapshot.
        for row in left_delta.iter() {
            let join_key = (self.left_key_fn)(&row.key, &row.value);
            if let Some(right_bucket) = self.right_arr.get(&join_key) {
                for ((rk, rv), rw) in right_bucket {
                    if *rw == 0 {
                        continue;
                    }
                    let out_w = row.weight.saturating_mul(*rw);
                    if out_w != 0 {
                        let (ok, ov) = (self.combine_fn)(&row.key, &row.value, rk, rv);
                        output.insert(ok, ov, out_w);
                    }
                }
            }
        }

        // Phase 2: update left arrangement (after output is produced).
        for row in left_delta.iter() {
            let join_key = (self.left_key_fn)(&row.key, &row.value);
            let bucket = self.left_arr.entry(join_key.clone()).or_default();
            let is_new = !bucket.contains_key(&(row.key.clone(), row.value.clone()));
            if is_new {
                if let Some(ref budget) = self.budget {
                    let charge = (join_key.len() + row.key.len() + row.value.len() + 8) as u64;
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
            *bucket
                .entry((row.key.clone(), row.value.clone()))
                .or_insert(0) += row.weight;
        }

        output
    }

    /// Process a right-side delta.
    ///
    /// Joins every row in `right_delta` against the **current** left
    /// arrangement (pre-change snapshot), then updates the right arrangement.
    ///
    /// Returns the output delta.
    pub fn process_right_delta(&mut self, right_delta: &ZSet) -> ZSet {
        let mut output = ZSet::new();

        // Phase 1: join each right delta row against the left-arrangement snapshot.
        for row in right_delta.iter() {
            let join_key = (self.right_key_fn)(&row.key, &row.value);
            if let Some(left_bucket) = self.left_arr.get(&join_key) {
                for ((lk, lv), lw) in left_bucket {
                    if *lw == 0 {
                        continue;
                    }
                    let out_w = (*lw).saturating_mul(row.weight);
                    if out_w != 0 {
                        let (ok, ov) = (self.combine_fn)(lk, lv, &row.key, &row.value);
                        output.insert(ok, ov, out_w);
                    }
                }
            }
        }

        // Phase 2: update right arrangement (after output is produced).
        for row in right_delta.iter() {
            let join_key = (self.right_key_fn)(&row.key, &row.value);
            let bucket = self.right_arr.entry(join_key.clone()).or_default();
            let is_new = !bucket.contains_key(&(row.key.clone(), row.value.clone()));
            if is_new {
                if let Some(ref budget) = self.budget {
                    let charge = (join_key.len() + row.key.len() + row.value.len() + 8) as u64;
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
            *bucket
                .entry((row.key.clone(), row.value.clone()))
                .or_insert(0) += row.weight;
        }

        output
    }

    /// Process a combined epoch: left delta then right delta.
    ///
    /// The two halves are processed in order with the standard DBSP identity:
    /// - `ΔL ⊗ R_snapshot` (left processed against old right arrangement)
    /// - `L_new ⊗ ΔR` (right processed against updated left arrangement)
    ///
    /// Returns the merged output delta.
    pub fn process_epoch(&mut self, left_delta: &ZSet, right_delta: &ZSet) -> ZSet {
        let left_out = self.process_left_delta(left_delta);
        let right_out = self.process_right_delta(right_delta);
        // Merge the two output deltas.
        let mut output = left_out;
        for row in right_out.iter() {
            output.insert(row.key.clone(), row.value.clone(), row.weight);
        }
        output
    }

    /// Compact the arrangements by removing zero-weight entries.
    ///
    /// Called at epoch boundaries to reclaim memory.
    pub fn compact(&mut self) {
        compact_arr(&mut self.left_arr, &self.budget);
        compact_arr(&mut self.right_arr, &self.budget);
    }

    /// Returns runtime metadata for `EXPLAIN INCREMENTAL`.
    pub fn metadata(&self) -> JoinMeta {
        let (lk, lr) = arr_stats(&self.left_arr);
        let (rk, rr) = arr_stats(&self.right_arr);
        JoinMeta {
            name: self.name.clone(),
            left_join_keys: lk,
            left_rows: lr,
            right_join_keys: rk,
            right_rows: rr,
        }
    }

    /// Name of this operator.
    pub fn name(&self) -> &str {
        &self.name
    }
}

// ─── Operator impl ────────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl Operator for HashJoinOp {
    async fn process(
        &mut self,
        input: &rockstream_types::batch::SourceBatch,
    ) -> rockstream_types::batch::SinkBatch {
        rockstream_types::batch::SinkBatch {
            record_count: input.record_count,
            epoch: input.epoch,
        }
    }

    async fn process_delta(&mut self, input: &ZSetBatch) -> ZSetBatch {
        // When used in a single-input pipeline, treat input as the left side.
        let out = self.process_left_delta(&input.zset);
        ZSetBatch {
            zset: out,
            epoch: input.epoch,
        }
    }

    async fn epoch_complete(&mut self, _epoch: Epoch) {
        self.compact();
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn merge_law(&self) -> Option<MergeLawId> {
        // Inner join produces a Z-set delta — no per-arrangement merge law at
        // the join level. Each side's arrangement uses WeightAdd semantics.
        None
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn compact_arr(
    arr: &mut Arrangement,
    budget: &Option<Arc<rockstream_types::state_budget::StateBudgetMeter>>,
) {
    arr.retain(|join_key, bucket| {
        bucket.retain(|(rk, rv), w| {
            if *w == 0 {
                if let Some(ref b) = budget {
                    let released = (join_key.len() + rk.len() + rv.len() + 8) as u64;
                    b.release(released);
                }
                false
            } else {
                true
            }
        });
        !bucket.is_empty()
    });
}

fn arr_stats(arr: &Arrangement) -> (usize, usize) {
    let keys = arr.len();
    let rows = arr
        .values()
        .map(|b| b.values().filter(|w| **w != 0).count())
        .sum();
    (keys, rows)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::batch::ZSet;

    /// Schema: key = 8-byte i64 id, value = 8-byte i64 join_key.
    fn encode(id: i64, join_key: i64) -> (Vec<u8>, Vec<u8>) {
        (id.to_be_bytes().to_vec(), join_key.to_be_bytes().to_vec())
    }

    fn key_fn() -> JoinKeyFn {
        Arc::new(|_key: &[u8], value: &[u8]| {
            // Join key is the first 8 bytes of value.
            value[..8.min(value.len())].to_vec()
        })
    }

    fn combine() -> CombineFn {
        Arc::new(|lk: &[u8], lv: &[u8], rk: &[u8], rv: &[u8]| {
            let mut out_key = lk.to_vec();
            out_key.extend_from_slice(rk);
            let mut out_val = lv.to_vec();
            out_val.extend_from_slice(rv);
            (out_key, out_val)
        })
    }

    fn make_op() -> HashJoinOp {
        HashJoinOp::new("test_join", key_fn(), key_fn(), combine())
    }

    #[test]
    fn empty_left_delta_produces_empty_output() {
        let mut op = make_op();
        let (k, v) = encode(10, 42);
        let mut right = ZSet::new();
        right.insert(k, v, 1);
        op.process_right_delta(&right);

        let out = op.process_left_delta(&ZSet::new());
        assert_eq!(out.len(), 0);
    }

    #[test]
    fn left_before_right_produces_no_output() {
        // If left arrives before right, no output yet.
        let mut op = make_op();
        let (lk, lv) = encode(1, 42);
        let mut left = ZSet::new();
        left.insert(lk, lv, 1);
        let out = op.process_left_delta(&left);
        assert_eq!(out.len(), 0);
    }

    #[test]
    fn matching_left_and_right_produce_output() {
        let mut op = make_op();

        // Insert right row: id=10, join_key=42
        let (rk, rv) = encode(10, 42);
        let mut right = ZSet::new();
        right.insert(rk, rv, 1);
        op.process_right_delta(&right);

        // Insert left row: id=1, join_key=42 → should join with right
        let (lk, lv) = encode(1, 42);
        let mut left = ZSet::new();
        left.insert(lk, lv, 1);
        let out = op.process_left_delta(&left);
        assert_eq!(out.len(), 1, "one joined row expected");
    }

    #[test]
    fn non_matching_join_keys_produce_no_output() {
        let mut op = make_op();
        // Right: join_key=99
        let (rk, rv) = encode(10, 99);
        let mut right = ZSet::new();
        right.insert(rk, rv, 1);
        op.process_right_delta(&right);

        // Left: join_key=42 ≠ 99
        let (lk, lv) = encode(1, 42);
        let mut left = ZSet::new();
        left.insert(lk, lv, 1);
        let out = op.process_left_delta(&left);
        assert_eq!(out.len(), 0);
    }

    #[test]
    fn deletion_retracts_join_product() {
        let mut op = make_op();

        // Insert both sides
        let (rk, rv) = encode(10, 42);
        let mut right = ZSet::new();
        right.insert(rk.clone(), rv.clone(), 1);
        op.process_right_delta(&right);

        let (lk, lv) = encode(1, 42);
        let mut left = ZSet::new();
        left.insert(lk.clone(), lv.clone(), 1);
        let out1 = op.process_left_delta(&left);
        assert_eq!(out1.len(), 1);

        // Now retract the left row: weight -1 should produce negative output
        let mut retract = ZSet::new();
        retract.insert(lk, lv, -1);
        let out2 = op.process_left_delta(&retract);
        assert_eq!(
            out2.len(),
            1,
            "retraction should produce one negative-weight row"
        );
        // The weight should be -1
        let rows: Vec<_> = out2.iter().collect();
        assert_eq!(rows[0].weight, -1);
    }

    #[test]
    fn metadata_reflects_arrangement_sizes() {
        let mut op = make_op();
        let (rk, rv) = encode(10, 42);
        let mut right = ZSet::new();
        right.insert(rk, rv, 1);
        op.process_right_delta(&right);

        let meta = op.metadata();
        assert_eq!(meta.right_join_keys, 1);
        assert_eq!(meta.right_rows, 1);
        assert_eq!(meta.left_join_keys, 0);
    }

    #[test]
    fn process_epoch_combines_both_sides() {
        let mut op = make_op();

        let (lk, lv) = encode(1, 42);
        let (rk, rv) = encode(10, 42);
        let mut left = ZSet::new();
        left.insert(lk, lv, 1);
        let mut right = ZSet::new();
        right.insert(rk, rv, 1);

        let out = op.process_epoch(&left, &right);
        // Left processed first (empty right) → 0 output.
        // Right processed with updated left (1 left row) → 1 output.
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn compact_removes_zero_weight_entries() {
        let mut op = make_op();
        let (rk, rv) = encode(10, 42);
        let mut right = ZSet::new();
        right.insert(rk.clone(), rv.clone(), 1);
        op.process_right_delta(&right);

        let mut retract = ZSet::new();
        retract.insert(rk, rv, -1);
        op.process_right_delta(&retract);

        // Before compact, zero-weight entry may still be present.
        op.compact();
        assert_eq!(op.metadata().right_join_keys, 0);
    }

    #[test]
    fn update_via_retract_insert_correctly_handles_arrangements() {
        let mut op = make_op();

        // Insert right row id=10, join_key=42
        let (rk, rv) = encode(10, 42);
        let mut right = ZSet::new();
        right.insert(rk, rv, 1);
        op.process_right_delta(&right);

        // Insert left row id=1, join_key=42
        let (lk, lv_old) = encode(1, 42);
        let mut left_insert = ZSet::new();
        left_insert.insert(lk.clone(), lv_old.clone(), 1);
        let out1 = op.process_left_delta(&left_insert);
        assert_eq!(out1.len(), 1);

        // "Update" left row: retract old value (join_key=42) and insert new (join_key=99)
        let (_, lv_new) = encode(1, 99);
        let mut left_update = ZSet::new();
        left_update.insert(lk.clone(), lv_old, -1); // retract old
        left_update.insert(lk, lv_new, 1); // insert new (different join_key)
        let out2 = op.process_left_delta(&left_update);

        // Should produce: -1 retraction (old join product) + 0 new (no right match for key 99)
        let total_weight: i64 = out2.iter().map(|r| r.weight).sum();
        assert_eq!(total_weight, -1, "net output should be -1 retraction");
    }
}
