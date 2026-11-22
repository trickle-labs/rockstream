//! Indexed incremental factorized join-aggregate operators (Issue #402 R1 & R3).
//!
//! Eliminates whole-state cloning, O(|L| * |R|) recomputation, and prefix-scan-and-rewrite
//! persistence. Uses join-key indexes, maintained per-key sufficient summaries, transactional
//! per-key overlays for atomic rollback safety, once-per-batch `BatchKeyCapsule` reuse, and
//! changed-key point updates/deletions.
//!
//! `FactorizedJoinCore` owns all mutable state directly (`&mut self`, zero `Mutex`es) for
//! single-owner shard actors, while `FactorizedJoinAggregateOp` wraps a single `Mutex` for
//! legacy `&self` callers and exposes zero-lock `&mut self` entry points via `Mutex::get_mut`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;

use rockstream_storage::{JoinSide, ShardDb, ShardKeyEncoder, WriteBatch};
use rockstream_types::ids::OperatorId;
use rockstream_types::state_mutation::{OperatorEpochMetrics, StateMutation};

use crate::error::OpError;
use crate::governor::{
    DeltaAmplificationBudget, DeltaAmplificationCounters, DeltaAmplificationGovernor,
    DEFAULT_FACTORIZED_DELTA_BUDGET, FACTORIZED_SELECTION_RULE_VERSION,
};
use crate::join::stable_row_id;
use crate::op::OperatorEpochResult;
use crate::zset::ArrowZSet;

pub const MAX_FACTOR_PAYLOAD_ROWS: usize = 100_000;
pub const MAX_FACTOR_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactorizedAggregateKind {
    Sum,
    Count,
    Avg,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Scalar {
    Null,
    Int64(i64),
    Utf8(String),
}

impl Scalar {
    fn from_array(array: &ArrayRef, row: usize) -> Result<Self, OpError> {
        if array.is_null(row) {
            return Ok(Self::Null);
        }
        if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
            return Ok(Self::Int64(values.value(row)));
        }
        if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
            return Ok(Self::Utf8(values.value(row).to_owned()));
        }
        Err(OpError::unsupported_plan_node(format!(
            "factorized payload does not support {} columns",
            array.data_type()
        )))
    }

    fn as_i64(&self) -> Result<i64, OpError> {
        match self {
            Self::Int64(value) => Ok(*value),
            other => Err(OpError::column_type_mismatch("Int64", format!("{other:?}"))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum SideTag {
    Left,
    Right,
}

impl SideTag {
    fn to_join_side(self) -> JoinSide {
        match self {
            Self::Left => JoinSide::Left,
            Self::Right => JoinSide::Right,
        }
    }
}

#[derive(Debug, Clone)]
struct SideLayout {
    group_side: SideTag,
    group_local_col: usize,
    value_side: Option<SideTag>,
    value_local_col: usize,
}

impl SideLayout {
    fn new(
        left_n_cols: usize,
        group_col: usize,
        value_col: usize,
        kind: FactorizedAggregateKind,
    ) -> Self {
        let (group_side, group_local_col) = if group_col < left_n_cols {
            (SideTag::Left, group_col)
        } else {
            (SideTag::Right, group_col - left_n_cols)
        };
        let (value_side, value_local_col) = match kind {
            FactorizedAggregateKind::Count => (None, 0),
            FactorizedAggregateKind::Sum | FactorizedAggregateKind::Avg => {
                if value_col < left_n_cols {
                    (Some(SideTag::Left), value_col)
                } else {
                    (Some(SideTag::Right), value_col - left_n_cols)
                }
            }
        };
        Self {
            group_side,
            group_local_col,
            value_side,
            value_local_col,
        }
    }

    fn value_on_group_side(&self) -> bool {
        self.value_side == Some(self.group_side)
    }

    fn value_on_other_side(&self) -> bool {
        self.value_side.is_some() && self.value_side != Some(self.group_side)
    }
}

/// Per-key sufficient summary on the grouped side: `group -> (val_weighted_sum, weight_sum)`.
type GroupedKeySummary = HashMap<Scalar, (i128, i128)>;
type SideDeltaMap = HashMap<Vec<u8>, HashMap<Vec<Scalar>, i64>>;
type StarFactCommits = Vec<(Vec<u8>, Vec<(Vec<Scalar>, i64)>)>;

/// Single-owner, lock-free (`&mut self`) state core for [`FactorizedJoinAggregateOp`].
///
/// Maintains join-key indexes, per-key algebraic summaries, running group aggregates,
/// and dirty row tracking with zero internal `Mutex`es.
pub struct FactorizedJoinCore {
    op_id: OperatorId,
    left_key_cols: Vec<usize>,
    right_key_cols: Vec<usize>,
    left_n_cols: usize,
    right_n_cols: usize,
    group_col: usize,
    value_col: usize,
    kind: FactorizedAggregateKind,
    layout: SideLayout,
    max_payload_rows: usize,
    max_payload_bytes: usize,
    left_by_key: HashMap<Vec<u8>, HashMap<Vec<Scalar>, i64>>,
    right_by_key: HashMap<Vec<u8>, HashMap<Vec<Scalar>, i64>>,
    /// Per-key sufficient summary for the side containing `group_col`.
    grouped_summary_by_key: HashMap<Vec<u8>, GroupedKeySummary>,
    /// Per-key sufficient summary `(val_weighted_sum, weight_sum)` for the opposite side.
    other_summary_by_key: HashMap<Vec<u8>, (i128, i128)>,
    /// Tracks live rows with non-Int64 value column on the value-bearing side per join key.
    invalid_value_rows_by_key: HashMap<Vec<u8>, usize>,
    /// Maintained global aggregate state `(sum, count)` per live group (`count != 0`).
    group_aggregates: BTreeMap<Scalar, (i64, i64)>,
    payload_rows: usize,
    payload_bytes: usize,
    group_type: Option<DataType>,
    /// Dirty logical rows awaiting persistence (`(side, join_key, row_values)`).
    dirty_rows: HashSet<(SideTag, Vec<u8>, Vec<Scalar>)>,
    /// Optional legacy persisted row_ids restored from storage whose key hash included weight.
    legacy_row_ids: HashMap<(SideTag, Vec<u8>, Vec<Scalar>), u128>,
    governor_dirty: bool,
    governor: DeltaAmplificationGovernor,
}

impl FactorizedJoinCore {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        op_id: OperatorId,
        left_key_cols: Vec<usize>,
        right_key_cols: Vec<usize>,
        left_n_cols: usize,
        right_n_cols: usize,
        group_col: usize,
        value_col: usize,
        kind: FactorizedAggregateKind,
        max_payload_rows: usize,
        max_payload_bytes: usize,
        governor: DeltaAmplificationGovernor,
    ) -> Self {
        let layout = SideLayout::new(left_n_cols, group_col, value_col, kind);
        Self {
            op_id,
            left_key_cols,
            right_key_cols,
            left_n_cols,
            right_n_cols,
            group_col,
            value_col,
            kind,
            layout,
            max_payload_rows,
            max_payload_bytes,
            left_by_key: HashMap::new(),
            right_by_key: HashMap::new(),
            grouped_summary_by_key: HashMap::new(),
            other_summary_by_key: HashMap::new(),
            invalid_value_rows_by_key: HashMap::new(),
            group_aggregates: BTreeMap::new(),
            payload_rows: 0,
            payload_bytes: 0,
            group_type: None,
            dirty_rows: HashSet::new(),
            legacy_row_ids: HashMap::new(),
            governor_dirty: false,
            governor,
        }
    }

    pub fn op_id(&self) -> OperatorId {
        self.op_id
    }

    pub fn governor(&self) -> &DeltaAmplificationGovernor {
        &self.governor
    }

    pub fn factor_payload_rows(&self) -> usize {
        self.payload_rows
    }

    pub fn factor_payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    pub fn joined_intermediate_rows(&self) -> usize {
        0
    }

    pub fn state_bytes(&self) -> u64 {
        self.payload_bytes as u64
    }

    pub fn dirty_row_count(&self) -> usize {
        self.dirty_rows.len()
    }

    /// Process one epoch incrementally in O(|delta| + |affected keys| + |affected groups|),
    /// using transactional per-key overlays for atomic rollback on any failure.
    pub fn process_epoch_with_result_mut(
        &mut self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<OperatorEpochResult, OpError> {
        left.validate()?;
        right.validate()?;
        let total_cols = self.left_n_cols + self.right_n_cols;
        if self.group_col >= total_cols {
            return Err(OpError::column_out_of_bounds(self.group_col, total_cols));
        }
        if !matches!(self.kind, FactorizedAggregateKind::Count) && self.value_col >= total_cols {
            return Err(OpError::column_out_of_bounds(self.value_col, total_cols));
        }
        let group_type = if !left.is_empty() {
            if self.group_col >= self.left_n_cols {
                return Err(OpError::column_out_of_bounds(
                    self.group_col,
                    self.left_n_cols,
                ));
            }
            Some(left.data.column(self.group_col).data_type().clone())
        } else if !right.is_empty() && self.group_col >= self.left_n_cols {
            let group_column = self.group_col - self.left_n_cols;
            if group_column >= self.right_n_cols {
                return Err(OpError::column_out_of_bounds(self.group_col, total_cols));
            }
            Some(right.data.column(group_column).data_type().clone())
        } else {
            None
        };

        let left_deltas = self.extract_side_deltas(&left, SideTag::Left)?;
        let right_deltas = self.extract_side_deltas(&right, SideTag::Right)?;

        let mut affected_keys: BTreeSet<Vec<u8>> = BTreeSet::new();
        affected_keys.extend(left_deltas.keys().cloned());
        affected_keys.extend(right_deltas.keys().cloned());

        struct KeyOverlay {
            key: Vec<u8>,
            left_updates: Vec<(Vec<Scalar>, i64)>,
            right_updates: Vec<(Vec<Scalar>, i64)>,
            grouped_delta: HashMap<Scalar, (i128, i128)>,
            other_delta: (i128, i128),
            invalid_delta: isize,
        }

        let mut overlays: Vec<KeyOverlay> = Vec::with_capacity(affected_keys.len());
        let mut group_agg_deltas: BTreeMap<Scalar, (i128, i128)> = BTreeMap::new();
        let mut projected_rows = self.payload_rows as isize;
        let mut projected_bytes = self.payload_bytes as isize;
        let mut total_state_writes = 0_u64;
        let mut total_probes = 0_u64;

        for key in affected_keys {
            let mut left_updates = Vec::new();
            let mut right_updates = Vec::new();
            let mut grouped_delta: HashMap<Scalar, (i128, i128)> = HashMap::new();
            let mut other_delta: (i128, i128) = (0, 0);
            let mut invalid_delta: isize = 0;

            let old_left_count = self.left_by_key.get(&key).map_or(0, HashMap::len);
            let old_right_count = self.right_by_key.get(&key).map_or(0, HashMap::len);
            let mut new_left_count = old_left_count as isize;
            let mut new_right_count = old_right_count as isize;

            if let Some(deltas) = left_deltas.get(&key) {
                let existing = self.left_by_key.get(&key);
                for (values, &dw) in deltas {
                    let old_w = existing.and_then(|m| m.get(values)).copied().unwrap_or(0);
                    let new_w = old_w
                        .checked_add(dw)
                        .ok_or_else(|| OpError::internal("factorized payload weight overflow"))?;
                    if new_w != old_w {
                        total_state_writes += 1;
                    }
                    let row_bytes = (key.len() + scalar_bytes(values) + 8) as isize;
                    match (old_w != 0, new_w != 0) {
                        (false, true) => {
                            projected_rows += 1;
                            projected_bytes += row_bytes;
                            new_left_count += 1;
                            if self.layout.value_side == Some(SideTag::Left)
                                && values[self.layout.value_local_col].as_i64().is_err()
                            {
                                invalid_delta += 1;
                            }
                        }
                        (true, false) => {
                            projected_rows -= 1;
                            projected_bytes -= row_bytes;
                            new_left_count -= 1;
                            if self.layout.value_side == Some(SideTag::Left)
                                && values[self.layout.value_local_col].as_i64().is_err()
                            {
                                invalid_delta -= 1;
                            }
                        }
                        _ => {}
                    }
                    self.accumulate_side_summary_delta(
                        SideTag::Left,
                        values,
                        old_w,
                        new_w,
                        &mut grouped_delta,
                        &mut other_delta,
                    )?;
                    if new_w != old_w {
                        left_updates.push((values.clone(), new_w));
                    }
                }
            }

            if let Some(deltas) = right_deltas.get(&key) {
                let existing = self.right_by_key.get(&key);
                for (values, &dw) in deltas {
                    let old_w = existing.and_then(|m| m.get(values)).copied().unwrap_or(0);
                    let new_w = old_w
                        .checked_add(dw)
                        .ok_or_else(|| OpError::internal("factorized payload weight overflow"))?;
                    if new_w != old_w {
                        total_state_writes += 1;
                    }
                    let row_bytes = (key.len() + scalar_bytes(values) + 8) as isize;
                    match (old_w != 0, new_w != 0) {
                        (false, true) => {
                            projected_rows += 1;
                            projected_bytes += row_bytes;
                            new_right_count += 1;
                            if self.layout.value_side == Some(SideTag::Right)
                                && values[self.layout.value_local_col].as_i64().is_err()
                            {
                                invalid_delta += 1;
                            }
                        }
                        (true, false) => {
                            projected_rows -= 1;
                            projected_bytes -= row_bytes;
                            new_right_count -= 1;
                            if self.layout.value_side == Some(SideTag::Right)
                                && values[self.layout.value_local_col].as_i64().is_err()
                            {
                                invalid_delta -= 1;
                            }
                        }
                        _ => {}
                    }
                    self.accumulate_side_summary_delta(
                        SideTag::Right,
                        values,
                        old_w,
                        new_w,
                        &mut grouped_delta,
                        &mut other_delta,
                    )?;
                    if new_w != old_w {
                        right_updates.push((values.clone(), new_w));
                    }
                }
            }

            let old_invalid = self
                .invalid_value_rows_by_key
                .get(&key)
                .copied()
                .unwrap_or(0) as isize;
            let new_invalid = (old_invalid + invalid_delta).max(0) as usize;
            if new_invalid > 0 && new_left_count > 0 && new_right_count > 0 {
                return Err(OpError::column_type_mismatch(
                    "Int64",
                    "non-Int64 value in factorized aggregate",
                ));
            }

            // Compute old vs new key contribution to each affected group using ONLY key summaries.
            let old_other = self
                .other_summary_by_key
                .get(&key)
                .copied()
                .unwrap_or((0, 0));
            let new_other = (
                old_other
                    .0
                    .checked_add(other_delta.0)
                    .ok_or_else(|| OpError::internal("factorized aggregate sum overflow"))?,
                old_other
                    .1
                    .checked_add(other_delta.1)
                    .ok_or_else(|| OpError::internal("factorized aggregate count overflow"))?,
            );

            let old_grouped = self.grouped_summary_by_key.get(&key);
            let other_changed = other_delta != (0, 0);
            let mut key_groups: BTreeSet<Scalar> = BTreeSet::new();
            if other_changed {
                if let Some(existing_groups) = old_grouped {
                    key_groups.extend(existing_groups.keys().cloned());
                }
            }
            key_groups.extend(grouped_delta.keys().cloned());

            let key_had_match = old_left_count > 0 && old_right_count > 0;
            let key_has_match = new_left_count > 0 && new_right_count > 0;
            if key_had_match || key_has_match {
                total_probes = total_probes.saturating_add(key_groups.len().max(1) as u64);
            }

            for group in key_groups {
                let old_g = old_grouped
                    .and_then(|m| m.get(&group))
                    .copied()
                    .unwrap_or((0, 0));
                let dg = grouped_delta.get(&group).copied().unwrap_or((0, 0));
                let new_g =
                    (
                        old_g.0.checked_add(dg.0).ok_or_else(|| {
                            OpError::internal("factorized aggregate sum overflow")
                        })?,
                        old_g.1.checked_add(dg.1).ok_or_else(|| {
                            OpError::internal("factorized aggregate count overflow")
                        })?,
                    );

                let (old_sum_c, old_cnt_c) = self.key_group_contribution(old_g, old_other)?;
                let (new_sum_c, new_cnt_c) = self.key_group_contribution(new_g, new_other)?;
                let delta_sum = new_sum_c
                    .checked_sub(old_sum_c)
                    .ok_or_else(|| OpError::internal("factorized aggregate sum overflow"))?;
                let delta_cnt = new_cnt_c
                    .checked_sub(old_cnt_c)
                    .ok_or_else(|| OpError::internal("factorized aggregate count overflow"))?;
                if delta_sum != 0 || delta_cnt != 0 {
                    let entry = group_agg_deltas.entry(group).or_insert((0, 0));
                    entry.0 = entry
                        .0
                        .checked_add(delta_sum)
                        .ok_or_else(|| OpError::internal("factorized aggregate sum overflow"))?;
                    entry.1 = entry
                        .1
                        .checked_add(delta_cnt)
                        .ok_or_else(|| OpError::internal("factorized aggregate count overflow"))?;
                }
            }

            overlays.push(KeyOverlay {
                key,
                left_updates,
                right_updates,
                grouped_delta,
                other_delta,
                invalid_delta,
            });
        }

        let final_rows = projected_rows.max(0) as usize;
        let final_bytes = projected_bytes.max(0) as usize;
        if final_rows > self.max_payload_rows || final_bytes > self.max_payload_bytes {
            return Err(OpError::factor_payload_overflow(
                final_rows,
                self.max_payload_rows,
                final_bytes,
                self.max_payload_bytes,
            ));
        }

        // Plan group aggregate transitions and output delta rows BEFORE mutating state.
        let mut group_updates: Vec<(Scalar, Option<(i64, i64)>)> =
            Vec::with_capacity(group_agg_deltas.len());
        let mut output_rows: Vec<(Scalar, ScalarResult, i64)> =
            Vec::with_capacity(group_agg_deltas.len() * 2);

        for (group, (d_sum, d_cnt)) in group_agg_deltas {
            let old_state = self.group_aggregates.get(&group).copied();
            let (old_sum, old_cnt) = old_state.unwrap_or((0, 0));
            let new_sum_128 = (old_sum as i128)
                .checked_add(d_sum)
                .ok_or_else(|| OpError::internal("factorized aggregate sum overflow"))?;
            let new_cnt_128 = (old_cnt as i128)
                .checked_add(d_cnt)
                .ok_or_else(|| OpError::internal("factorized aggregate count overflow"))?;
            let new_sum = i64::try_from(new_sum_128)
                .map_err(|_| OpError::internal("factorized aggregate sum overflow"))?;
            let new_cnt = i64::try_from(new_cnt_128)
                .map_err(|_| OpError::internal("factorized aggregate count overflow"))?;
            let new_state = (new_cnt != 0).then_some((new_sum, new_cnt));

            let old_result = old_state.map(|s| self.result_value(s));
            let new_result = new_state.map(|s| self.result_value(s));
            if old_result != new_result {
                if let Some(val) = old_result {
                    output_rows.push((group.clone(), val, -1));
                }
                if let Some(val) = new_result {
                    output_rows.push((group.clone(), val, 1));
                }
            }
            group_updates.push((group, new_state));
        }

        let resolved_group_type = group_type
            .clone()
            .or_else(|| self.group_type.clone())
            .or_else(|| {
                output_rows.iter().find_map(|(key, _, _)| match key {
                    Scalar::Utf8(_) => Some(DataType::Utf8),
                    Scalar::Int64(_) => Some(DataType::Int64),
                    Scalar::Null => None,
                })
            });

        let output = self.make_output(output_rows, resolved_group_type)?;

        let delta_counters = DeltaAmplificationCounters {
            input_deltas: (left.num_rows() + right.num_rows()) as u64,
            probes: total_probes,
            shuffled_bytes: 0,
            intermediate_tuples: 0,
            output_deltas: output.num_rows() as u64,
            state_writes: total_state_writes,
        };
        if let Some(dimension) = self.governor.exceeded(delta_counters) {
            let current = self.governor.projected(delta_counters).get(dimension);
            return Err(OpError::delta_amplification_exceeded(
                dimension.name(),
                current,
                self.governor.limit(dimension),
            ));
        }

        // Build deterministic changed-key state mutations before infallible commit.
        let mut mutations: Vec<StateMutation> = Vec::new();
        for overlay in &overlays {
            for (values, new_w) in &overlay.left_updates {
                self.push_row_mutation(SideTag::Left, &overlay.key, values, *new_w, &mut mutations);
            }
            for (values, new_w) in &overlay.right_updates {
                self.push_row_mutation(
                    SideTag::Right,
                    &overlay.key,
                    values,
                    *new_w,
                    &mut mutations,
                );
            }
        }

        // Infallible commit of the transactional overlay.
        for overlay in overlays {
            let key = overlay.key;
            if !overlay.left_updates.is_empty() {
                let side_map = self.left_by_key.entry(key.clone()).or_default();
                for (values, new_w) in overlay.left_updates {
                    if new_w == 0 {
                        side_map.remove(&values);
                    } else {
                        side_map.insert(values.clone(), new_w);
                    }
                    self.dirty_rows.insert((SideTag::Left, key.clone(), values));
                }
                if side_map.is_empty() {
                    self.left_by_key.remove(&key);
                }
            }
            if !overlay.right_updates.is_empty() {
                let side_map = self.right_by_key.entry(key.clone()).or_default();
                for (values, new_w) in overlay.right_updates {
                    if new_w == 0 {
                        side_map.remove(&values);
                    } else {
                        side_map.insert(values.clone(), new_w);
                    }
                    self.dirty_rows
                        .insert((SideTag::Right, key.clone(), values));
                }
                if side_map.is_empty() {
                    self.right_by_key.remove(&key);
                }
            }
            if !overlay.grouped_delta.is_empty() {
                let summary_map = self.grouped_summary_by_key.entry(key.clone()).or_default();
                for (group, (ds, dw)) in overlay.grouped_delta {
                    let entry = summary_map.entry(group.clone()).or_insert((0, 0));
                    entry.0 += ds;
                    entry.1 += dw;
                    if entry.0 == 0 && entry.1 == 0 {
                        summary_map.remove(&group);
                    }
                }
                if summary_map.is_empty() {
                    self.grouped_summary_by_key.remove(&key);
                }
            }
            if overlay.other_delta != (0, 0) {
                let entry = self
                    .other_summary_by_key
                    .entry(key.clone())
                    .or_insert((0, 0));
                entry.0 += overlay.other_delta.0;
                entry.1 += overlay.other_delta.1;
                if entry.0 == 0 && entry.1 == 0 {
                    self.other_summary_by_key.remove(&key);
                }
            }
            if overlay.invalid_delta != 0 {
                let cur = self
                    .invalid_value_rows_by_key
                    .get(&key)
                    .copied()
                    .unwrap_or(0) as isize;
                let next = (cur + overlay.invalid_delta).max(0) as usize;
                if next == 0 {
                    self.invalid_value_rows_by_key.remove(&key);
                } else {
                    self.invalid_value_rows_by_key.insert(key, next);
                }
            }
        }

        for (group, new_state) in group_updates {
            if let Some(state) = new_state {
                self.group_aggregates.insert(group, state);
            } else {
                self.group_aggregates.remove(&group);
            }
        }

        self.payload_rows = final_rows;
        self.payload_bytes = final_bytes;
        self.governor.record(delta_counters);
        self.governor_dirty = true;
        if let Some(gt) = group_type {
            self.group_type = Some(gt);
        }

        let logical_mutation_bytes: usize = mutations.iter().map(StateMutation::size_bytes).sum();
        let metrics = OperatorEpochMetrics {
            input_records: left.num_rows() + right.num_rows(),
            output_records: output.num_rows(),
            dirty_keys: mutations.len(),
            state_mutations: mutations.len(),
            logical_mutation_bytes,
            full_state_entries_visited: 0,
            state_bytes: self.payload_bytes,
        };
        rockstream_types::metrics::record_r1_persistence(
            self.op_id,
            metrics.state_mutations as u64,
            metrics.logical_mutation_bytes as u64,
            metrics.dirty_keys as u64,
        );

        Ok(OperatorEpochResult::new(output, mutations, metrics))
    }

    /// Process one epoch returning only the output delta Z-set.
    pub fn process_epoch_mut(
        &mut self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<ArrowZSet, OpError> {
        self.process_epoch_with_result_mut(left, right)
            .map(|r| r.output_delta)
    }

    fn extract_side_deltas(
        &self,
        delta: &ArrowZSet,
        side: SideTag,
    ) -> Result<SideDeltaMap, OpError> {
        let mut out: SideDeltaMap = HashMap::new();
        if delta.is_empty() {
            return Ok(out);
        }
        let (expected_cols, key_cols) = match side {
            SideTag::Left => (self.left_n_cols, &self.left_key_cols),
            SideTag::Right => (self.right_n_cols, &self.right_key_cols),
        };
        if delta.data.num_columns() != expected_cols {
            return Err(OpError::unsupported_plan_node(format!(
                "factorized join side has {} columns; expected {expected_cols}",
                delta.data.num_columns()
            )));
        }
        if let Some(&column) = key_cols.iter().find(|column| **column >= expected_cols) {
            return Err(OpError::column_out_of_bounds(column, expected_cols));
        }

        // Reuse once-per-batch canonical BatchKeyCapsule instead of per-row key encoding.
        let capsule = delta.get_or_build_key_capsule(key_cols)?;

        for row in 0..delta.num_rows() {
            let values = delta
                .data
                .columns()
                .iter()
                .map(|column| Scalar::from_array(column, row))
                .collect::<Result<Vec<_>, _>>()?;
            if capsule.row_contains_null(row) {
                continue;
            }
            let w = delta.weights[row];
            if w == 0 {
                continue;
            }
            let key = capsule.row_bytes(row).to_vec();
            let entry = out.entry(key).or_default().entry(values).or_insert(0);
            *entry = entry
                .checked_add(w)
                .ok_or_else(|| OpError::internal("factorized payload weight overflow"))?;
        }
        Ok(out)
    }

    fn accumulate_side_summary_delta(
        &self,
        side: SideTag,
        values: &[Scalar],
        old_w: i64,
        new_w: i64,
        grouped_delta: &mut HashMap<Scalar, (i128, i128)>,
        other_delta: &mut (i128, i128),
    ) -> Result<(), OpError> {
        let dw = (new_w as i128) - (old_w as i128);
        if dw == 0 {
            return Ok(());
        }
        if side == self.layout.group_side {
            let group = values
                .get(self.layout.group_local_col)
                .ok_or_else(|| {
                    OpError::column_out_of_bounds(
                        self.group_col,
                        self.left_n_cols + self.right_n_cols,
                    )
                })?
                .clone();
            let d_val = if self.layout.value_on_group_side() {
                match values
                    .get(self.layout.value_local_col)
                    .and_then(|s| s.as_i64().ok())
                {
                    Some(v) => (v as i128).checked_mul(dw).ok_or_else(|| {
                        OpError::internal("factorized aggregate value multiplication overflow")
                    })?,
                    None => 0,
                }
            } else if matches!(self.kind, FactorizedAggregateKind::Count) {
                dw
            } else {
                0
            };
            let entry = grouped_delta.entry(group).or_insert((0, 0));
            entry.0 = entry
                .0
                .checked_add(d_val)
                .ok_or_else(|| OpError::internal("factorized aggregate sum overflow"))?;
            entry.1 = entry
                .1
                .checked_add(dw)
                .ok_or_else(|| OpError::internal("factorized aggregate count overflow"))?;
        } else {
            let d_val = if self.layout.value_on_other_side() {
                match values
                    .get(self.layout.value_local_col)
                    .and_then(|s| s.as_i64().ok())
                {
                    Some(v) => (v as i128).checked_mul(dw).ok_or_else(|| {
                        OpError::internal("factorized aggregate value multiplication overflow")
                    })?,
                    None => 0,
                }
            } else {
                0
            };
            other_delta.0 = other_delta
                .0
                .checked_add(d_val)
                .ok_or_else(|| OpError::internal("factorized aggregate sum overflow"))?;
            other_delta.1 = other_delta
                .1
                .checked_add(dw)
                .ok_or_else(|| OpError::internal("factorized aggregate count overflow"))?;
        }
        Ok(())
    }

    fn key_group_contribution(
        &self,
        grouped: (i128, i128),
        other: (i128, i128),
    ) -> Result<(i128, i128), OpError> {
        let cnt = grouped
            .1
            .checked_mul(other.1)
            .ok_or_else(|| OpError::internal("factorized join weight multiplication overflow"))?;
        let sum = if self.layout.value_on_other_side() {
            grouped.1.checked_mul(other.0).ok_or_else(|| {
                OpError::internal("factorized aggregate value multiplication overflow")
            })?
        } else {
            grouped.0.checked_mul(other.1).ok_or_else(|| {
                OpError::internal("factorized aggregate value multiplication overflow")
            })?
        };
        Ok((sum, cnt))
    }

    fn result_value(&self, (sum, count): (i64, i64)) -> ScalarResult {
        match self.kind {
            FactorizedAggregateKind::Sum => ScalarResult::Int64(sum),
            FactorizedAggregateKind::Count => ScalarResult::Int64(count),
            FactorizedAggregateKind::Avg => ScalarResult::Float64(sum as f64 / count as f64),
        }
    }

    fn make_output(
        &self,
        rows: Vec<(Scalar, ScalarResult, i64)>,
        group_type: Option<DataType>,
    ) -> Result<ArrowZSet, OpError> {
        let data_type = group_type.unwrap_or(DataType::Int64);
        let (groups, values, weights): (Vec<_>, Vec<_>, Vec<_>) = rows.into_iter().fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |mut result, (group, value, weight)| {
                result.0.push(group);
                result.1.push(value);
                result.2.push(weight);
                result
            },
        );
        let group_array: ArrayRef = match data_type {
            DataType::Utf8 => ArcString::from_scalars(&groups),
            _ => ArcInt64::from_scalars(&groups),
        }?;
        let value_array: ArrayRef = match self.kind {
            FactorizedAggregateKind::Avg => Arc::new(Float64Array::from(
                values.iter().map(ScalarResult::as_f64).collect::<Vec<_>>(),
            )) as ArrayRef,
            FactorizedAggregateKind::Sum | FactorizedAggregateKind::Count => Arc::new(
                Int64Array::from(values.iter().map(ScalarResult::as_i64).collect::<Vec<_>>()),
            )
                as ArrayRef,
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("group", group_array.data_type().clone(), true),
            Field::new("aggregate", value_array.data_type().clone(), false),
        ]));
        let data =
            RecordBatch::try_new(schema, vec![group_array, value_array]).map_err(OpError::arrow)?;
        Ok(ArrowZSet::new(data, weights))
    }

    fn stable_logical_row_id(&self, key: &[u8], values: &[Scalar]) -> u128 {
        let id_bytes = encode_row_identity(values);
        stable_row_id(self.op_id.0, key, &id_bytes)
    }

    fn push_row_mutation(
        &self,
        side: SideTag,
        key: &[u8],
        values: &[Scalar],
        new_w: i64,
        out: &mut Vec<StateMutation>,
    ) {
        let canonical_id = self.stable_logical_row_id(key, values);
        let lookup = (side, key.to_vec(), values.to_vec());
        if let Some(&legacy_id) = self.legacy_row_ids.get(&lookup) {
            if legacy_id != canonical_id {
                let legacy_key = ShardKeyEncoder::factor_payload_key(
                    side.to_join_side(),
                    self.op_id.0,
                    key,
                    legacy_id,
                );
                out.push(StateMutation::Delete { key: legacy_key });
            }
        }
        let storage_key = ShardKeyEncoder::factor_payload_key(
            side.to_join_side(),
            self.op_id.0,
            key,
            canonical_id,
        );
        if new_w == 0 {
            out.push(StateMutation::Delete { key: storage_key });
        } else {
            let row_bytes = encode_row(values, new_w);
            out.push(StateMutation::Put {
                key: storage_key,
                value: Bytes::from(row_bytes),
            });
        }
    }

    /// Emit ONLY changed-key point `Put`s and `Delete`s into `target` — zero prefix scans,
    /// zero full-state rewrites.
    pub fn append_state(&self, target: &mut WriteBatch) -> Result<(), OpError> {
        let mut sorted_dirty: Vec<&(SideTag, Vec<u8>, Vec<Scalar>)> =
            self.dirty_rows.iter().collect();
        sorted_dirty.sort();

        for (side, key, values) in sorted_dirty {
            let weight = match side {
                SideTag::Left => self
                    .left_by_key
                    .get(key)
                    .and_then(|m| m.get(values))
                    .copied()
                    .unwrap_or(0),
                SideTag::Right => self
                    .right_by_key
                    .get(key)
                    .and_then(|m| m.get(values))
                    .copied()
                    .unwrap_or(0),
            };
            let canonical_id = self.stable_logical_row_id(key, values);
            let lookup = (*side, key.clone(), values.clone());
            if let Some(&legacy_id) = self.legacy_row_ids.get(&lookup) {
                if legacy_id != canonical_id {
                    let legacy_key = ShardKeyEncoder::factor_payload_key(
                        side.to_join_side(),
                        self.op_id.0,
                        key,
                        legacy_id,
                    );
                    target.delete(&legacy_key);
                }
            }
            let storage_key = ShardKeyEncoder::factor_payload_key(
                side.to_join_side(),
                self.op_id.0,
                key,
                canonical_id,
            );
            if weight == 0 {
                target.delete(&storage_key);
            } else {
                let row_bytes = encode_row(values, weight);
                target.put(&storage_key, &row_bytes);
            }
        }

        if self.governor_dirty {
            let counters = self.governor.counters();
            let mut encoded = Vec::with_capacity(4 + 6 * 8);
            encoded.extend_from_slice(&FACTORIZED_SELECTION_RULE_VERSION.to_be_bytes());
            for value in [
                counters.input_deltas,
                counters.probes,
                counters.shuffled_bytes,
                counters.intermediate_tuples,
                counters.output_deltas,
                counters.state_writes,
            ] {
                encoded.extend_from_slice(&value.to_be_bytes());
            }
            target.put(
                &ShardKeyEncoder::factor_governor_key(self.op_id.0),
                &encoded,
            );
        }
        Ok(())
    }

    /// Clear dirty-key tracking only after the enclosing `WriteBatch` commits durably.
    pub fn ack_persisted_state(&mut self) {
        for dirty in self.dirty_rows.drain() {
            self.legacy_row_ids.remove(&dirty);
        }
        self.governor_dirty = false;
    }

    /// Rebuild indexed state, per-key summaries, and group aggregates from persisted storage.
    pub async fn restore_in_place(&mut self, db: &ShardDb) -> Result<(), OpError> {
        let mut left_by_key: HashMap<Vec<u8>, HashMap<Vec<Scalar>, i64>> = HashMap::new();
        let mut right_by_key: HashMap<Vec<u8>, HashMap<Vec<Scalar>, i64>> = HashMap::new();
        let mut grouped_summary_by_key: HashMap<Vec<u8>, GroupedKeySummary> = HashMap::new();
        let mut other_summary_by_key: HashMap<Vec<u8>, (i128, i128)> = HashMap::new();
        let mut invalid_value_rows_by_key: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut legacy_row_ids: HashMap<(SideTag, Vec<u8>, Vec<Scalar>), u128> = HashMap::new();
        let mut payload_rows = 0usize;
        let mut payload_bytes = 0usize;

        for (side_tag, expected_cols) in [
            (SideTag::Left, self.left_n_cols),
            (SideTag::Right, self.right_n_cols),
        ] {
            let prefix =
                ShardKeyEncoder::factor_payload_op_prefix(side_tag.to_join_side(), self.op_id.0);
            let (entries, truncated) = db
                .scan_prefix_bounded(&prefix, MAX_FACTOR_PAYLOAD_BYTES)
                .await
                .map_err(OpError::storage)?;
            if truncated {
                return Err(OpError::factor_payload_overflow(
                    self.payload_rows,
                    self.max_payload_rows,
                    MAX_FACTOR_PAYLOAD_BYTES,
                    self.max_payload_bytes,
                ));
            }
            for (key, value) in entries {
                if key.len() < prefix.len() + 4 + 16 {
                    continue;
                }
                let key_len = u32::from_be_bytes(
                    key[prefix.len()..prefix.len() + 4]
                        .try_into()
                        .map_err(|_| OpError::storage_error("factor payload key length"))?,
                ) as usize;
                let key_start = prefix.len() + 4;
                let key_end = key_start + key_len;
                if key_end + 16 > key.len() {
                    continue;
                }
                let stored_row_id = u128::from_be_bytes(
                    key[key_end..key_end + 16]
                        .try_into()
                        .map_err(|_| OpError::storage_error("factor payload row id"))?,
                );
                let (values, weight) = decode_row(&value, expected_cols)?;
                if weight == 0 {
                    continue;
                }
                let join_key = key[key_start..key_end].to_vec();
                let canonical_id = self.stable_logical_row_id(&join_key, &values);
                if stored_row_id != canonical_id {
                    legacy_row_ids
                        .insert((side_tag, join_key.clone(), values.clone()), stored_row_id);
                }

                let target_side = match side_tag {
                    SideTag::Left => &mut left_by_key,
                    SideTag::Right => &mut right_by_key,
                };
                let prev = target_side
                    .entry(join_key.clone())
                    .or_default()
                    .insert(values.clone(), weight);
                if prev.is_none() {
                    payload_rows += 1;
                    payload_bytes += join_key.len() + scalar_bytes(&values) + 8;
                }

                if self.layout.value_side == Some(side_tag)
                    && values[self.layout.value_local_col].as_i64().is_err()
                {
                    *invalid_value_rows_by_key
                        .entry(join_key.clone())
                        .or_insert(0) += 1;
                }

                let mut g_delta = HashMap::new();
                let mut o_delta = (0i128, 0i128);
                self.accumulate_side_summary_delta(
                    side_tag,
                    &values,
                    0,
                    weight,
                    &mut g_delta,
                    &mut o_delta,
                )?;
                if !g_delta.is_empty() {
                    let g_map = grouped_summary_by_key.entry(join_key.clone()).or_default();
                    for (g, (ds, dw)) in g_delta {
                        let e = g_map.entry(g).or_insert((0, 0));
                        e.0 += ds;
                        e.1 += dw;
                    }
                }
                if o_delta != (0, 0) {
                    let e = other_summary_by_key.entry(join_key).or_insert((0, 0));
                    e.0 += o_delta.0;
                    e.1 += o_delta.1;
                }
            }
        }

        if payload_rows > self.max_payload_rows || payload_bytes > self.max_payload_bytes {
            return Err(OpError::factor_payload_overflow(
                payload_rows,
                self.max_payload_rows,
                payload_bytes,
                self.max_payload_bytes,
            ));
        }

        // Reconstruct global group aggregates from per-key summaries in O(keys * groups_per_key).
        let mut group_totals_128: BTreeMap<Scalar, (i128, i128)> = BTreeMap::new();
        for (join_key, groups) in &grouped_summary_by_key {
            let Some(&other) = other_summary_by_key.get(join_key) else {
                continue;
            };
            for (group, &g_summary) in groups {
                let (s, c) = self.key_group_contribution(g_summary, other)?;
                let entry = group_totals_128.entry(group.clone()).or_insert((0, 0));
                entry.0 = entry
                    .0
                    .checked_add(s)
                    .ok_or_else(|| OpError::internal("factorized aggregate sum overflow"))?;
                entry.1 = entry
                    .1
                    .checked_add(c)
                    .ok_or_else(|| OpError::internal("factorized aggregate count overflow"))?;
            }
        }
        let mut group_aggregates = BTreeMap::new();
        for (group, (s, c)) in group_totals_128 {
            if c != 0 {
                let s64 = i64::try_from(s)
                    .map_err(|_| OpError::internal("factorized aggregate sum overflow"))?;
                let c64 = i64::try_from(c)
                    .map_err(|_| OpError::internal("factorized aggregate count overflow"))?;
                group_aggregates.insert(group, (s64, c64));
            }
        }

        if let Some(bytes) = db
            .get(&ShardKeyEncoder::factor_governor_key(self.op_id.0))
            .await
            .map_err(OpError::storage)?
        {
            if bytes.len() == 4 + 6 * 8
                && u32::from_be_bytes(bytes[0..4].try_into().unwrap())
                    == FACTORIZED_SELECTION_RULE_VERSION
            {
                let values = bytes
                    .get(4..)
                    .unwrap()
                    .as_chunks::<8>()
                    .0
                    .iter()
                    .map(|chunk| u64::from_be_bytes(*chunk))
                    .collect::<Vec<_>>();
                self.governor.restore(DeltaAmplificationCounters {
                    input_deltas: values[0],
                    probes: values[1],
                    shuffled_bytes: values[2],
                    intermediate_tuples: values[3],
                    output_deltas: values[4],
                    state_writes: values[5],
                });
            }
        }

        self.left_by_key = left_by_key;
        self.right_by_key = right_by_key;
        self.grouped_summary_by_key = grouped_summary_by_key;
        self.other_summary_by_key = other_summary_by_key;
        self.invalid_value_rows_by_key = invalid_value_rows_by_key;
        self.group_aggregates = group_aggregates;
        self.payload_rows = payload_rows;
        self.payload_bytes = payload_bytes;
        self.legacy_row_ids = legacy_row_ids;
        self.dirty_rows.clear();
        self.governor_dirty = false;
        Ok(())
    }
}

/// A bounded indexed incremental factorized two-input join whose public output is only an aggregate delta.
pub struct FactorizedJoinAggregateOp {
    op_id: OperatorId,
    governor: DeltaAmplificationGovernor,
    core: Mutex<FactorizedJoinCore>,
}

impl FactorizedJoinAggregateOp {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        op_id: OperatorId,
        left_key_cols: Vec<usize>,
        right_key_cols: Vec<usize>,
        left_n_cols: usize,
        right_n_cols: usize,
        group_col: usize,
        value_col: usize,
        kind: FactorizedAggregateKind,
    ) -> Self {
        Self::with_limits(
            op_id,
            left_key_cols,
            right_key_cols,
            left_n_cols,
            right_n_cols,
            group_col,
            value_col,
            kind,
            MAX_FACTOR_PAYLOAD_ROWS,
            MAX_FACTOR_PAYLOAD_BYTES,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_limits(
        op_id: OperatorId,
        left_key_cols: Vec<usize>,
        right_key_cols: Vec<usize>,
        left_n_cols: usize,
        right_n_cols: usize,
        group_col: usize,
        value_col: usize,
        kind: FactorizedAggregateKind,
        max_payload_rows: usize,
        max_payload_bytes: usize,
    ) -> Self {
        Self::with_limits_and_budget(
            op_id,
            left_key_cols,
            right_key_cols,
            left_n_cols,
            right_n_cols,
            group_col,
            value_col,
            kind,
            max_payload_rows,
            max_payload_bytes,
            DEFAULT_FACTORIZED_DELTA_BUDGET,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_limits_and_budget(
        op_id: OperatorId,
        left_key_cols: Vec<usize>,
        right_key_cols: Vec<usize>,
        left_n_cols: usize,
        right_n_cols: usize,
        group_col: usize,
        value_col: usize,
        kind: FactorizedAggregateKind,
        max_payload_rows: usize,
        max_payload_bytes: usize,
        budget: DeltaAmplificationBudget,
    ) -> Self {
        let governor = DeltaAmplificationGovernor::new(budget);
        let core = FactorizedJoinCore::new(
            op_id,
            left_key_cols,
            right_key_cols,
            left_n_cols,
            right_n_cols,
            group_col,
            value_col,
            kind,
            max_payload_rows,
            max_payload_bytes,
            governor.clone(),
        );
        Self {
            op_id,
            governor,
            core: Mutex::new(core),
        }
    }

    pub fn with_governor(mut self, governor: DeltaAmplificationGovernor) -> Self {
        self.core.get_mut().unwrap().governor = governor.clone();
        self.governor = governor;
        self
    }

    /// Consume this wrapper and return the lock-free single-owner [`FactorizedJoinCore`].
    pub fn into_core(self) -> FactorizedJoinCore {
        self.core.into_inner().unwrap()
    }

    pub fn governor(&self) -> &DeltaAmplificationGovernor {
        &self.governor
    }

    pub fn factor_payload_rows(&self) -> usize {
        self.core.lock().unwrap().factor_payload_rows()
    }

    pub fn factor_payload_bytes(&self) -> usize {
        self.core.lock().unwrap().factor_payload_bytes()
    }

    pub fn op_id(&self) -> OperatorId {
        self.op_id
    }

    pub fn joined_intermediate_rows(&self) -> usize {
        0
    }

    pub fn state_bytes(&self) -> u64 {
        self.core.lock().unwrap().state_bytes()
    }

    /// Zero-lock single-owner epoch execution (`&mut self`).
    pub fn process_epoch_mut(
        &mut self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<ArrowZSet, OpError> {
        self.core.get_mut().unwrap().process_epoch_mut(left, right)
    }

    /// Zero-lock single-owner epoch execution returning changed-key `StateMutation`s and metrics.
    pub fn process_epoch_with_result_mut(
        &mut self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<OperatorEpochResult, OpError> {
        self.core
            .get_mut()
            .unwrap()
            .process_epoch_with_result_mut(left, right)
    }

    pub fn process_epoch_with_result(
        &self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<OperatorEpochResult, OpError> {
        self.core
            .lock()
            .unwrap()
            .process_epoch_with_result_mut(left, right)
    }

    pub fn process_epoch(&self, left: ArrowZSet, right: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.core.lock().unwrap().process_epoch_mut(left, right)
    }

    pub fn append_state(&self, target: &mut WriteBatch) -> Result<(), OpError> {
        self.core.lock().unwrap().append_state(target)
    }

    /// Changed-key persistence into `target` without scanning unrelated retained keys in `db`.
    pub async fn append_state_with_db(
        &self,
        _db: &ShardDb,
        target: &mut WriteBatch,
    ) -> Result<(), OpError> {
        self.core.lock().unwrap().append_state(target)
    }

    /// Mark dirty keys durable after the enclosing `WriteBatch` commits.
    pub fn ack_persisted_state(&self) {
        self.core.lock().unwrap().ack_persisted_state();
    }

    pub fn clear_dirty_keys(&self) {
        self.ack_persisted_state();
    }

    pub async fn restore_in_place(&self, db: &ShardDb) -> Result<(), OpError> {
        // Restore into a fresh core outside the lock to avoid holding a std::sync::Mutex across `.await`.
        let mut staged = {
            let guard = self.core.lock().unwrap();
            FactorizedJoinCore::new(
                guard.op_id,
                guard.left_key_cols.clone(),
                guard.right_key_cols.clone(),
                guard.left_n_cols,
                guard.right_n_cols,
                guard.group_col,
                guard.value_col,
                guard.kind,
                guard.max_payload_rows,
                guard.max_payload_bytes,
                self.governor.clone(),
            )
        };
        staged.restore_in_place(db).await?;
        *self.core.lock().unwrap() = staged;
        Ok(())
    }

    pub async fn persist_state(&self, db: &ShardDb) -> Result<(), OpError> {
        let mut batch = WriteBatch::new();
        self.append_state(&mut batch)?;
        if !batch.is_empty() {
            db.write_batch(batch).await.map_err(OpError::storage)?;
        }
        self.ack_persisted_state();
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ScalarResult {
    Int64(i64),
    Float64(f64),
}

impl ScalarResult {
    fn as_i64(&self) -> i64 {
        match self {
            Self::Int64(value) => *value,
            Self::Float64(_) => 0,
        }
    }
    fn as_f64(&self) -> f64 {
        match self {
            Self::Int64(value) => *value as f64,
            Self::Float64(value) => *value,
        }
    }
}

struct ArcInt64;
impl ArcInt64 {
    fn from_scalars(values: &[Scalar]) -> Result<ArrayRef, OpError> {
        Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| match value {
                    Scalar::Int64(v) => Some(*v),
                    Scalar::Null => None,
                    Scalar::Utf8(_) => None,
                })
                .collect::<Vec<_>>(),
        )))
    }
}

struct ArcString;
impl ArcString {
    fn from_scalars(values: &[Scalar]) -> Result<ArrayRef, OpError> {
        Ok(Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| match value {
                    Scalar::Utf8(v) => Some(v.as_str()),
                    Scalar::Null => None,
                    Scalar::Int64(_) => None,
                })
                .collect::<Vec<_>>(),
        )))
    }
}

fn scalar_bytes(values: &[Scalar]) -> usize {
    values
        .iter()
        .map(|value| match value {
            Scalar::Null => 1,
            Scalar::Int64(_) => 9,
            Scalar::Utf8(value) => 5 + value.len(),
        })
        .sum()
}

fn encode_row_identity(values: &[Scalar]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(scalar_bytes(values));
    for value in values {
        match value {
            Scalar::Null => bytes.extend_from_slice(&[0, 0, 0, 0, 0]),
            Scalar::Int64(value) => {
                bytes.extend_from_slice(&[1, 0, 0, 0, 8]);
                bytes.extend_from_slice(&value.to_be_bytes());
            }
            Scalar::Utf8(value) => {
                bytes.push(2);
                bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
                bytes.extend_from_slice(value.as_bytes());
            }
        }
    }
    bytes
}

fn encode_row(values: &[Scalar], weight: i64) -> Vec<u8> {
    let mut bytes = encode_row_identity(values);
    bytes.extend_from_slice(&weight.to_be_bytes());
    bytes
}

fn decode_row(bytes: &[u8], columns: usize) -> Result<(Vec<Scalar>, i64), OpError> {
    let mut offset = 0;
    let mut values = Vec::with_capacity(columns);
    for _ in 0..columns {
        if offset + 5 > bytes.len() {
            return Err(OpError::storage_error("truncated factor payload row"));
        }
        let tag = bytes[offset];
        let len = u32::from_be_bytes(
            bytes[offset + 1..offset + 5]
                .try_into()
                .map_err(|_| OpError::storage_error("factor payload value length"))?,
        ) as usize;
        offset += 5;
        if offset + len > bytes.len() {
            return Err(OpError::storage_error("truncated factor payload value"));
        }
        let value = match tag {
            0 if len == 0 => Scalar::Null,
            1 if len == 8 => Scalar::Int64(i64::from_be_bytes(
                bytes[offset..offset + len]
                    .try_into()
                    .map_err(|_| OpError::storage_error("factor payload int64"))?,
            )),
            2 => Scalar::Utf8(
                String::from_utf8(bytes[offset..offset + len].to_vec())
                    .map_err(|_| OpError::storage_error("factor payload utf8"))?,
            ),
            _ => return Err(OpError::storage_error("unsupported factor payload value")),
        };
        values.push(value);
        offset += len;
    }
    if offset + 8 != bytes.len() {
        return Err(OpError::storage_error("invalid factor payload weight"));
    }
    Ok((
        values,
        i64::from_be_bytes(bytes[offset..].try_into().unwrap()),
    ))
}

/// Indexed incremental bounded payload-tree owner for star-join maintenance (O(affected keys)).
pub struct FactorizedStarJoinOp {
    dimensions: usize,
    rows: usize,
    bytes: usize,
    max_rows: usize,
    max_bytes: usize,
    /// `join_key -> (row_values -> weight)`
    fact_by_key: HashMap<Vec<u8>, HashMap<Vec<Scalar>, i64>>,
    /// Maintained dimension weight tables `dim_idx -> (join_key -> weight)`.
    dimension_state: Vec<HashMap<Vec<u8>, i64>>,
    /// Maintained global group totals for O(affected keys) delta emission.
    group_totals: BTreeMap<Scalar, i64>,
    cached_cols: Option<(usize, usize)>,
}

impl FactorizedStarJoinOp {
    pub fn new(dimensions: usize) -> Self {
        Self::with_limits(
            dimensions,
            MAX_FACTOR_PAYLOAD_ROWS,
            MAX_FACTOR_PAYLOAD_BYTES,
        )
    }

    pub fn with_limits(dimensions: usize, max_rows: usize, max_bytes: usize) -> Self {
        Self {
            dimensions,
            rows: 0,
            bytes: 0,
            max_rows,
            max_bytes,
            fact_by_key: HashMap::new(),
            dimension_state: vec![HashMap::new(); dimensions],
            group_totals: BTreeMap::new(),
            cached_cols: None,
        }
    }

    pub fn dimension_count(&self) -> usize {
        self.dimensions
    }
    pub fn joined_intermediate_rows(&self) -> usize {
        0
    }
    pub fn factor_payload_rows(&self) -> usize {
        self.rows
    }
    pub fn factor_payload_bytes(&self) -> usize {
        self.bytes
    }

    /// Apply one fact delta and one delta per dimension in O(|delta| + |affected keys|),
    /// using transactional per-key overlays with zero whole-state cloning.
    pub fn process_epoch(
        &mut self,
        fact: ArrowZSet,
        dimensions: Vec<ArrowZSet>,
        fact_key_col: usize,
        dimension_key_cols: &[usize],
        group_col: usize,
        value_col: usize,
    ) -> Result<ArrowZSet, OpError> {
        if dimensions.len() != self.dimensions || dimension_key_cols.len() != self.dimensions {
            return Err(OpError::unsupported_plan_node(format!(
                "star join has {} dimensions; expected {}",
                dimensions.len(),
                self.dimensions
            )));
        }
        if fact_key_col >= fact.data.num_columns()
            || group_col >= fact.data.num_columns()
            || value_col >= fact.data.num_columns()
        {
            return Err(OpError::column_out_of_bounds(
                [fact_key_col, group_col, value_col]
                    .into_iter()
                    .max()
                    .unwrap_or(0),
                fact.data.num_columns(),
            ));
        }
        for (delta, &key_col) in dimensions.iter().zip(dimension_key_cols) {
            if key_col >= delta.data.num_columns() {
                return Err(OpError::column_out_of_bounds(
                    key_col,
                    delta.data.num_columns(),
                ));
            }
        }

        // Consolidate incoming fact and dimension deltas using once-per-batch BatchKeyCapsules.
        let mut fact_deltas: HashMap<Vec<u8>, HashMap<Vec<Scalar>, i64>> = HashMap::new();
        if !fact.is_empty() {
            let capsule = fact.get_or_build_key_capsule(&[fact_key_col])?;
            for row in 0..fact.num_rows() {
                let values = fact
                    .data
                    .columns()
                    .iter()
                    .map(|column| Scalar::from_array(column, row))
                    .collect::<Result<Vec<_>, _>>()?;
                if capsule.row_contains_null(row) {
                    continue;
                }
                let w = fact.weights[row];
                if w == 0 {
                    continue;
                }
                let key = capsule.row_bytes(row).to_vec();
                let entry = fact_deltas
                    .entry(key)
                    .or_default()
                    .entry(values)
                    .or_insert(0);
                *entry = entry
                    .checked_add(w)
                    .ok_or_else(|| OpError::internal("star fact weight overflow"))?;
            }
        }

        let mut dim_deltas: Vec<HashMap<Vec<u8>, i64>> = vec![HashMap::new(); self.dimensions];
        for (idx, (delta, &key_col)) in dimensions.iter().zip(dimension_key_cols).enumerate() {
            if delta.is_empty() {
                continue;
            }
            let capsule = delta.get_or_build_key_capsule(&[key_col])?;
            for (row, &w) in delta.weights.iter().enumerate() {
                if capsule.row_contains_null(row) || w == 0 {
                    continue;
                }
                let key = capsule.row_bytes(row).to_vec();
                let entry = dim_deltas[idx].entry(key).or_insert(0);
                *entry = entry
                    .checked_add(w)
                    .ok_or_else(|| OpError::internal("star dimension weight overflow"))?;
            }
        }

        let mut affected_keys: BTreeSet<Vec<u8>> = BTreeSet::new();
        affected_keys.extend(fact_deltas.keys().cloned());
        for dm in &dim_deltas {
            affected_keys.extend(dm.keys().cloned());
        }

        let mut projected_rows = self.rows as isize;
        let mut projected_bytes = self.bytes as isize;
        let mut group_deltas: BTreeMap<Scalar, i128> = BTreeMap::new();
        let mut fact_commits: StarFactCommits = Vec::new();
        let mut dim_commits: Vec<(usize, Vec<u8>, i64)> = Vec::new();

        for key in affected_keys {
            let old_dim_mult = self.dimension_product(&key)?;
            let mut new_dim_mult: i64 = 1;
            for (idx, dm) in dim_deltas.iter().enumerate() {
                let old_dw = self.dimension_state[idx].get(&key).copied().unwrap_or(0);
                let delta_dw = dm.get(&key).copied().unwrap_or(0);
                let new_dw = old_dw
                    .checked_add(delta_dw)
                    .ok_or_else(|| OpError::internal("star dimension weight overflow"))?;
                if new_dw != old_dw {
                    let dim_bytes = (key.len() + 8) as isize;
                    match (old_dw != 0, new_dw != 0) {
                        (false, true) => {
                            projected_rows += 1;
                            projected_bytes += dim_bytes;
                        }
                        (true, false) => {
                            projected_rows -= 1;
                            projected_bytes -= dim_bytes;
                        }
                        _ => {}
                    }
                    dim_commits.push((idx, key.clone(), new_dw));
                }
                new_dim_mult = new_dim_mult
                    .checked_mul(new_dw)
                    .ok_or_else(|| OpError::internal("star join multiplicity overflow"))?;
            }

            let existing_rows = self.fact_by_key.get(&key);
            let row_deltas = fact_deltas.get(&key);
            let mut key_fact_updates = Vec::new();

            // Compute old vs new fact row contributions for ONLY this affected key.
            let mut touched_rows: HashSet<&Vec<Scalar>> = HashSet::new();
            if let Some(deltas) = row_deltas {
                for (values, &dw) in deltas {
                    touched_rows.insert(values);
                    let old_w = existing_rows
                        .and_then(|m| m.get(values))
                        .copied()
                        .unwrap_or(0);
                    let new_w = old_w
                        .checked_add(dw)
                        .ok_or_else(|| OpError::internal("star fact weight overflow"))?;
                    let row_bytes = (key.len() + scalar_bytes(values) + 8) as isize;
                    match (old_w != 0, new_w != 0) {
                        (false, true) => {
                            projected_rows += 1;
                            projected_bytes += row_bytes;
                        }
                        (true, false) => {
                            projected_rows -= 1;
                            projected_bytes -= row_bytes;
                        }
                        _ => {}
                    }
                    if new_w != old_w {
                        key_fact_updates.push((values.clone(), new_w));
                    }
                    accumulate_star_row_delta(
                        values,
                        old_w,
                        old_dim_mult,
                        new_w,
                        new_dim_mult,
                        group_col,
                        value_col,
                        &mut group_deltas,
                    )?;
                }
            }
            if old_dim_mult != new_dim_mult {
                if let Some(existing) = existing_rows {
                    for (values, &w) in existing {
                        if touched_rows.contains(values) {
                            continue;
                        }
                        accumulate_star_row_delta(
                            values,
                            w,
                            old_dim_mult,
                            w,
                            new_dim_mult,
                            group_col,
                            value_col,
                            &mut group_deltas,
                        )?;
                    }
                }
            }

            if !key_fact_updates.is_empty() {
                fact_commits.push((key, key_fact_updates));
            }
        }

        let final_rows = projected_rows.max(0) as usize;
        let final_bytes = projected_bytes.max(0) as usize;
        if final_rows > self.max_rows || final_bytes > self.max_bytes {
            return Err(OpError::factor_payload_overflow(
                final_rows,
                self.max_rows,
                final_bytes,
                self.max_bytes,
            ));
        }

        let mut planned_groups: Vec<(Scalar, Option<i64>)> = Vec::with_capacity(group_deltas.len());
        let mut out_rows: Vec<(Scalar, i64, i64)> = Vec::with_capacity(group_deltas.len() * 2);
        for (group, d_val) in group_deltas {
            if d_val == 0 {
                continue;
            }
            let old_val = self.group_totals.get(&group).copied();
            let new_128 = (old_val.unwrap_or(0) as i128)
                .checked_add(d_val)
                .ok_or_else(|| OpError::internal("star aggregate overflow"))?;
            let new_val_i64 =
                i64::try_from(new_128).map_err(|_| OpError::internal("star aggregate overflow"))?;
            let new_val = (new_val_i64 != 0).then_some(new_val_i64);
            match (old_val, new_val) {
                (Some(o), Some(n)) if o == n => {}
                (Some(o), Some(n)) => {
                    out_rows.push((group.clone(), o, -1));
                    out_rows.push((group.clone(), n, 1));
                }
                (Some(o), None) => out_rows.push((group.clone(), o, -1)),
                (None, Some(n)) => out_rows.push((group.clone(), n, 1)),
                (None, None) => {}
            }
            planned_groups.push((group, new_val));
        }

        let output = build_star_output(out_rows)?;

        // Infallible commit.
        for (key, updates) in fact_commits {
            let map = self.fact_by_key.entry(key.clone()).or_default();
            for (values, new_w) in updates {
                if new_w == 0 {
                    map.remove(&values);
                } else {
                    map.insert(values, new_w);
                }
            }
            if map.is_empty() {
                self.fact_by_key.remove(&key);
            }
        }
        for (idx, key, new_dw) in dim_commits {
            if new_dw == 0 {
                self.dimension_state[idx].remove(&key);
            } else {
                self.dimension_state[idx].insert(key, new_dw);
            }
        }
        for (group, new_val) in planned_groups {
            if let Some(v) = new_val {
                self.group_totals.insert(group, v);
            } else {
                self.group_totals.remove(&group);
            }
        }
        self.rows = final_rows;
        self.bytes = final_bytes;
        self.cached_cols = Some((group_col, value_col));
        Ok(output)
    }

    fn dimension_product(&self, key: &[u8]) -> Result<i64, OpError> {
        self.dimension_state.iter().try_fold(1_i64, |acc, dim| {
            acc.checked_mul(dim.get(key).copied().unwrap_or(0))
                .ok_or_else(|| OpError::internal("star join multiplicity overflow"))
        })
    }

    pub fn reserve_payload(&self, rows: usize, bytes: usize) -> Result<(), OpError> {
        if self.rows.saturating_add(rows) > self.max_rows
            || self.bytes.saturating_add(bytes) > self.max_bytes
        {
            return Err(OpError::factor_payload_overflow(
                self.rows.saturating_add(rows),
                self.max_rows,
                self.bytes.saturating_add(bytes),
                self.max_bytes,
            ));
        }
        Ok(())
    }

    pub fn append_payload(&mut self, rows: usize, bytes: usize) -> Result<(), OpError> {
        self.reserve_payload(rows, bytes)?;
        self.rows += rows;
        self.bytes += bytes;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn accumulate_star_row_delta(
    values: &[Scalar],
    old_w: i64,
    old_dim_mult: i64,
    new_w: i64,
    new_dim_mult: i64,
    group_col: usize,
    value_col: usize,
    group_deltas: &mut BTreeMap<Scalar, i128>,
) -> Result<(), OpError> {
    let old_mult = old_w
        .checked_mul(old_dim_mult)
        .ok_or_else(|| OpError::internal("star join multiplicity overflow"))?;
    let new_mult = new_w
        .checked_mul(new_dim_mult)
        .ok_or_else(|| OpError::internal("star join multiplicity overflow"))?;
    if old_mult == 0 && new_mult == 0 {
        return Ok(());
    }
    let group = values
        .get(group_col)
        .ok_or_else(|| OpError::column_out_of_bounds(group_col, values.len()))?
        .clone();
    let value = values
        .get(value_col)
        .ok_or_else(|| OpError::column_out_of_bounds(value_col, values.len()))?
        .as_i64()?;
    let old_contrib = value
        .checked_mul(old_mult)
        .ok_or_else(|| OpError::internal("star aggregate overflow"))? as i128;
    let new_contrib = value
        .checked_mul(new_mult)
        .ok_or_else(|| OpError::internal("star aggregate overflow"))? as i128;
    let delta = new_contrib - old_contrib;
    if delta != 0 {
        let entry = group_deltas.entry(group).or_insert(0);
        *entry = entry
            .checked_add(delta)
            .ok_or_else(|| OpError::internal("star aggregate overflow"))?;
    }
    Ok(())
}

fn build_star_output(rows: Vec<(Scalar, i64, i64)>) -> Result<ArrowZSet, OpError> {
    let group_type = rows
        .iter()
        .find_map(|(group, _, _)| match group {
            Scalar::Int64(_) => Some(DataType::Int64),
            Scalar::Utf8(_) => Some(DataType::Utf8),
            Scalar::Null => None,
        })
        .unwrap_or(DataType::Int64);
    let groups = rows
        .iter()
        .map(|(group, _, _)| group.clone())
        .collect::<Vec<_>>();
    let values = rows.iter().map(|(_, value, _)| *value).collect::<Vec<_>>();
    let weights = rows
        .iter()
        .map(|(_, _, weight)| *weight)
        .collect::<Vec<_>>();
    let group_array: ArrayRef = match group_type {
        DataType::Utf8 => ArcString::from_scalars(&groups),
        _ => ArcInt64::from_scalars(&groups),
    }?;
    let value_array: ArrayRef = Arc::new(Int64Array::from(values));
    let schema = Arc::new(Schema::new(vec![
        Field::new("group", group_array.data_type().clone(), true),
        Field::new("aggregate", DataType::Int64, false),
    ]));
    let data =
        RecordBatch::try_new(schema, vec![group_array, value_array]).map_err(OpError::arrow)?;
    Ok(ArrowZSet::new(data, weights))
}
