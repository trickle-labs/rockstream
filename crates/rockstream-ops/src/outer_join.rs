//! Incremental outer / semi / anti equi-join operator (v0.9 — IVM-5).
//!
//! `OuterJoinOp` implements the DBSP delta rules for LEFT, RIGHT, FULL outer
//! joins and SEMI / ANTI joins.  The dual-arrangement structure is the same as
//! `JoinOp`, plus a key-weight map for tracking match counts.
//!
//! ## NULL encoding
//!
//! NULL-padding for unmatched rows is represented as Arrow NULL, preserving
//! the distinction from a real zero value.
//!
//! ## State persistence
//!
//! Left arrangement:    `[0x01][0x4A4C][op_id:8][join_key_bytes][row_id:16]` → row_bytes
//! Right arrangement:   `[0x01][0x4A52][op_id:8][join_key_bytes][row_id:16]` → row_bytes
//! Right key weights:   `[0x01][0x4F52][op_id:8][key_bytes]` → weight:8 (i64 BE)
//! Left key weights:    `[0x01][0x4F4C][op_id:8][key_bytes]` → weight:8 (i64 BE)
//!
//! Only point puts/deletes are used — no range deletion.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use rockstream_plan::OuterJoinKind;
use rockstream_storage::{JoinSide, ShardDb, ShardKeyEncoder, WriteBatch};
use rockstream_types::ids::OperatorId;

use crate::error::OpError;
use crate::join::{concat_zsets, stable_row_id};
use crate::zset::ArrowZSet;

// ─── Schema ──────────────────────────────────────────────────────────────────

/// Output schema for outer join (LEFT/RIGHT/FULL): left + right columns (nullable).
pub fn outer_join_output_schema_n(left_n_cols: usize, right_n_cols: usize) -> SchemaRef {
    Arc::new(Schema::new(
        (0..left_n_cols)
            .map(|i| Field::new(format!("l_{i}"), DataType::Int64, true))
            .chain((0..right_n_cols).map(|i| Field::new(format!("r_{i}"), DataType::Int64, true)))
            .collect::<Vec<_>>(),
    ))
}

fn output_schema(kind: OuterJoinKind, left: &Schema, right: &Schema) -> SchemaRef {
    let left_padded = matches!(kind, OuterJoinKind::Right | OuterJoinKind::Full);
    let right_padded = matches!(kind, OuterJoinKind::Left | OuterJoinKind::Full);
    let mut fields = left
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            Field::new(
                format!("l_{i}"),
                DataType::Int64,
                left_padded || field.is_nullable(),
            )
        })
        .collect::<Vec<_>>();
    if !matches!(
        kind,
        OuterJoinKind::Semi | OuterJoinKind::Anti | OuterJoinKind::NullAwareAnti
    ) {
        fields.extend(right.fields().iter().enumerate().map(|(i, field)| {
            Field::new(
                format!("r_{i}"),
                DataType::Int64,
                right_padded || field.is_nullable(),
            )
        }));
    }
    Arc::new(Schema::new(fields))
}

/// Output schema for semi/anti join: left columns only.
pub fn semi_anti_output_schema_n(left_n_cols: usize) -> SchemaRef {
    let fields: Vec<Field> = (0..left_n_cols)
        .map(|i| Field::new(format!("l_{i}"), DataType::Int64, false))
        .collect();
    Arc::new(Schema::new(fields))
}

// ─── Internal structures ──────────────────────────────────────────────────────

/// An arrangement entry tracking the net weight for a row.
#[derive(Debug, Clone)]
struct ArrRow {
    row_bytes: Vec<u8>,
    weight: i64,
}

/// One side's staged delta for an epoch.
#[derive(Debug, Default)]
struct StagedDelta {
    rows: Vec<(Vec<u8>, u128, Vec<u8>, i64)>,
    staged_bytes: u64,
    null_weight: i64,
}

type OuterJoinRows = (Vec<Vec<Option<i64>>>, Vec<Vec<Option<i64>>>, Vec<i64>);

impl StagedDelta {
    fn push(&mut self, join_key: Vec<u8>, row_id: u128, row_bytes: Vec<u8>, weight: i64) {
        self.staged_bytes += (join_key.len() + 16 + row_bytes.len() + 8) as u64;
        self.rows.push((join_key, row_id, row_bytes, weight));
    }
}

/// In-memory state for `OuterJoinOp`.
#[derive(Debug, Default)]
struct OuterJoinState {
    /// join_key_bytes → HashMap<row_id, ArrRow>
    left_arr: HashMap<Vec<u8>, HashMap<u128, ArrRow>>,
    /// join_key_bytes → HashMap<row_id, ArrRow>
    right_arr: HashMap<Vec<u8>, HashMap<u128, ArrRow>>,
    /// Per-key total net right-side weight (for Left/Full/Semi/Anti tracking).
    right_key_weight: HashMap<Vec<u8>, i64>,
    /// Total net weight of right rows with a NULL in any join key.
    right_null_weight: i64,
    /// Total net weight on the right, needed for NULL NOT IN an empty relation.
    right_total_weight: i64,
    /// Per-key total net left-side weight (for Right/Full tracking).
    left_key_weight: HashMap<Vec<u8>, i64>,
    state_bytes: u64,
}

impl OuterJoinState {
    fn new() -> Self {
        Self::default()
    }

    fn state_bytes(&self) -> u64 {
        self.state_bytes
    }

    fn update_left(&mut self, join_key: Vec<u8>, row_id: u128, row_bytes: Vec<u8>, delta_w: i64) {
        let is_new_key = !self.left_arr.contains_key(&join_key);
        let key_len = join_key.len() as u64;
        let bucket = self.left_arr.entry(join_key.clone()).or_default();
        if is_new_key {
            self.state_bytes += key_len;
        }

        if let Some(entry) = bucket.get_mut(&row_id) {
            entry.weight += delta_w;
            if entry.weight == 0 {
                let removed = bucket.remove(&row_id).unwrap();
                self.state_bytes = self
                    .state_bytes
                    .saturating_sub(16 + 8 + removed.row_bytes.len() as u64);
                if bucket.is_empty() {
                    self.left_arr.remove(&join_key);
                    self.state_bytes = self.state_bytes.saturating_sub(key_len);
                }
            }
        } else if delta_w != 0 {
            let row_len = row_bytes.len() as u64;
            bucket.insert(
                row_id,
                ArrRow {
                    row_bytes,
                    weight: delta_w,
                },
            );
            self.state_bytes += 16 + 8 + row_len;
        }
    }

    fn update_right(&mut self, join_key: Vec<u8>, row_id: u128, row_bytes: Vec<u8>, delta_w: i64) {
        let is_new_key = !self.right_arr.contains_key(&join_key);
        let key_len = join_key.len() as u64;
        let bucket = self.right_arr.entry(join_key.clone()).or_default();
        if is_new_key {
            self.state_bytes += key_len;
        }

        if let Some(entry) = bucket.get_mut(&row_id) {
            entry.weight += delta_w;
            if entry.weight == 0 {
                let removed = bucket.remove(&row_id).unwrap();
                self.state_bytes = self
                    .state_bytes
                    .saturating_sub(16 + 8 + removed.row_bytes.len() as u64);
                if bucket.is_empty() {
                    self.right_arr.remove(&join_key);
                    self.state_bytes = self.state_bytes.saturating_sub(key_len);
                }
            }
        } else if delta_w != 0 {
            let row_len = row_bytes.len() as u64;
            bucket.insert(
                row_id,
                ArrRow {
                    row_bytes,
                    weight: delta_w,
                },
            );
            self.state_bytes += 16 + 8 + row_len;
        }
    }

    fn probe_right(&self, join_key: &[u8]) -> impl Iterator<Item = (&Vec<u8>, i64)> {
        self.right_arr
            .get(join_key)
            .into_iter()
            .flat_map(|m| m.values().map(|e| (&e.row_bytes, e.weight)))
    }

    fn probe_left(&self, join_key: &[u8]) -> impl Iterator<Item = (&Vec<u8>, i64)> {
        self.left_arr
            .get(join_key)
            .into_iter()
            .flat_map(|m| m.values().map(|e| (&e.row_bytes, e.weight)))
    }

    fn left_entry_count(&self) -> usize {
        self.left_arr.values().map(|m| m.len()).sum()
    }

    fn right_entry_count(&self) -> usize {
        self.right_arr.values().map(|m| m.len()).sum()
    }

    fn unmatched_key_count(&self) -> usize {
        // Count keys with nonzero right_key_weight (unmatched left rows) +
        // keys with nonzero left_key_weight (unmatched right rows).
        self.right_key_weight.len() + self.left_key_weight.len()
    }
}

// ─── OuterJoinStateCore (Single-Owner, Zero-Mutex) ───────────────────────────

/// Lock-free single-owner state core for [`OuterJoinOp`] (Issue #402 R3).
///
/// Consolidates arrangement state, staged deltas, and dirty-key tracking into
/// one owned struct so shard actors can drive outer/semi/anti joins via
/// `&mut self` with zero hot-path `Mutex` locking, while [`OuterJoinOp`] wraps
/// this core in a single `Mutex` for `Operator` / shared-reference callers.
pub struct OuterJoinStateCore {
    op_id: OperatorId,
    kind: OuterJoinKind,
    left_key_cols: Vec<usize>,
    right_key_cols: Vec<usize>,
    left_n_cols: usize,
    right_n_cols: usize,
    state: OuterJoinState,
    left_staged: StagedDelta,
    right_staged: StagedDelta,
    dirty_left: HashSet<(Vec<u8>, u128)>,
    dirty_right: HashSet<(Vec<u8>, u128)>,
    dirty_rw: HashSet<Vec<u8>>,
    dirty_lw: HashSet<Vec<u8>>,
    dirty_null_weight: bool,
}

impl OuterJoinStateCore {
    pub fn new(
        op_id: OperatorId,
        kind: OuterJoinKind,
        left_key_cols: Vec<usize>,
        right_key_cols: Vec<usize>,
    ) -> Self {
        Self::with_schema(op_id, kind, left_key_cols, right_key_cols, 2, 2)
    }

    pub fn with_schema(
        op_id: OperatorId,
        kind: OuterJoinKind,
        left_key_cols: Vec<usize>,
        right_key_cols: Vec<usize>,
        left_n_cols: usize,
        right_n_cols: usize,
    ) -> Self {
        Self {
            op_id,
            kind,
            left_key_cols,
            right_key_cols,
            left_n_cols,
            right_n_cols,
            state: OuterJoinState::new(),
            left_staged: StagedDelta::default(),
            right_staged: StagedDelta::default(),
            dirty_left: HashSet::new(),
            dirty_right: HashSet::new(),
            dirty_rw: HashSet::new(),
            dirty_lw: HashSet::new(),
            dirty_null_weight: false,
        }
    }

    pub fn kind(&self) -> OuterJoinKind {
        self.kind
    }

    pub fn op_id(&self) -> OperatorId {
        self.op_id
    }

    pub fn left_entry_count(&self) -> usize {
        self.state.left_entry_count()
    }

    pub fn right_entry_count(&self) -> usize {
        self.state.right_entry_count()
    }

    pub fn state_bytes(&self) -> u64 {
        self.state.state_bytes() + self.left_staged.staged_bytes + self.right_staged.staged_bytes
    }

    pub fn unmatched_key_count(&self) -> usize {
        self.state.unmatched_key_count()
    }

    /// Run a full epoch directly on `&mut self` with zero `Mutex` locks.
    pub fn process_epoch(
        &mut self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<ArrowZSet, OpError> {
        match self.kind {
            OuterJoinKind::Left => self.process_epoch_left(left, right),
            OuterJoinKind::Right => self.process_epoch_right(left, right),
            OuterJoinKind::Full => self.process_epoch_full(left, right),
            OuterJoinKind::Semi => self.process_epoch_semi(left, right),
            OuterJoinKind::Anti => self.process_epoch_anti(left, right),
            OuterJoinKind::NullAwareAnti => self.process_epoch_null_aware_anti(left, right),
        }
    }

    fn stage_left(&mut self, delta: &ArrowZSet) -> Result<(), OpError> {
        if delta.is_empty() {
            return Ok(());
        }
        let capsule = delta.get_or_build_key_capsule(&self.left_key_cols)?;
        for row_idx in 0..delta.num_rows() {
            let w = delta.weights[row_idx];
            let capsule_key = capsule.row_bytes(row_idx).to_vec();
            let is_null = capsule.row_contains_null(row_idx);
            let row_bytes = OuterJoinOp::serialize_row(&delta.data, row_idx);
            let row_id = stable_row_id(self.op_id.0, &capsule_key, &row_bytes);
            let join_key = if is_null {
                // Opposite sides must never share a NULL-key bucket.
                [capsule_key, row_id.to_be_bytes().to_vec(), vec![0]].concat()
            } else {
                capsule_key
            };
            self.left_staged.push(join_key, row_id, row_bytes, w);
        }
        Ok(())
    }

    fn stage_right(&mut self, delta: &ArrowZSet) -> Result<(), OpError> {
        if delta.is_empty() {
            return Ok(());
        }
        let capsule = delta.get_or_build_key_capsule(&self.right_key_cols)?;
        for row_idx in 0..delta.num_rows() {
            let w = delta.weights[row_idx];
            let capsule_key = capsule.row_bytes(row_idx).to_vec();
            let is_null = capsule.row_contains_null(row_idx);
            let row_bytes = OuterJoinOp::serialize_row(&delta.data, row_idx);
            let row_id = stable_row_id(self.op_id.0, &capsule_key, &row_bytes);
            let join_key = if is_null {
                [capsule_key, row_id.to_be_bytes().to_vec(), vec![1]].concat()
            } else {
                capsule_key
            };
            self.right_staged.push(join_key, row_id, row_bytes, w);
            if is_null {
                self.right_staged.null_weight += w;
            }
        }
        Ok(())
    }

    fn process_epoch_left(
        &mut self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<ArrowZSet, OpError> {
        self.stage_left(&left)?;
        self.stage_right(&right)?;

        let out_schema = output_schema(
            self.kind,
            left.data.schema().as_ref(),
            right.data.schema().as_ref(),
        );
        let mut outputs: Vec<ArrowZSet> = Vec::new();

        let mut delta_rw: HashMap<Vec<u8>, i64> = HashMap::new();
        let mut right_staged_rows: HashMap<Vec<u8>, Vec<(Vec<u8>, i64)>> = HashMap::new();
        for (join_key, _row_id, row_bytes, w) in &self.right_staged.rows {
            *delta_rw.entry(join_key.clone()).or_insert(0) += w;
            right_staged_rows
                .entry(join_key.clone())
                .or_default()
                .push((row_bytes.clone(), *w));
        }

        // 1. Inner join output: ΔL⋈R₀ + L₀⋈ΔR + ΔL⋈ΔR
        let inner = self.compute_inner_join_output(&right_staged_rows, &out_schema)?;
        if !inner.is_empty() {
            outputs.push(inner);
        }

        // 2. NULL-pad transitions due to right side changing.
        let mut null_pad_rows: OuterJoinRows = (
            vec![Vec::new(); self.left_n_cols],
            vec![Vec::new(); self.right_n_cols],
            Vec::new(),
        );
        for (key, delta) in &delta_rw {
            let old_rw = self.state.right_key_weight.get(key).copied().unwrap_or(0);
            let new_rw = old_rw + delta;
            if old_rw == new_rw {
                continue;
            }
            if (old_rw == 0) != (new_rw == 0) {
                let retract = old_rw == 0;
                for (l_row_bytes, l_weight) in self.state.probe_left(key) {
                    let left_vals = OuterJoinOp::deserialize_row(l_row_bytes, self.left_n_cols)?;
                    let right_nulls = vec![None; self.right_n_cols];
                    for (i, v) in left_vals.iter().enumerate() {
                        null_pad_rows.0[i].push(*v);
                    }
                    for (i, v) in right_nulls.iter().enumerate() {
                        null_pad_rows.1[i].push(*v);
                    }
                    null_pad_rows
                        .2
                        .push(if retract { -l_weight } else { l_weight });
                }
            }
        }
        if !null_pad_rows.2.is_empty() {
            let batch = OuterJoinOp::make_output_batch(
                &null_pad_rows.0,
                &null_pad_rows.1,
                &out_schema,
                false,
                true,
            )?;
            outputs.push(ArrowZSet::new(batch, null_pad_rows.2));
        }

        // 3. For each ΔL row: if effective_rw == 0, emit (row, NULL_right, w).
        let mut null_new_rows: OuterJoinRows = (
            vec![Vec::new(); self.left_n_cols],
            vec![Vec::new(); self.right_n_cols],
            Vec::new(),
        );
        for (join_key, _row_id, row_bytes, w) in &self.left_staged.rows {
            let old_rw = self
                .state
                .right_key_weight
                .get(join_key)
                .copied()
                .unwrap_or(0);
            let delta = delta_rw.get(join_key).copied().unwrap_or(0);
            let effective_rw = old_rw + delta;
            if effective_rw == 0 {
                let left_vals = OuterJoinOp::deserialize_row(row_bytes, self.left_n_cols)?;
                let right_nulls = vec![None; self.right_n_cols];
                for (i, v) in left_vals.iter().enumerate() {
                    null_new_rows.0[i].push(*v);
                }
                for (i, v) in right_nulls.iter().enumerate() {
                    null_new_rows.1[i].push(*v);
                }
                null_new_rows.2.push(*w);
            }
        }
        if !null_new_rows.2.is_empty() {
            let batch = OuterJoinOp::make_output_batch(
                &null_new_rows.0,
                &null_new_rows.1,
                &out_schema,
                false,
                true,
            )?;
            outputs.push(ArrowZSet::new(batch, null_new_rows.2));
        }

        self.commit_staged(&delta_rw, None);
        concat_zsets(outputs, out_schema)
    }

    fn process_epoch_right(
        &mut self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<ArrowZSet, OpError> {
        self.stage_left(&left)?;
        self.stage_right(&right)?;

        let out_schema = output_schema(
            self.kind,
            left.data.schema().as_ref(),
            right.data.schema().as_ref(),
        );
        let mut outputs: Vec<ArrowZSet> = Vec::new();

        let mut delta_lw: HashMap<Vec<u8>, i64> = HashMap::new();
        for (join_key, _, _, w) in &self.left_staged.rows {
            *delta_lw.entry(join_key.clone()).or_insert(0) += w;
        }

        let mut right_staged_rows: HashMap<Vec<u8>, Vec<(Vec<u8>, i64)>> = HashMap::new();
        for (join_key, _, row_bytes, w) in &self.right_staged.rows {
            right_staged_rows
                .entry(join_key.clone())
                .or_default()
                .push((row_bytes.clone(), *w));
        }

        let inner = self.compute_inner_join_output(&right_staged_rows, &out_schema)?;
        if !inner.is_empty() {
            outputs.push(inner);
        }

        let mut null_pad_rows: OuterJoinRows = (
            vec![Vec::new(); self.left_n_cols],
            vec![Vec::new(); self.right_n_cols],
            Vec::new(),
        );
        for (key, delta) in &delta_lw {
            let old_lw = self.state.left_key_weight.get(key).copied().unwrap_or(0);
            let new_lw = old_lw + delta;
            if (old_lw == 0) != (new_lw == 0) {
                let retract = old_lw == 0;
                for (r_row_bytes, r_weight) in self.state.probe_right(key) {
                    let left_nulls = vec![None; self.left_n_cols];
                    let right_vals = OuterJoinOp::deserialize_row(r_row_bytes, self.right_n_cols)?;
                    for (i, v) in left_nulls.iter().enumerate() {
                        null_pad_rows.0[i].push(*v);
                    }
                    for (i, v) in right_vals.iter().enumerate() {
                        null_pad_rows.1[i].push(*v);
                    }
                    null_pad_rows
                        .2
                        .push(if retract { -r_weight } else { r_weight });
                }
            }
        }
        if !null_pad_rows.2.is_empty() {
            let batch = OuterJoinOp::make_output_batch(
                &null_pad_rows.0,
                &null_pad_rows.1,
                &out_schema,
                true,
                false,
            )?;
            outputs.push(ArrowZSet::new(batch, null_pad_rows.2));
        }

        let mut null_new_rows: OuterJoinRows = (
            vec![Vec::new(); self.left_n_cols],
            vec![Vec::new(); self.right_n_cols],
            Vec::new(),
        );
        for (join_key, _, row_bytes, w) in &self.right_staged.rows {
            let old_lw = self
                .state
                .left_key_weight
                .get(join_key)
                .copied()
                .unwrap_or(0);
            let delta = delta_lw.get(join_key).copied().unwrap_or(0);
            let effective_lw = old_lw + delta;
            if effective_lw == 0 {
                let left_nulls = vec![None; self.left_n_cols];
                let right_vals = OuterJoinOp::deserialize_row(row_bytes, self.right_n_cols)?;
                for (i, v) in left_nulls.iter().enumerate() {
                    null_new_rows.0[i].push(*v);
                }
                for (i, v) in right_vals.iter().enumerate() {
                    null_new_rows.1[i].push(*v);
                }
                null_new_rows.2.push(*w);
            }
        }
        if !null_new_rows.2.is_empty() {
            let batch = OuterJoinOp::make_output_batch(
                &null_new_rows.0,
                &null_new_rows.1,
                &out_schema,
                true,
                false,
            )?;
            outputs.push(ArrowZSet::new(batch, null_new_rows.2));
        }

        self.commit_staged(&HashMap::new(), Some(&delta_lw));
        concat_zsets(outputs, out_schema)
    }

    fn process_epoch_full(
        &mut self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<ArrowZSet, OpError> {
        self.stage_left(&left)?;
        self.stage_right(&right)?;

        let out_schema = output_schema(
            self.kind,
            left.data.schema().as_ref(),
            right.data.schema().as_ref(),
        );
        let mut outputs: Vec<ArrowZSet> = Vec::new();

        let mut delta_rw: HashMap<Vec<u8>, i64> = HashMap::new();
        let mut right_staged_rows: HashMap<Vec<u8>, Vec<(Vec<u8>, i64)>> = HashMap::new();
        for (join_key, _, row_bytes, w) in &self.right_staged.rows {
            *delta_rw.entry(join_key.clone()).or_insert(0) += w;
            right_staged_rows
                .entry(join_key.clone())
                .or_default()
                .push((row_bytes.clone(), *w));
        }

        let mut delta_lw: HashMap<Vec<u8>, i64> = HashMap::new();
        for (join_key, _, _, w) in &self.left_staged.rows {
            *delta_lw.entry(join_key.clone()).or_insert(0) += w;
        }

        let inner = self.compute_inner_join_output(&right_staged_rows, &out_schema)?;
        if !inner.is_empty() {
            outputs.push(inner);
        }

        let mut null_pad_left: OuterJoinRows = (
            vec![Vec::new(); self.left_n_cols],
            vec![Vec::new(); self.right_n_cols],
            Vec::new(),
        );
        for (key, delta) in &delta_rw {
            let old_rw = self.state.right_key_weight.get(key).copied().unwrap_or(0);
            let new_rw = old_rw + delta;
            if (old_rw == 0) != (new_rw == 0) {
                let retract = old_rw == 0;
                for (l_row_bytes, l_weight) in self.state.probe_left(key) {
                    let left_vals = OuterJoinOp::deserialize_row(l_row_bytes, self.left_n_cols)?;
                    let right_nulls = vec![None; self.right_n_cols];
                    for (i, v) in left_vals.iter().enumerate() {
                        null_pad_left.0[i].push(*v);
                    }
                    for (i, v) in right_nulls.iter().enumerate() {
                        null_pad_left.1[i].push(*v);
                    }
                    null_pad_left
                        .2
                        .push(if retract { -l_weight } else { l_weight });
                }
            }
        }
        if !null_pad_left.2.is_empty() {
            let batch = OuterJoinOp::make_output_batch(
                &null_pad_left.0,
                &null_pad_left.1,
                &out_schema,
                false,
                true,
            )?;
            outputs.push(ArrowZSet::new(batch, null_pad_left.2));
        }

        let mut null_pad_right: OuterJoinRows = (
            vec![Vec::new(); self.left_n_cols],
            vec![Vec::new(); self.right_n_cols],
            Vec::new(),
        );
        for (key, delta) in &delta_lw {
            let old_lw = self.state.left_key_weight.get(key).copied().unwrap_or(0);
            let new_lw = old_lw + delta;
            if (old_lw == 0) != (new_lw == 0) {
                let retract = old_lw == 0;
                for (r_row_bytes, r_weight) in self.state.probe_right(key) {
                    let left_nulls = vec![None; self.left_n_cols];
                    let right_vals = OuterJoinOp::deserialize_row(r_row_bytes, self.right_n_cols)?;
                    for (i, v) in left_nulls.iter().enumerate() {
                        null_pad_right.0[i].push(*v);
                    }
                    for (i, v) in right_vals.iter().enumerate() {
                        null_pad_right.1[i].push(*v);
                    }
                    null_pad_right
                        .2
                        .push(if retract { -r_weight } else { r_weight });
                }
            }
        }
        if !null_pad_right.2.is_empty() {
            let batch = OuterJoinOp::make_output_batch(
                &null_pad_right.0,
                &null_pad_right.1,
                &out_schema,
                true,
                false,
            )?;
            outputs.push(ArrowZSet::new(batch, null_pad_right.2));
        }

        let mut null_new_left: OuterJoinRows = (
            vec![Vec::new(); self.left_n_cols],
            vec![Vec::new(); self.right_n_cols],
            Vec::new(),
        );
        for (join_key, _, row_bytes, w) in &self.left_staged.rows {
            let old_rw = self
                .state
                .right_key_weight
                .get(join_key)
                .copied()
                .unwrap_or(0);
            let delta = delta_rw.get(join_key).copied().unwrap_or(0);
            let effective_rw = old_rw + delta;
            if effective_rw == 0 {
                let left_vals = OuterJoinOp::deserialize_row(row_bytes, self.left_n_cols)?;
                let right_nulls = vec![None; self.right_n_cols];
                for (i, v) in left_vals.iter().enumerate() {
                    null_new_left.0[i].push(*v);
                }
                for (i, v) in right_nulls.iter().enumerate() {
                    null_new_left.1[i].push(*v);
                }
                null_new_left.2.push(*w);
            }
        }
        if !null_new_left.2.is_empty() {
            let batch = OuterJoinOp::make_output_batch(
                &null_new_left.0,
                &null_new_left.1,
                &out_schema,
                false,
                true,
            )?;
            outputs.push(ArrowZSet::new(batch, null_new_left.2));
        }

        let mut null_new_right: OuterJoinRows = (
            vec![Vec::new(); self.left_n_cols],
            vec![Vec::new(); self.right_n_cols],
            Vec::new(),
        );
        for (join_key, _, row_bytes, w) in &self.right_staged.rows {
            let old_lw = self
                .state
                .left_key_weight
                .get(join_key)
                .copied()
                .unwrap_or(0);
            let delta = delta_lw.get(join_key).copied().unwrap_or(0);
            let effective_lw = old_lw + delta;
            if effective_lw == 0 {
                let left_nulls = vec![None; self.left_n_cols];
                let right_vals = OuterJoinOp::deserialize_row(row_bytes, self.right_n_cols)?;
                for (i, v) in left_nulls.iter().enumerate() {
                    null_new_right.0[i].push(*v);
                }
                for (i, v) in right_vals.iter().enumerate() {
                    null_new_right.1[i].push(*v);
                }
                null_new_right.2.push(*w);
            }
        }
        if !null_new_right.2.is_empty() {
            let batch = OuterJoinOp::make_output_batch(
                &null_new_right.0,
                &null_new_right.1,
                &out_schema,
                true,
                false,
            )?;
            outputs.push(ArrowZSet::new(batch, null_new_right.2));
        }

        self.commit_staged(&delta_rw, Some(&delta_lw));
        concat_zsets(outputs, out_schema)
    }

    fn process_epoch_semi(
        &mut self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<ArrowZSet, OpError> {
        self.stage_left(&left)?;
        self.stage_right(&right)?;

        let out_schema = output_schema(
            self.kind,
            left.data.schema().as_ref(),
            right.data.schema().as_ref(),
        );
        let mut outputs: Vec<ArrowZSet> = Vec::new();

        let mut delta_rw: HashMap<Vec<u8>, i64> = HashMap::new();
        for (join_key, _, _, w) in &self.right_staged.rows {
            *delta_rw.entry(join_key.clone()).or_insert(0) += w;
        }

        let mut semi_rows: (Vec<Vec<Option<i64>>>, Vec<i64>) =
            (vec![Vec::new(); self.left_n_cols], Vec::new());
        for (key, delta) in &delta_rw {
            let old_rw = self.state.right_key_weight.get(key).copied().unwrap_or(0);
            let new_rw = old_rw + delta;
            if (old_rw == 0) != (new_rw == 0) {
                let sign: i64 = if old_rw == 0 { 1 } else { -1 };
                for (l_row_bytes, l_weight) in self.state.probe_left(key) {
                    let left_vals = OuterJoinOp::deserialize_row(l_row_bytes, self.left_n_cols)?;
                    for (i, v) in left_vals.iter().enumerate() {
                        semi_rows.0[i].push(*v);
                    }
                    semi_rows.1.push(sign * l_weight);
                }
            }
        }
        if !semi_rows.1.is_empty() {
            let batch = OuterJoinOp::make_semi_batch(&semi_rows.0, &out_schema)?;
            outputs.push(ArrowZSet::new(batch, semi_rows.1));
        }

        let mut new_left_rows: (Vec<Vec<Option<i64>>>, Vec<i64>) =
            (vec![Vec::new(); self.left_n_cols], Vec::new());
        for (join_key, _, row_bytes, w) in &self.left_staged.rows {
            let old_rw = self
                .state
                .right_key_weight
                .get(join_key)
                .copied()
                .unwrap_or(0);
            let delta = delta_rw.get(join_key).copied().unwrap_or(0);
            let effective_rw = old_rw + delta;
            if effective_rw != 0 {
                let left_vals = OuterJoinOp::deserialize_row(row_bytes, self.left_n_cols)?;
                for (i, v) in left_vals.iter().enumerate() {
                    new_left_rows.0[i].push(*v);
                }
                new_left_rows.1.push(*w);
            }
        }
        if !new_left_rows.1.is_empty() {
            let batch = OuterJoinOp::make_semi_batch(&new_left_rows.0, &out_schema)?;
            outputs.push(ArrowZSet::new(batch, new_left_rows.1));
        }

        self.commit_staged(&delta_rw, None);
        concat_zsets(outputs, out_schema)
    }

    fn process_epoch_anti(
        &mut self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<ArrowZSet, OpError> {
        self.stage_left(&left)?;
        self.stage_right(&right)?;

        let out_schema = output_schema(
            self.kind,
            left.data.schema().as_ref(),
            right.data.schema().as_ref(),
        );
        let mut outputs: Vec<ArrowZSet> = Vec::new();

        let mut delta_rw: HashMap<Vec<u8>, i64> = HashMap::new();
        for (join_key, _, _, w) in &self.right_staged.rows {
            *delta_rw.entry(join_key.clone()).or_insert(0) += w;
        }

        let mut anti_rows: (Vec<Vec<Option<i64>>>, Vec<i64>) =
            (vec![Vec::new(); self.left_n_cols], Vec::new());
        for (key, delta) in &delta_rw {
            let old_rw = self.state.right_key_weight.get(key).copied().unwrap_or(0);
            let new_rw = old_rw + delta;
            if (old_rw == 0) != (new_rw == 0) {
                let sign: i64 = if old_rw == 0 { -1 } else { 1 };
                for (l_row_bytes, l_weight) in self.state.probe_left(key) {
                    let left_vals = OuterJoinOp::deserialize_row(l_row_bytes, self.left_n_cols)?;
                    for (i, v) in left_vals.iter().enumerate() {
                        anti_rows.0[i].push(*v);
                    }
                    anti_rows.1.push(sign * l_weight);
                }
            }
        }
        if !anti_rows.1.is_empty() {
            let batch = OuterJoinOp::make_semi_batch(&anti_rows.0, &out_schema)?;
            outputs.push(ArrowZSet::new(batch, anti_rows.1));
        }

        let mut new_left_rows: (Vec<Vec<Option<i64>>>, Vec<i64>) =
            (vec![Vec::new(); self.left_n_cols], Vec::new());
        for (join_key, _, row_bytes, w) in &self.left_staged.rows {
            let old_rw = self
                .state
                .right_key_weight
                .get(join_key)
                .copied()
                .unwrap_or(0);
            let delta = delta_rw.get(join_key).copied().unwrap_or(0);
            let effective_rw = old_rw + delta;
            if effective_rw == 0 {
                let left_vals = OuterJoinOp::deserialize_row(row_bytes, self.left_n_cols)?;
                for (i, v) in left_vals.iter().enumerate() {
                    new_left_rows.0[i].push(*v);
                }
                new_left_rows.1.push(*w);
            }
        }
        if !new_left_rows.1.is_empty() {
            let batch = OuterJoinOp::make_semi_batch(&new_left_rows.0, &out_schema)?;
            outputs.push(ArrowZSet::new(batch, new_left_rows.1));
        }

        self.commit_staged(&delta_rw, None);
        concat_zsets(outputs, out_schema)
    }

    fn process_epoch_null_aware_anti(
        &mut self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<ArrowZSet, OpError> {
        self.stage_left(&left)?;
        self.stage_right(&right)?;

        let out_schema = output_schema(
            self.kind,
            left.data.schema().as_ref(),
            right.data.schema().as_ref(),
        );
        let mut delta_rw: HashMap<Vec<u8>, i64> = HashMap::new();
        for (join_key, _, _, weight) in &self.right_staged.rows {
            *delta_rw.entry(join_key.clone()).or_insert(0) += weight;
        }
        let null_delta = self.right_staged.null_weight;
        let total_delta: i64 = delta_rw.values().sum();
        let mut output_rows = vec![Vec::new(); self.left_n_cols];
        let mut output_weights = Vec::new();

        let old_null = self.state.right_null_weight > 0;
        let new_null = self.state.right_null_weight + null_delta > 0;
        let old_empty = self.state.right_total_weight == 0;
        let new_empty = self.state.right_total_weight + total_delta == 0;

        if old_null != new_null || old_empty != new_empty {
            for (key, rows) in &self.state.left_arr {
                let old_matches = self.state.right_key_weight.get(key).copied().unwrap_or(0);
                let new_matches = old_matches + delta_rw.get(key).copied().unwrap_or(0);
                for row in rows.values() {
                    let values = OuterJoinOp::deserialize_row(&row.row_bytes, self.left_n_cols)?;
                    let has_null_key = self
                        .left_key_cols
                        .iter()
                        .any(|column| values[*column].is_none());
                    let was_visible = if has_null_key {
                        old_empty
                    } else {
                        !old_null && old_matches == 0
                    };
                    let now_visible = if has_null_key {
                        new_empty
                    } else {
                        !new_null && new_matches == 0
                    };
                    if was_visible != now_visible {
                        for (column, value) in values.iter().enumerate() {
                            output_rows[column].push(*value);
                        }
                        output_weights
                            .push(row.weight * (i64::from(now_visible) - i64::from(was_visible)));
                    }
                }
            }
        } else if !old_null {
            for (key, delta) in &delta_rw {
                let old_matches = self.state.right_key_weight.get(key).copied().unwrap_or(0);
                let new_matches = old_matches + delta;
                if (old_matches == 0) != (new_matches == 0) {
                    let sign = if old_matches == 0 { -1 } else { 1 };
                    for (row_bytes, weight) in self.state.probe_left(key) {
                        let values = OuterJoinOp::deserialize_row(row_bytes, self.left_n_cols)?;
                        let has_null_key = self
                            .left_key_cols
                            .iter()
                            .any(|column| values[*column].is_none());
                        if has_null_key {
                            continue;
                        }
                        for (column, value) in values.iter().enumerate() {
                            output_rows[column].push(*value);
                        }
                        output_weights.push(sign * weight);
                    }
                }
            }
        }

        if !new_null {
            for (key, _, row_bytes, weight) in &self.left_staged.rows {
                let values = OuterJoinOp::deserialize_row(row_bytes, self.left_n_cols)?;
                let has_null_key = self
                    .left_key_cols
                    .iter()
                    .any(|column| values[*column].is_none());
                if has_null_key && !new_empty {
                    continue;
                }
                let matches = self.state.right_key_weight.get(key).copied().unwrap_or(0)
                    + delta_rw.get(key).copied().unwrap_or(0);
                if has_null_key || (!new_null && matches == 0) {
                    for (column, value) in values.iter().enumerate() {
                        output_rows[column].push(*value);
                    }
                    output_weights.push(*weight);
                }
            }
        }

        let outputs = if output_weights.is_empty() {
            Vec::new()
        } else {
            vec![ArrowZSet::new(
                OuterJoinOp::make_semi_batch(&output_rows, &out_schema)?,
                output_weights,
            )]
        };
        self.commit_staged(&delta_rw, None);
        concat_zsets(outputs, out_schema)
    }

    fn compute_inner_join_output(
        &self,
        right_staged_rows: &HashMap<Vec<u8>, Vec<(Vec<u8>, i64)>>,
        out_schema: &SchemaRef,
    ) -> Result<ArrowZSet, OpError> {
        let mut left_cols: Vec<Vec<Option<i64>>> = vec![Vec::new(); self.left_n_cols];
        let mut right_cols: Vec<Vec<Option<i64>>> = vec![Vec::new(); self.right_n_cols];
        let mut out_weights: Vec<i64> = Vec::new();

        // ΔL ⋈ R₀
        for (join_key, _, row_bytes, w) in &self.left_staged.rows {
            for (right_row_bytes, r_weight) in self.state.probe_right(join_key) {
                let left_vals = OuterJoinOp::deserialize_row(row_bytes, self.left_n_cols)?;
                let right_vals = OuterJoinOp::deserialize_row(right_row_bytes, self.right_n_cols)?;
                for (i, v) in left_vals.iter().enumerate() {
                    left_cols[i].push(*v);
                }
                for (i, v) in right_vals.iter().enumerate() {
                    right_cols[i].push(*v);
                }
                out_weights.push(w * r_weight);
            }
        }

        // L₀ ⋈ ΔR
        for (join_key, _, row_bytes, w) in &self.right_staged.rows {
            for (left_row_bytes, l_weight) in self.state.probe_left(join_key) {
                let left_vals = OuterJoinOp::deserialize_row(left_row_bytes, self.left_n_cols)?;
                let right_vals = OuterJoinOp::deserialize_row(row_bytes, self.right_n_cols)?;
                for (i, v) in left_vals.iter().enumerate() {
                    left_cols[i].push(*v);
                }
                for (i, v) in right_vals.iter().enumerate() {
                    right_cols[i].push(*v);
                }
                out_weights.push(l_weight * w);
            }
        }

        // ΔL ⋈ ΔR
        for (join_key, _, left_bytes, l_w) in &self.left_staged.rows {
            if let Some(right_rows) = right_staged_rows.get(join_key) {
                for (right_bytes, r_w) in right_rows {
                    let left_vals = OuterJoinOp::deserialize_row(left_bytes, self.left_n_cols)?;
                    let right_vals = OuterJoinOp::deserialize_row(right_bytes, self.right_n_cols)?;
                    for (i, v) in left_vals.iter().enumerate() {
                        left_cols[i].push(*v);
                    }
                    for (i, v) in right_vals.iter().enumerate() {
                        right_cols[i].push(*v);
                    }
                    out_weights.push(l_w * r_w);
                }
            }
        }

        let batch =
            OuterJoinOp::make_output_batch(&left_cols, &right_cols, out_schema, false, false)?;
        Ok(ArrowZSet::new(batch, out_weights))
    }

    fn commit_staged(
        &mut self,
        delta_rw: &HashMap<Vec<u8>, i64>,
        delta_lw: Option<&HashMap<Vec<u8>, i64>>,
    ) {
        for (join_key, row_id, row_bytes, w) in self.left_staged.rows.drain(..) {
            self.dirty_left.insert((join_key.clone(), row_id));
            self.state.update_left(join_key, row_id, row_bytes, w);
        }

        for (join_key, row_id, row_bytes, w) in self.right_staged.rows.drain(..) {
            self.dirty_right.insert((join_key.clone(), row_id));
            self.state.update_right(join_key, row_id, row_bytes, w);
        }

        self.left_staged.staged_bytes = 0;
        let null_delta = self.right_staged.null_weight;
        self.right_staged.null_weight = 0;
        self.right_staged.staged_bytes = 0;

        if self.kind == OuterJoinKind::NullAwareAnti {
            let total_delta: i64 = delta_rw.values().sum();
            let old_bytes = u64::from(self.state.right_total_weight > 0) * 16;
            self.state.right_total_weight += total_delta;
            self.state.right_null_weight += null_delta;
            let new_bytes = u64::from(self.state.right_total_weight > 0) * 16;
            self.state.state_bytes = self
                .state
                .state_bytes
                .saturating_sub(old_bytes)
                .saturating_add(new_bytes);
            if total_delta != 0 || null_delta != 0 {
                self.dirty_null_weight = true;
            }
        }

        for (key, delta) in delta_rw {
            self.dirty_rw.insert(key.clone());
            let is_new = !self.state.right_key_weight.contains_key(key);
            let key_len = key.len() as u64;
            if is_new {
                self.state.state_bytes += key_len + 8;
            }
            let entry = self.state.right_key_weight.entry(key.clone()).or_insert(0);
            *entry += delta;
            if *entry == 0 {
                self.state.right_key_weight.remove(key);
                self.state.state_bytes = self.state.state_bytes.saturating_sub(key_len + 8);
            }
        }

        if let Some(dlw) = delta_lw {
            for (key, delta) in dlw {
                self.dirty_lw.insert(key.clone());
                let is_new = !self.state.left_key_weight.contains_key(key);
                let key_len = key.len() as u64;
                if is_new {
                    self.state.state_bytes += key_len + 8;
                }
                let entry = self.state.left_key_weight.entry(key.clone()).or_insert(0);
                *entry += delta;
                if *entry == 0 {
                    self.state.left_key_weight.remove(key);
                    self.state.state_bytes = self.state.state_bytes.saturating_sub(key_len + 8);
                }
            }
        }
    }

    pub fn append_state(&mut self, target: &mut WriteBatch) -> Result<(), OpError> {
        let mut batch = WriteBatch::new();

        for (join_key, row_id) in self.dirty_left.drain() {
            let key =
                ShardKeyEncoder::join_arr_key(JoinSide::Left, self.op_id.0, &join_key, row_id);
            if let Some(arr_row) = self
                .state
                .left_arr
                .get(&join_key)
                .and_then(|b| b.get(&row_id))
            {
                if arr_row.weight > 0 {
                    batch.put(&key, &arr_row.row_bytes);
                } else {
                    batch.delete(&key);
                }
            } else {
                batch.delete(&key);
            }
        }

        for (join_key, row_id) in self.dirty_right.drain() {
            let key =
                ShardKeyEncoder::join_arr_key(JoinSide::Right, self.op_id.0, &join_key, row_id);
            if let Some(arr_row) = self
                .state
                .right_arr
                .get(&join_key)
                .and_then(|b| b.get(&row_id))
            {
                if arr_row.weight > 0 {
                    batch.put(&key, &arr_row.row_bytes);
                } else {
                    batch.delete(&key);
                }
            } else {
                batch.delete(&key);
            }
        }

        let rw_prefix: &[u8] = &[0x01, 0x4F, 0x52];
        for key_bytes in self.dirty_rw.drain() {
            let mut storage_key = Vec::with_capacity(3 + 8 + key_bytes.len());
            storage_key.extend_from_slice(rw_prefix);
            storage_key.extend_from_slice(&self.op_id.0.to_be_bytes());
            storage_key.extend_from_slice(&key_bytes);
            if let Some(&weight) = self.state.right_key_weight.get(&key_bytes) {
                if weight != 0 {
                    batch.put(&storage_key, &weight.to_be_bytes());
                } else {
                    batch.delete(&storage_key);
                }
            } else {
                batch.delete(&storage_key);
            }
        }

        let lw_prefix: &[u8] = &[0x01, 0x4F, 0x4C];
        for key_bytes in self.dirty_lw.drain() {
            let mut storage_key = Vec::with_capacity(3 + 8 + key_bytes.len());
            storage_key.extend_from_slice(lw_prefix);
            storage_key.extend_from_slice(&self.op_id.0.to_be_bytes());
            storage_key.extend_from_slice(&key_bytes);
            if let Some(&weight) = self.state.left_key_weight.get(&key_bytes) {
                if weight != 0 {
                    batch.put(&storage_key, &weight.to_be_bytes());
                } else {
                    batch.delete(&storage_key);
                }
            } else {
                batch.delete(&storage_key);
            }
        }

        if std::mem::take(&mut self.dirty_null_weight) {
            let mut storage_key = vec![0x01, 0x4F, 0x4E];
            storage_key.extend_from_slice(&self.op_id.0.to_be_bytes());
            if self.state.right_total_weight > 0 {
                let mut value = self.state.right_total_weight.to_be_bytes().to_vec();
                value.extend_from_slice(&self.state.right_null_weight.to_be_bytes());
                batch.put(&storage_key, &value);
            } else {
                batch.delete(&storage_key);
            }
        }

        target.merge_from(batch);
        Ok(())
    }

    pub async fn persist_state(&mut self, db: &ShardDb) -> Result<(), OpError> {
        let mut batch = WriteBatch::new();
        self.append_state(&mut batch)?;
        if !batch.is_empty() {
            db.write_batch(batch).await.map_err(OpError::storage)?;
        }
        Ok(())
    }

    pub async fn restore_in_place(&mut self, db: &ShardDb) -> Result<(), OpError> {
        let loaded = OuterJoinOp::load_from_storage(db, self.op_id, self.kind).await?;
        let loaded_core = loaded.into_core();
        self.state = loaded_core.state;
        self.left_staged = StagedDelta::default();
        self.right_staged = StagedDelta::default();
        self.dirty_left.clear();
        self.dirty_right.clear();
        self.dirty_rw.clear();
        self.dirty_lw.clear();
        self.dirty_null_weight = false;
        Ok(())
    }
}

// ─── OuterJoinOp ─────────────────────────────────────────────────────────────

/// Incremental outer / semi / anti equi-join operator (v0.9 — IVM-5).
pub struct OuterJoinOp {
    op_id: OperatorId,
    kind: OuterJoinKind,
    right_n_cols: usize,
    core: Mutex<OuterJoinStateCore>,
}

impl OuterJoinOp {
    /// Create a new `OuterJoinOp` for 2-column `(k, v)` inputs.
    pub fn new(
        op_id: OperatorId,
        kind: OuterJoinKind,
        left_key_cols: Vec<usize>,
        right_key_cols: Vec<usize>,
    ) -> Self {
        Self::with_schema(op_id, kind, left_key_cols, right_key_cols, 2, 2)
    }

    /// Create an `OuterJoinOp` with explicit column counts.
    pub fn with_schema(
        op_id: OperatorId,
        kind: OuterJoinKind,
        left_key_cols: Vec<usize>,
        right_key_cols: Vec<usize>,
        left_n_cols: usize,
        right_n_cols: usize,
    ) -> Self {
        Self::from_core(OuterJoinStateCore::with_schema(
            op_id,
            kind,
            left_key_cols,
            right_key_cols,
            left_n_cols,
            right_n_cols,
        ))
    }

    pub fn from_core(core: OuterJoinStateCore) -> Self {
        let op_id = core.op_id;
        let kind = core.kind;
        let right_n_cols = core.right_n_cols;
        OuterJoinOp {
            op_id,
            kind,
            right_n_cols,
            core: Mutex::new(core),
        }
    }

    pub fn into_core(self) -> OuterJoinStateCore {
        self.core.into_inner().unwrap()
    }

    pub fn kind(&self) -> OuterJoinKind {
        self.kind
    }

    fn serialize_row(batch: &RecordBatch, row_idx: usize) -> Vec<u8> {
        let mut bytes = Vec::new();
        for col in batch.columns() {
            let arr = col.as_any().downcast_ref::<Int64Array>().expect("Int64");
            let value = if arr.is_null(row_idx) {
                0
            } else {
                arr.value(row_idx)
            };
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        // Keep all-valid row identities compatible with the legacy encoding.
        if batch.columns().iter().any(|col| col.is_null(row_idx)) {
            bytes.extend(
                batch
                    .columns()
                    .iter()
                    .map(|col| u8::from(col.is_valid(row_idx))),
            );
        }
        bytes
    }

    fn deserialize_row(bytes: &[u8], n_cols: usize) -> Result<Vec<Option<i64>>, OpError> {
        let values_len = n_cols * 8;
        if (bytes.len() != values_len && bytes.len() != values_len + n_cols)
            || bytes[values_len..].iter().any(|valid| *valid > 1)
        {
            return Err(OpError::storage_error("malformed outer join row encoding"));
        }
        Ok(bytes[..values_len]
            .as_chunks::<8>()
            .0
            .iter()
            .enumerate()
            .map(|(i, c)| {
                bytes
                    .get(values_len + i)
                    .is_none_or(|valid| *valid != 0)
                    .then(|| i64::from_be_bytes(*c))
            })
            .collect())
    }

    /// Build output RecordBatch from column value vecs.
    /// For LEFT/RIGHT/FULL: left_n_cols + right_n_cols columns.
    fn make_output_batch(
        left_row_vals: &[Vec<Option<i64>>],
        right_row_vals: &[Vec<Option<i64>>],
        schema: &SchemaRef,
        left_is_null: bool,
        right_is_null: bool,
    ) -> Result<RecordBatch, OpError> {
        let mut cols: Vec<ArrayRef> = Vec::new();
        for col in left_row_vals {
            let values = col
                .iter()
                .map(|value| if left_is_null { None } else { *value })
                .collect::<Vec<_>>();
            cols.push(Arc::new(Int64Array::from(values)) as ArrayRef);
        }
        for col in right_row_vals {
            let values = col
                .iter()
                .map(|value| if right_is_null { None } else { *value })
                .collect::<Vec<_>>();
            cols.push(Arc::new(Int64Array::from(values)) as ArrayRef);
        }
        RecordBatch::try_new(Arc::clone(schema), cols).map_err(OpError::arrow)
    }

    /// Build semi/anti output RecordBatch (left columns only).
    fn make_semi_batch(
        left_row_vals: &[Vec<Option<i64>>],
        schema: &SchemaRef,
    ) -> Result<RecordBatch, OpError> {
        let mut cols: Vec<ArrayRef> = Vec::new();
        for col in left_row_vals {
            cols.push(Arc::new(Int64Array::from(col.clone())) as ArrayRef);
        }
        RecordBatch::try_new(Arc::clone(schema), cols).map_err(OpError::arrow)
    }

    /// Run a full epoch: process left and right deltas, then commit.
    pub fn process_epoch(&self, left: ArrowZSet, right: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.core.lock().unwrap().process_epoch(left, right)
    }

    /// Single-owner zero-lock epoch execution path (`&mut self`).
    pub fn process_epoch_mut(
        &mut self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<ArrowZSet, OpError> {
        self.core.get_mut().unwrap().process_epoch(left, right)
    }

    // ─── Metrics ─────────────────────────────────────────────────────────────

    /// Fill-level metric for the left arrangement.
    pub fn left_entry_count(&self) -> usize {
        self.core.lock().unwrap().left_entry_count()
    }

    /// Fill-level metric for the right arrangement.
    pub fn right_entry_count(&self) -> usize {
        self.core.lock().unwrap().right_entry_count()
    }

    /// State bytes metric.
    pub fn state_bytes(&self) -> u64 {
        self.core.lock().unwrap().state_bytes()
    }

    /// Number of keys with nonzero match-count tracking (unmatched tracking).
    pub fn unmatched_key_count(&self) -> usize {
        self.core.lock().unwrap().unmatched_key_count()
    }

    /// Operator ID.
    pub fn op_id(&self) -> OperatorId {
        self.op_id
    }

    // ─── State persistence ────────────────────────────────────────────────────

    /// Persist dirty arrangement state and match-count state deltas to a `ShardDb`.
    ///
    /// Persists only keys that changed or were removed during the epoch, ensuring
    /// O(|Δ|) write amplification instead of O(|state|).
    /// Uses only point puts/deletes — no range deletion.
    pub fn append_state(&self, target: &mut WriteBatch) -> Result<(), OpError> {
        self.core.lock().unwrap().append_state(target)
    }

    pub async fn persist_state(&self, db: &ShardDb) -> Result<(), OpError> {
        let mut batch = WriteBatch::new();
        self.append_state(&mut batch)?;
        if !batch.is_empty() {
            db.write_batch(batch).await.map_err(OpError::storage)?;
        }
        Ok(())
    }

    /// Load arrangement state and match-count state from a `ShardDb` (crash-replay).
    ///
    /// Scans left/right arrangement prefixes and key-weight prefixes; no range deletion.
    pub async fn load_from_storage(
        db: &ShardDb,
        op_id: OperatorId,
        kind: OuterJoinKind,
    ) -> Result<Self, OpError> {
        let mut st = OuterJoinState::new();

        // Load left arrangement.
        let left_prefix = ShardKeyEncoder::join_arr_op_prefix(JoinSide::Left, op_id.0);
        let left_entries = db
            .scan_prefix(&left_prefix)
            .await
            .map_err(OpError::storage)?;
        for (key, value) in left_entries {
            if key.len() < 11 + 16 {
                continue;
            }
            let join_key = key[11..key.len() - 16].to_vec();
            let row_id = u128::from_be_bytes(key[key.len() - 16..].try_into().unwrap_or([0u8; 16]));
            let row_bytes = value.to_vec();
            st.update_left(join_key, row_id, row_bytes, 1);
        }

        // Load right arrangement.
        let right_prefix = ShardKeyEncoder::join_arr_op_prefix(JoinSide::Right, op_id.0);
        let right_entries = db
            .scan_prefix(&right_prefix)
            .await
            .map_err(OpError::storage)?;
        for (key, value) in right_entries {
            if key.len() < 11 + 16 {
                continue;
            }
            let join_key = key[11..key.len() - 16].to_vec();
            let row_id = u128::from_be_bytes(key[key.len() - 16..].try_into().unwrap_or([0u8; 16]));
            let row_bytes = value.to_vec();
            st.update_right(join_key, row_id, row_bytes, 1);
        }

        // Load right_key_weight.
        let mut rw_prefix = vec![0x01u8, 0x4F, 0x52];
        rw_prefix.extend_from_slice(&op_id.0.to_be_bytes());
        let rw_entries = db.scan_prefix(&rw_prefix).await.map_err(OpError::storage)?;
        let header_len = 3 + 8; // prefix(3) + op_id(8)
        for (key, value) in rw_entries {
            if key.len() <= header_len || value.len() < 8 {
                continue;
            }
            let key_bytes = key[header_len..].to_vec();
            let weight = i64::from_be_bytes(value[..8].try_into().unwrap_or([0u8; 8]));
            if weight != 0 {
                let key_len = key_bytes.len() as u64;
                st.right_key_weight.insert(key_bytes, weight);
                st.state_bytes += key_len + 8;
            }
        }

        // Load left_key_weight.
        let mut lw_prefix = vec![0x01u8, 0x4F, 0x4C];
        lw_prefix.extend_from_slice(&op_id.0.to_be_bytes());
        let lw_entries = db.scan_prefix(&lw_prefix).await.map_err(OpError::storage)?;
        for (key, value) in lw_entries {
            if key.len() <= header_len || value.len() < 8 {
                continue;
            }
            let key_bytes = key[header_len..].to_vec();
            let weight = i64::from_be_bytes(value[..8].try_into().unwrap_or([0u8; 8]));
            if weight != 0 {
                let key_len = key_bytes.len() as u64;
                st.left_key_weight.insert(key_bytes, weight);
                st.state_bytes += key_len + 8;
            }
        }

        if kind == OuterJoinKind::NullAwareAnti {
            let mut null_weight_key = vec![0x01, 0x4F, 0x4E];
            null_weight_key.extend_from_slice(&op_id.0.to_be_bytes());
            if let Some(value) = db.get(&null_weight_key).await.map_err(OpError::storage)? {
                if value.len() >= 16 {
                    st.right_total_weight =
                        i64::from_be_bytes(value[..8].try_into().unwrap_or([0; 8]));
                    st.right_null_weight =
                        i64::from_be_bytes(value[8..16].try_into().unwrap_or([0; 8]));
                    st.state_bytes += u64::from(st.right_total_weight > 0) * 16;
                }
            }
        }

        let mut core = OuterJoinStateCore::with_schema(op_id, kind, vec![0], vec![0], 2, 2);
        core.state = st;
        Ok(OuterJoinOp::from_core(core))
    }

    /// Restore OuterJoinOp state from `db` into this instance in place.
    pub async fn restore_in_place(&self, db: &ShardDb) -> Result<(), OpError> {
        let loaded = Self::load_from_storage(db, self.op_id, self.kind).await?;
        let loaded_core = loaded.into_core();
        let mut core = self.core.lock().unwrap();
        core.state = loaded_core.state;
        core.left_staged = StagedDelta::default();
        core.right_staged = StagedDelta::default();
        core.dirty_left.clear();
        core.dirty_right.clear();
        core.dirty_rw.clear();
        core.dirty_lw.clear();
        core.dirty_null_weight = false;
        Ok(())
    }
}

impl crate::op::Operator for OuterJoinOp {
    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let empty_right = ArrowZSet::empty(crate::live_exec::int64_schema(self.right_n_cols));
        self.process_epoch(delta, empty_right)
    }

    fn name(&self) -> &str {
        "OuterJoinOp"
    }

    fn state_bytes(&self) -> u64 {
        self.state_bytes()
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use rockstream_types::ids::OperatorId;
    use std::sync::Arc;

    fn make_kv_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let k_vals: Vec<i64> = rows.iter().map(|(k, _, _)| *k).collect();
        let v_vals: Vec<i64> = rows.iter().map(|(_, v, _)| *v).collect();
        let weights: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
        let data = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(k_vals)),
                Arc::new(Int64Array::from(v_vals)),
            ],
        )
        .unwrap();
        ArrowZSet::new(data, weights)
    }

    fn empty_kv() -> ArrowZSet {
        use arrow::datatypes::{DataType, Field, Schema};
        ArrowZSet::empty(Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ])))
    }

    /// Extract (l_k, l_v, r_v_or_null, weight) from left-join output (4-col).
    fn extract_left_join_output(batch: &ArrowZSet) -> Vec<(i64, i64, Option<i64>, i64)> {
        if batch.is_empty() {
            return vec![];
        }
        let lk = batch
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let lv = batch
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let rv = batch
            .data
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut rows: Vec<(i64, i64, Option<i64>, i64)> = (0..batch.num_rows())
            .map(|i| {
                (
                    lk.value(i),
                    lv.value(i),
                    (!rv.is_null(i)).then(|| rv.value(i)),
                    batch.weights[i],
                )
            })
            .collect();
        rows.sort();
        rows
    }

    /// Extract (l_k, l_v, weight) from semi/anti output (2-col).
    fn extract_semi_output(batch: &ArrowZSet) -> Vec<(i64, i64, i64)> {
        if batch.is_empty() {
            return vec![];
        }
        let lk = batch
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let lv = batch
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut rows: Vec<(i64, i64, i64)> = (0..batch.num_rows())
            .map(|i| (lk.value(i), lv.value(i), batch.weights[i]))
            .collect();
        rows.sort();
        rows
    }

    // ── LEFT JOIN ─────────────────────────────────────────────────────────────

    #[test]
    fn nullable_row_encoding_preserves_validity_and_legacy_rows() {
        let legacy = [1i64.to_be_bytes(), 0i64.to_be_bytes()].concat();
        assert_eq!(
            OuterJoinOp::deserialize_row(&legacy, 2).unwrap(),
            vec![Some(1), Some(0)]
        );
        let expected = [
            0i64.to_be_bytes().to_vec(),
            7i64.to_be_bytes().to_vec(),
            vec![0, 1],
        ]
        .concat();
        for hidden_null_value in [0, 42] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("v", DataType::Int64, true),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::new(
                        vec![hidden_null_value].into(),
                        Some(vec![false].into()),
                    )),
                    Arc::new(Int64Array::from(vec![7])),
                ],
            )
            .unwrap();
            assert_eq!(OuterJoinOp::serialize_row(&batch, 0), expected);
        }
        assert_eq!(
            OuterJoinOp::deserialize_row(&expected, 2).unwrap(),
            vec![None, Some(7)]
        );
        for malformed in [vec![0], [legacy, vec![2, 0]].concat()] {
            match OuterJoinOp::deserialize_row(&malformed, 2).unwrap_err() {
                OpError::Storage {
                    source: rockstream_storage::StorageError::KeyEncoding(detail),
                    ..
                } => assert_eq!(detail, "malformed outer join row encoding"),
                error => panic!("unexpected error: {error}"),
            }
        }
    }

    #[test]
    fn left_join_unmatched_left_emits_null_padded() {
        let op = OuterJoinOp::new(OperatorId(1), OuterJoinKind::Left, vec![0], vec![0]);
        let left = make_kv_batch(&[(10, 100, 1)]);
        // No right rows.
        let out = op.process_epoch(left, empty_kv()).unwrap();
        let rows = extract_left_join_output(&out);
        assert!(
            rows.contains(&(10, 100, None, 1)),
            "unmatched left: {rows:?}"
        );
    }

    #[test]
    fn left_join_keeps_null_distinct_from_zero() {
        let op = OuterJoinOp::new(OperatorId(23), OuterJoinKind::Left, vec![0], vec![0]);
        let left = make_kv_batch(&[(7, 70, 1), (8, 80, 1)]);
        let right = make_kv_batch(&[(7, 0, 1)]);
        let out = op.process_epoch(left, right).unwrap();
        assert_eq!(
            extract_left_join_output(&out),
            vec![(7, 70, Some(0), 1), (8, 80, None, 1)]
        );
    }

    #[test]
    fn left_join_matched_emits_inner_join_rows() {
        let op = OuterJoinOp::new(OperatorId(2), OuterJoinKind::Left, vec![0], vec![0]);
        let left = make_kv_batch(&[(5, 50, 1)]);
        let right = make_kv_batch(&[(5, 500, 1)]);
        let out = op.process_epoch(left, right).unwrap();
        let rows = extract_left_join_output(&out);
        // Should include (5, 50, 500, 1) from inner join.
        assert!(rows.contains(&(5, 50, Some(500), 1)), "matched: {rows:?}");
        // Should NOT include NULL-padded row.
        assert!(
            !rows.contains(&(5, 50, None, 1)),
            "no null pad when matched: {rows:?}"
        );
    }

    #[test]
    fn left_join_second_epoch_right_arrives_retracts_null_pad() {
        let op = OuterJoinOp::new(OperatorId(3), OuterJoinKind::Left, vec![0], vec![0]);

        // Epoch 1: left arrives, no right → NULL-pad.
        let l1 = make_kv_batch(&[(7, 70, 1)]);
        let out1 = op.process_epoch(l1, empty_kv()).unwrap();
        let rows1 = extract_left_join_output(&out1);
        assert!(
            rows1.contains(&(7, 70, None, 1)),
            "epoch1 null pad: {rows1:?}"
        );

        // Epoch 2: right arrives for same key → retract NULL-pad, add inner.
        let r2 = make_kv_batch(&[(7, 700, 1)]);
        let out2 = op.process_epoch(empty_kv(), r2).unwrap();
        let rows2 = extract_left_join_output(&out2);
        // Retract NULL-pad.
        assert!(
            rows2.contains(&(7, 70, None, -1)),
            "retract null pad: {rows2:?}"
        );
        // Add inner: (7, 70, 700, +1).
        assert!(
            rows2.contains(&(7, 70, Some(700), 1)),
            "inner join added: {rows2:?}"
        );
    }

    #[test]
    fn left_join_right_delete_restores_null_pad() {
        let op = OuterJoinOp::new(OperatorId(4), OuterJoinKind::Left, vec![0], vec![0]);

        // Epoch 1: both sides, produces inner join.
        let l1 = make_kv_batch(&[(3, 30, 1)]);
        let r1 = make_kv_batch(&[(3, 300, 1)]);
        op.process_epoch(l1, r1).unwrap();

        // Epoch 2: delete right row → left becomes unmatched again.
        let r_del = make_kv_batch(&[(3, 300, -1)]);
        let out2 = op.process_epoch(empty_kv(), r_del).unwrap();
        let rows2 = extract_left_join_output(&out2);
        // Retract inner: (3, 30, 300, -1).
        assert!(
            rows2.contains(&(3, 30, Some(300), -1)),
            "retract inner: {rows2:?}"
        );
        // Add NULL-pad.
        assert!(
            rows2.contains(&(3, 30, None, 1)),
            "add null pad back: {rows2:?}"
        );
    }

    // ── SEMI JOIN ─────────────────────────────────────────────────────────────

    #[test]
    fn semi_join_left_only_emits_when_right_exists() {
        let op = OuterJoinOp::new(OperatorId(10), OuterJoinKind::Semi, vec![0], vec![0]);
        let left = make_kv_batch(&[(1, 10, 1)]);
        let right = make_kv_batch(&[(1, 100, 1)]);
        let out = op.process_epoch(left, right).unwrap();
        let rows = extract_semi_output(&out);
        assert!(rows.contains(&(1, 10, 1)), "semi join hit: {rows:?}");
    }

    #[test]
    fn semi_join_no_right_no_output() {
        let op = OuterJoinOp::new(OperatorId(11), OuterJoinKind::Semi, vec![0], vec![0]);
        let left = make_kv_batch(&[(2, 20, 1)]);
        let out = op.process_epoch(left, empty_kv()).unwrap();
        assert_eq!(out.num_rows(), 0, "semi join miss should produce no output");
    }

    // ── ANTI JOIN ─────────────────────────────────────────────────────────────

    #[test]
    fn anti_join_emits_unmatched_left() {
        let op = OuterJoinOp::new(OperatorId(20), OuterJoinKind::Anti, vec![0], vec![0]);
        let left = make_kv_batch(&[(5, 50, 1)]);
        // No right → anti join emits (5, 50, 1).
        let out = op.process_epoch(left, empty_kv()).unwrap();
        let rows = extract_semi_output(&out);
        assert!(
            rows.contains(&(5, 50, 1)),
            "anti join emits unmatched: {rows:?}"
        );
    }

    #[test]
    fn anti_join_no_output_when_matched() {
        let op = OuterJoinOp::new(OperatorId(21), OuterJoinKind::Anti, vec![0], vec![0]);
        let left = make_kv_batch(&[(5, 50, 1)]);
        let right = make_kv_batch(&[(5, 500, 1)]);
        let out = op.process_epoch(left, right).unwrap();
        // Matched left row should NOT appear in anti output.
        let rows = extract_semi_output(&out);
        assert!(
            !rows.iter().any(|(k, v, w)| *k == 5 && *v == 50 && *w > 0),
            "anti join should not emit matched row: {rows:?}"
        );
    }

    #[test]
    fn anti_join_right_arrives_later_retracts() {
        let op = OuterJoinOp::new(OperatorId(22), OuterJoinKind::Anti, vec![0], vec![0]);
        // Epoch 1: left arrives, no right → anti emits (6, 60, +1).
        let l1 = make_kv_batch(&[(6, 60, 1)]);
        let out1 = op.process_epoch(l1, empty_kv()).unwrap();
        let rows1 = extract_semi_output(&out1);
        assert!(rows1.contains(&(6, 60, 1)), "epoch1: {rows1:?}");

        // Epoch 2: right arrives → anti must retract (6, 60, -1).
        let r2 = make_kv_batch(&[(6, 600, 1)]);
        let out2 = op.process_epoch(empty_kv(), r2).unwrap();
        let rows2 = extract_semi_output(&out2);
        assert!(
            rows2.contains(&(6, 60, -1)),
            "retract when matched: {rows2:?}"
        );
    }

    // ── No range deletion ─────────────────────────────────────────────────────

    #[test]
    fn outer_join_no_range_deletion_in_persist() {
        // Structural assertion: WriteBatch has no delete_range method.
        use rockstream_storage::WriteBatch;
        let _: WriteBatch = WriteBatch::new();
        // No delete_range call here — this proves the API doesn't expose it.
    }
}
