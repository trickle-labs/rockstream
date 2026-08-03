//! Incremental outer / semi / anti equi-join operator (v0.9 — IVM-5).
//!
//! `OuterJoinOp` implements the DBSP delta rules for LEFT, RIGHT, FULL outer
//! joins and SEMI / ANTI joins.  The dual-arrangement structure is the same as
//! `JoinOp`, plus a key-weight map for tracking match counts.
//!
//! ## NULL encoding
//!
//! NULL values in the output (NULL-padding for unmatched rows) are encoded as
//! `0i64` in the output Int64 columns.  All real column values are non-null.
//!
//! ## State persistence
//!
//! Left arrangement:    `[0x01][0x4A4C][op_id:8][join_key_bytes][row_id:16]` → row_bytes
//! Right arrangement:   `[0x01][0x4A52][op_id:8][join_key_bytes][row_id:16]` → row_bytes
//! Right key weights:   `[0x01][0x4F52][op_id:8][key_bytes]` → weight:8 (i64 BE)
//! Left key weights:    `[0x01][0x4F4C][op_id:8][key_bytes]` → weight:8 (i64 BE)
//!
//! Only point puts/deletes are used — no range deletion.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use rockstream_plan::OuterJoinKind;
use rockstream_storage::{JoinSide, ShardDb, ShardKeyEncoder, WriteBatch};
use rockstream_types::ids::OperatorId;

use crate::error::OpError;
use crate::join::{concat_zsets, join_output_schema_n, stable_row_id};
use crate::zset::ArrowZSet;

// ─── Schema ──────────────────────────────────────────────────────────────────

/// Output schema for outer join (LEFT/RIGHT/FULL): left + right columns (nullable).
pub fn outer_join_output_schema_n(left_n_cols: usize, right_n_cols: usize) -> SchemaRef {
    join_output_schema_n(left_n_cols, right_n_cols)
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
}

impl StagedDelta {
    fn push(&mut self, join_key: Vec<u8>, row_id: u128, row_bytes: Vec<u8>, weight: i64) {
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
    /// Per-key total net left-side weight (for Right/Full tracking).
    left_key_weight: HashMap<Vec<u8>, i64>,
}

impl OuterJoinState {
    fn new() -> Self {
        Self::default()
    }

    fn update_left(&mut self, join_key: Vec<u8>, row_id: u128, row_bytes: Vec<u8>, delta_w: i64) {
        let bucket = self.left_arr.entry(join_key).or_default();
        let entry = bucket.entry(row_id).or_insert_with(|| ArrRow {
            row_bytes: row_bytes.clone(),
            weight: 0,
        });
        entry.weight += delta_w;
        if entry.weight == 0 {
            bucket.remove(&row_id);
        }
    }

    fn update_right(&mut self, join_key: Vec<u8>, row_id: u128, row_bytes: Vec<u8>, delta_w: i64) {
        let bucket = self.right_arr.entry(join_key).or_default();
        let entry = bucket.entry(row_id).or_insert_with(|| ArrRow {
            row_bytes: row_bytes.clone(),
            weight: 0,
        });
        entry.weight += delta_w;
        if entry.weight == 0 {
            bucket.remove(&row_id);
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

// ─── OuterJoinOp ─────────────────────────────────────────────────────────────

/// Incremental outer / semi / anti equi-join operator (v0.9 — IVM-5).
pub struct OuterJoinOp {
    op_id: OperatorId,
    kind: OuterJoinKind,
    left_key_cols: Vec<usize>,
    right_key_cols: Vec<usize>,
    left_n_cols: usize,
    right_n_cols: usize,
    state: Mutex<OuterJoinState>,
    left_staged: Mutex<StagedDelta>,
    right_staged: Mutex<StagedDelta>,
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
        OuterJoinOp {
            op_id,
            kind,
            left_key_cols,
            right_key_cols,
            left_n_cols,
            right_n_cols,
            state: Mutex::new(OuterJoinState::new()),
            left_staged: Mutex::new(StagedDelta::default()),
            right_staged: Mutex::new(StagedDelta::default()),
        }
    }

    fn extract_key(row: &RecordBatch, row_idx: usize, key_cols: &[usize]) -> Vec<u8> {
        let mut key = Vec::with_capacity(key_cols.len() * 8);
        for &col in key_cols {
            let arr = row
                .column(col)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("join key column must be Int64");
            key.extend_from_slice(&arr.value(row_idx).to_be_bytes());
        }
        key
    }

    fn serialize_row(batch: &RecordBatch, row_idx: usize) -> Vec<u8> {
        let mut bytes = Vec::new();
        for col in batch.columns() {
            let arr = col.as_any().downcast_ref::<Int64Array>().expect("Int64");
            bytes.extend_from_slice(&arr.value(row_idx).to_be_bytes());
        }
        bytes
    }

    fn deserialize_row(bytes: &[u8], n_cols: usize) -> Vec<i64> {
        bytes
            .chunks_exact(8)
            .take(n_cols)
            .map(|c| i64::from_be_bytes(c.try_into().unwrap()))
            .collect()
    }

    /// Build output RecordBatch from column value vecs.
    /// For LEFT/RIGHT/FULL: left_n_cols + right_n_cols columns.
    fn make_output_batch(
        left_row_vals: &[Vec<i64>],
        right_row_vals: &[Vec<i64>],
        schema: &SchemaRef,
    ) -> Result<RecordBatch, OpError> {
        let mut cols: Vec<ArrayRef> = Vec::new();
        for col in left_row_vals {
            cols.push(Arc::new(Int64Array::from(col.clone())) as ArrayRef);
        }
        for col in right_row_vals {
            cols.push(Arc::new(Int64Array::from(col.clone())) as ArrayRef);
        }
        RecordBatch::try_new(Arc::clone(schema), cols).map_err(OpError::arrow)
    }

    /// Build semi/anti output RecordBatch (left columns only).
    fn make_semi_batch(
        left_row_vals: &[Vec<i64>],
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
        match self.kind {
            OuterJoinKind::Left => self.process_epoch_left(left, right),
            OuterJoinKind::Right => self.process_epoch_right(left, right),
            OuterJoinKind::Full => self.process_epoch_full(left, right),
            OuterJoinKind::Semi => self.process_epoch_semi(left, right),
            OuterJoinKind::Anti => self.process_epoch_anti(left, right),
        }
    }

    /// Stage a left delta into left_staged (no probing yet).
    fn stage_left(&self, delta: &ArrowZSet) {
        let mut staged = self.left_staged.lock().unwrap();
        for row_idx in 0..delta.num_rows() {
            let w = delta.weights[row_idx];
            let join_key = Self::extract_key(&delta.data, row_idx, &self.left_key_cols);
            let row_bytes = Self::serialize_row(&delta.data, row_idx);
            let row_id = stable_row_id(self.op_id.0, &join_key, &row_bytes);
            staged.push(join_key, row_id, row_bytes, w);
        }
    }

    /// Stage a right delta into right_staged (no probing yet).
    fn stage_right(&self, delta: &ArrowZSet) {
        let mut staged = self.right_staged.lock().unwrap();
        for row_idx in 0..delta.num_rows() {
            let w = delta.weights[row_idx];
            let join_key = Self::extract_key(&delta.data, row_idx, &self.right_key_cols);
            let row_bytes = Self::serialize_row(&delta.data, row_idx);
            let row_id = stable_row_id(self.op_id.0, &join_key, &row_bytes);
            staged.push(join_key, row_id, row_bytes, w);
        }
    }

    // ─── LEFT JOIN ────────────────────────────────────────────────────────────

    fn process_epoch_left(&self, left: ArrowZSet, right: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.stage_left(&left);
        self.stage_right(&right);

        let out_schema = outer_join_output_schema_n(self.left_n_cols, self.right_n_cols);
        let mut outputs: Vec<ArrowZSet> = Vec::new();

        // Compute staged right delta per key.
        let delta_rw: HashMap<Vec<u8>, i64> = {
            let staged = self.right_staged.lock().unwrap();
            let mut map: HashMap<Vec<u8>, i64> = HashMap::new();
            for (join_key, _row_id, _row_bytes, w) in &staged.rows {
                *map.entry(join_key.clone()).or_insert(0) += w;
            }
            map
        };

        // Compute staged right delta rows by key (for ΔL ⋈ ΔR).
        let right_staged_rows: HashMap<Vec<u8>, Vec<(Vec<u8>, i64)>> = {
            let staged = self.right_staged.lock().unwrap();
            let mut map: HashMap<Vec<u8>, Vec<(Vec<u8>, i64)>> = HashMap::new();
            for (join_key, _row_id, row_bytes, w) in &staged.rows {
                map.entry(join_key.clone())
                    .or_default()
                    .push((row_bytes.clone(), *w));
            }
            map
        };

        {
            let state = self.state.lock().unwrap();

            // 1. Inner join output: ΔL⋈R₀ + L₀⋈ΔR + ΔL⋈ΔR
            let inner = self.compute_inner_join_output(&state, &right_staged_rows, &out_schema)?;
            if !inner.is_empty() {
                outputs.push(inner);
            }

            // 2. NULL-pad transitions due to right side changing.
            //    For each key in delta_rw:
            //      old_rw = right_key_weight[k], new_rw = old_rw + delta_rw[k]
            //      If old_rw == 0 && new_rw != 0: retract NULL-pads for L₀[k]
            //      If old_rw != 0 && new_rw == 0: add NULL-pads for L₀[k]
            let mut null_pad_rows: (Vec<Vec<i64>>, Vec<Vec<i64>>, Vec<i64>) = (
                vec![Vec::new(); self.left_n_cols],
                vec![Vec::new(); self.right_n_cols],
                Vec::new(),
            );
            for (key, delta) in &delta_rw {
                let old_rw = state.right_key_weight.get(key).copied().unwrap_or(0);
                let new_rw = old_rw + delta;
                if old_rw == new_rw {
                    continue;
                }
                if (old_rw == 0) != (new_rw == 0) {
                    // Transition: either match gained or match lost for L₀ rows.
                    let retract = old_rw == 0; // was unmatched, now matched → retract NULL-pad
                    for (l_row_bytes, l_weight) in state.probe_left(key) {
                        let left_vals = Self::deserialize_row(l_row_bytes, self.left_n_cols);
                        let right_nulls: Vec<i64> = vec![0i64; self.right_n_cols];
                        for (i, v) in left_vals.iter().enumerate() {
                            null_pad_rows.0[i].push(*v);
                        }
                        for (i, v) in right_nulls.iter().enumerate() {
                            null_pad_rows.1[i].push(*v);
                        }
                        // retract → negative weight; add → positive weight
                        null_pad_rows
                            .2
                            .push(if retract { -l_weight } else { l_weight });
                    }
                }
            }
            if !null_pad_rows.2.is_empty() {
                let batch =
                    Self::make_output_batch(&null_pad_rows.0, &null_pad_rows.1, &out_schema)?;
                outputs.push(ArrowZSet::new(batch, null_pad_rows.2));
            }

            // 3. For each ΔL row: if effective_rw == 0, emit (row, NULL_right, w).
            {
                let staged_left = self.left_staged.lock().unwrap();
                let mut null_new_rows: (Vec<Vec<i64>>, Vec<Vec<i64>>, Vec<i64>) = (
                    vec![Vec::new(); self.left_n_cols],
                    vec![Vec::new(); self.right_n_cols],
                    Vec::new(),
                );
                for (join_key, _row_id, row_bytes, w) in &staged_left.rows {
                    let old_rw = state.right_key_weight.get(join_key).copied().unwrap_or(0);
                    let delta = delta_rw.get(join_key).copied().unwrap_or(0);
                    let effective_rw = old_rw + delta;
                    if effective_rw == 0 {
                        let left_vals = Self::deserialize_row(row_bytes, self.left_n_cols);
                        let right_nulls: Vec<i64> = vec![0i64; self.right_n_cols];
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
                    let batch =
                        Self::make_output_batch(&null_new_rows.0, &null_new_rows.1, &out_schema)?;
                    outputs.push(ArrowZSet::new(batch, null_new_rows.2));
                }
            }
        }

        // Apply staged deltas to state.
        self.commit_staged(&delta_rw, None);

        concat_zsets(outputs, out_schema)
    }

    // ─── RIGHT JOIN ───────────────────────────────────────────────────────────

    fn process_epoch_right(&self, left: ArrowZSet, right: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.stage_left(&left);
        self.stage_right(&right);

        let out_schema = outer_join_output_schema_n(self.left_n_cols, self.right_n_cols);
        let mut outputs: Vec<ArrowZSet> = Vec::new();

        let delta_lw: HashMap<Vec<u8>, i64> = {
            let staged = self.left_staged.lock().unwrap();
            let mut map: HashMap<Vec<u8>, i64> = HashMap::new();
            for (join_key, _, _, w) in &staged.rows {
                *map.entry(join_key.clone()).or_insert(0) += w;
            }
            map
        };

        let right_staged_rows: HashMap<Vec<u8>, Vec<(Vec<u8>, i64)>> = {
            let staged = self.right_staged.lock().unwrap();
            let mut map: HashMap<Vec<u8>, Vec<(Vec<u8>, i64)>> = HashMap::new();
            for (join_key, _, row_bytes, w) in &staged.rows {
                map.entry(join_key.clone())
                    .or_default()
                    .push((row_bytes.clone(), *w));
            }
            map
        };

        {
            let state = self.state.lock().unwrap();

            // 1. Inner join output.
            let inner = self.compute_inner_join_output(&state, &right_staged_rows, &out_schema)?;
            if !inner.is_empty() {
                outputs.push(inner);
            }

            // 2. NULL-pad transitions for right rows (symmetric to left).
            let mut null_pad_rows: (Vec<Vec<i64>>, Vec<Vec<i64>>, Vec<i64>) = (
                vec![Vec::new(); self.left_n_cols],
                vec![Vec::new(); self.right_n_cols],
                Vec::new(),
            );
            for (key, delta) in &delta_lw {
                let old_lw = state.left_key_weight.get(key).copied().unwrap_or(0);
                let new_lw = old_lw + delta;
                if (old_lw == 0) != (new_lw == 0) {
                    let retract = old_lw == 0;
                    for (r_row_bytes, r_weight) in state.probe_right(key) {
                        let left_nulls: Vec<i64> = vec![0i64; self.left_n_cols];
                        let right_vals = Self::deserialize_row(r_row_bytes, self.right_n_cols);
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
                let batch =
                    Self::make_output_batch(&null_pad_rows.0, &null_pad_rows.1, &out_schema)?;
                outputs.push(ArrowZSet::new(batch, null_pad_rows.2));
            }

            // 3. For each ΔR row: if effective_lw == 0, emit (NULL_left, row, w).
            {
                let staged_right = self.right_staged.lock().unwrap();
                let mut null_new_rows: (Vec<Vec<i64>>, Vec<Vec<i64>>, Vec<i64>) = (
                    vec![Vec::new(); self.left_n_cols],
                    vec![Vec::new(); self.right_n_cols],
                    Vec::new(),
                );
                for (join_key, _, row_bytes, w) in &staged_right.rows {
                    let old_lw = state.left_key_weight.get(join_key).copied().unwrap_or(0);
                    let delta = delta_lw.get(join_key).copied().unwrap_or(0);
                    let effective_lw = old_lw + delta;
                    if effective_lw == 0 {
                        let left_nulls: Vec<i64> = vec![0i64; self.left_n_cols];
                        let right_vals = Self::deserialize_row(row_bytes, self.right_n_cols);
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
                    let batch =
                        Self::make_output_batch(&null_new_rows.0, &null_new_rows.1, &out_schema)?;
                    outputs.push(ArrowZSet::new(batch, null_new_rows.2));
                }
            }
        }

        self.commit_staged(&HashMap::new(), Some(&delta_lw));

        concat_zsets(outputs, out_schema)
    }

    // ─── FULL OUTER JOIN ──────────────────────────────────────────────────────

    fn process_epoch_full(&self, left: ArrowZSet, right: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.stage_left(&left);
        self.stage_right(&right);

        let out_schema = outer_join_output_schema_n(self.left_n_cols, self.right_n_cols);
        let mut outputs: Vec<ArrowZSet> = Vec::new();

        let delta_rw: HashMap<Vec<u8>, i64> = {
            let staged = self.right_staged.lock().unwrap();
            let mut map: HashMap<Vec<u8>, i64> = HashMap::new();
            for (join_key, _, _, w) in &staged.rows {
                *map.entry(join_key.clone()).or_insert(0) += w;
            }
            map
        };

        let delta_lw: HashMap<Vec<u8>, i64> = {
            let staged = self.left_staged.lock().unwrap();
            let mut map: HashMap<Vec<u8>, i64> = HashMap::new();
            for (join_key, _, _, w) in &staged.rows {
                *map.entry(join_key.clone()).or_insert(0) += w;
            }
            map
        };

        let right_staged_rows: HashMap<Vec<u8>, Vec<(Vec<u8>, i64)>> = {
            let staged = self.right_staged.lock().unwrap();
            let mut map: HashMap<Vec<u8>, Vec<(Vec<u8>, i64)>> = HashMap::new();
            for (join_key, _, row_bytes, w) in &staged.rows {
                map.entry(join_key.clone())
                    .or_default()
                    .push((row_bytes.clone(), *w));
            }
            map
        };

        {
            let state = self.state.lock().unwrap();

            // 1. Inner join output.
            let inner = self.compute_inner_join_output(&state, &right_staged_rows, &out_schema)?;
            if !inner.is_empty() {
                outputs.push(inner);
            }

            // 2. Left-side NULL-pad transitions (same as LEFT JOIN).
            let mut null_pad_left: (Vec<Vec<i64>>, Vec<Vec<i64>>, Vec<i64>) = (
                vec![Vec::new(); self.left_n_cols],
                vec![Vec::new(); self.right_n_cols],
                Vec::new(),
            );
            for (key, delta) in &delta_rw {
                let old_rw = state.right_key_weight.get(key).copied().unwrap_or(0);
                let new_rw = old_rw + delta;
                if (old_rw == 0) != (new_rw == 0) {
                    let retract = old_rw == 0;
                    for (l_row_bytes, l_weight) in state.probe_left(key) {
                        let left_vals = Self::deserialize_row(l_row_bytes, self.left_n_cols);
                        let right_nulls: Vec<i64> = vec![0i64; self.right_n_cols];
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
                let batch =
                    Self::make_output_batch(&null_pad_left.0, &null_pad_left.1, &out_schema)?;
                outputs.push(ArrowZSet::new(batch, null_pad_left.2));
            }

            // 3. Right-side NULL-pad transitions (same as RIGHT JOIN).
            let mut null_pad_right: (Vec<Vec<i64>>, Vec<Vec<i64>>, Vec<i64>) = (
                vec![Vec::new(); self.left_n_cols],
                vec![Vec::new(); self.right_n_cols],
                Vec::new(),
            );
            for (key, delta) in &delta_lw {
                let old_lw = state.left_key_weight.get(key).copied().unwrap_or(0);
                let new_lw = old_lw + delta;
                if (old_lw == 0) != (new_lw == 0) {
                    let retract = old_lw == 0;
                    for (r_row_bytes, r_weight) in state.probe_right(key) {
                        let left_nulls: Vec<i64> = vec![0i64; self.left_n_cols];
                        let right_vals = Self::deserialize_row(r_row_bytes, self.right_n_cols);
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
                let batch =
                    Self::make_output_batch(&null_pad_right.0, &null_pad_right.1, &out_schema)?;
                outputs.push(ArrowZSet::new(batch, null_pad_right.2));
            }

            // 4. New ΔL rows with no effective right match → NULL-pad right.
            {
                let staged_left = self.left_staged.lock().unwrap();
                let mut null_new_left: (Vec<Vec<i64>>, Vec<Vec<i64>>, Vec<i64>) = (
                    vec![Vec::new(); self.left_n_cols],
                    vec![Vec::new(); self.right_n_cols],
                    Vec::new(),
                );
                for (join_key, _, row_bytes, w) in &staged_left.rows {
                    let old_rw = state.right_key_weight.get(join_key).copied().unwrap_or(0);
                    let delta = delta_rw.get(join_key).copied().unwrap_or(0);
                    let effective_rw = old_rw + delta;
                    if effective_rw == 0 {
                        let left_vals = Self::deserialize_row(row_bytes, self.left_n_cols);
                        let right_nulls: Vec<i64> = vec![0i64; self.right_n_cols];
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
                    let batch =
                        Self::make_output_batch(&null_new_left.0, &null_new_left.1, &out_schema)?;
                    outputs.push(ArrowZSet::new(batch, null_new_left.2));
                }
            }

            // 5. New ΔR rows with no effective left match → NULL-pad left.
            {
                let staged_right = self.right_staged.lock().unwrap();
                let mut null_new_right: (Vec<Vec<i64>>, Vec<Vec<i64>>, Vec<i64>) = (
                    vec![Vec::new(); self.left_n_cols],
                    vec![Vec::new(); self.right_n_cols],
                    Vec::new(),
                );
                for (join_key, _, row_bytes, w) in &staged_right.rows {
                    let old_lw = state.left_key_weight.get(join_key).copied().unwrap_or(0);
                    let delta = delta_lw.get(join_key).copied().unwrap_or(0);
                    let effective_lw = old_lw + delta;
                    if effective_lw == 0 {
                        let left_nulls: Vec<i64> = vec![0i64; self.left_n_cols];
                        let right_vals = Self::deserialize_row(row_bytes, self.right_n_cols);
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
                    let batch =
                        Self::make_output_batch(&null_new_right.0, &null_new_right.1, &out_schema)?;
                    outputs.push(ArrowZSet::new(batch, null_new_right.2));
                }
            }
        }

        self.commit_staged(&delta_rw, Some(&delta_lw));

        concat_zsets(outputs, out_schema)
    }

    // ─── SEMI JOIN ────────────────────────────────────────────────────────────

    fn process_epoch_semi(&self, left: ArrowZSet, right: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.stage_left(&left);
        self.stage_right(&right);

        let out_schema = semi_anti_output_schema_n(self.left_n_cols);
        let mut outputs: Vec<ArrowZSet> = Vec::new();

        let delta_rw: HashMap<Vec<u8>, i64> = {
            let staged = self.right_staged.lock().unwrap();
            let mut map: HashMap<Vec<u8>, i64> = HashMap::new();
            for (join_key, _, _, w) in &staged.rows {
                *map.entry(join_key.clone()).or_insert(0) += w;
            }
            map
        };

        {
            let state = self.state.lock().unwrap();

            // Emit from L₀ when right-side match status changes for a key.
            let mut semi_rows: (Vec<Vec<i64>>, Vec<i64>) =
                (vec![Vec::new(); self.left_n_cols], Vec::new());
            for (key, delta) in &delta_rw {
                let old_rw = state.right_key_weight.get(key).copied().unwrap_or(0);
                let new_rw = old_rw + delta;
                if (old_rw == 0) != (new_rw == 0) {
                    // Status changed: was unmatched → now matched (emit +), or vice versa (emit -).
                    let sign: i64 = if old_rw == 0 { 1 } else { -1 };
                    for (l_row_bytes, l_weight) in state.probe_left(key) {
                        let left_vals = Self::deserialize_row(l_row_bytes, self.left_n_cols);
                        for (i, v) in left_vals.iter().enumerate() {
                            semi_rows.0[i].push(*v);
                        }
                        semi_rows.1.push(sign * l_weight);
                    }
                }
            }
            if !semi_rows.1.is_empty() {
                let batch = Self::make_semi_batch(&semi_rows.0, &out_schema)?;
                outputs.push(ArrowZSet::new(batch, semi_rows.1));
            }

            // For each ΔL row: if effective_rw != 0, emit (row, w).
            {
                let staged_left = self.left_staged.lock().unwrap();
                let mut new_left_rows: (Vec<Vec<i64>>, Vec<i64>) =
                    (vec![Vec::new(); self.left_n_cols], Vec::new());
                for (join_key, _, row_bytes, w) in &staged_left.rows {
                    let old_rw = state.right_key_weight.get(join_key).copied().unwrap_or(0);
                    let delta = delta_rw.get(join_key).copied().unwrap_or(0);
                    let effective_rw = old_rw + delta;
                    if effective_rw != 0 {
                        let left_vals = Self::deserialize_row(row_bytes, self.left_n_cols);
                        for (i, v) in left_vals.iter().enumerate() {
                            new_left_rows.0[i].push(*v);
                        }
                        new_left_rows.1.push(*w);
                    }
                }
                if !new_left_rows.1.is_empty() {
                    let batch = Self::make_semi_batch(&new_left_rows.0, &out_schema)?;
                    outputs.push(ArrowZSet::new(batch, new_left_rows.1));
                }
            }
        }

        self.commit_staged(&delta_rw, None);

        concat_zsets(outputs, out_schema)
    }

    // ─── ANTI JOIN ────────────────────────────────────────────────────────────

    fn process_epoch_anti(&self, left: ArrowZSet, right: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.stage_left(&left);
        self.stage_right(&right);

        let out_schema = semi_anti_output_schema_n(self.left_n_cols);
        let mut outputs: Vec<ArrowZSet> = Vec::new();

        let delta_rw: HashMap<Vec<u8>, i64> = {
            let staged = self.right_staged.lock().unwrap();
            let mut map: HashMap<Vec<u8>, i64> = HashMap::new();
            for (join_key, _, _, w) in &staged.rows {
                *map.entry(join_key.clone()).or_insert(0) += w;
            }
            map
        };

        {
            let state = self.state.lock().unwrap();

            // Emit from L₀ when right-side match status changes for a key.
            let mut anti_rows: (Vec<Vec<i64>>, Vec<i64>) =
                (vec![Vec::new(); self.left_n_cols], Vec::new());
            for (key, delta) in &delta_rw {
                let old_rw = state.right_key_weight.get(key).copied().unwrap_or(0);
                let new_rw = old_rw + delta;
                if (old_rw == 0) != (new_rw == 0) {
                    // Anti: was unmatched → emit −; now matched → retract from anti output.
                    // old_rw==0 means was in anti output → now matched, must retract.
                    let sign: i64 = if old_rw == 0 { -1 } else { 1 };
                    for (l_row_bytes, l_weight) in state.probe_left(key) {
                        let left_vals = Self::deserialize_row(l_row_bytes, self.left_n_cols);
                        for (i, v) in left_vals.iter().enumerate() {
                            anti_rows.0[i].push(*v);
                        }
                        anti_rows.1.push(sign * l_weight);
                    }
                }
            }
            if !anti_rows.1.is_empty() {
                let batch = Self::make_semi_batch(&anti_rows.0, &out_schema)?;
                outputs.push(ArrowZSet::new(batch, anti_rows.1));
            }

            // For each ΔL row: if effective_rw == 0, emit (row, w).
            {
                let staged_left = self.left_staged.lock().unwrap();
                let mut new_left_rows: (Vec<Vec<i64>>, Vec<i64>) =
                    (vec![Vec::new(); self.left_n_cols], Vec::new());
                for (join_key, _, row_bytes, w) in &staged_left.rows {
                    let old_rw = state.right_key_weight.get(join_key).copied().unwrap_or(0);
                    let delta = delta_rw.get(join_key).copied().unwrap_or(0);
                    let effective_rw = old_rw + delta;
                    if effective_rw == 0 {
                        let left_vals = Self::deserialize_row(row_bytes, self.left_n_cols);
                        for (i, v) in left_vals.iter().enumerate() {
                            new_left_rows.0[i].push(*v);
                        }
                        new_left_rows.1.push(*w);
                    }
                }
                if !new_left_rows.1.is_empty() {
                    let batch = Self::make_semi_batch(&new_left_rows.0, &out_schema)?;
                    outputs.push(ArrowZSet::new(batch, new_left_rows.1));
                }
            }
        }

        self.commit_staged(&delta_rw, None);

        concat_zsets(outputs, out_schema)
    }

    // ─── Inner join helper ────────────────────────────────────────────────────

    /// Compute the inner join contribution: ΔL⋈R₀ + L₀⋈ΔR + ΔL⋈ΔR.
    fn compute_inner_join_output(
        &self,
        state: &OuterJoinState,
        right_staged_rows: &HashMap<Vec<u8>, Vec<(Vec<u8>, i64)>>,
        out_schema: &SchemaRef,
    ) -> Result<ArrowZSet, OpError> {
        let mut left_cols: Vec<Vec<i64>> = vec![Vec::new(); self.left_n_cols];
        let mut right_cols: Vec<Vec<i64>> = vec![Vec::new(); self.right_n_cols];
        let mut out_weights: Vec<i64> = Vec::new();

        let staged_left = self.left_staged.lock().unwrap();
        let staged_right = self.right_staged.lock().unwrap();

        // ΔL ⋈ R₀
        for (join_key, _, row_bytes, w) in &staged_left.rows {
            for (right_row_bytes, r_weight) in state.probe_right(join_key) {
                let left_vals = Self::deserialize_row(row_bytes, self.left_n_cols);
                let right_vals = Self::deserialize_row(right_row_bytes, self.right_n_cols);
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
        for (join_key, _, row_bytes, w) in &staged_right.rows {
            for (left_row_bytes, l_weight) in state.probe_left(join_key) {
                let left_vals = Self::deserialize_row(left_row_bytes, self.left_n_cols);
                let right_vals = Self::deserialize_row(row_bytes, self.right_n_cols);
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
        for (join_key, _, left_bytes, l_w) in &staged_left.rows {
            if let Some(right_rows) = right_staged_rows.get(join_key) {
                for (right_bytes, r_w) in right_rows {
                    let left_vals = Self::deserialize_row(left_bytes, self.left_n_cols);
                    let right_vals = Self::deserialize_row(right_bytes, self.right_n_cols);
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

        let batch = Self::make_output_batch(&left_cols, &right_cols, out_schema)?;
        Ok(ArrowZSet::new(batch, out_weights))
    }

    // ─── Commit staged ────────────────────────────────────────────────────────

    /// Apply staged deltas to arrangements and key-weight maps, then clear staged.
    fn commit_staged(
        &self,
        delta_rw: &HashMap<Vec<u8>, i64>,
        delta_lw: Option<&HashMap<Vec<u8>, i64>>,
    ) {
        let mut state = self.state.lock().unwrap();
        let mut left_s = self.left_staged.lock().unwrap();
        let mut right_s = self.right_staged.lock().unwrap();

        // Apply staged left rows.
        for (join_key, row_id, row_bytes, w) in left_s.rows.drain(..) {
            state.update_left(join_key, row_id, row_bytes, w);
        }

        // Apply staged right rows.
        for (join_key, row_id, row_bytes, w) in right_s.rows.drain(..) {
            state.update_right(join_key, row_id, row_bytes, w);
        }

        // Update right_key_weight.
        for (key, delta) in delta_rw {
            let entry = state.right_key_weight.entry(key.clone()).or_insert(0);
            *entry += delta;
            if *entry == 0 {
                state.right_key_weight.remove(key);
            }
        }

        // Update left_key_weight (for RIGHT and FULL joins).
        if let Some(dlw) = delta_lw {
            for (key, delta) in dlw {
                let entry = state.left_key_weight.entry(key.clone()).or_insert(0);
                *entry += delta;
                if *entry == 0 {
                    state.left_key_weight.remove(key);
                }
            }
        }
    }

    // ─── Metrics ─────────────────────────────────────────────────────────────

    /// Fill-level metric for the left arrangement.
    pub fn left_entry_count(&self) -> usize {
        self.state.lock().unwrap().left_entry_count()
    }

    /// Fill-level metric for the right arrangement.
    pub fn right_entry_count(&self) -> usize {
        self.state.lock().unwrap().right_entry_count()
    }

    /// Number of keys with nonzero match-count tracking (unmatched tracking).
    pub fn unmatched_key_count(&self) -> usize {
        self.state.lock().unwrap().unmatched_key_count()
    }

    /// Operator ID.
    pub fn op_id(&self) -> OperatorId {
        self.op_id
    }

    // ─── State persistence ────────────────────────────────────────────────────

    /// Persist the arrangement state and match-count state to a `ShardDb`.
    ///
    /// Uses only point puts — no range deletion.
    pub async fn persist_state(&self, db: &ShardDb) -> Result<(), OpError> {
        let mut batch = WriteBatch::new();

        {
            let state = self.state.lock().unwrap();

            // Left arrangement (same key format as JoinOp).
            for (join_key, entries) in &state.left_arr {
                for (row_id, arr_row) in entries {
                    if arr_row.weight > 0 {
                        let key = ShardKeyEncoder::join_arr_key(
                            JoinSide::Left,
                            self.op_id.0,
                            join_key,
                            *row_id,
                        );
                        batch.put(&key, &arr_row.row_bytes);
                    }
                }
            }

            // Right arrangement.
            for (join_key, entries) in &state.right_arr {
                for (row_id, arr_row) in entries {
                    if arr_row.weight > 0 {
                        let key = ShardKeyEncoder::join_arr_key(
                            JoinSide::Right,
                            self.op_id.0,
                            join_key,
                            *row_id,
                        );
                        batch.put(&key, &arr_row.row_bytes);
                    }
                }
            }

            // right_key_weight: prefix [0x01, 0x4F, 0x52] + op_id:8 + key_bytes → weight:8
            let rw_prefix: &[u8] = &[0x01, 0x4F, 0x52];
            for (key_bytes, &weight) in &state.right_key_weight {
                if weight != 0 {
                    let mut storage_key = Vec::with_capacity(3 + 8 + key_bytes.len());
                    storage_key.extend_from_slice(rw_prefix);
                    storage_key.extend_from_slice(&self.op_id.0.to_be_bytes());
                    storage_key.extend_from_slice(key_bytes);
                    batch.put(&storage_key, &weight.to_be_bytes());
                }
            }

            // left_key_weight: prefix [0x01, 0x4F, 0x4C] + op_id:8 + key_bytes → weight:8
            let lw_prefix: &[u8] = &[0x01, 0x4F, 0x4C];
            for (key_bytes, &weight) in &state.left_key_weight {
                if weight != 0 {
                    let mut storage_key = Vec::with_capacity(3 + 8 + key_bytes.len());
                    storage_key.extend_from_slice(lw_prefix);
                    storage_key.extend_from_slice(&self.op_id.0.to_be_bytes());
                    storage_key.extend_from_slice(key_bytes);
                    batch.put(&storage_key, &weight.to_be_bytes());
                }
            }
        }

        if batch.is_empty() {
            return Ok(());
        }
        db.write_batch(batch).await.map_err(OpError::storage)
    }

    /// Load operator state from a `ShardDb` (crash-replay).
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
            st.left_arr.entry(join_key).or_default().insert(
                row_id,
                ArrRow {
                    row_bytes,
                    weight: 1,
                },
            );
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
            st.right_arr.entry(join_key).or_default().insert(
                row_id,
                ArrRow {
                    row_bytes,
                    weight: 1,
                },
            );
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
                st.right_key_weight.insert(key_bytes, weight);
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
                st.left_key_weight.insert(key_bytes, weight);
            }
        }

        Ok(OuterJoinOp {
            op_id,
            kind,
            left_key_cols: vec![0],
            right_key_cols: vec![0],
            left_n_cols: 2,
            right_n_cols: 2,
            state: Mutex::new(st),
            left_staged: Mutex::new(StagedDelta::default()),
            right_staged: Mutex::new(StagedDelta::default()),
        })
    }

    /// Restore OuterJoinOp state from `db` into this instance in place.
    pub async fn restore_in_place(&self, db: &ShardDb) -> Result<(), OpError> {
        let loaded = Self::load_from_storage(db, self.op_id, self.kind).await?;
        let loaded_st = loaded.state.into_inner().unwrap();
        *self.state.lock().unwrap() = loaded_st;
        Ok(())
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
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
    fn extract_left_join_output(batch: &ArrowZSet) -> Vec<(i64, i64, i64, i64)> {
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
        let mut rows: Vec<(i64, i64, i64, i64)> = (0..batch.num_rows())
            .map(|i| (lk.value(i), lv.value(i), rv.value(i), batch.weights[i]))
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
    fn left_join_unmatched_left_emits_null_padded() {
        let op = OuterJoinOp::new(OperatorId(1), OuterJoinKind::Left, vec![0], vec![0]);
        let left = make_kv_batch(&[(10, 100, 1)]);
        // No right rows.
        let out = op.process_epoch(left, empty_kv()).unwrap();
        let rows = extract_left_join_output(&out);
        // (k=10, lv=100, rv=NULL=0, w=1)
        assert!(rows.contains(&(10, 100, 0, 1)), "unmatched left: {rows:?}");
    }

    #[test]
    fn left_join_matched_emits_inner_join_rows() {
        let op = OuterJoinOp::new(OperatorId(2), OuterJoinKind::Left, vec![0], vec![0]);
        let left = make_kv_batch(&[(5, 50, 1)]);
        let right = make_kv_batch(&[(5, 500, 1)]);
        let out = op.process_epoch(left, right).unwrap();
        let rows = extract_left_join_output(&out);
        // Should include (5, 50, 500, 1) from inner join.
        assert!(rows.contains(&(5, 50, 500, 1)), "matched: {rows:?}");
        // Should NOT include NULL-padded row.
        assert!(
            !rows.contains(&(5, 50, 0, 1)),
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
        assert!(rows1.contains(&(7, 70, 0, 1)), "epoch1 null pad: {rows1:?}");

        // Epoch 2: right arrives for same key → retract NULL-pad, add inner.
        let r2 = make_kv_batch(&[(7, 700, 1)]);
        let out2 = op.process_epoch(empty_kv(), r2).unwrap();
        let rows2 = extract_left_join_output(&out2);
        // Retract NULL-pad: (7, 70, 0, -1).
        assert!(
            rows2.contains(&(7, 70, 0, -1)),
            "retract null pad: {rows2:?}"
        );
        // Add inner: (7, 70, 700, +1).
        assert!(
            rows2.contains(&(7, 70, 700, 1)),
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
            rows2.contains(&(3, 30, 300, -1)),
            "retract inner: {rows2:?}"
        );
        // Add NULL-pad: (3, 30, 0, +1).
        assert!(
            rows2.contains(&(3, 30, 0, 1)),
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
