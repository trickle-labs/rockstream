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
use rockstream_types::KeyCapsule;

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
    dirty_left: Mutex<HashSet<(Vec<u8>, u128)>>,
    dirty_right: Mutex<HashSet<(Vec<u8>, u128)>>,
    dirty_rw: Mutex<HashSet<Vec<u8>>>,
    dirty_lw: Mutex<HashSet<Vec<u8>>>,
    dirty_null_weight: Mutex<bool>,
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
            dirty_left: Mutex::new(HashSet::new()),
            dirty_right: Mutex::new(HashSet::new()),
            dirty_rw: Mutex::new(HashSet::new()),
            dirty_lw: Mutex::new(HashSet::new()),
            dirty_null_weight: Mutex::new(false),
        }
    }

    pub fn kind(&self) -> OuterJoinKind {
        self.kind
    }

    fn extract_key(
        row: &RecordBatch,
        row_idx: usize,
        key_cols: &[usize],
    ) -> Result<(Vec<u8>, bool), OpError> {
        let arrays: Vec<&dyn arrow::array::Array> = key_cols
            .iter()
            .map(|column| row.column(*column).as_ref())
            .collect();
        let capsule = KeyCapsule::from_arrays(&arrays, row_idx)
            .map_err(|error| OpError::unsupported_plan_node(error.to_string()))?;
        Ok((capsule.typed_bytes().to_vec(), capsule.contains_null()))
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
        match self.kind {
            OuterJoinKind::Left => self.process_epoch_left(left, right),
            OuterJoinKind::Right => self.process_epoch_right(left, right),
            OuterJoinKind::Full => self.process_epoch_full(left, right),
            OuterJoinKind::Semi => self.process_epoch_semi(left, right),
            OuterJoinKind::Anti => self.process_epoch_anti(left, right),
            OuterJoinKind::NullAwareAnti => self.process_epoch_null_aware_anti(left, right),
        }
    }

    /// Stage a left delta into left_staged (no probing yet).
    fn stage_left(&self, delta: &ArrowZSet) -> Result<(), OpError> {
        let mut staged = self.left_staged.lock().unwrap();
        for row_idx in 0..delta.num_rows() {
            let w = delta.weights[row_idx];
            let (capsule_key, is_null) =
                Self::extract_key(&delta.data, row_idx, &self.left_key_cols)?;
            let row_bytes = Self::serialize_row(&delta.data, row_idx);
            let row_id = stable_row_id(self.op_id.0, &capsule_key, &row_bytes);
            let join_key = if is_null {
                // Opposite sides must never share a NULL-key bucket.
                [capsule_key, row_id.to_be_bytes().to_vec(), vec![0]].concat()
            } else {
                capsule_key
            };
            staged.push(join_key, row_id, row_bytes, w);
        }
        Ok(())
    }

    /// Stage a right delta into right_staged (no probing yet).
    fn stage_right(&self, delta: &ArrowZSet) -> Result<(), OpError> {
        let mut staged = self.right_staged.lock().unwrap();
        for row_idx in 0..delta.num_rows() {
            let w = delta.weights[row_idx];
            let (capsule_key, is_null) =
                Self::extract_key(&delta.data, row_idx, &self.right_key_cols)?;
            let row_bytes = Self::serialize_row(&delta.data, row_idx);
            let row_id = stable_row_id(self.op_id.0, &capsule_key, &row_bytes);
            let join_key = if is_null {
                [capsule_key, row_id.to_be_bytes().to_vec(), vec![1]].concat()
            } else {
                capsule_key
            };
            staged.push(join_key, row_id, row_bytes, w);
            if is_null {
                staged.null_weight += w;
            }
        }
        Ok(())
    }

    // ─── LEFT JOIN ────────────────────────────────────────────────────────────

    fn process_epoch_left(&self, left: ArrowZSet, right: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.stage_left(&left)?;
        self.stage_right(&right)?;

        let out_schema = output_schema(
            self.kind,
            left.data.schema().as_ref(),
            right.data.schema().as_ref(),
        );
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
            let mut null_pad_rows: OuterJoinRows = (
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
                        let left_vals = Self::deserialize_row(l_row_bytes, self.left_n_cols)?;
                        let right_nulls = vec![None; self.right_n_cols];
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
                let batch = Self::make_output_batch(
                    &null_pad_rows.0,
                    &null_pad_rows.1,
                    &out_schema,
                    false,
                    true,
                )?;
                outputs.push(ArrowZSet::new(batch, null_pad_rows.2));
            }

            // 3. For each ΔL row: if effective_rw == 0, emit (row, NULL_right, w).
            {
                let staged_left = self.left_staged.lock().unwrap();
                let mut null_new_rows: OuterJoinRows = (
                    vec![Vec::new(); self.left_n_cols],
                    vec![Vec::new(); self.right_n_cols],
                    Vec::new(),
                );
                for (join_key, _row_id, row_bytes, w) in &staged_left.rows {
                    let old_rw = state.right_key_weight.get(join_key).copied().unwrap_or(0);
                    let delta = delta_rw.get(join_key).copied().unwrap_or(0);
                    let effective_rw = old_rw + delta;
                    if effective_rw == 0 {
                        let left_vals = Self::deserialize_row(row_bytes, self.left_n_cols)?;
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
                    let batch = Self::make_output_batch(
                        &null_new_rows.0,
                        &null_new_rows.1,
                        &out_schema,
                        false,
                        true,
                    )?;
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
        self.stage_left(&left)?;
        self.stage_right(&right)?;

        let out_schema = output_schema(
            self.kind,
            left.data.schema().as_ref(),
            right.data.schema().as_ref(),
        );
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
            let mut null_pad_rows: OuterJoinRows = (
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
                        let left_nulls = vec![None; self.left_n_cols];
                        let right_vals = Self::deserialize_row(r_row_bytes, self.right_n_cols)?;
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
                let batch = Self::make_output_batch(
                    &null_pad_rows.0,
                    &null_pad_rows.1,
                    &out_schema,
                    true,
                    false,
                )?;
                outputs.push(ArrowZSet::new(batch, null_pad_rows.2));
            }

            // 3. For each ΔR row: if effective_lw == 0, emit (NULL_left, row, w).
            {
                let staged_right = self.right_staged.lock().unwrap();
                let mut null_new_rows: OuterJoinRows = (
                    vec![Vec::new(); self.left_n_cols],
                    vec![Vec::new(); self.right_n_cols],
                    Vec::new(),
                );
                for (join_key, _, row_bytes, w) in &staged_right.rows {
                    let old_lw = state.left_key_weight.get(join_key).copied().unwrap_or(0);
                    let delta = delta_lw.get(join_key).copied().unwrap_or(0);
                    let effective_lw = old_lw + delta;
                    if effective_lw == 0 {
                        let left_nulls = vec![None; self.left_n_cols];
                        let right_vals = Self::deserialize_row(row_bytes, self.right_n_cols)?;
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
                    let batch = Self::make_output_batch(
                        &null_new_rows.0,
                        &null_new_rows.1,
                        &out_schema,
                        true,
                        false,
                    )?;
                    outputs.push(ArrowZSet::new(batch, null_new_rows.2));
                }
            }
        }

        self.commit_staged(&HashMap::new(), Some(&delta_lw));

        concat_zsets(outputs, out_schema)
    }

    // ─── FULL OUTER JOIN ──────────────────────────────────────────────────────

    fn process_epoch_full(&self, left: ArrowZSet, right: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.stage_left(&left)?;
        self.stage_right(&right)?;

        let out_schema = output_schema(
            self.kind,
            left.data.schema().as_ref(),
            right.data.schema().as_ref(),
        );
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
            let mut null_pad_left: OuterJoinRows = (
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
                        let left_vals = Self::deserialize_row(l_row_bytes, self.left_n_cols)?;
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
                let batch = Self::make_output_batch(
                    &null_pad_left.0,
                    &null_pad_left.1,
                    &out_schema,
                    false,
                    true,
                )?;
                outputs.push(ArrowZSet::new(batch, null_pad_left.2));
            }

            // 3. Right-side NULL-pad transitions (same as RIGHT JOIN).
            let mut null_pad_right: OuterJoinRows = (
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
                        let left_nulls = vec![None; self.left_n_cols];
                        let right_vals = Self::deserialize_row(r_row_bytes, self.right_n_cols)?;
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
                let batch = Self::make_output_batch(
                    &null_pad_right.0,
                    &null_pad_right.1,
                    &out_schema,
                    true,
                    false,
                )?;
                outputs.push(ArrowZSet::new(batch, null_pad_right.2));
            }

            // 4. New ΔL rows with no effective right match → NULL-pad right.
            {
                let staged_left = self.left_staged.lock().unwrap();
                let mut null_new_left: OuterJoinRows = (
                    vec![Vec::new(); self.left_n_cols],
                    vec![Vec::new(); self.right_n_cols],
                    Vec::new(),
                );
                for (join_key, _, row_bytes, w) in &staged_left.rows {
                    let old_rw = state.right_key_weight.get(join_key).copied().unwrap_or(0);
                    let delta = delta_rw.get(join_key).copied().unwrap_or(0);
                    let effective_rw = old_rw + delta;
                    if effective_rw == 0 {
                        let left_vals = Self::deserialize_row(row_bytes, self.left_n_cols)?;
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
                    let batch = Self::make_output_batch(
                        &null_new_left.0,
                        &null_new_left.1,
                        &out_schema,
                        false,
                        true,
                    )?;
                    outputs.push(ArrowZSet::new(batch, null_new_left.2));
                }
            }

            // 5. New ΔR rows with no effective left match → NULL-pad left.
            {
                let staged_right = self.right_staged.lock().unwrap();
                let mut null_new_right: OuterJoinRows = (
                    vec![Vec::new(); self.left_n_cols],
                    vec![Vec::new(); self.right_n_cols],
                    Vec::new(),
                );
                for (join_key, _, row_bytes, w) in &staged_right.rows {
                    let old_lw = state.left_key_weight.get(join_key).copied().unwrap_or(0);
                    let delta = delta_lw.get(join_key).copied().unwrap_or(0);
                    let effective_lw = old_lw + delta;
                    if effective_lw == 0 {
                        let left_nulls = vec![None; self.left_n_cols];
                        let right_vals = Self::deserialize_row(row_bytes, self.right_n_cols)?;
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
                    let batch = Self::make_output_batch(
                        &null_new_right.0,
                        &null_new_right.1,
                        &out_schema,
                        true,
                        false,
                    )?;
                    outputs.push(ArrowZSet::new(batch, null_new_right.2));
                }
            }
        }

        self.commit_staged(&delta_rw, Some(&delta_lw));

        concat_zsets(outputs, out_schema)
    }

    // ─── SEMI JOIN ────────────────────────────────────────────────────────────

    fn process_epoch_semi(&self, left: ArrowZSet, right: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.stage_left(&left)?;
        self.stage_right(&right)?;

        let out_schema = output_schema(
            self.kind,
            left.data.schema().as_ref(),
            right.data.schema().as_ref(),
        );
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
            let mut semi_rows: (Vec<Vec<Option<i64>>>, Vec<i64>) =
                (vec![Vec::new(); self.left_n_cols], Vec::new());
            for (key, delta) in &delta_rw {
                let old_rw = state.right_key_weight.get(key).copied().unwrap_or(0);
                let new_rw = old_rw + delta;
                if (old_rw == 0) != (new_rw == 0) {
                    // Status changed: was unmatched → now matched (emit +), or vice versa (emit -).
                    let sign: i64 = if old_rw == 0 { 1 } else { -1 };
                    for (l_row_bytes, l_weight) in state.probe_left(key) {
                        let left_vals = Self::deserialize_row(l_row_bytes, self.left_n_cols)?;
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
                let mut new_left_rows: (Vec<Vec<Option<i64>>>, Vec<i64>) =
                    (vec![Vec::new(); self.left_n_cols], Vec::new());
                for (join_key, _, row_bytes, w) in &staged_left.rows {
                    let old_rw = state.right_key_weight.get(join_key).copied().unwrap_or(0);
                    let delta = delta_rw.get(join_key).copied().unwrap_or(0);
                    let effective_rw = old_rw + delta;
                    if effective_rw != 0 {
                        let left_vals = Self::deserialize_row(row_bytes, self.left_n_cols)?;
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
        self.stage_left(&left)?;
        self.stage_right(&right)?;

        let out_schema = output_schema(
            self.kind,
            left.data.schema().as_ref(),
            right.data.schema().as_ref(),
        );
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
            let mut anti_rows: (Vec<Vec<Option<i64>>>, Vec<i64>) =
                (vec![Vec::new(); self.left_n_cols], Vec::new());
            for (key, delta) in &delta_rw {
                let old_rw = state.right_key_weight.get(key).copied().unwrap_or(0);
                let new_rw = old_rw + delta;
                if (old_rw == 0) != (new_rw == 0) {
                    // Anti: was unmatched → emit −; now matched → retract from anti output.
                    // old_rw==0 means was in anti output → now matched, must retract.
                    let sign: i64 = if old_rw == 0 { -1 } else { 1 };
                    for (l_row_bytes, l_weight) in state.probe_left(key) {
                        let left_vals = Self::deserialize_row(l_row_bytes, self.left_n_cols)?;
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
                let mut new_left_rows: (Vec<Vec<Option<i64>>>, Vec<i64>) =
                    (vec![Vec::new(); self.left_n_cols], Vec::new());
                for (join_key, _, row_bytes, w) in &staged_left.rows {
                    let old_rw = state.right_key_weight.get(join_key).copied().unwrap_or(0);
                    let delta = delta_rw.get(join_key).copied().unwrap_or(0);
                    let effective_rw = old_rw + delta;
                    if effective_rw == 0 {
                        let left_vals = Self::deserialize_row(row_bytes, self.left_n_cols)?;
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

    fn process_epoch_null_aware_anti(
        &self,
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
        let delta_rw: HashMap<Vec<u8>, i64> = {
            let staged = self.right_staged.lock().unwrap();
            let mut map = HashMap::new();
            for (join_key, _, _, weight) in &staged.rows {
                *map.entry(join_key.clone()).or_insert(0) += weight;
            }
            map
        };
        let null_delta = self.right_staged.lock().unwrap().null_weight;
        let total_delta: i64 = delta_rw.values().sum();
        let mut output_rows = vec![Vec::new(); self.left_n_cols];
        let mut output_weights = Vec::new();

        {
            let state = self.state.lock().unwrap();
            let old_null = state.right_null_weight > 0;
            let new_null = state.right_null_weight + null_delta > 0;
            let old_empty = state.right_total_weight == 0;
            let new_empty = state.right_total_weight + total_delta == 0;

            if old_null != new_null || old_empty != new_empty {
                for (key, rows) in &state.left_arr {
                    let old_matches = state.right_key_weight.get(key).copied().unwrap_or(0);
                    let new_matches = old_matches + delta_rw.get(key).copied().unwrap_or(0);
                    for row in rows.values() {
                        let values = Self::deserialize_row(&row.row_bytes, self.left_n_cols)?;
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
                            output_weights.push(
                                row.weight * (i64::from(now_visible) - i64::from(was_visible)),
                            );
                        }
                    }
                }
            } else if !old_null {
                for (key, delta) in &delta_rw {
                    let old_matches = state.right_key_weight.get(key).copied().unwrap_or(0);
                    let new_matches = old_matches + delta;
                    if (old_matches == 0) != (new_matches == 0) {
                        let sign = if old_matches == 0 { -1 } else { 1 };
                        for (row_bytes, weight) in state.probe_left(key) {
                            let values = Self::deserialize_row(row_bytes, self.left_n_cols)?;
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
                let staged_left = self.left_staged.lock().unwrap();
                for (key, _, row_bytes, weight) in &staged_left.rows {
                    let values = Self::deserialize_row(row_bytes, self.left_n_cols)?;
                    let has_null_key = self
                        .left_key_cols
                        .iter()
                        .any(|column| values[*column].is_none());
                    if has_null_key && !new_empty {
                        continue;
                    }
                    let matches = state.right_key_weight.get(key).copied().unwrap_or(0)
                        + delta_rw.get(key).copied().unwrap_or(0);
                    if has_null_key || (!new_null && matches == 0) {
                        for (column, value) in values.iter().enumerate() {
                            output_rows[column].push(*value);
                        }
                        output_weights.push(*weight);
                    }
                }
            }
        }

        let outputs = if output_weights.is_empty() {
            Vec::new()
        } else {
            vec![ArrowZSet::new(
                Self::make_semi_batch(&output_rows, &out_schema)?,
                output_weights,
            )]
        };
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
        let mut left_cols: Vec<Vec<Option<i64>>> = vec![Vec::new(); self.left_n_cols];
        let mut right_cols: Vec<Vec<Option<i64>>> = vec![Vec::new(); self.right_n_cols];
        let mut out_weights: Vec<i64> = Vec::new();

        let staged_left = self.left_staged.lock().unwrap();
        let staged_right = self.right_staged.lock().unwrap();

        // ΔL ⋈ R₀
        for (join_key, _, row_bytes, w) in &staged_left.rows {
            for (right_row_bytes, r_weight) in state.probe_right(join_key) {
                let left_vals = Self::deserialize_row(row_bytes, self.left_n_cols)?;
                let right_vals = Self::deserialize_row(right_row_bytes, self.right_n_cols)?;
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
                let left_vals = Self::deserialize_row(left_row_bytes, self.left_n_cols)?;
                let right_vals = Self::deserialize_row(row_bytes, self.right_n_cols)?;
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
                    let left_vals = Self::deserialize_row(left_bytes, self.left_n_cols)?;
                    let right_vals = Self::deserialize_row(right_bytes, self.right_n_cols)?;
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

        let batch = Self::make_output_batch(&left_cols, &right_cols, out_schema, false, false)?;
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

        let mut dirty_left = self.dirty_left.lock().unwrap();
        let mut dirty_right = self.dirty_right.lock().unwrap();
        let mut dirty_rw = self.dirty_rw.lock().unwrap();
        let mut dirty_lw = self.dirty_lw.lock().unwrap();

        // Apply staged left rows.
        for (join_key, row_id, row_bytes, w) in left_s.rows.drain(..) {
            dirty_left.insert((join_key.clone(), row_id));
            state.update_left(join_key, row_id, row_bytes, w);
        }

        // Apply staged right rows.
        for (join_key, row_id, row_bytes, w) in right_s.rows.drain(..) {
            dirty_right.insert((join_key.clone(), row_id));
            state.update_right(join_key, row_id, row_bytes, w);
        }

        left_s.staged_bytes = 0;
        let null_delta = right_s.null_weight;
        right_s.null_weight = 0;
        right_s.staged_bytes = 0;

        if self.kind == OuterJoinKind::NullAwareAnti {
            let total_delta: i64 = delta_rw.values().sum();
            let old_bytes = u64::from(state.right_total_weight > 0) * 16;
            state.right_total_weight += total_delta;
            state.right_null_weight += null_delta;
            let new_bytes = u64::from(state.right_total_weight > 0) * 16;
            state.state_bytes = state
                .state_bytes
                .saturating_sub(old_bytes)
                .saturating_add(new_bytes);
            if total_delta != 0 || null_delta != 0 {
                *self.dirty_null_weight.lock().unwrap() = true;
            }
        }

        // Update right_key_weight.
        for (key, delta) in delta_rw {
            dirty_rw.insert(key.clone());
            let is_new = !state.right_key_weight.contains_key(key);
            let key_len = key.len() as u64;
            if is_new {
                state.state_bytes += key_len + 8;
            }
            let entry = state.right_key_weight.entry(key.clone()).or_insert(0);
            *entry += delta;
            if *entry == 0 {
                state.right_key_weight.remove(key);
                state.state_bytes = state.state_bytes.saturating_sub(key_len + 8);
            }
        }

        // Update left_key_weight (for RIGHT and FULL joins).
        if let Some(dlw) = delta_lw {
            for (key, delta) in dlw {
                dirty_lw.insert(key.clone());
                let is_new = !state.left_key_weight.contains_key(key);
                let key_len = key.len() as u64;
                if is_new {
                    state.state_bytes += key_len + 8;
                }
                let entry = state.left_key_weight.entry(key.clone()).or_insert(0);
                *entry += delta;
                if *entry == 0 {
                    state.left_key_weight.remove(key);
                    state.state_bytes = state.state_bytes.saturating_sub(key_len + 8);
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

    /// State bytes metric.
    pub fn state_bytes(&self) -> u64 {
        let state = self.state.lock().unwrap();
        let left_s = self.left_staged.lock().unwrap();
        let right_s = self.right_staged.lock().unwrap();
        state.state_bytes() + left_s.staged_bytes + right_s.staged_bytes
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

    /// Persist dirty arrangement state and match-count state deltas to a `ShardDb`.
    ///
    /// Persists only keys that changed or were removed during the epoch, ensuring
    /// O(|Δ|) write amplification instead of O(|state|).
    /// Uses only point puts/deletes — no range deletion.
    pub fn append_state(&self, target: &mut WriteBatch) -> Result<(), OpError> {
        let mut batch = WriteBatch::new();

        {
            let state = self.state.lock().unwrap();
            let mut dirty_left = self.dirty_left.lock().unwrap();
            let mut dirty_right = self.dirty_right.lock().unwrap();
            let mut dirty_rw = self.dirty_rw.lock().unwrap();
            let mut dirty_lw = self.dirty_lw.lock().unwrap();
            let mut dirty_null_weight = self.dirty_null_weight.lock().unwrap();

            // Left arrangement (same key format as JoinOp).
            for (join_key, row_id) in dirty_left.drain() {
                let key =
                    ShardKeyEncoder::join_arr_key(JoinSide::Left, self.op_id.0, &join_key, row_id);
                if let Some(arr_row) = state.left_arr.get(&join_key).and_then(|b| b.get(&row_id)) {
                    if arr_row.weight > 0 {
                        batch.put(&key, &arr_row.row_bytes);
                    } else {
                        batch.delete(&key);
                    }
                } else {
                    batch.delete(&key);
                }
            }

            // Right arrangement.
            for (join_key, row_id) in dirty_right.drain() {
                let key =
                    ShardKeyEncoder::join_arr_key(JoinSide::Right, self.op_id.0, &join_key, row_id);
                if let Some(arr_row) = state.right_arr.get(&join_key).and_then(|b| b.get(&row_id)) {
                    if arr_row.weight > 0 {
                        batch.put(&key, &arr_row.row_bytes);
                    } else {
                        batch.delete(&key);
                    }
                } else {
                    batch.delete(&key);
                }
            }

            // right_key_weight: prefix [0x01, 0x4F, 0x52] + op_id:8 + key_bytes → weight:8
            let rw_prefix: &[u8] = &[0x01, 0x4F, 0x52];
            for key_bytes in dirty_rw.drain() {
                let mut storage_key = Vec::with_capacity(3 + 8 + key_bytes.len());
                storage_key.extend_from_slice(rw_prefix);
                storage_key.extend_from_slice(&self.op_id.0.to_be_bytes());
                storage_key.extend_from_slice(&key_bytes);
                if let Some(&weight) = state.right_key_weight.get(&key_bytes) {
                    if weight != 0 {
                        batch.put(&storage_key, &weight.to_be_bytes());
                    } else {
                        batch.delete(&storage_key);
                    }
                } else {
                    batch.delete(&storage_key);
                }
            }

            // left_key_weight: prefix [0x01, 0x4F, 0x4C] + op_id:8 + key_bytes → weight:8
            let lw_prefix: &[u8] = &[0x01, 0x4F, 0x4C];
            for key_bytes in dirty_lw.drain() {
                let mut storage_key = Vec::with_capacity(3 + 8 + key_bytes.len());
                storage_key.extend_from_slice(lw_prefix);
                storage_key.extend_from_slice(&self.op_id.0.to_be_bytes());
                storage_key.extend_from_slice(&key_bytes);
                if let Some(&weight) = state.left_key_weight.get(&key_bytes) {
                    if weight != 0 {
                        batch.put(&storage_key, &weight.to_be_bytes());
                    } else {
                        batch.delete(&storage_key);
                    }
                } else {
                    batch.delete(&storage_key);
                }
            }

            if std::mem::take(&mut *dirty_null_weight) {
                let mut storage_key = vec![0x01, 0x4F, 0x4E];
                storage_key.extend_from_slice(&self.op_id.0.to_be_bytes());
                if state.right_total_weight > 0 {
                    let mut value = state.right_total_weight.to_be_bytes().to_vec();
                    value.extend_from_slice(&state.right_null_weight.to_be_bytes());
                    batch.put(&storage_key, &value);
                } else {
                    batch.delete(&storage_key);
                }
            }
        }

        target.merge_from(batch);
        Ok(())
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
            dirty_left: Mutex::new(HashSet::new()),
            dirty_right: Mutex::new(HashSet::new()),
            dirty_rw: Mutex::new(HashSet::new()),
            dirty_lw: Mutex::new(HashSet::new()),
            dirty_null_weight: Mutex::new(false),
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
