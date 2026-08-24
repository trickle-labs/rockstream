//! Bounded factorized join payloads for v0.59.7.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use rockstream_storage::{JoinSide, ShardDb, ShardKeyEncoder, WriteBatch};
use rockstream_types::ids::OperatorId;
use rockstream_types::KeyCapsule;

use crate::error::OpError;
use crate::governor::{
    DeltaAmplificationBudget, DeltaAmplificationCounters, DeltaAmplificationGovernor,
    DEFAULT_FACTORIZED_DELTA_BUDGET, FACTORIZED_SELECTION_RULE_VERSION,
};
use crate::join::stable_row_id;
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

#[derive(Debug, Clone, Default)]
struct PayloadState {
    left: HashMap<(Vec<u8>, Vec<Scalar>), i64>,
    right: HashMap<(Vec<u8>, Vec<Scalar>), i64>,
}

impl PayloadState {
    fn usage(&self) -> (usize, usize) {
        let rows = self.left.len() + self.right.len();
        let bytes = self
            .left
            .iter()
            .chain(self.right.iter())
            .map(|((key, values), _)| key.len() + scalar_bytes(values) + 8)
            .sum();
        (rows, bytes)
    }
}

/// A bounded factorized two-input PK/FK join whose public output is only an aggregate delta.
pub struct FactorizedJoinAggregateOp {
    op_id: OperatorId,
    left_key_cols: Vec<usize>,
    right_key_cols: Vec<usize>,
    left_n_cols: usize,
    right_n_cols: usize,
    group_col: usize,
    value_col: usize,
    kind: FactorizedAggregateKind,
    state: Mutex<PayloadState>,
    group_type: Mutex<Option<DataType>>,
    governor: DeltaAmplificationGovernor,
    max_payload_rows: usize,
    max_payload_bytes: usize,
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
        Self {
            op_id,
            left_key_cols,
            right_key_cols,
            left_n_cols,
            right_n_cols,
            group_col,
            value_col,
            kind,
            state: Mutex::new(PayloadState::default()),
            group_type: Mutex::new(None),
            governor: DeltaAmplificationGovernor::new(budget),
            max_payload_rows,
            max_payload_bytes,
        }
    }

    pub fn with_governor(mut self, governor: DeltaAmplificationGovernor) -> Self {
        self.governor = governor;
        self
    }

    pub fn governor(&self) -> &DeltaAmplificationGovernor {
        &self.governor
    }

    pub fn factor_payload_rows(&self) -> usize {
        self.state.lock().unwrap().usage().0
    }

    pub fn factor_payload_bytes(&self) -> usize {
        self.state.lock().unwrap().usage().1
    }

    pub fn op_id(&self) -> OperatorId {
        self.op_id
    }

    pub fn joined_intermediate_rows(&self) -> usize {
        0
    }

    pub fn state_bytes(&self) -> u64 {
        self.factor_payload_bytes() as u64
    }

    pub fn process_epoch(&self, left: ArrowZSet, right: ArrowZSet) -> Result<ArrowZSet, OpError> {
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
                return Err(OpError::column_out_of_bounds(
                    self.group_col,
                    self.left_n_cols + self.right_n_cols,
                ));
            }
            Some(right.data.column(group_column).data_type().clone())
        } else {
            None
        };
        let mut next = self.state.lock().unwrap().clone();
        let old_probes = (next.left.len() as u64).saturating_mul(next.right.len() as u64);
        let old_aggregate = self.aggregate(&next)?;
        let state_writes = self
            .apply_delta(&mut next, &left, true)?
            .saturating_add(self.apply_delta(&mut next, &right, false)?);
        let (rows, bytes) = next.usage();
        if rows > self.max_payload_rows || bytes > self.max_payload_bytes {
            return Err(OpError::factor_payload_overflow(
                rows,
                self.max_payload_rows,
                bytes,
                self.max_payload_bytes,
            ));
        }
        let new_probes = (next.left.len() as u64).saturating_mul(next.right.len() as u64);
        let new_aggregate = self.aggregate(&next)?;
        let output = self.output_delta(&old_aggregate, &new_aggregate)?;
        let delta_counters = DeltaAmplificationCounters {
            input_deltas: (left.num_rows() + right.num_rows()) as u64,
            probes: old_probes.saturating_add(new_probes),
            shuffled_bytes: 0,
            intermediate_tuples: 0,
            output_deltas: output.num_rows() as u64,
            state_writes,
        };
        if let Some(dimension) = self.governor.exceeded(delta_counters) {
            let current = self.governor.projected(delta_counters).get(dimension);
            return Err(OpError::delta_amplification_exceeded(
                dimension.name(),
                current,
                self.governor.limit(dimension),
            ));
        }
        *self.state.lock().unwrap() = next;
        self.governor.record(delta_counters);
        if let Some(group_type) = group_type {
            *self.group_type.lock().unwrap() = Some(group_type);
        }
        Ok(output)
    }

    fn apply_delta(
        &self,
        state: &mut PayloadState,
        delta: &ArrowZSet,
        left: bool,
    ) -> Result<u64, OpError> {
        if delta.is_empty() {
            return Ok(0);
        }
        let expected_cols = if left {
            self.left_n_cols
        } else {
            self.right_n_cols
        };
        if delta.data.num_columns() != expected_cols {
            return Err(OpError::unsupported_plan_node(format!(
                "factorized join side has {} columns; expected {expected_cols}",
                delta.data.num_columns()
            )));
        }
        let key_cols = if left {
            &self.left_key_cols
        } else {
            &self.right_key_cols
        };
        let target = if left {
            &mut state.left
        } else {
            &mut state.right
        };
        if let Some(&column) = key_cols.iter().find(|column| **column >= expected_cols) {
            return Err(OpError::column_out_of_bounds(column, expected_cols));
        }
        let mut state_writes = 0_u64;
        for row in 0..delta.num_rows() {
            let values = delta
                .data
                .columns()
                .iter()
                .map(|column| Scalar::from_array(column, row))
                .collect::<Result<Vec<_>, _>>()?;
            let key_arrays: Vec<&dyn arrow::array::Array> = key_cols
                .iter()
                .map(|column| delta.data.column(*column).as_ref())
                .collect();
            let capsule = KeyCapsule::from_arrays(&key_arrays, row)
                .map_err(|error| OpError::unsupported_plan_node(error.to_string()))?;
            if capsule.contains_null() {
                continue;
            }
            let key = capsule.typed_bytes().to_vec();
            let row_key = (key, values);
            let old_weight = target.get(&row_key).copied().unwrap_or(0);
            let weight = old_weight
                .checked_add(delta.weights[row])
                .ok_or_else(|| OpError::internal("factorized payload weight overflow"))?;
            state_writes += u64::from(weight != old_weight);
            if weight == 0 {
                target.remove(&row_key);
            } else {
                target.insert(row_key, weight);
            }
        }
        Ok(state_writes)
    }

    fn aggregate(&self, state: &PayloadState) -> Result<BTreeMap<Scalar, (i64, i64)>, OpError> {
        let mut output = BTreeMap::new();
        // ponytail: bounded O(n²) payload scan; replace with keyed dimension indexes if
        // the named payload bound becomes too large for the single-shard path.
        for ((left_key, left_values), left_weight) in &state.left {
            for ((right_key, right_values), right_weight) in &state.right {
                if left_key != right_key {
                    continue;
                }
                let weight = left_weight.checked_mul(*right_weight).ok_or_else(|| {
                    OpError::internal("factorized join weight multiplication overflow")
                })?;
                let mut joined = left_values.clone();
                joined.extend_from_slice(right_values);
                let group = joined
                    .get(self.group_col)
                    .ok_or_else(|| OpError::column_out_of_bounds(self.group_col, joined.len()))?
                    .clone();
                let value: i64 = match self.kind {
                    FactorizedAggregateKind::Count => 1,
                    FactorizedAggregateKind::Sum | FactorizedAggregateKind::Avg => joined
                        .get(self.value_col)
                        .ok_or_else(|| OpError::column_out_of_bounds(self.value_col, joined.len()))?
                        .as_i64()?,
                };
                let entry = output.entry(group).or_insert((0_i64, 0_i64));
                entry.0 = entry
                    .0
                    .checked_add(value.checked_mul(weight).ok_or_else(|| {
                        OpError::internal("factorized aggregate value multiplication overflow")
                    })?)
                    .ok_or_else(|| OpError::internal("factorized aggregate sum overflow"))?;
                entry.1 = entry
                    .1
                    .checked_add(weight)
                    .ok_or_else(|| OpError::internal("factorized aggregate count overflow"))?;
            }
        }
        output.retain(|_, (_, count)| *count != 0);
        Ok(output)
    }

    fn output_delta(
        &self,
        old: &BTreeMap<Scalar, (i64, i64)>,
        new: &BTreeMap<Scalar, (i64, i64)>,
    ) -> Result<ArrowZSet, OpError> {
        let mut rows = Vec::new();
        for key in old.keys().chain(new.keys()) {
            if rows.iter().any(|(existing, _, _)| existing == key) {
                continue;
            }
            let old_value = old.get(key).map(|value| self.result_value(*value));
            let new_value = new.get(key).map(|value| self.result_value(*value));
            if old_value == new_value {
                continue;
            }
            if let Some(value) = old_value {
                rows.push((key.clone(), value, -1));
            }
            if let Some(value) = new_value {
                rows.push((key.clone(), value, 1));
            }
        }
        let group_type = self.group_type.lock().unwrap().clone().or_else(|| {
            old.keys().chain(new.keys()).find_map(|key| match key {
                Scalar::Utf8(_) => Some(DataType::Utf8),
                Scalar::Int64(_) => Some(DataType::Int64),
                Scalar::Null => None,
            })
        });
        self.make_output(rows, group_type)
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

    pub fn append_state(&self, target: &mut WriteBatch) -> Result<(), OpError> {
        let state = self.state.lock().unwrap();
        for (side, entries) in [
            (JoinSide::Left, &state.left),
            (JoinSide::Right, &state.right),
        ] {
            for ((key, values), weight) in entries {
                let row_bytes = encode_row(values, *weight);
                let row_id = stable_row_id(self.op_id.0, key, &row_bytes);
                let storage_key =
                    ShardKeyEncoder::factor_payload_key(side, self.op_id.0, key, row_id);
                target.put(&storage_key, &row_bytes);
            }
        }
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
        Ok(())
    }

    pub async fn append_state_with_db(
        &self,
        db: &ShardDb,
        target: &mut WriteBatch,
    ) -> Result<(), OpError> {
        let mut current = HashSet::new();
        {
            let state = self.state.lock().unwrap();
            for (side, entries) in [
                (JoinSide::Left, &state.left),
                (JoinSide::Right, &state.right),
            ] {
                for (key, values) in entries.keys() {
                    let row_bytes = encode_row(
                        values,
                        *entries.get(&(key.clone(), values.clone())).unwrap(),
                    );
                    let row_id = stable_row_id(self.op_id.0, key, &row_bytes);
                    current.insert(ShardKeyEncoder::factor_payload_key(
                        side,
                        self.op_id.0,
                        key,
                        row_id,
                    ));
                }
            }
        }
        for side in [JoinSide::Left, JoinSide::Right] {
            let prefix = ShardKeyEncoder::factor_payload_op_prefix(side, self.op_id.0);
            let (entries, truncated) = db
                .scan_prefix_bounded(&prefix, MAX_FACTOR_PAYLOAD_BYTES)
                .await
                .map_err(OpError::storage)?;
            if truncated {
                return Err(OpError::factor_payload_overflow(
                    self.factor_payload_rows(),
                    self.max_payload_rows,
                    MAX_FACTOR_PAYLOAD_BYTES,
                    self.max_payload_bytes,
                ));
            }
            for (key, _) in entries {
                if !current.contains(key.as_ref()) {
                    target.delete(&key);
                }
            }
        }
        self.append_state(target)
    }

    pub async fn restore_in_place(&self, db: &ShardDb) -> Result<(), OpError> {
        let mut restored = PayloadState::default();
        for (side, expected_cols) in [
            (JoinSide::Left, self.left_n_cols),
            (JoinSide::Right, self.right_n_cols),
        ] {
            let prefix = ShardKeyEncoder::factor_payload_op_prefix(side, self.op_id.0);
            let (entries, truncated) = db
                .scan_prefix_bounded(&prefix, MAX_FACTOR_PAYLOAD_BYTES)
                .await
                .map_err(OpError::storage)?;
            if truncated {
                return Err(OpError::factor_payload_overflow(
                    self.factor_payload_rows(),
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
                let values = decode_row(&value, expected_cols)?;
                let weight = values.1;
                let entry = (key[key_start..key_end].to_vec(), values.0);
                if side == JoinSide::Left {
                    restored.left.insert(entry, weight);
                } else {
                    restored.right.insert(entry, weight);
                }
            }
        }
        let (rows, bytes) = restored.usage();
        if rows > self.max_payload_rows || bytes > self.max_payload_bytes {
            return Err(OpError::factor_payload_overflow(
                rows,
                self.max_payload_rows,
                bytes,
                self.max_payload_bytes,
            ));
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
        *self.state.lock().unwrap() = restored;
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

fn encode_row(values: &[Scalar], weight: i64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(scalar_bytes(values) + 8);
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

/// Minimal bounded payload-tree owner used by star-join planning and limit checks.
pub struct FactorizedStarJoinOp {
    dimensions: usize,
    rows: usize,
    bytes: usize,
    max_rows: usize,
    max_bytes: usize,
    fact: HashMap<(Vec<u8>, Vec<Scalar>), i64>,
    dimension_state: Vec<HashMap<Vec<u8>, i64>>,
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
            fact: HashMap::new(),
            dimension_state: vec![HashMap::new(); dimensions],
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

    /// Apply one fact delta and one delta per dimension, returning only the
    /// aggregate delta. Each fact row is multiplied by the dimension key
    /// counts; no fact×dimension row is materialized.
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
        let old = star_aggregate(&self.fact, &self.dimension_state, group_col, value_col)?;
        let mut next_fact = self.fact.clone();
        let mut next_dimensions = self.dimension_state.clone();
        apply_star_fact_delta(&mut next_fact, &fact, fact_key_col)?;
        for (delta, &key_col) in dimensions.iter().zip(dimension_key_cols) {
            if key_col >= delta.data.num_columns() {
                return Err(OpError::column_out_of_bounds(
                    key_col,
                    delta.data.num_columns(),
                ));
            }
        }
        for (index, (delta, &key_col)) in dimensions.iter().zip(dimension_key_cols).enumerate() {
            apply_star_dimension_delta(&mut next_dimensions[index], delta, key_col)?;
        }
        let (rows, bytes) = star_usage(&next_fact, &next_dimensions);
        if rows > self.max_rows || bytes > self.max_bytes {
            return Err(OpError::factor_payload_overflow(
                rows,
                self.max_rows,
                bytes,
                self.max_bytes,
            ));
        }
        let new = star_aggregate(&next_fact, &next_dimensions, group_col, value_col)?;
        let output = star_output_delta(&old, &new)?;
        self.fact = next_fact;
        self.dimension_state = next_dimensions;
        self.rows = rows;
        self.bytes = bytes;
        Ok(output)
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

fn apply_star_fact_delta(
    state: &mut HashMap<(Vec<u8>, Vec<Scalar>), i64>,
    delta: &ArrowZSet,
    key_col: usize,
) -> Result<(), OpError> {
    for row in 0..delta.num_rows() {
        let values = delta
            .data
            .columns()
            .iter()
            .map(|column| Scalar::from_array(column, row))
            .collect::<Result<Vec<_>, _>>()?;
        let capsule = KeyCapsule::from_array(delta.data.column(key_col), row)
            .map_err(|error| OpError::unsupported_plan_node(error.to_string()))?;
        if capsule.contains_null() {
            continue;
        }
        let row_key = (capsule.typed_bytes().to_vec(), values);
        let weight = state
            .get(&row_key)
            .copied()
            .unwrap_or(0)
            .checked_add(delta.weights[row])
            .ok_or_else(|| OpError::internal("star fact weight overflow"))?;
        if weight == 0 {
            state.remove(&row_key);
        } else {
            state.insert(row_key, weight);
        }
    }
    Ok(())
}

fn apply_star_dimension_delta(
    state: &mut HashMap<Vec<u8>, i64>,
    delta: &ArrowZSet,
    key_col: usize,
) -> Result<(), OpError> {
    for row in 0..delta.num_rows() {
        let capsule = KeyCapsule::from_array(delta.data.column(key_col), row)
            .map_err(|error| OpError::unsupported_plan_node(error.to_string()))?;
        if capsule.contains_null() {
            continue;
        }
        let key = capsule.typed_bytes().to_vec();
        let weight = state
            .get(&key)
            .copied()
            .unwrap_or(0)
            .checked_add(delta.weights[row])
            .ok_or_else(|| OpError::internal("star dimension weight overflow"))?;
        if weight == 0 {
            state.remove(&key);
        } else {
            state.insert(key, weight);
        }
    }
    Ok(())
}

fn star_usage(
    fact: &HashMap<(Vec<u8>, Vec<Scalar>), i64>,
    dimensions: &[HashMap<Vec<u8>, i64>],
) -> (usize, usize) {
    let rows = fact.len() + dimensions.iter().map(HashMap::len).sum::<usize>();
    let bytes = fact
        .iter()
        .map(|((key, values), _)| key.len() + scalar_bytes(values) + 8)
        .sum::<usize>()
        + dimensions
            .iter()
            .flat_map(|state| state.iter())
            .map(|(key, _)| key.len() + 8)
            .sum::<usize>();
    (rows, bytes)
}

fn star_aggregate(
    fact: &HashMap<(Vec<u8>, Vec<Scalar>), i64>,
    dimensions: &[HashMap<Vec<u8>, i64>],
    group_col: usize,
    value_col: usize,
) -> Result<BTreeMap<Scalar, i64>, OpError> {
    let mut output = BTreeMap::new();
    for ((key, values), fact_weight) in fact {
        let multiplicity = dimensions.iter().try_fold(*fact_weight, |total, state| {
            total
                .checked_mul(state.get(key).copied().unwrap_or(0))
                .ok_or_else(|| OpError::internal("star join multiplicity overflow"))
        })?;
        if multiplicity == 0 {
            continue;
        }
        let group = values
            .get(group_col)
            .ok_or_else(|| OpError::column_out_of_bounds(group_col, values.len()))?
            .clone();
        let value = values
            .get(value_col)
            .ok_or_else(|| OpError::column_out_of_bounds(value_col, values.len()))?
            .as_i64()?;
        let contribution = value
            .checked_mul(multiplicity)
            .ok_or_else(|| OpError::internal("star aggregate overflow"))?;
        let entry: &mut i64 = output.entry(group).or_insert(0_i64);
        *entry = (*entry)
            .checked_add(contribution)
            .ok_or_else(|| OpError::internal("star aggregate overflow"))?;
    }
    output.retain(|_, value| *value != 0);
    Ok(output)
}

fn star_output_delta(
    old: &BTreeMap<Scalar, i64>,
    new: &BTreeMap<Scalar, i64>,
) -> Result<ArrowZSet, OpError> {
    let mut rows = Vec::new();
    for group in old.keys().chain(new.keys()) {
        if rows.iter().any(|(existing, _, _)| existing == group) {
            continue;
        }
        match (old.get(group), new.get(group)) {
            (Some(old), Some(new)) if old == new => {}
            (Some(old), Some(new)) => {
                rows.push((group.clone(), *old, -1));
                rows.push((group.clone(), *new, 1));
            }
            (Some(old), None) => rows.push((group.clone(), *old, -1)),
            (None, Some(new)) => rows.push((group.clone(), *new, 1)),
            (None, None) => {}
        }
    }
    let group_type = old
        .keys()
        .chain(new.keys())
        .find_map(|group| match group {
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
