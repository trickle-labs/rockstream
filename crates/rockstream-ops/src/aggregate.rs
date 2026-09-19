//! Incremental aggregate operator (v0.5 — IVM-2).
//!
//! `AggregateOp` implements the DBSP delta rule for GROUP BY aggregates:
//!
//! ```text
//! For each incoming row (k, v) with weight w:
//!   1. Look up old state (sum, count) for group key k.
//!   2. Compute new_sum = old_sum + v * w, new_count = old_count + w.
//!   3. If old_count > 0 → retract: emit (k, old_sum, old_count, avg) with weight -1.
//!   4. If new_count > 0 → insert:  emit (k, new_sum, new_count, avg) with weight +1.
//!   5. Update state: remove k if new_count == 0, else store (new_sum, new_count).
//! ```
//!
//! ## Input schema
//!
//! Two Int64 columns: `k` (group key) and `v` (value to aggregate).
//!
//! ## Output schema
//!
//! `k` (Int64, group key), `sum_v` (Int64), `count` (Int64), `avg_v`
//! (Float64, `avg_v = sum_v / count` as correct floating-point division).
//!
//! ## State persistence
//!
//! `AggregateOp` optionally persists its arrangement to a `ShardDb` under the
//! `op_state` namespace so that state survives shard restart:
//!
//! - key:   `[0x01 (OpState)][op_id: 8 bytes BE][group_key: 8 bytes BE]`
//! - value: `[sum: 8 bytes BE][count: 8 bytes BE]`
//!
//! Call `persist_state(db)` after each epoch commit to write the full
//! arrangement.  Call `AggregateOp::load_from_storage(db, op_id)` on restart
//! to restore state.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::array::{Array, ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use tracing::debug;

use rockstream_plan::virtual_bucket::{
    normalize_power_of_two_bucket_count, route_power_of_two_bucket,
};
use rockstream_storage::{ShardDb, ShardKeyEncoder, ShardPrefix, WriteBatch};
use rockstream_types::ids::OperatorId;
use rockstream_types::laws::arithmetic::{
    checked_add_i64, checked_i128_to_i64, checked_mul_i64, decode_i64, decode_u64, encode_i64,
    encode_u64,
};
use rockstream_types::laws::sum_count::avg_from_sum_count;

use crate::error::OpError;
use crate::op::Operator;
use crate::zset::ArrowZSet;

// ─── Schema ──────────────────────────────────────────────────────────────────

/// Output schema for the aggregate operator.
fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("sum_v", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
        Field::new("avg_v", DataType::Float64, false),
    ]))
}

/// Converts scaled integer aggregate state to exact decimal text for storage.
pub struct DecimalAggregateFormatOp {
    scale: i8,
    average: bool,
}

impl DecimalAggregateFormatOp {
    pub fn new(scale: i8, average: bool) -> Self {
        Self { scale, average }
    }

    fn format_scaled(value: i64, scale: i8) -> String {
        let divisor = 10_i64.pow(scale as u32) as u64;
        let sign = if value < 0 { "-" } else { "" };
        let value = value.unsigned_abs();
        let whole = value / divisor;
        let fraction = format!("{:0width$}", value % divisor, width = scale as usize)
            .trim_end_matches('0')
            .to_string();
        if scale == 0 || fraction.is_empty() {
            format!("{sign}{whole}")
        } else {
            format!("{sign}{whole}.{fraction}")
        }
    }

    fn format_average(sum: i64, count: i64, scale: i8) -> Result<String, OpError> {
        if count == 0 {
            return Err(OpError::unimplemented("decimal average with zero count"));
        }
        let negative = (sum < 0) != (count < 0);
        let numerator = sum.unsigned_abs() as u128;
        let denominator = (count.unsigned_abs() as u128) * 10_u128.pow(scale as u32);
        let whole = numerator / denominator;
        let mut remainder = numerator % denominator;
        let mut fraction = String::new();
        for _ in 0..18 {
            if remainder == 0 {
                break;
            }
            remainder *= 10;
            fraction.push((b'0' + (remainder / denominator) as u8) as char);
            remainder %= denominator;
        }
        if remainder != 0 {
            return Err(OpError::unimplemented(
                "decimal average requires more than 18 decimal places",
            ));
        }
        let sign = if negative { "-" } else { "" };
        Ok(if fraction.is_empty() {
            format!("{sign}{whole}")
        } else {
            format!("{sign}{whole}.{fraction}")
        })
    }
}

impl Operator for DecimalAggregateFormatOp {
    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("agg", DataType::Utf8, false),
        ]));
        if delta.is_empty() {
            return Ok(ArrowZSet::empty(schema));
        }
        let keys = delta
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                OpError::column_type_mismatch(
                    "Int64",
                    format!("{:?}", delta.data.column(0).data_type()),
                )
            })?;
        let sums = delta
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                OpError::column_type_mismatch(
                    "Int64",
                    format!("{:?}", delta.data.column(1).data_type()),
                )
            })?;
        let counts = delta
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                OpError::column_type_mismatch(
                    "Int64",
                    format!("{:?}", delta.data.column(2).data_type()),
                )
            })?;
        let values = (0..delta.num_rows())
            .map(|row| {
                if self.average {
                    Self::format_average(sums.value(row), counts.value(row), self.scale)
                } else {
                    Ok(Self::format_scaled(sums.value(row), self.scale))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let data = RecordBatch::try_new(
            schema,
            vec![Arc::new(keys.clone()), Arc::new(StringArray::from(values))],
        )
        .map_err(OpError::arrow)?;
        Ok(ArrowZSet::new(data, delta.weights))
    }

    fn name(&self) -> &str {
        "DecimalAggregateFormatOp"
    }
}

// ─── AggState ────────────────────────────────────────────────────────────────

/// In-memory aggregate arrangement: group_key → (sum, count).
///
/// Only entries with count > 0 are stored; entries are removed when count
/// reaches 0 (group deleted).
///
/// # Bound
///
/// The arrangement is bounded by the number of distinct group keys in the input
/// stream.  The fill level is tracked via `entry_count()`.
fn encode_state_mutation(
    op_id: OperatorId,
    group_key: i64,
    state: Option<(i64, i64)>,
) -> rockstream_types::state_mutation::StateMutation {
    let key = ShardKeyEncoder::encode(ShardPrefix::OpState, op_id.0, &group_key.to_be_bytes());
    match state {
        Some((sum, count)) => {
            let mut value = [0u8; 16];
            value[..8].copy_from_slice(&encode_i64(sum));
            value[8..].copy_from_slice(&encode_i64(count));
            rockstream_types::state_mutation::StateMutation::Put {
                key,
                value: bytes::Bytes::copy_from_slice(&value),
            }
        }
        None => rockstream_types::state_mutation::StateMutation::Delete { key },
    }
}

#[derive(Debug, Default)]
pub struct AggState {
    /// Group key → (sum_v, count).
    entries: HashMap<i64, (i64, i64)>,
}

impl AggState {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Number of live groups (fill level metric).
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// State bytes metric (24 bytes per (k, sum, count) tuple).
    pub fn state_bytes(&self) -> u64 {
        (self.entries.len() * 24) as u64
    }

    /// Lookup state for a group key.
    pub fn get(&self, k: &i64) -> Option<(i64, i64)> {
        self.entries.get(k).copied()
    }

    /// Insert or update state for a group key.
    pub fn insert(&mut self, k: i64, val: (i64, i64)) {
        self.entries.insert(k, val);
    }

    /// Remove a group key from state.
    pub fn remove(&mut self, k: &i64) -> Option<(i64, i64)> {
        self.entries.remove(k)
    }

    /// Check if state contains a group key.
    pub fn contains_key(&self, k: &i64) -> bool {
        self.entries.contains_key(k)
    }

    /// Immutable reference to the underlying entries map.
    pub fn entries(&self) -> &HashMap<i64, (i64, i64)> {
        &self.entries
    }

    /// Apply one delta `(key, value_delta * weight)` to the arrangement.
    ///
    /// Returns `(old_state, new_state)` where each is `Option<(sum, count)>`.
    /// `old_state` is `None` when the group did not previously exist.
    /// `new_state` is `None` when the group count drops to zero (group deleted).
    #[allow(clippy::type_complexity)]
    pub fn apply_delta(
        &mut self,
        k: i64,
        v: i64,
        w: i64,
    ) -> Result<(Option<(i64, i64)>, Option<(i64, i64)>), OpError> {
        let (old_sum, old_count) = self.entries.get(&k).copied().unwrap_or((0, 0));
        let old_state = if old_count > 0 {
            Some((old_sum, old_count))
        } else {
            None
        };
        if old_count < 0 || (old_count == 0 && old_sum != 0) {
            return Err(OpError::invalid_literal(format!(
                "aggregate group {k} has an invalid existing state"
            )));
        }

        // Calculate every candidate before changing the arrangement.
        let contribution = checked_mul_i64(v, w).map_err(|_| OpError::aggregate_overflow(k))?;
        let new_count =
            checked_add_i64(old_count, w).map_err(|_| OpError::aggregate_overflow(k))?;
        if new_count < 0 {
            return Err(OpError::invalid_multiplicity(k, new_count));
        }
        let next =
            rockstream_verified::aggregate::transition(old_sum, old_count, contribution as i128, w)
                .ok_or_else(|| {
                    if new_count == 0 {
                        OpError::invalid_literal(format!(
                            "aggregate group {k} has zero count with nonzero sum"
                        ))
                    } else {
                        OpError::aggregate_overflow(k)
                    }
                })?;

        let new_state = if new_count > 0 {
            self.entries.insert(k, next);
            Some(next)
        } else {
            self.entries.remove(&k);
            None
        };

        Ok((old_state, new_state))
    }

    /// Encode this state as a `WriteBatch` for the `op_state` namespace.
    ///
    /// Call after each epoch commit to persist state to `ShardDb`.
    pub fn encode_as_write_batch(&self, op_id: OperatorId) -> WriteBatch {
        let mut wb = WriteBatch::new();
        for (&k, &(sum, count)) in &self.entries {
            let key = ShardKeyEncoder::encode(ShardPrefix::OpState, op_id.0, &k.to_be_bytes());
            let mut value = [0u8; 16];
            value[..8].copy_from_slice(&encode_i64(sum));
            value[8..].copy_from_slice(&encode_i64(count));
            wb.put(&key, &value);
        }
        wb
    }

    /// Encode only the mutations for the dirty keys as a `Vec<StateMutation>`.
    pub fn encode_mutations_for_keys(
        &self,
        op_id: OperatorId,
        dirty_keys: &[i64],
    ) -> Vec<rockstream_types::state_mutation::StateMutation> {
        dirty_keys
            .iter()
            .map(|&key| encode_state_mutation(op_id, key, self.entries.get(&key).copied()))
            .collect()
    }

    /// Decode from the raw entries stored by a previous `encode_as_write_batch`.
    ///
    /// `raw_entries` is the result of scanning the `op_state` namespace for
    /// the given `op_id`.
    pub fn decode_from_entries(
        raw_entries: &[(bytes::Bytes, bytes::Bytes)],
        op_id: OperatorId,
    ) -> Result<Self, OpError> {
        let op_prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, op_id.0);
        let invalid_key = || {
            OpError::internal(format!(
                "corrupt persisted aggregate state for operator {}: invalid key",
                op_id.0
            ))
        };
        let invalid_value = || {
            OpError::internal(format!(
                "corrupt persisted aggregate state for operator {}: invalid value",
                op_id.0
            ))
        };
        let mut state = AggState::new();
        for (key, value) in raw_entries {
            // Strip the operator prefix to get the group key bytes.
            if key.len() != op_prefix.len() + 8 || !key.starts_with(&op_prefix) {
                return Err(invalid_key());
            }
            let k_bytes: [u8; 8] = key[op_prefix.len()..op_prefix.len() + 8]
                .try_into()
                .map_err(|_| invalid_key())?;
            if value.len() != 16 {
                return Err(invalid_value());
            }
            let sum_bytes: [u8; 8] = value[..8].try_into().map_err(|_| invalid_value())?;
            let count_bytes: [u8; 8] = value[8..16].try_into().map_err(|_| invalid_value())?;
            let k = decode_i64(&k_bytes).map_err(|_| invalid_key())?;
            let sum = decode_i64(&sum_bytes).map_err(|_| invalid_value())?;
            let count = decode_i64(&count_bytes).map_err(|_| invalid_value())?;
            if count <= 0 {
                return Err(OpError::internal(format!(
                    "corrupt persisted aggregate state for operator {}: non-positive count",
                    op_id.0
                )));
            }
            state.entries.insert(k, (sum, count));
        }
        Ok(state)
    }
}

// ─── StagedEpochAggregator ──────────────────────────────────────────────────

/// Named upper bound for epoch consolidation distinct group keys.
pub const MAX_EPOCH_CONSOLIDATION_GROUPS: usize = 1_000_000;
/// Named upper bound for epoch consolidation staging memory (64 MiB).
pub const MAX_EPOCH_CONSOLIDATION_BYTES: usize = 64 * 1024 * 1024;
#[allow(dead_code)]
pub const MAX_AGGREGATE_RESTORE_BYTES: usize = 64 * 1024 * 1024;

/// In-memory staged accumulator for epoch group input consolidation.
/// Consolidates inputs per group key `k`, computes net sum/count in i128/i64,
/// and enforces checked arithmetic and strict resource bounds.
#[derive(Debug, Clone)]
pub struct StagedEpochAggregator {
    /// Group key -> (net_delta_sum in i128, net_delta_count in i64).
    pub entries: HashMap<i64, (i128, i64)>,
    /// Insertion order of group keys for deterministic output ordering.
    pub order: Vec<i64>,
    max_groups: usize,
    max_bytes: usize,
    estimated_bytes: usize,
}

impl StagedEpochAggregator {
    pub fn new() -> Self {
        Self::with_limits(
            MAX_EPOCH_CONSOLIDATION_GROUPS,
            MAX_EPOCH_CONSOLIDATION_BYTES,
        )
    }

    pub fn with_limits(max_groups: usize, max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: Vec::new(),
            max_groups,
            max_bytes,
            estimated_bytes: 0,
        }
    }

    pub fn group_count(&self) -> usize {
        self.entries.len()
    }

    pub fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Ingest a single (key, value, weight) delta.
    ///
    /// Checks:
    /// 1. Weight != 0 (weight == 0 is a no-op).
    /// 2. Per-row multiplication overflow: `v.checked_mul(w)`.
    /// 3. Resource bounds: group count limit and byte memory limit.
    /// 4. Net group delta accumulation in i128 (sum) and i64 (count).
    pub fn ingest_delta(&mut self, k: i64, v: i64, w: i64) -> Result<(), OpError> {
        if w == 0 {
            return Ok(());
        }

        // Per-row multiplication checked strictly upon ingestion
        let prod = checked_mul_i64(v, w).map_err(|_| OpError::aggregate_overflow(k))?;

        if let Some((sum, count)) = self.entries.get_mut(&k) {
            let next_sum = sum
                .checked_add(prod as i128)
                .ok_or_else(|| OpError::aggregate_overflow(k))?;
            let next_count =
                checked_add_i64(*count, w).map_err(|_| OpError::aggregate_overflow(k))?;
            *sum = next_sum;
            *count = next_count;
        } else {
            let next_groups = self.entries.len().checked_add(1).ok_or_else(|| {
                OpError::capacity_exceeded(
                    "epoch consolidation groups",
                    usize::MAX,
                    self.max_groups,
                    "reduce epoch distinct groups or increase MAX_EPOCH_CONSOLIDATION_GROUPS",
                )
            })?;
            if next_groups > self.max_groups {
                return Err(OpError::capacity_exceeded(
                    "epoch consolidation groups",
                    next_groups,
                    self.max_groups,
                    "reduce epoch distinct groups or increase MAX_EPOCH_CONSOLIDATION_GROUPS",
                ));
            }

            // Approximate memory: 8 bytes key + 24 bytes (i128, i64) + 8 bytes order + 24 bytes map overhead = 64 bytes
            const ENTRY_ESTIMATED_BYTES: usize = 64;
            let next_bytes = self
                .estimated_bytes
                .checked_add(ENTRY_ESTIMATED_BYTES)
                .ok_or_else(|| {
                    OpError::capacity_exceeded(
                        "epoch consolidation bytes",
                        usize::MAX,
                        self.max_bytes,
                        "reduce epoch batch size or increase MAX_EPOCH_CONSOLIDATION_BYTES",
                    )
                })?;
            if next_bytes > self.max_bytes {
                return Err(OpError::capacity_exceeded(
                    "epoch consolidation bytes",
                    next_bytes,
                    self.max_bytes,
                    "reduce epoch batch size or increase MAX_EPOCH_CONSOLIDATION_BYTES",
                ));
            }

            self.entries.insert(k, (prod as i128, w));
            self.order.push(k);
            self.estimated_bytes = next_bytes;
        }

        Ok(())
    }
}

impl Default for StagedEpochAggregator {
    fn default() -> Self {
        Self::new()
    }
}

// ─── AggregateOp ─────────────────────────────────────────────────────────────

/// Named upper bound for clean LRU capacity in AggregateOp.
pub const MAX_CLEAN_LRU_CAPACITY: usize = 262_144;

/// Stateful incremental aggregate operator.
///
/// Input:  two Int64 columns `(k, v)`.
/// Output: `(k, sum_v, count, avg_v)` — first three columns Int64, `avg_v` Float64.
///
/// Uses interior mutability (`Mutex`) so it satisfies `Operator: &self`.
pub struct AggregateOp {
    db: Mutex<Option<Arc<ShardDb>>>,
    state: Mutex<AggState>,
    dirty_keys: Mutex<HashSet<i64>>,
    clean_lru: Mutex<VecDeque<i64>>,
    pub op_id: OperatorId,
    max_groups: AtomicUsize,
    max_bytes: AtomicUsize,
    max_state_bytes: AtomicUsize,
    last_consolidation_groups: AtomicUsize,
    last_consolidation_bytes: AtomicUsize,
}

impl AggregateOp {
    /// Create a new aggregate operator with default consolidation limits and empty state.
    pub fn new(op_id: OperatorId) -> Self {
        Self::with_limits(
            op_id,
            MAX_EPOCH_CONSOLIDATION_GROUPS,
            MAX_EPOCH_CONSOLIDATION_BYTES,
        )
    }

    /// Create from pre-loaded state (used after loading from storage).
    pub fn with_state(op_id: OperatorId, state: AggState) -> Self {
        let clean_keys: VecDeque<i64> = state.entries.keys().copied().collect();
        AggregateOp {
            db: Mutex::new(None),
            state: Mutex::new(state),
            dirty_keys: Mutex::new(HashSet::new()),
            clean_lru: Mutex::new(clean_keys),
            op_id,
            max_groups: AtomicUsize::new(MAX_EPOCH_CONSOLIDATION_GROUPS),
            max_bytes: AtomicUsize::new(MAX_EPOCH_CONSOLIDATION_BYTES),
            max_state_bytes: AtomicUsize::new(0),
            last_consolidation_groups: AtomicUsize::new(0),
            last_consolidation_bytes: AtomicUsize::new(0),
        }
    }

    /// Create with custom consolidation limits.
    pub fn with_limits(op_id: OperatorId, max_groups: usize, max_bytes: usize) -> Self {
        AggregateOp {
            db: Mutex::new(None),
            state: Mutex::new(AggState::new()),
            dirty_keys: Mutex::new(HashSet::new()),
            clean_lru: Mutex::new(VecDeque::new()),
            op_id,
            max_groups: AtomicUsize::new(max_groups),
            max_bytes: AtomicUsize::new(max_bytes),
            max_state_bytes: AtomicUsize::new(0),
            last_consolidation_groups: AtomicUsize::new(0),
            last_consolidation_bytes: AtomicUsize::new(0),
        }
    }

    /// Attach a ShardDb for transparent spill-to-disk and demand-loading.
    pub fn with_db(self, db: Arc<ShardDb>) -> Self {
        *self.db.lock().unwrap() = Some(db);
        self
    }

    /// Set a ShardDb for transparent spill-to-disk and demand-loading.
    pub fn set_db(&self, db: Arc<ShardDb>) {
        *self.db.lock().unwrap() = Some(db);
    }

    /// Set in-memory state capacity budget in bytes.
    pub fn with_memory_limit(self, max_bytes: usize) -> Self {
        self.max_state_bytes.store(max_bytes, Ordering::Relaxed);
        self
    }

    /// Set in-memory state capacity budget in bytes dynamically.
    pub fn set_memory_limit(&self, max_bytes: usize) {
        self.max_state_bytes.store(max_bytes, Ordering::Relaxed);
    }

    /// Number of in-memory cached groups.
    pub fn in_memory_groups(&self) -> usize {
        self.state
            .lock()
            .expect("AggregateOp mutex poisoned")
            .entry_count()
    }

    /// Set consolidation limits dynamically.
    pub fn set_limits(&self, max_groups: usize, max_bytes: usize) {
        self.max_groups.store(max_groups, Ordering::Relaxed);
        self.max_bytes.store(max_bytes, Ordering::Relaxed);
    }

    /// Observable occupancy: distinct groups touched in the last epoch consolidation.
    pub fn consolidation_groups(&self) -> usize {
        self.last_consolidation_groups.load(Ordering::Relaxed)
    }

    /// Observable occupancy: estimated byte memory used in the last epoch consolidation.
    pub fn consolidation_bytes(&self) -> usize {
        self.last_consolidation_bytes.load(Ordering::Relaxed)
    }

    /// Mark the current dirty-key set durable after its caller's batch commits.
    pub fn clear_dirty_keys(&self) {
        let mut dirty = self
            .dirty_keys
            .lock()
            .expect("AggregateOp dirty-key mutex poisoned");
        let mut clean_lru = self
            .clean_lru
            .lock()
            .expect("AggregateOp clean_lru mutex poisoned");
        let mut state = self.state.lock().expect("AggregateOp mutex poisoned");

        for &k in dirty.iter() {
            if state.entries.contains_key(&k) {
                clean_lru.push_back(k);
                if clean_lru.len() > MAX_CLEAN_LRU_CAPACITY {
                    clean_lru.pop_front();
                }
            }
        }
        dirty.clear();

        let limit = self.max_state_bytes.load(Ordering::Relaxed);
        let has_db = self
            .db
            .lock()
            .expect("AggregateOp db mutex poisoned")
            .is_some();
        if limit > 0 && has_db {
            while state.state_bytes() as usize > limit && !clean_lru.is_empty() {
                if let Some(cold_k) = clean_lru.pop_front() {
                    state.entries.remove(&cold_k);
                }
            }
        }
    }

    /// Number of live groups (fill-level metric).
    pub fn live_groups(&self) -> usize {
        self.state
            .lock()
            .expect("AggregateOp mutex poisoned")
            .entry_count()
    }

    /// Encode dirty state mutations as a `WriteBatch` for persistence.
    ///
    /// Persists only keys that changed (put) or were removed (delete) during the
    /// current epoch, ensuring O(|Δ|) write amplification instead of O(|state|).
    /// The caller (usually `ViewSinkOp` or group commit) merges this batch
    /// into the epoch's group-commit `WriteBatch`.
    pub fn state_write_batch(&self) -> WriteBatch {
        let state = self.state.lock().expect("AggregateOp mutex poisoned");
        let dirty = self
            .dirty_keys
            .lock()
            .expect("AggregateOp dirty_keys mutex poisoned");
        let mut wb = WriteBatch::new();
        for &k in dirty.iter() {
            let key = ShardKeyEncoder::encode(ShardPrefix::OpState, self.op_id.0, &k.to_be_bytes());
            if let Some(&(sum, count)) = state.entries.get(&k) {
                let mut value = [0u8; 16];
                value[..8].copy_from_slice(&sum.to_be_bytes());
                value[8..].copy_from_slice(&count.to_be_bytes());
                wb.put(&key, &value);
            } else {
                wb.delete(&key);
            }
        }
        wb
    }

    /// Restore an `AggregateOp` from a `ShardDb` (called at shard startup).
    pub async fn load_from_storage(db: &ShardDb, op_id: OperatorId) -> Result<Self, OpError> {
        let prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, op_id.0);
        let mut state = AggState::new();
        let mut next_token: Option<bytes::Bytes> = None;
        let mut clean_lru = VecDeque::new();

        loop {
            let page = db
                .scan_prefix_page(&prefix, next_token.as_deref(), 1024, 1024 * 1024)
                .await
                .map_err(OpError::storage)?;

            for (key, value) in &page.rows {
                if key.len() < prefix.len() + 8 || !key.starts_with(&prefix) {
                    return Err(OpError::storage_error(format!(
                        "RS-3616: corrupted recovery record: key prefix mismatch or undersized key length {}",
                        key.len()
                    )));
                }
                let k_bytes: [u8; 8] =
                    key[prefix.len()..prefix.len() + 8]
                        .try_into()
                        .map_err(|_| {
                            OpError::storage_error("RS-3616: corrupted group key".to_string())
                        })?;
                if value.len() < 16 {
                    return Err(OpError::storage_error(format!(
                        "RS-3616: corrupted recovery record: value length {} < 16",
                        value.len()
                    )));
                }
                let sum = i64::from_be_bytes(value[..8].try_into().unwrap());
                let count = i64::from_be_bytes(value[8..16].try_into().unwrap());
                if count != 0 {
                    let k = i64::from_be_bytes(k_bytes);
                    state.entries.insert(k, (sum, count));
                    clean_lru.push_back(k);
                    if clean_lru.len() > MAX_CLEAN_LRU_CAPACITY {
                        clean_lru.pop_front();
                    }
                }
            }

            if page.is_last_page {
                break;
            }
            next_token = page.next_token;
        }
        let op = Self::with_state(op_id, state);
        *op.db.lock().expect("AggregateOp db mutex poisoned") = Some(Arc::new(db.clone()));
        *op.clean_lru
            .lock()
            .expect("AggregateOp clean_lru mutex poisoned") = clean_lru;
        Ok(op)
    }

    /// Load persisted state from `db` into this already-constructed
    /// instance in place (used by `GatewayHandler::recover_compiled_views`
    /// to restore a recompiled view's arrangement after a process restart —
    /// unlike `load_from_storage`, this keeps the same `Arc<AggregateOp>`
    /// already installed in the pipeline rather than requiring the caller
    /// to rebuild the pipeline around a freshly-returned instance).
    pub async fn restore_in_place(&self, db: &ShardDb) -> Result<(), OpError> {
        let prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, self.op_id.0);
        let mut state = AggState::new();
        let mut next_token: Option<bytes::Bytes> = None;
        let mut clean_lru = VecDeque::new();

        loop {
            let page = db
                .scan_prefix_page(&prefix, next_token.as_deref(), 1024, 1024 * 1024)
                .await
                .map_err(OpError::storage)?;

            for (key, value) in &page.rows {
                if key.len() < prefix.len() + 8 || !key.starts_with(&prefix) {
                    return Err(OpError::storage_error(format!(
                        "RS-3616: corrupted recovery record: key prefix mismatch or undersized key length {}",
                        key.len()
                    )));
                }
                let k_bytes: [u8; 8] =
                    key[prefix.len()..prefix.len() + 8]
                        .try_into()
                        .map_err(|_| {
                            OpError::storage_error("RS-3616: corrupted group key".to_string())
                        })?;
                if value.len() < 16 {
                    return Err(OpError::storage_error(format!(
                        "RS-3616: corrupted recovery record: value length {} < 16",
                        value.len()
                    )));
                }
                let sum = i64::from_be_bytes(value[..8].try_into().unwrap());
                let count = i64::from_be_bytes(value[8..16].try_into().unwrap());
                if count != 0 {
                    let k = i64::from_be_bytes(k_bytes);
                    state.entries.insert(k, (sum, count));
                    clean_lru.push_back(k);
                    if clean_lru.len() > MAX_CLEAN_LRU_CAPACITY {
                        clean_lru.pop_front();
                    }
                }
            }

            if page.is_last_page {
                break;
            }
            next_token = page.next_token;
        }

        let limit = self.max_state_bytes.load(Ordering::Relaxed);
        if limit > 0 {
            while state.state_bytes() as usize > limit && !clean_lru.is_empty() {
                if let Some(cold_k) = clean_lru.pop_front() {
                    state.entries.remove(&cold_k);
                }
            }
        }
        *self.state.lock().expect("AggregateOp mutex poisoned") = state;
        *self.db.lock().expect("AggregateOp db mutex poisoned") = Some(Arc::new(db.clone()));
        *self
            .clean_lru
            .lock()
            .expect("AggregateOp clean_lru mutex poisoned") = clean_lru;
        Ok(())
    }

    /// State bytes metric.
    pub fn state_bytes(&self) -> u64 {
        self.state.lock().unwrap().state_bytes()
    }

    /// Process one Z-set delta batch and return the output delta and delta-native state mutations.
    pub fn process_delta_with_result(
        &self,
        delta: ArrowZSet,
    ) -> Result<crate::op::OperatorEpochResult, OpError> {
        let started_at = Instant::now();
        delta.validate()?;
        if delta.is_empty() {
            return Ok(crate::op::OperatorEpochResult::new(
                ArrowZSet::empty(output_schema()),
                Vec::new(),
                rockstream_types::state_mutation::OperatorEpochMetrics::default(),
            ));
        }

        // Validate input schema: need at least 2 columns (k, v).
        if delta.data.num_columns() < 2 {
            return Err(OpError::column_out_of_bounds(1, delta.data.num_columns()));
        }

        let k_raw = delta.data.column(0);
        let k_col_owned = if let Some(arr) = k_raw.as_any().downcast_ref::<Int64Array>() {
            arr.clone()
        } else if let Ok(cast_arr) = arrow::compute::cast(k_raw.as_ref(), &DataType::Int64) {
            cast_arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .cloned()
                .ok_or_else(|| OpError::column_type_mismatch("Int64", "other"))?
        } else {
            return Err(OpError::column_type_mismatch(
                "Int64",
                format!("{:?}", k_raw.data_type()),
            ));
        };
        let k_col = &k_col_owned;

        let v_raw = delta.data.column(1);
        let v_col_owned = if let Some(arr) = v_raw.as_any().downcast_ref::<Int64Array>() {
            arr.clone()
        } else if let Some(arr) = v_raw.as_any().downcast_ref::<Decimal128Array>() {
            Int64Array::from(
                (0..arr.len())
                    .map(|row| {
                        if arr.is_null(row) {
                            Ok(0)
                        } else {
                            checked_i128_to_i64(arr.value(row)).map_err(|_| {
                                OpError::column_type_mismatch(
                                    "Decimal128 fitting Int64",
                                    "Decimal128",
                                )
                            })
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
        } else if let Ok(cast_arr) = arrow::compute::cast(v_raw.as_ref(), &DataType::Int64) {
            cast_arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .cloned()
                .ok_or_else(|| OpError::column_type_mismatch("Int64", "other"))?
        } else if let Ok(cast_arr) = arrow::compute::cast(v_raw.as_ref(), &DataType::Float64) {
            let f_arr = cast_arr
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            let ints: Vec<i64> = (0..f_arr.len())
                .map(|r| {
                    if f_arr.is_null(r) {
                        0
                    } else {
                        f_arr.value(r) as i64
                    }
                })
                .collect();
            Int64Array::from(ints)
        } else {
            return Err(OpError::column_type_mismatch(
                "Int64",
                format!("{:?}", v_raw.data_type()),
            ));
        };
        let v_col = &v_col_owned;

        let n = delta.num_rows();

        let max_groups = self.max_groups.load(Ordering::Relaxed);
        let max_bytes = self.max_bytes.load(Ordering::Relaxed);
        let mut staged = StagedEpochAggregator::with_limits(max_groups, max_bytes);

        for row in 0..n {
            if k_col.is_null(row) || v_col.is_null(row) || v_raw.is_null(row) {
                continue;
            }
            let k = k_col.value(row);
            let v = v_col.value(row);
            let w = delta.weights[row];
            if w == 0 {
                continue;
            }
            staged.ingest_delta(k, v, w)?;
        }

        self.last_consolidation_groups
            .store(staged.group_count(), Ordering::Relaxed);
        self.last_consolidation_bytes
            .store(staged.estimated_bytes(), Ordering::Relaxed);

        struct GroupTransition {
            key: i64,
            old_state: Option<(i64, i64)>,
            new_state: Option<(i64, i64)>,
            changed: bool,
        }

        let mut state = self.state.lock().expect("AggregateOp mutex poisoned");
        let mut transitions = Vec::with_capacity(staged.order.len());
        let db_opt = self
            .db
            .lock()
            .expect("AggregateOp db mutex poisoned")
            .clone();
        let current_dirty = self
            .dirty_keys
            .lock()
            .expect("AggregateOp dirty-key mutex poisoned")
            .clone();

        let mut demand_loaded: HashMap<i64, (i64, i64)> = HashMap::new();

        for &k in &staged.order {
            let (delta_sum, delta_count) = staged.entries[&k];
            let mut old = state.entries.get(&k).copied();

            if old.is_none() {
                // If the key was deleted in the current uncommitted epoch, old is None
                if !current_dirty.contains(&k) {
                    if let Some(db) = &db_opt {
                        let key_bytes = ShardKeyEncoder::encode(
                            ShardPrefix::OpState,
                            self.op_id.0,
                            &k.to_be_bytes(),
                        );
                        let opt_bytes =
                            crate::spill::block_on_future(db.get(&key_bytes)).map_err(|e| {
                                OpError::storage_error(format!(
                                    "AggregateOp demand load failed: {e}"
                                ))
                            })?;
                        if let Some(bytes) = opt_bytes {
                            if bytes.len() >= 16 {
                                let sum = i64::from_be_bytes(bytes[..8].try_into().unwrap());
                                let count = i64::from_be_bytes(bytes[8..16].try_into().unwrap());
                                if count > 0 {
                                    rockstream_types::metrics::inc_spill_faults_total();
                                    old = Some((sum, count));
                                    demand_loaded.insert(k, (sum, count));
                                }
                            }
                        }
                    }
                }
            }

            let (old_sum, old_count) = old.unwrap_or((0, 0));
            let old_state = if old_count > 0 {
                Some((old_sum, old_count))
            } else {
                None
            };
            if old_count < 0 || (old_count == 0 && old_sum != 0) {
                return Err(OpError::invalid_literal(format!(
                    "aggregate group {k} has an invalid existing state"
                )));
            }

            if delta_sum == 0 && delta_count == 0 {
                transitions.push(GroupTransition {
                    key: k,
                    old_state,
                    new_state: old_state,
                    changed: false,
                });
                continue;
            }

            let new_count = checked_add_i64(old_count, delta_count)
                .map_err(|_| OpError::aggregate_overflow(k))?;
            if new_count < 0 {
                return Err(OpError::invalid_multiplicity(k, new_count));
            }

            let next = rockstream_verified::aggregate::transition(
                old_sum,
                old_count,
                delta_sum,
                delta_count,
            )
            .ok_or_else(|| {
                if new_count == 0 {
                    OpError::invalid_literal(format!(
                        "aggregate group {k} has zero count with nonzero sum"
                    ))
                } else {
                    OpError::aggregate_overflow(k)
                }
            })?;
            let new_state = (new_count > 0).then_some(next);

            let changed = old_state != new_state;
            transitions.push(GroupTransition {
                key: k,
                old_state,
                new_state,
                changed,
            });
        }

        // If we reached here, no overflow occurred! Commit changes atomically.
        {
            let mut clean_lru = self
                .clean_lru
                .lock()
                .expect("AggregateOp clean_lru mutex poisoned");
            for (dk, dv) in demand_loaded {
                state.entries.insert(dk, dv);
                clean_lru.push_back(dk);
                if clean_lru.len() > MAX_CLEAN_LRU_CAPACITY {
                    clean_lru.pop_front();
                }
            }
        }

        let mut out_k: Vec<i64> = Vec::with_capacity(transitions.len() * 2);
        let mut out_sum: Vec<i64> = Vec::with_capacity(transitions.len() * 2);
        let mut out_count: Vec<i64> = Vec::with_capacity(transitions.len() * 2);
        let mut out_avg: Vec<f64> = Vec::with_capacity(transitions.len() * 2);
        let mut out_weights: Vec<i64> = Vec::with_capacity(transitions.len() * 2);
        for t in &transitions {
            if !t.changed {
                continue;
            }

            // Retract old aggregate row
            if let Some((old_sum, old_count)) = t.old_state {
                let old_avg = avg_from_sum_count(old_sum, old_count).unwrap_or(0.0);
                out_k.push(t.key);
                out_sum.push(old_sum);
                out_count.push(old_count);
                out_avg.push(old_avg);
                out_weights.push(-1);
            }

            // Insert new aggregate row
            if let Some((new_sum, new_count)) = t.new_state {
                let new_avg = avg_from_sum_count(new_sum, new_count).unwrap_or(0.0);
                out_k.push(t.key);
                out_sum.push(new_sum);
                out_count.push(new_count);
                out_avg.push(new_avg);
                out_weights.push(1);
            }
        }

        let mut dirty_keys_vec: Vec<i64> = transitions
            .iter()
            .filter(|transition| transition.changed)
            .map(|transition| transition.key)
            .collect();
        dirty_keys_vec.sort_unstable();
        let mut mutation_states: Vec<(i64, Option<(i64, i64)>)> = transitions
            .iter()
            .filter(|transition| transition.changed)
            .map(|transition| (transition.key, transition.new_state))
            .collect();
        mutation_states.sort_unstable_by_key(|(key, _)| *key);
        let mutations = mutation_states
            .iter()
            .map(|&(key, new_state)| encode_state_mutation(self.op_id, key, new_state))
            .collect::<Vec<_>>();
        let logical_mutation_bytes = mutations.iter().map(|mutation| mutation.size_bytes()).sum();

        // Build the fallible output before installing any planned state.
        let output_zset = if out_k.is_empty() {
            ArrowZSet::empty(output_schema())
        } else {
            let schema = output_schema();
            let cols: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(out_k)),
                Arc::new(Int64Array::from(out_sum)),
                Arc::new(Int64Array::from(out_count)),
                Arc::new(Float64Array::from(out_avg)),
            ];
            let data = RecordBatch::try_new(schema, cols).map_err(OpError::arrow)?;
            ArrowZSet::try_new(data, out_weights)?
        };

        for transition in &transitions {
            if !transition.changed {
                continue;
            }
            if let Some(new_state) = transition.new_state {
                state.entries.insert(transition.key, new_state);
            } else {
                state.entries.remove(&transition.key);
            }
        }
        let state_bytes = state.state_bytes() as usize;

        let mut dirty_guard = self
            .dirty_keys
            .lock()
            .expect("AggregateOp dirty-key mutex poisoned");
        dirty_guard.extend(dirty_keys_vec.iter().copied());

        // Evict cold clean entries if over max_state_bytes
        let limit = self.max_state_bytes.load(Ordering::Relaxed);
        if limit > 0 && db_opt.is_some() {
            let mut clean_lru = self
                .clean_lru
                .lock()
                .expect("AggregateOp clean_lru mutex poisoned");
            while state.state_bytes() as usize > limit && !clean_lru.is_empty() {
                if let Some(cold_k) = clean_lru.pop_front() {
                    if !dirty_guard.contains(&cold_k) {
                        state.entries.remove(&cold_k);
                    }
                }
            }
        }

        drop(dirty_guard);
        drop(state);
        debug!(
            op_id = self.op_id.0,
            input_rows = n,
            output_rows = output_zset.num_rows(),
            dirty_keys = dirty_keys_vec.len(),
            "AggregateOp: processed delta"
        );
        let metric_key = rockstream_types::metrics::LawMetricKey {
            law_id: rockstream_types::laws::weight_add::WEIGHT_ADD_ID,
            law_name: "WeightAdd",
            law_version: 1,
            operator_id: Some(self.op_id),
        };
        for _ in 0..n {
            rockstream_types::metrics::inc_rmw_avoided(&metric_key);
        }
        rockstream_types::metrics::record_operator_runtime_sample(
            self.op_id,
            n as u64,
            n as u64,
            started_at.elapsed(),
            0,
        );

        let metrics = rockstream_types::state_mutation::OperatorEpochMetrics {
            input_records: n,
            output_records: output_zset.num_rows(),
            dirty_keys: dirty_keys_vec.len(),
            state_mutations: mutations.len(),
            logical_mutation_bytes,
            full_state_entries_visited: 0,
            state_bytes,
        };
        rockstream_types::metrics::record_r1_persistence(
            self.op_id,
            metrics.state_mutations as u64,
            metrics.logical_mutation_bytes as u64,
            metrics.dirty_keys as u64,
        );
        rockstream_types::metrics::record_current_r1_execution(
            self.op_id,
            rockstream_types::metrics::R1ExecutionStrategy::Classic,
            rockstream_types::metrics::R1ExecutionCounters {
                input_deltas: metrics.input_records as u64,
                arrangement_probes: metrics.dirty_keys as u64,
                output_deltas: metrics.output_records as u64,
                changed_state_writes: metrics.state_mutations as u64,
                ..Default::default()
            },
        );

        Ok(crate::op::OperatorEpochResult::new(
            output_zset,
            mutations,
            metrics,
        ))
    }

    /// Process delta and return only the output ZSet.
    pub fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.process_delta_with_result(delta)
            .map(|res| res.output_delta)
    }
}

impl Operator for AggregateOp {
    fn name(&self) -> &str {
        "AggregateOp"
    }

    fn state_bytes(&self) -> u64 {
        self.state_bytes()
    }

    /// Apply one Z-set delta batch through the aggregate arrangement.
    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.process_delta(delta)
    }
}

/// Aggregate operator that splits one designated hot key into virtual buckets
/// and combines the partial states back into the unsalted aggregate output.
#[derive(Debug)]
pub struct BucketedAggregateOp {
    combined: Mutex<HashMap<i64, (i64, i64)>>,
    partials: Mutex<HashMap<(i64, u16), (i64, i64)>>,
    op_id: OperatorId,
    hot_key: i64,
    bucket_count: u16,
}

impl BucketedAggregateOp {
    pub fn new(op_id: OperatorId, hot_key: i64, bucket_count: u16) -> Self {
        Self {
            combined: Mutex::new(HashMap::new()),
            partials: Mutex::new(HashMap::new()),
            op_id,
            hot_key,
            bucket_count: normalize_power_of_two_bucket_count(bucket_count),
        }
    }

    fn bucket_for(&self, k: i64, v: i64) -> u16 {
        let mut key = [0u8; 16];
        key[..8].copy_from_slice(&k.to_be_bytes());
        key[8..].copy_from_slice(&v.to_be_bytes());
        route_power_of_two_bucket(&key, self.bucket_count, key.len()).unwrap_or(0)
    }

    pub fn live_groups(&self) -> usize {
        self.combined
            .lock()
            .expect("BucketedAggregateOp mutex poisoned")
            .len()
    }

    pub fn live_partials(&self) -> usize {
        self.partials
            .lock()
            .expect("BucketedAggregateOp mutex poisoned")
            .len()
    }

    pub fn restore_from_entries(
        &self,
        entries: &[(bytes::Bytes, bytes::Bytes)],
    ) -> Result<(), OpError> {
        let prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, self.op_id.0);
        let mut local_combined = HashMap::new();
        let mut local_partials = HashMap::new();

        for (key, value) in entries {
            if !key.starts_with(&prefix) {
                continue;
            }
            if key.len() != prefix.len() + 8 && key.len() != prefix.len() + 10 {
                return Err(OpError::internal(format!(
                    "corrupt persisted bucketed aggregate state for operator {}: invalid key length",
                    self.op_id.0
                )));
            }
            let group_key_bytes: [u8; 8] = key[prefix.len()..prefix.len() + 8]
                .try_into()
                .map_err(|_| {
                    OpError::internal(format!(
                        "corrupt persisted bucketed aggregate state for operator {}: invalid group key",
                        self.op_id.0
                    ))
                })?;
            let group_key = decode_i64(&group_key_bytes).map_err(|_| {
                OpError::internal(format!(
                    "corrupt persisted bucketed aggregate state for operator {}: invalid group key",
                    self.op_id.0
                ))
            })?;
            if value.len() != 16 {
                return Err(OpError::internal(format!(
                    "corrupt persisted bucketed aggregate state for operator {}: invalid value length",
                    self.op_id.0
                )));
            }
            let sum_bytes: [u8; 8] = value[..8].try_into().map_err(|_| {
                OpError::internal(format!(
                    "corrupt persisted bucketed aggregate state for operator {}: invalid sum",
                    self.op_id.0
                ))
            })?;
            let count_bytes: [u8; 8] = value[8..16].try_into().map_err(|_| {
                OpError::internal(format!(
                    "corrupt persisted bucketed aggregate state for operator {}: invalid count",
                    self.op_id.0
                ))
            })?;
            let sum = decode_i64(&sum_bytes).map_err(|_| {
                OpError::internal(format!(
                    "corrupt persisted bucketed aggregate state for operator {}: invalid sum",
                    self.op_id.0
                ))
            })?;
            let count = decode_i64(&count_bytes).map_err(|_| {
                OpError::internal(format!(
                    "corrupt persisted bucketed aggregate state for operator {}: invalid count",
                    self.op_id.0
                ))
            })?;
            if count <= 0 {
                return Err(OpError::internal(format!(
                    "corrupt persisted bucketed aggregate state for operator {}: non-positive count",
                    self.op_id.0
                )));
            }
            if key.len() == prefix.len() + 10 {
                let bucket_bytes: [u8; 2] = key[prefix.len() + 8..prefix.len() + 10]
                    .try_into()
                    .map_err(|_| {
                        OpError::internal(format!(
                            "corrupt persisted bucketed aggregate state for operator {}: invalid key length",
                            self.op_id.0
                        ))
                    })?;
                let bucket = u16::from_be_bytes(bucket_bytes);
                local_partials.insert((group_key, bucket), (sum, count));
            } else {
                local_combined.insert(group_key, (sum, count));
            }
        }

        for (&(group_key, _bucket), &(sum, count)) in local_partials.iter() {
            if let Some((combined_sum, combined_count)) = local_combined.get_mut(&group_key) {
                if *combined_count == 0 {
                    *combined_sum = checked_add_i64(*combined_sum, sum)
                        .map_err(|_| OpError::aggregate_overflow(group_key))?;
                    *combined_count = checked_add_i64(*combined_count, count)
                        .map_err(|_| OpError::aggregate_overflow(group_key))?;
                    if *combined_count < 0 {
                        return Err(OpError::invalid_multiplicity(group_key, *combined_count));
                    }
                }
            } else {
                let entry = local_combined.entry(group_key).or_insert((0, 0));
                entry.0 = checked_add_i64(entry.0, sum)
                    .map_err(|_| OpError::aggregate_overflow(group_key))?;
                entry.1 = checked_add_i64(entry.1, count)
                    .map_err(|_| OpError::aggregate_overflow(group_key))?;
                if entry.1 < 0 {
                    return Err(OpError::invalid_multiplicity(group_key, entry.1));
                }
            }
        }

        let mut combined = self
            .combined
            .lock()
            .expect("BucketedAggregateOp mutex poisoned");
        let mut partials = self
            .partials
            .lock()
            .expect("BucketedAggregateOp mutex poisoned");
        *combined = local_combined;
        *partials = local_partials;
        Ok(())
    }

    pub fn restore(&self, entries: &[(bytes::Bytes, bytes::Bytes)]) -> Result<(), OpError> {
        self.restore_from_entries(entries)
    }

    pub async fn load_from_storage(
        db: &ShardDb,
        op_id: OperatorId,
        hot_key: i64,
        bucket_count: u16,
    ) -> Result<Self, OpError> {
        let prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, op_id.0);
        let (entries, truncated) = db
            .scan_prefix_bounded(&prefix, MAX_AGGREGATE_RESTORE_BYTES)
            .await
            .map_err(OpError::storage)?;
        if truncated {
            return Err(OpError::internal(format!(
                "bucketed aggregate state exceeds {MAX_AGGREGATE_RESTORE_BYTES} byte restore limit"
            )));
        }
        let op = Self::new(op_id, hot_key, bucket_count);
        op.restore_from_entries(&entries)?;
        Ok(op)
    }
}

impl Operator for BucketedAggregateOp {
    fn name(&self) -> &str {
        "BucketedAggregateOp"
    }

    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let started_at = Instant::now();
        if delta.is_empty() {
            return Ok(ArrowZSet::empty(output_schema()));
        }
        if delta.data.num_columns() < 2 {
            return Err(OpError::column_out_of_bounds(1, delta.data.num_columns()));
        }

        let k_col = delta
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| OpError::column_type_mismatch("Int64", "other"))?;
        let v_col = delta
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| OpError::column_type_mismatch("Int64", "other"))?;

        let mut consolidated: std::collections::HashMap<(i64, i64), i64> =
            std::collections::HashMap::new();
        let mut order: Vec<(i64, i64)> = Vec::new();
        for row in 0..delta.num_rows() {
            let k = k_col.value(row);
            let v = v_col.value(row);
            let w = delta.weights[row];
            let entry = consolidated.entry((k, v)).or_insert_with(|| {
                order.push((k, v));
                0
            });
            *entry += w;
        }

        let mut out_k: Vec<i64> = Vec::with_capacity(order.len() * 2);
        let mut out_sum: Vec<i64> = Vec::with_capacity(order.len() * 2);
        let mut out_count: Vec<i64> = Vec::with_capacity(order.len() * 2);
        let mut out_avg: Vec<f64> = Vec::with_capacity(order.len() * 2);
        let mut out_weights: Vec<i64> = Vec::with_capacity(order.len() * 2);

        let mut combined = self
            .combined
            .lock()
            .expect("BucketedAggregateOp mutex poisoned");
        let mut partials = self
            .partials
            .lock()
            .expect("BucketedAggregateOp mutex poisoned");

        for (k, v) in order {
            let w = consolidated[&(k, v)];
            if w == 0 {
                continue;
            }

            let (old_sum, old_count) = combined.get(&k).copied().unwrap_or((0, 0));
            let old_state = (old_count > 0).then_some((old_sum, old_count));

            let contribution = checked_mul_i64(v, w).map_err(|_| OpError::aggregate_overflow(k))?;
            let new_sum = checked_add_i64(old_sum, contribution)
                .map_err(|_| OpError::aggregate_overflow(k))?;
            let new_count =
                checked_add_i64(old_count, w).map_err(|_| OpError::aggregate_overflow(k))?;
            if new_count < 0 {
                return Err(OpError::invalid_multiplicity(k, new_count));
            }

            if k == self.hot_key && self.bucket_count > 1 {
                let bucket = self.bucket_for(k, v);
                let partial_key = (k, bucket);
                let (partial_sum, partial_count) =
                    partials.get(&partial_key).copied().unwrap_or((0, 0));
                let next_partial_sum = checked_add_i64(partial_sum, contribution)
                    .map_err(|_| OpError::aggregate_overflow(k))?;
                let next_partial_count = checked_add_i64(partial_count, w)
                    .map_err(|_| OpError::aggregate_overflow(k))?;
                if next_partial_count != 0 {
                    partials.insert(partial_key, (next_partial_sum, next_partial_count));
                } else {
                    partials.remove(&partial_key);
                }
            }

            let new_state = if new_count > 0 {
                combined.insert(k, (new_sum, new_count));
                Some((new_sum, new_count))
            } else {
                combined.remove(&k);
                None
            };

            if let Some((old_sum, old_count)) = old_state {
                out_k.push(k);
                out_sum.push(old_sum);
                out_count.push(old_count);
                out_avg.push(avg_from_sum_count(old_sum, old_count).unwrap_or(0.0));
                out_weights.push(-1);
            }
            if let Some((new_sum, new_count)) = new_state {
                out_k.push(k);
                out_sum.push(new_sum);
                out_count.push(new_count);
                out_avg.push(avg_from_sum_count(new_sum, new_count).unwrap_or(0.0));
                out_weights.push(1);
            }
        }

        drop(partials);
        drop(combined);

        debug!(
            op_id = self.op_id.0,
            input_rows = delta.num_rows(),
            output_rows = out_k.len(),
            hot_key = self.hot_key,
            bucket_count = self.bucket_count,
            "BucketedAggregateOp: processed delta"
        );
        rockstream_types::metrics::record_operator_runtime_sample(
            self.op_id,
            delta.num_rows() as u64,
            delta.num_rows() as u64,
            started_at.elapsed(),
            0,
        );

        if out_k.is_empty() {
            return Ok(ArrowZSet::empty(output_schema()));
        }

        let data = RecordBatch::try_new(
            output_schema(),
            vec![
                Arc::new(Int64Array::from(out_k)) as ArrayRef,
                Arc::new(Int64Array::from(out_sum)) as ArrayRef,
                Arc::new(Int64Array::from(out_count)) as ArrayRef,
                Arc::new(Float64Array::from(out_avg)) as ArrayRef,
            ],
        )
        .map_err(OpError::arrow)?;
        Ok(ArrowZSet::new(data, out_weights))
    }
}

// ─── Frontier persistence ─────────────────────────────────────────────────────

/// Persist the shard frontier (current committed epoch) to `ShardDb`.
///
/// Key: `[0x06 (ShardMeta)][b"frontier"]` (defined by `ShardKeyEncoder::frontier_key()`).
/// Value: `epoch: u64` as 8 bytes big-endian.
pub async fn persist_frontier(db: &ShardDb, epoch: u64) -> Result<(), OpError> {
    let key = ShardKeyEncoder::frontier_key();
    let value = encode_u64(epoch);
    db.put(&key, &value).await.map_err(OpError::storage)
}

/// Load the persisted frontier from `ShardDb`.
///
/// Returns `None` if no frontier has been committed yet (fresh shard).
pub async fn load_frontier(db: &ShardDb) -> Result<Option<u64>, OpError> {
    let key = ShardKeyEncoder::frontier_key();
    let raw = db.get(&key).await.map_err(OpError::storage)?;
    match raw {
        None => Ok(None),
        Some(bytes) if bytes.len() == 8 => Ok(decode_u64(&bytes).ok()),
        Some(_) => Ok(None), // malformed — treat as absent
    }
}

/// Append the aggregate's dirty-key mutations to a caller-owned write batch.
///
/// The caller commits the batch atomically with the epoch's other writes.
/// This path touches only keys changed since the previous append and uses
/// point deletes for removed groups.
pub async fn append_agg_state(
    _db: &ShardDb,
    op: &AggregateOp,
    target: &mut WriteBatch,
) -> Result<(), OpError> {
    let mutations = {
        let state = op.state.lock().expect("AggregateOp mutex poisoned");
        let dirty_keys = op
            .dirty_keys
            .lock()
            .expect("AggregateOp dirty-key mutex poisoned");
        let mut keys = dirty_keys.iter().copied().collect::<Vec<_>>();
        keys.sort_unstable();
        state.encode_mutations_for_keys(op.op_id, &keys)
    };
    for mutation in mutations {
        match mutation {
            rockstream_types::state_mutation::StateMutation::Put { key, value } => {
                target.put(&key, &value)
            }
            rockstream_types::state_mutation::StateMutation::Delete { key } => target.delete(&key),
            rockstream_types::state_mutation::StateMutation::Merge { key, operand, .. } => {
                target.merge(&key, &operand)
            }
        }
    }
    Ok(())
}

/// Persist aggregate state as one standalone batch for legacy callers.
pub async fn persist_agg_state(db: &ShardDb, op: &AggregateOp) -> Result<(), OpError> {
    let mut batch = WriteBatch::new();
    append_agg_state(db, op, &mut batch).await?;
    if !batch.is_empty() {
        db.write_batch(batch).await.map_err(OpError::storage)?;
    }
    op.clear_dirty_keys();
    Ok(())
}

pub async fn persist_bucketed_agg_state(
    db: &ShardDb,
    op: &BucketedAggregateOp,
) -> Result<(), OpError> {
    let prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, op.op_id.0);
    let (existing, truncated) = db
        .scan_prefix_bounded(&prefix, MAX_AGGREGATE_RESTORE_BYTES)
        .await
        .map_err(OpError::storage)?;
    if truncated {
        return Err(OpError::internal(format!(
            "bucketed aggregate state exceeds {MAX_AGGREGATE_RESTORE_BYTES} byte restore limit"
        )));
    }

    let wb = {
        let combined = op
            .combined
            .lock()
            .expect("BucketedAggregateOp mutex poisoned");
        let partials = op
            .partials
            .lock()
            .expect("BucketedAggregateOp mutex poisoned");
        let mut wb = WriteBatch::new();
        let expected_keys: std::collections::HashSet<Vec<u8>> = combined
            .iter()
            .map(|(&k, _)| bucketed_combined_key(op.op_id, k))
            .chain(
                partials
                    .iter()
                    .map(|(&(k, bucket), _)| bucketed_partial_key(op.op_id, k, bucket)),
            )
            .collect();

        for (key, _) in &existing {
            if !expected_keys.contains(key.as_ref()) {
                wb.delete(key);
            }
        }

        for (&k, &(sum, count)) in combined.iter() {
            let mut value = [0u8; 16];
            value[..8].copy_from_slice(&encode_i64(sum));
            value[8..].copy_from_slice(&encode_i64(count));
            wb.put(&bucketed_combined_key(op.op_id, k), &value);
        }
        for (&(k, bucket), &(sum, count)) in partials.iter() {
            let mut value = [0u8; 16];
            value[..8].copy_from_slice(&encode_i64(sum));
            value[8..].copy_from_slice(&encode_i64(count));
            wb.put(&bucketed_partial_key(op.op_id, k, bucket), &value);
        }
        wb
    };

    if !wb.is_empty() {
        db.write_batch(wb).await.map_err(OpError::storage)?;
    }
    Ok(())
}

pub fn bucketed_combined_key(op_id: OperatorId, group_key: i64) -> Vec<u8> {
    ShardKeyEncoder::encode(ShardPrefix::OpState, op_id.0, &group_key.to_be_bytes())
}

pub fn bucketed_partial_key(op_id: OperatorId, group_key: i64, bucket: u16) -> Vec<u8> {
    let mut suffix = Vec::with_capacity(10);
    suffix.extend_from_slice(&group_key.to_be_bytes());
    suffix.extend_from_slice(&bucket.to_be_bytes());
    ShardKeyEncoder::encode(ShardPrefix::OpState, op_id.0, &suffix)
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::ids::OperatorId;

    fn make_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
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

    fn extract_rows(batch: &ArrowZSet) -> Vec<(i64, i64, i64, f64, i64)> {
        // (k, sum_v, count, avg_v, weight)
        let k_col = batch
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let s_col = batch
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let c_col = batch
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let a_col = batch
            .data
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|i| {
                (
                    k_col.value(i),
                    s_col.value(i),
                    c_col.value(i),
                    a_col.value(i),
                    batch.weights[i],
                )
            })
            .collect()
    }

    #[test]
    fn decimal_sum_and_avg_keep_the_full_scaled_value() {
        use arrow::array::Decimal128Array;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Decimal128(12, 2), false),
        ]));
        let data = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(
                    Decimal128Array::from(vec![1025_i128, 550, 375])
                        .with_precision_and_scale(12, 2)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();
        let aggregate = AggregateOp::new(OperatorId(0));
        let output = aggregate
            .process_delta(ArrowZSet::new(data, vec![1, 1, 1]))
            .unwrap();
        let sum = DecimalAggregateFormatOp::new(2, false)
            .process_delta(output.clone())
            .unwrap();
        let avg = DecimalAggregateFormatOp::new(2, true)
            .process_delta(output)
            .unwrap();
        let rows = |batch: &ArrowZSet| {
            let keys = batch
                .data
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let values = batch
                .data
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| {
                    (
                        keys.value(row),
                        values.value(row).to_string(),
                        batch.weights[row],
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            rows(&sum),
            vec![(1, "15.75".to_string(), 1), (2, "3.75".to_string(), 1),]
        );
        assert_eq!(
            rows(&avg),
            vec![(1, "7.875".to_string(), 1), (2, "3.75".to_string(), 1),]
        );
    }

    #[test]
    fn single_insert_creates_group() {
        let op = AggregateOp::new(OperatorId(0));
        let delta = make_batch(&[(1, 10, 1)]);
        let out = op.process_delta(delta).unwrap();
        let rows = extract_rows(&out);
        // No retraction; one insertion of (k=1, sum=10, count=1, avg=10).
        assert_eq!(rows, vec![(1, 10, 1, 10.0, 1)]);
        assert_eq!(op.live_groups(), 1);
    }

    #[test]
    fn second_insert_into_same_group_retracts_and_inserts() {
        let op = AggregateOp::new(OperatorId(0));
        // Insert k=1, v=10.
        let _ = op.process_delta(make_batch(&[(1, 10, 1)])).unwrap();
        // Insert k=1, v=6.
        let out = op.process_delta(make_batch(&[(1, 6, 1)])).unwrap();
        let rows = extract_rows(&out);
        // Retract (k=1, sum=10, count=1) and insert (k=1, sum=16, count=2, avg=8).
        assert!(
            rows.contains(&(1, 10, 1, 10.0, -1)),
            "missing retraction: {rows:?}"
        );
        assert!(
            rows.contains(&(1, 16, 2, 8.0, 1)),
            "missing insertion: {rows:?}"
        );
        assert_eq!(op.live_groups(), 1);
    }

    #[test]
    fn delete_last_row_removes_group() {
        let op = AggregateOp::new(OperatorId(0));
        let _ = op.process_delta(make_batch(&[(1, 10, 1)])).unwrap();
        let out = op.process_delta(make_batch(&[(1, 10, -1)])).unwrap();
        let rows = extract_rows(&out);
        // Retraction of (k=1, sum=10, count=1); no new insertion.
        assert_eq!(rows, vec![(1, 10, 1, 10.0, -1)]);
        assert_eq!(op.live_groups(), 0);
    }

    #[test]
    fn multiple_groups_independent() {
        let op = AggregateOp::new(OperatorId(0));
        let delta = make_batch(&[(1, 5, 1), (2, 20, 1), (1, 3, 1)]);
        let out = op.process_delta(delta).unwrap();
        let rows = extract_rows(&out);
        // Consolidated in epoch:
        // k=1: 5 + 3 = 8, count = 2 -> insert (k=1, sum=8, count=2, avg=4)
        // k=2: 20, count = 1 -> insert (k=2, sum=20, count=1, avg=20)
        // Zero intermediate retractions or insertions emitted.
        assert_eq!(rows, vec![(1, 8, 2, 4.0, 1), (2, 20, 1, 20.0, 1)]);
        assert!(
            rows.contains(&(2, 20, 1, 20.0, 1)),
            "k=2 insert missing: {rows:?}"
        );
        assert_eq!(op.live_groups(), 2);
    }

    #[test]
    fn empty_delta_returns_empty_output() {
        let op = AggregateOp::new(OperatorId(0));
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, false),
        ]));
        let empty = ArrowZSet::empty(schema);
        let out = op.process_delta(empty).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn state_encode_decode_roundtrip() {
        let op = AggregateOp::new(OperatorId(7));
        let _ = op
            .process_delta(make_batch(&[(1, 10, 1), (2, 20, 1)]))
            .unwrap();
        let wb = op.state_write_batch();
        // The batch should have 2 entries (one per live group).
        assert_eq!(wb.len(), 2);
    }

    #[test]
    fn avg_computes_correct_fractional_average() {
        let op = AggregateOp::new(OperatorId(0));
        // Insert 3 rows for k=1 with v=-7,-7,-7 → sum=-21, count=3, avg=-7.0 (exact).
        let _ = op.process_delta(make_batch(&[(1, -7, 1)])).unwrap();
        let _ = op.process_delta(make_batch(&[(1, -7, 1)])).unwrap();
        let out = op.process_delta(make_batch(&[(1, -7, 1)])).unwrap();
        let rows = extract_rows(&out);
        let new_row = rows.iter().find(|r| r.4 == 1).expect("no +1 row");
        assert_eq!(new_row.1, -21); // sum
        assert_eq!(new_row.2, 3); // count
        assert_eq!(new_row.3, -7.0); // avg = -21.0 / 3.0, exact

        // Genuinely fractional case: sum=10, count=3 → avg = 10.0/3.0, not truncated to 3.
        let op2 = AggregateOp::new(OperatorId(1));
        let _ = op2.process_delta(make_batch(&[(1, 1, 1)])).unwrap();
        let _ = op2.process_delta(make_batch(&[(1, 2, 1)])).unwrap();
        let out2 = op2.process_delta(make_batch(&[(1, 4, 1)])).unwrap();
        let rows2 = extract_rows(&out2);
        let new_row2 = rows2.iter().find(|r| r.4 == 1).expect("no +1 row");
        assert_eq!(new_row2.1, 7); // sum = 1 + 2 + 4
        assert_eq!(new_row2.2, 3); // count
        assert!(
            (new_row2.3 - (7.0 / 3.0)).abs() < f64::EPSILON,
            "expected avg ~= 7/3, got {}",
            new_row2.3
        );
        assert_ne!(new_row2.3, 2.0, "avg must not be truncated to an integer");
    }

    #[test]
    fn group_count_zero_after_matching_retractions() {
        let op = AggregateOp::new(OperatorId(0));
        let _ = op
            .process_delta(make_batch(&[(3, 5, 1), (3, 7, 1)]))
            .unwrap();
        // Retract both rows.
        op.process_delta(make_batch(&[(3, 5, -1), (3, 7, -1)]))
            .unwrap();
        assert_eq!(op.live_groups(), 0);
    }

    #[test]
    fn staging_overflow_leaves_the_existing_entry_unchanged() {
        let mut staged = StagedEpochAggregator::with_limits(4, 256);
        staged.ingest_delta(7, i64::MAX, 1).unwrap();
        let before = staged.entries.clone();

        assert!(matches!(
            staged.ingest_delta(7, 0, i64::MAX),
            Err(OpError::AggregateOverflow { group_key: 7, .. })
        ));
        assert_eq!(staged.entries, before);
        assert_eq!(staged.order, vec![7]);
        assert_eq!(staged.estimated_bytes(), 64);
    }

    #[test]
    fn persisted_aggregate_corruption_fails_closed() {
        let op_id = OperatorId(9);
        let key = ShardKeyEncoder::encode(ShardPrefix::OpState, op_id.0, &1i64.to_be_bytes());
        let error = AggState::decode_from_entries(
            &[(bytes::Bytes::from(key), bytes::Bytes::from_static(b"short"))],
            op_id,
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "[RS-0001] Internal error: corrupt persisted aggregate state for operator 9: invalid value; next_steps: report this issue"
        );
    }

    #[test]
    fn failed_later_transition_does_not_install_earlier_state() {
        let mut initial = AggState::new();
        initial.insert(1, (1, 1));
        initial.insert(2, (i64::MAX, 1));
        let op = AggregateOp::with_state(OperatorId(9), initial);

        assert!(matches!(
            op.process_delta(make_batch(&[(1, 1, 1), (2, 1, 1)])),
            Err(OpError::AggregateOverflow { group_key: 2, .. })
        ));
        assert_eq!(op.live_groups(), 2);

        let output = op.process_delta(make_batch(&[(1, 1, 1)])).unwrap();
        assert_eq!(
            extract_rows(&output),
            vec![(1, 1, 1, 1.0, -1), (1, 2, 2, 1.0, 1)]
        );
    }

    fn encode_bucketed_value(sum: i64, count: i64) -> bytes::Bytes {
        let mut val = [0u8; 16];
        val[..8].copy_from_slice(&encode_i64(sum));
        val[8..16].copy_from_slice(&encode_i64(count));
        bytes::Bytes::copy_from_slice(&val)
    }

    #[test]
    fn bucketed_aggregate_restore_valid_entry_succeeds() {
        let op_id = OperatorId(10);
        let op = BucketedAggregateOp::new(op_id, 2, 4);

        let combined_k = bytes::Bytes::from(bucketed_combined_key(op_id, 1));
        let combined_v = encode_bucketed_value(100, 5);
        let partial_k = bytes::Bytes::from(bucketed_partial_key(op_id, 2, 1));
        let partial_v = encode_bucketed_value(50, 2);

        let entries = vec![(combined_k, combined_v), (partial_k, partial_v)];
        op.restore_from_entries(&entries).unwrap();

        assert_eq!(op.live_groups(), 2);
        assert_eq!(op.live_partials(), 1);

        // Alias check
        let op2 = BucketedAggregateOp::new(op_id, 2, 4);
        op2.restore(&entries).unwrap();
        assert_eq!(op2.live_groups(), 2);
        assert_eq!(op2.live_partials(), 1);
    }

    #[test]
    fn bucketed_aggregate_restore_one_valid_one_malformed_value_fails_closed() {
        let op_id = OperatorId(11);
        let op = BucketedAggregateOp::new(op_id, 2, 4);

        let valid_k = bytes::Bytes::from(bucketed_combined_key(op_id, 1));
        let valid_v = encode_bucketed_value(100, 5);
        let malformed_k = bytes::Bytes::from(bucketed_combined_key(op_id, 2));
        let malformed_v = bytes::Bytes::from_static(b"short_val");

        let entries = vec![(valid_k, valid_v), (malformed_k, malformed_v)];
        let error = op.restore_from_entries(&entries).unwrap_err();

        assert_eq!(
            error.to_string(),
            "[RS-0001] Internal error: corrupt persisted bucketed aggregate state for operator 11: invalid value length; next_steps: report this issue"
        );
        assert_eq!(op.live_groups(), 0);
        assert_eq!(op.live_partials(), 0);
    }

    #[test]
    fn bucketed_aggregate_restore_one_valid_one_malformed_key_fails_closed() {
        let op_id = OperatorId(12);
        let op = BucketedAggregateOp::new(op_id, 2, 4);

        let valid_k = bytes::Bytes::from(bucketed_combined_key(op_id, 1));
        let valid_v = encode_bucketed_value(100, 5);

        let prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, op_id.0);
        let malformed_k = bytes::Bytes::from([prefix.as_slice(), b"short"].concat());
        let malformed_v = encode_bucketed_value(50, 2);

        let entries = vec![(valid_k, valid_v), (malformed_k, malformed_v)];
        let error = op.restore_from_entries(&entries).unwrap_err();

        assert_eq!(
            error.to_string(),
            "[RS-0001] Internal error: corrupt persisted bucketed aggregate state for operator 12: invalid key length; next_steps: report this issue"
        );
        assert_eq!(op.live_groups(), 0);
        assert_eq!(op.live_partials(), 0);
    }

    #[test]
    fn bucketed_aggregate_restore_non_positive_count_fails_closed() {
        let op_id = OperatorId(13);
        let op = BucketedAggregateOp::new(op_id, 2, 4);

        let valid_k = bytes::Bytes::from(bucketed_combined_key(op_id, 1));
        let valid_v = encode_bucketed_value(100, 5);
        let zero_count_k = bytes::Bytes::from(bucketed_combined_key(op_id, 2));
        let zero_count_v = encode_bucketed_value(0, 0);

        let entries = vec![
            (valid_k.clone(), valid_v.clone()),
            (zero_count_k.clone(), zero_count_v),
        ];
        let error = op.restore_from_entries(&entries).unwrap_err();

        assert_eq!(
            error.to_string(),
            "[RS-0001] Internal error: corrupt persisted bucketed aggregate state for operator 13: non-positive count; next_steps: report this issue"
        );
        assert_eq!(op.live_groups(), 0);
        assert_eq!(op.live_partials(), 0);

        // Negative count test
        let neg_count_v = encode_bucketed_value(10, -3);
        let entries_neg = vec![(valid_k, valid_v), (zero_count_k, neg_count_v)];
        let error_neg = op.restore_from_entries(&entries_neg).unwrap_err();
        assert_eq!(
            error_neg.to_string(),
            "[RS-0001] Internal error: corrupt persisted bucketed aggregate state for operator 13: non-positive count; next_steps: report this issue"
        );
        assert_eq!(op.live_groups(), 0);
        assert_eq!(op.live_partials(), 0);
    }

    #[test]
    fn bucketed_aggregate_restore_unrelated_out_of_namespace_key_ignored() {
        let op_id = OperatorId(14);
        let other_op_id = OperatorId(999);
        let op = BucketedAggregateOp::new(op_id, 2, 4);

        let valid_k = bytes::Bytes::from(bucketed_combined_key(op_id, 1));
        let valid_v = encode_bucketed_value(100, 5);
        let unrelated_k = bytes::Bytes::from(bucketed_combined_key(other_op_id, 99));
        let unrelated_v = bytes::Bytes::from_static(b"completely_random_foreign_value");

        let entries = vec![(valid_k, valid_v), (unrelated_k, unrelated_v)];
        op.restore_from_entries(&entries).unwrap();

        assert_eq!(op.live_groups(), 1);
        assert_eq!(op.live_partials(), 0);
    }

    #[test]
    fn bucketed_aggregate_restore_failure_leaves_prior_state_untouched() {
        let op_id = OperatorId(15);
        let op = BucketedAggregateOp::new(op_id, 2, 4);

        // Initial valid state via process_delta
        op.process_delta(make_batch(&[(1, 10, 1), (2, 20, 1)])).unwrap();
        assert_eq!(op.live_groups(), 2);

        // Attempt restore with corrupted entry
        let bad_k = bytes::Bytes::from(bucketed_combined_key(op_id, 3));
        let bad_v = bytes::Bytes::from_static(b"bad_len");
        let entries = vec![(bad_k, bad_v)];

        assert!(op.restore_from_entries(&entries).is_err());

        // Prior state must remain untouched
        assert_eq!(op.live_groups(), 2);
        let output = op.process_delta(make_batch(&[(1, 10, -1)])).unwrap();
        assert_eq!(
            extract_rows(&output),
            vec![(1, 10, 1, 10.0, -1)]
        );
    }
}
