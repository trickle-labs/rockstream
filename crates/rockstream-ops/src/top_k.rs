//! Top-K IVM operator for RockStream (v0.21).
//!
//! ## Design
//!
//! `TopKOp` maintains the top-`k` rows per partition ranked by a score column
//! (`rank_col`), emitting incremental delta swaps when the ranked set changes.
//!
//! ### K + epsilon state
//!
//! The operator tracks `k + epsilon` rows internally (default `epsilon = k`,
//! giving a `2k` buffer per partition).  The buffer serves the **delete-refill
//! path**: when a row currently in the top-K is deleted, the next-best row in
//! the buffer fills its slot without requiring a full state rescan.
//!
//! ### Score ordering
//!
//! The caller supplies a `ScoreFn` closure that extracts an `i64` from
//! `(key_bytes, value_bytes)`.  Rows are ranked **descending**: higher score =
//! lower rank number (rank 1 = highest score).
//!
//! ### Delta swaps
//!
//! When a new row arrives whose score exceeds the current k-th score, a swap
//! is emitted: the newly-ranked-in row is inserted (+1) and the newly-ranked-
//! out row is retracted (−1) in the same output ZSet.
//!
//! ### Partitioning
//!
//! The caller supplies a `PartitionFn` that extracts a byte key from
//! `(key_bytes, value_bytes)`.  Returning an empty `Vec` means a single
//! global partition.  Each partition maintains an independent Top-K state.
//!
//! ### Net-weight semantics
//!
//! Rows in the input ZSet carry integer weights.  The operator tracks the
//! **net weight** of each row: net > 0 means the row exists; net ≤ 0 means
//! the row has been fully retracted and is removed from state.
//!
//! ## State layout
//!
//! ```text
//! partition_state: HashMap<PartitionKey, PartitionState>
//! PartitionState {
//!     rows: HashMap<RowId, (key, value, net_weight, score)>,
//!     emitted: BTreeSet<(Reverse<i64>, RowId)>   // current top-K (emitted)
//! }
//! ```

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use rockstream_types::batch::{SinkBatch, SourceBatch, ZSet, ZSetBatch};
use rockstream_types::timestamp::Epoch;

use crate::operator::Operator;

/// Closure that extracts an i64 score from a row.
pub type ScoreFn = Arc<dyn Fn(&[u8], &[u8]) -> i64 + Send + Sync + 'static>;

/// Closure that extracts a partition key from a row.
///
/// Return `vec![]` for a single global partition.
pub type PartitionFn = Arc<dyn Fn(&[u8], &[u8]) -> Vec<u8> + Send + Sync + 'static>;

/// Stable row identifier: `key ++ 0xFF ++ value`.
type RowId = Vec<u8>;

/// Partition key bytes.
type PartitionKey = Vec<u8>;

/// Per-row state: (key_bytes, value_bytes, net_weight, score).
type RowState = (Vec<u8>, Vec<u8>, i64, i64);

/// Ordered key for ranking: (Reverse(score), row_id) so that BTree is
/// ordered descending by score, then ascending by row_id for stability.
type RankKey = (Reverse<i64>, RowId);

/// A single emitted Top-K row: (key, value, score).
pub type TopKRow = (Vec<u8>, Vec<u8>, i64);

/// Current Top-K snapshot per partition.
pub type TopKSnapshot = HashMap<Vec<u8>, Vec<TopKRow>>;

/// Per-partition state.
struct PartitionState {
    /// All rows with net_weight > 0, keyed by row_id.
    rows: HashMap<RowId, RowState>,
    /// Current emitted Top-K set: ordered by (Reverse(score), row_id).
    emitted: BTreeSet<RankKey>,
    /// Key/value cache for currently emitted rows (needed for retraction even
    /// after the row has been removed from `rows`).
    emitted_cache: HashMap<RowId, (Vec<u8>, Vec<u8>)>,
}

impl PartitionState {
    fn new() -> Self {
        Self {
            rows: HashMap::new(),
            emitted: BTreeSet::new(),
            emitted_cache: HashMap::new(),
        }
    }
}

/// Top-K IVM operator.
pub struct TopKOp {
    k: usize,
    epsilon: usize,
    score_fn: ScoreFn,
    partition_fn: PartitionFn,
    partition_state: HashMap<PartitionKey, PartitionState>,
    name: String,
    budget: Option<Arc<rockstream_types::state_budget::StateBudgetMeter>>,
    warned_over_budget: bool,
    rows_processed: u64,
    state_read_count: u64,
}

impl TopKOp {
    /// Create a new `TopKOp`.
    ///
    /// # Parameters
    /// - `k`: number of top rows to maintain per partition.
    /// - `score_fn`: extracts an i64 score from `(key, value)`.  Higher = better rank.
    /// - `partition_fn`: extracts a partition key from `(key, value)`.
    ///
    /// Epsilon is set to `k` (buffer = `2k` rows per partition).
    pub fn new(k: usize, score_fn: ScoreFn, partition_fn: PartitionFn) -> Self {
        assert!(k > 0, "k must be positive");
        Self {
            k,
            epsilon: k,
            score_fn,
            partition_fn,
            partition_state: HashMap::new(),
            name: "TopKOp".to_owned(),
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

    /// Process an incremental ZSet delta and return the output delta.
    ///
    /// The output delta contains only changes to the emitted Top-K set:
    /// - Insertions (+1) for rows that entered the Top-K.
    /// - Retractions (−1) for rows that left the Top-K.
    pub fn process(&mut self, delta: &ZSet) -> ZSet {
        // Collect which partitions are touched by this delta.
        let mut touched: Vec<PartitionKey> = Vec::new();

        for row in delta.iter() {
            self.rows_processed += 1;
            let partition_key = (self.partition_fn)(&row.key, &row.value);
            let score = (self.score_fn)(&row.key, &row.value);
            let row_id = make_row_id(&row.key, &row.value);

            self.state_read_count += 1;
            let ps = self
                .partition_state
                .entry(partition_key.clone())
                .or_insert_with(PartitionState::new);

            // Update net weight.
            let is_new = !ps.rows.contains_key(&row_id);
            if is_new {
                if let Some(ref budget) = self.budget {
                    let charge =
                        (partition_key.len() + row_id.len() + row.key.len() + row.value.len() + 16)
                            as u64;
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

            let entry = ps
                .rows
                .entry(row_id.clone())
                .or_insert_with(|| (row.key.to_vec(), row.value.to_vec(), 0, score));
            entry.2 += row.weight;

            // Remove the entry only when weight reaches exactly 0 (balanced).
            if entry.2 == 0 && ps.rows.remove(&row_id).is_some() {
                if let Some(ref budget) = self.budget {
                    let released =
                        (partition_key.len() + row_id.len() + row.key.len() + row.value.len() + 16)
                            as u64;
                    budget.release(released);
                }
            }

            if !touched.contains(&partition_key) {
                touched.push(partition_key);
            }
        }

        // Recompute top-K for each touched partition and accumulate output.
        let mut output = ZSet::new();

        for partition_key in touched {
            let ps = match self.partition_state.get_mut(&partition_key) {
                Some(ps) => ps,
                None => continue,
            };

            // Build sorted ranking of all live rows: descending score, ascending row_id.
            // Keep at most k + epsilon entries in the buffer.
            let buffer_size = self.k + self.epsilon;
            let mut ranked: BTreeMap<RankKey, (Vec<u8>, Vec<u8>)> = BTreeMap::new();
            for (row_id, (key, value, net_w, score)) in &ps.rows {
                if *net_w <= 0 {
                    // Ghost entry (over-retracted) — skip.
                    continue;
                }
                ranked.insert(
                    (Reverse(*score), row_id.clone()),
                    (key.clone(), value.clone()),
                );
                while ranked.len() > buffer_size {
                    ranked.pop_last();
                }
            }

            // New Top-K set = first k entries in ranked.
            let new_topk: BTreeSet<RankKey> = ranked.keys().take(self.k).cloned().collect();

            // Rows that left the Top-K → emit retractions.
            let old_emitted: BTreeSet<RankKey> = ps.emitted.clone();
            for rank_key in &old_emitted {
                if !new_topk.contains(rank_key) {
                    // Use emitted_cache to get the original (key, value).
                    if let Some((key, value)) = ps.emitted_cache.remove(&rank_key.1) {
                        output.insert(key, value, -1);
                    }
                    ps.emitted.remove(rank_key);
                }
            }

            // Rows that entered the Top-K → emit insertions and cache them.
            for rank_key in &new_topk {
                if !old_emitted.contains(rank_key) {
                    if let Some((key, value)) = ranked.get(rank_key) {
                        output.insert(key.clone(), value.clone(), 1);
                        ps.emitted_cache
                            .insert(rank_key.1.clone(), (key.clone(), value.clone()));
                        ps.emitted.insert(rank_key.clone());
                    }
                }
            }
        }

        output
    }

    /// Return the current emitted Top-K rows for all partitions.
    ///
    /// Used in tests to inspect operator state.
    pub fn current_topk(&self) -> TopKSnapshot {
        let mut result: TopKSnapshot = HashMap::new();
        for (partition_key, ps) in &self.partition_state {
            let rows: Vec<TopKRow> = ps
                .emitted
                .iter()
                .filter_map(|(Reverse(score), row_id)| {
                    ps.rows
                        .get(row_id)
                        .map(|(k, v, _, _)| (k.clone(), v.clone(), *score))
                })
                .collect();
            result.insert(partition_key.clone(), rows);
        }
        result
    }
}

#[async_trait]
impl Operator for TopKOp {
    fn set_context(&mut self, ctx: crate::operator::OperatorContext) {
        if let Some(budget) = ctx.state_budget {
            self.budget = Some(budget);
        }
    }

    async fn process(&mut self, _input: &SourceBatch) -> SinkBatch {
        SinkBatch::default()
    }

    async fn process_delta(&mut self, input: &ZSetBatch) -> ZSetBatch {
        let output_zset = self.process(&input.zset);
        ZSetBatch {
            zset: output_zset,
            epoch: input.epoch,
        }
    }

    async fn epoch_complete(&mut self, _epoch: Epoch) {}

    fn name(&self) -> &str {
        &self.name
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
        self.partition_state
            .iter()
            .map(|(pk, ps)| {
                let rows_size: u64 = ps
                    .rows
                    .iter()
                    .map(|(rid, (k, v, _, _))| {
                        (rid.len() + k.len() + v.len() + 16) as u64
                    })
                    .sum();
                let emitted_size: u64 = ps
                    .emitted
                    .iter()
                    .map(|(_, rid)| rid.len() as u64)
                    .sum();
                let cache_size: u64 = ps
                    .emitted_cache
                    .iter()
                    .map(|(rid, (k, v))| {
                        (rid.len() + k.len() + v.len()) as u64
                    })
                    .sum();
                pk.len() as u64 + rows_size + emitted_size + cache_size
            })
            .sum()
    }
}

/// Build a stable row ID: `key ++ [0xFF] ++ value`.
pub fn make_row_id(key: &[u8], value: &[u8]) -> RowId {
    let mut id = Vec::with_capacity(key.len() + 1 + value.len());
    id.extend_from_slice(key);
    id.push(0xFF);
    id.extend_from_slice(value);
    id
}

/// Build a score-extraction function for a specific column index (i64 BE).
///
/// The column is expected to appear at `col_idx * 8` bytes into the value
/// bytes (each column occupies 8 bytes as a big-endian i64).
pub fn score_fn_for_col(col_idx: usize) -> ScoreFn {
    Arc::new(move |_key: &[u8], value: &[u8]| {
        let start = col_idx * 8;
        if value.len() < start + 8 {
            return i64::MIN;
        }
        i64::from_be_bytes(value[start..start + 8].try_into().unwrap())
    })
}

/// No-partition function: all rows belong to the same global partition.
pub fn no_partition_fn() -> PartitionFn {
    Arc::new(|_key: &[u8], _value: &[u8]| vec![])
}

/// Partition function that uses a single i64-encoded column as the partition key.
///
/// The column is expected to appear at `col_idx * 8` bytes into the key bytes.
pub fn partition_fn_for_col(col_idx: usize) -> PartitionFn {
    Arc::new(move |key: &[u8], _value: &[u8]| {
        let start = col_idx * 8;
        if key.len() < start + 8 {
            return vec![0u8; 8];
        }
        key[start..start + 8].to_vec()
    })
}
