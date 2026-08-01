//! Tumbling time-window operator (v0.12 — IVM-8).
//!
//! ## Cost Model
//!
//! Recomputation cost per epoch = O(dirty_rows + Σ_{closed_windows} |window_rows|).
//! Only windows with changed rows or newly-closeable windows are re-emitted.
//!
//! Bound: TUMBLE_WINDOW_STATE_LIMIT = 100_000 window × group_key pairs per operator.
//! Metric: `fill_level()` = total positive-weight rows across all open windows.
//! Backpressure: epoch backpressure from scheduler when fill_level > TUMBLE_WINDOW_STATE_LIMIT.
//!
//! Hop windows use the same keyspace and compaction filter machinery, but with
//! an overlap-aware bound: `HOP_WINDOW_STATE_LIMIT = 100_000` positive
//! window×row pairs per operator.
//!
//! Session windows use the same persisted key family, but maintain dynamic
//! gap-delimited sessions per partition. Their explicit bound is
//! `SESSION_WINDOW_STATE_LIMIT = 100_000` open-session × group-key pairs.

use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use rockstream_plan::LateDataPolicy;
use rockstream_storage::{
    keys::{ShardKeyEncoder, TW_DISCRIMINATOR},
    ShardDb, WriteBatch,
};
use rockstream_types::ids::OperatorId;

use crate::error::OpError;
use crate::op::Operator;
use crate::zset::ArrowZSet;

// ─── Constants ───────────────────────────────────────────────────────────────

pub const TUMBLE_WINDOW_STATE_LIMIT: usize = 100_000;
pub const HOP_WINDOW_STATE_LIMIT: usize = 100_000;
pub const SESSION_WINDOW_STATE_LIMIT: usize = 100_000;

// ─── WatermarkState ──────────────────────────────────────────────────────────

/// MaxRegister semilattice for watermark tracking.
///
/// Merge law: `merge(a, b) = max(a, b)`. Idempotent: `merge(w, w) = w`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatermarkState {
    pub watermark_ms: i64,
}

impl WatermarkState {
    pub fn new() -> Self {
        Self {
            watermark_ms: i64::MIN,
        }
    }

    pub fn merge(self, other: Self) -> Self {
        Self {
            watermark_ms: self.watermark_ms.max(other.watermark_ms),
        }
    }
}

impl Default for WatermarkState {
    fn default() -> Self {
        Self::new()
    }
}

// ─── CompactionFilter ────────────────────────────────────────────────────────

/// Compaction filter for tumbling-window state keys.
///
/// A key may only be deleted when BOTH conditions are satisfied:
/// 1. `window_id + window_size_ms + allowed_lateness_ms < watermark_ms` (TTL)
/// 2. `frontier_ms > window_id + window_size_ms` (correctness gate)
pub struct CompactionFilter {
    pub watermark_ms: i64,
    pub window_size_ms: i64,
    pub allowed_lateness_ms: i64,
    pub frontier_ms: i64,
}

impl CompactionFilter {
    /// Returns `true` only when both TTL and frontier gate are satisfied.
    ///
    /// The key format is:
    /// `[0x01][TW][op_id:8][window_id:8 BE][group_key:var]`
    pub fn may_delete(&self, key: &[u8]) -> bool {
        // Minimum key length: 1 + 2 + 8 + 8 = 19 bytes.
        if key.len() < 19 {
            return false;
        }
        // Check discriminator bytes.
        if key[0] != 0x01 || key[1..3] != TW_DISCRIMINATOR {
            return false;
        }
        // op_id is bytes 3..11; window_id is bytes 11..19.
        let window_id = i64::from_be_bytes(key[11..19].try_into().unwrap_or([0; 8]));
        let window_end = window_id.saturating_add(self.window_size_ms);

        // Condition 1: TTL — window has expired including lateness allowance.
        let ttl_satisfied = window_end.saturating_add(self.allowed_lateness_ms) < self.watermark_ms;

        // Condition 2: frontier gate — input frontier has advanced past window_end.
        let frontier_satisfied = self.frontier_ms > window_end;

        ttl_satisfied && frontier_satisfied
    }
}

// ─── State ───────────────────────────────────────────────────────────────────

/// Per-window arrangement: group_key_bytes → (row_vals, accumulated_weight).
type WindowMap = HashMap<Vec<u8>, (Vec<i64>, i64)>;

/// Previously-emitted output: group_key_bytes → row_vals.
type EmittedMap = HashMap<Vec<u8>, Vec<i64>>;

#[derive(Clone)]
struct TumbleWindowState {
    /// window_id → row entries (positive and negative weight).
    windows: HashMap<i64, WindowMap>,
    /// window_id → previously emitted output for diff computation.
    prev_output: HashMap<i64, EmittedMap>,
    /// Current watermark.
    watermark: WatermarkState,
    /// Windows that have been finalized (closed and fully emitted).
    finalized: HashSet<i64>,
    /// The current input frontier progress token.
    input_frontier: Option<rockstream_types::frontier::FreshnessToken>,
}

impl TumbleWindowState {
    fn new() -> Self {
        Self {
            windows: HashMap::new(),
            prev_output: HashMap::new(),
            watermark: WatermarkState::new(),
            finalized: HashSet::new(),
            input_frontier: None,
        }
    }

    fn total_rows(&self) -> usize {
        self.windows
            .values()
            .flat_map(|m| m.values())
            .filter(|(_, w)| *w > 0)
            .count()
    }
}

#[derive(Clone)]
struct HopWindowState {
    windows: HashMap<i64, WindowMap>,
    prev_output: HashMap<i64, EmittedMap>,
    watermark: WatermarkState,
    finalized: HashSet<i64>,
    input_frontier: Option<rockstream_types::frontier::FreshnessToken>,
}

impl HopWindowState {
    fn new() -> Self {
        Self {
            windows: HashMap::new(),
            prev_output: HashMap::new(),
            watermark: WatermarkState::new(),
            finalized: HashSet::new(),
            input_frontier: None,
        }
    }

    fn total_rows(&self) -> usize {
        self.windows
            .values()
            .flat_map(|m| m.values())
            .filter(|(_, w)| *w > 0)
            .count()
    }
}

#[derive(Clone, Default)]
struct SessionPartitionState {
    rows: WindowMap,
    prev_output: EmittedMap,
    live_session_count: usize,
}

#[derive(Clone)]
struct SessionWindowState {
    partitions: HashMap<Vec<u8>, SessionPartitionState>,
    watermark: WatermarkState,
    input_frontier: Option<rockstream_types::frontier::FreshnessToken>,
}

impl SessionWindowState {
    fn new() -> Self {
        Self {
            partitions: HashMap::new(),
            watermark: WatermarkState::new(),
            input_frontier: None,
        }
    }

    fn total_sessions(&self) -> usize {
        self.partitions
            .values()
            .map(|partition| partition.live_session_count)
            .sum()
    }
}

// ─── TumbleWindowOp ──────────────────────────────────────────────────────────

/// Tumbling time-window operator (v0.12 — IVM-8).
///
/// Assigns each input row to a fixed-size, non-overlapping time window.
/// Windows close when the watermark advances past the window end.
/// Late rows (event_time_ms < watermark_ms) are handled per late_data_policy.
pub struct TumbleWindowOp {
    /// Output schema: [window_id: i64, ...input_cols...]
    pub schema: SchemaRef,
    n_input_cols: usize,
    time_col: usize,
    window_size_ms: i64,
    late_data_policy: LateDataPolicy,
    state: Mutex<TumbleWindowState>,
    fill_level: Arc<AtomicUsize>,
}

impl TumbleWindowOp {
    /// Build the output schema: window_id column prepended to input schema.
    pub fn output_schema(input_schema: &Schema) -> SchemaRef {
        let mut fields = vec![Field::new("window_id", DataType::Int64, false)];
        for f in input_schema.fields() {
            fields.push(f.as_ref().clone());
        }
        Arc::new(Schema::new(fields))
    }

    /// Create an in-memory TumbleWindowOp (no LFS persistence).
    pub fn new(
        input_schema: SchemaRef,
        time_col: usize,
        window_size_ms: i64,
        late_data_policy: LateDataPolicy,
    ) -> Self {
        let schema = Self::output_schema(&input_schema);
        let n_input_cols = input_schema.fields().len();
        Self {
            schema,
            n_input_cols,
            time_col,
            window_size_ms,
            late_data_policy,
            state: Mutex::new(TumbleWindowState::new()),
            fill_level: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn fill_level(&self) -> usize {
        self.fill_level.load(Ordering::Relaxed)
    }

    /// Load persisted state from `db` into this already-constructed
    /// instance in place (used by `GatewayHandler::recover_compiled_views`
    /// to restore a recompiled view's arrangement after a process
    /// restart — keeps the same `Arc<TumbleWindowOp>` already installed in
    /// the pipeline, unlike `load_tumble_window_state`'s freshly-returned
    /// instance).
    pub async fn restore_in_place(&self, db: &ShardDb, op_id: OperatorId) -> Result<(), OpError> {
        let oid = op_id.0;
        let prefix = ShardKeyEncoder::tumble_window_op_prefix(oid);
        let entries = db.scan_prefix(&prefix).await.map_err(OpError::storage)?;
        let prefix_len = prefix.len();
        let wm_key = ShardKeyEncoder::watermark_key(oid);
        let wm_bytes = db.get(&wm_key).await.ok().flatten();

        let mut st = self.state.lock().unwrap();
        for (key, value) in entries {
            if let Some((row_vals, weight)) = decode_window_value(&value, self.n_input_cols) {
                if key.len() < prefix_len + 8 {
                    continue;
                }
                let window_id = i64::from_be_bytes(
                    key[prefix_len..prefix_len + 8].try_into().unwrap_or([0; 8]),
                );
                let group_key = key[prefix_len + 8..].to_vec();
                st.windows
                    .entry(window_id)
                    .or_default()
                    .insert(group_key, (row_vals, weight));
            }
        }
        if let Some(bytes) = wm_bytes {
            if bytes.len() >= 8 {
                let wm = i64::from_be_bytes(bytes[0..8].try_into().unwrap_or([0; 8]));
                st.watermark = WatermarkState { watermark_ms: wm };
            }
        }
        // Persisted state only carries raw window contents + watermark, not
        // `prev_output`/`finalized` (the diff/close bookkeeping
        // `process_epoch` needs to emit a correct *delta* rather than
        // re-emitting a window's full content as brand-new inserts on the
        // next touch). Reconstruct both from invariants that hold for any
        // window whose content was last correctly synced by `process_epoch`
        // itself: `prev_output[wid]` always equals `windows[wid]`'s
        // positive-weight rows as of the last commit that touched it, and a
        // window is finalized iff the watermark has already passed its
        // close boundary — both are fully derivable from the just-loaded
        // `windows`/`watermark_ms`, so nothing needs its own persisted copy.
        let watermark_ms = st.watermark.watermark_ms;
        let window_size_ms = self.window_size_ms;
        let window_ids: Vec<i64> = st.windows.keys().copied().collect();
        for window_id in window_ids {
            let positive_rows: EmittedMap = st
                .windows
                .get(&window_id)
                .map(|m| {
                    m.iter()
                        .filter(|(_, (_, w))| *w > 0)
                        .map(|(k, (vals, _))| (k.clone(), vals.clone()))
                        .collect()
                })
                .unwrap_or_default();
            st.prev_output.insert(window_id, positive_rows);
            if watermark_ms > window_id + window_size_ms {
                st.finalized.insert(window_id);
            }
        }
        let total = st.total_rows();
        drop(st);
        self.fill_level.store(total, Ordering::Relaxed);
        Ok(())
    }

    /// Current watermark.
    pub fn watermark_ms(&self) -> i64 {
        let state = self.state.lock().unwrap();
        state
            .input_frontier
            .as_ref()
            .and_then(|f| f.watermark_ms())
            .unwrap_or(state.watermark.watermark_ms)
    }

    /// Process one epoch: apply `delta` and return the output delta.
    pub fn process_epoch(&self, delta: ArrowZSet, _epoch: u64) -> Result<ArrowZSet, OpError> {
        if delta.is_empty() && delta.frontier.is_none() {
            return Ok(ArrowZSet::empty(self.schema.clone()));
        }

        let mut state = self.state.lock().unwrap();
        if let Some(ref frontier) = delta.frontier {
            state.input_frontier = Some(frontier.clone());
        }

        // Snapshot the watermark once, as of the start of this epoch/commit.
        // A single `process_epoch` call can carry many rows batched together
        // from one commit (e.g. dozens of DML statements applied in one
        // transaction) whose event times are NOT necessarily monotonic
        // relative to each other — an ordinary streaming source wouldn't do
        // this, but this operator's caller can. Using a live, per-row
        // watermark (re-read from `state.watermark`, which the loop below
        // mutates as it goes) would make the late-data verdict for a row
        // depend on which *other* rows in the same batch happened to be
        // processed earlier. Freezing the watermark here makes every row in
        // this batch judged against the same, prior-epoch-only cutoff,
        // independent of intra-batch ordering.
        let epoch_start_watermark = state
            .input_frontier
            .as_ref()
            .and_then(|f| f.watermark_ms())
            .unwrap_or(state.watermark.watermark_ms);

        let mut dirty_windows: HashSet<i64> = HashSet::new();
        // Windows an earlier retraction in *this same* batch has already
        // reopened (see the closed-window check below for why this
        // matters — a single `process_epoch` call can batch many rows from
        // one commit whose relative order doesn't reflect real arrival
        // order).
        let mut reopened_this_epoch: HashSet<i64> = HashSet::new();

        // ── Apply input delta ────────────────────────────────────────────────
        for row_idx in 0..delta.num_rows() {
            let w = delta.weights[row_idx];
            if w == 0 {
                continue;
            }
            let row_vals = extract_row_vals(&delta.data, row_idx, self.n_input_cols);
            let event_time_ms = if self.time_col < self.n_input_cols {
                row_vals[self.time_col]
            } else {
                0
            };

            // Assign to window.
            let window_id = floor_div(event_time_ms, self.window_size_ms) * self.window_size_ms;

            // A window is "closed" if either (a) its own close boundary is
            // already behind the watermark (frozen as of this epoch's
            // start — see `epoch_start_watermark`'s doc comment), or (b) a
            // *previous* epoch already explicitly finalized it — unless
            // this same batch already reopened it via an earlier
            // retraction, in which case every row targeting it for the
            // rest of this epoch is treated as belonging to an open window
            // again (an UPDATE's "new" half, paired with a retraction of
            // the "old" half processed earlier in this same batch, isn't
            // stray late data — it's the other half of a correction this
            // batch has already decided to apply).
            //
            // Comparing against *this window's own* close boundary (rather
            // than just the raw watermark) matters because a much-larger
            // window (e.g. a 1-day tumble) can have its end boundary far
            // ahead of the watermark even though many rows inside it have
            // timestamps behind that same watermark (the watermark tracks
            // the single latest event time seen across the whole
            // batch/stream, not this row's own window) — using the raw
            // watermark would wrongly treat those rows as late. This
            // formula is exactly the one the output phase below uses to
            // *set* `finalized`, so it agrees with that bookkeeping even
            // for a window that was never previously touched (and so never
            // appeared in `state.finalized` at all).
            let window_closed = (epoch_start_watermark > window_id + self.window_size_ms
                || state.finalized.contains(&window_id))
                && !reopened_this_epoch.contains(&window_id);

            if window_closed {
                if w > 0 {
                    // A retraction (w < 0) always corresponds to a row this
                    // operator already accepted earlier (it can only arrive
                    // after the matching insertion was processed), so it
                    // must never be dropped as "late" — doing so would leave
                    // the retracted row's effect permanently baked into an
                    // already-emitted window output, silently corrupting
                    // every downstream aggregate forever.
                    if self.late_data_policy == LateDataPolicy::Drop {
                        continue;
                    }
                } else {
                    state.finalized.remove(&window_id);
                    reopened_this_epoch.insert(window_id);
                }
            }

            // Advance watermark (MaxRegister).
            state.watermark = state.watermark.merge(WatermarkState {
                watermark_ms: event_time_ms,
            });

            let group_key = encode_row(&row_vals);
            let window = state.windows.entry(window_id).or_default();
            let entry = window.entry(group_key).or_insert((row_vals, 0i64));
            entry.1 += w;
            dirty_windows.insert(window_id);
        }

        // ── Compute output delta ─────────────────────────────────────────────
        // Also check for newly-closeable windows even if not dirty.
        let watermark_ms = state
            .input_frontier
            .as_ref()
            .and_then(|f| f.watermark_ms())
            .unwrap_or(state.watermark.watermark_ms);
        let window_size_ms = self.window_size_ms;

        // Collect all window_ids that need processing.
        let all_windows: Vec<i64> = {
            let mut ws: HashSet<i64> = dirty_windows.clone();
            // Include open windows that newly became closeable.
            for &wid in state.windows.keys() {
                if !state.finalized.contains(&wid) && watermark_ms > wid + window_size_ms {
                    ws.insert(wid);
                }
            }
            ws.into_iter().collect()
        };

        let mut output_rows: Vec<(Vec<i64>, i64)> = Vec::new();

        for window_id in all_windows {
            // Build new current state (positive-weight rows only).
            let new_state: HashMap<Vec<u8>, Vec<i64>> = state
                .windows
                .get(&window_id)
                .map(|m| {
                    m.iter()
                        .filter(|(_, (_, w))| *w > 0)
                        .map(|(k, (vals, _))| (k.clone(), vals.clone()))
                        .collect()
                })
                .unwrap_or_default();

            // Get previous emitted state (clone to avoid borrow conflict).
            let old_emitted: EmittedMap = state
                .prev_output
                .get(&window_id)
                .cloned()
                .unwrap_or_default();

            // Retract rows no longer present.
            for (key, old_vals) in &old_emitted {
                if !new_state.contains_key(key) {
                    let mut out_vals = vec![window_id];
                    out_vals.extend_from_slice(old_vals);
                    output_rows.push((out_vals, -1));
                }
            }

            // Insert newly-present rows.
            let mut new_emitted: EmittedMap = HashMap::new();
            for (key, vals) in &new_state {
                if !old_emitted.contains_key(key) {
                    let mut out_vals = vec![window_id];
                    out_vals.extend_from_slice(vals);
                    output_rows.push((out_vals, 1));
                }
                new_emitted.insert(key.clone(), vals.to_vec());
            }

            state.prev_output.insert(window_id, new_emitted);

            // Mark as finalized if watermark has advanced past window end.
            if watermark_ms > window_id + window_size_ms {
                state.finalized.insert(window_id);
            }
        }

        let total = state.total_rows();
        drop(state);

        self.fill_level.store(total, Ordering::Relaxed);
        build_output(&self.schema, output_rows)
    }
}

impl Operator for TumbleWindowOp {
    fn name(&self) -> &str {
        "TumbleWindowOp"
    }

    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.process_epoch(delta, 0)
    }

    fn push_input_frontier(
        &self,
        frontier: rockstream_types::frontier::FreshnessToken,
    ) -> Result<(), OpError> {
        let mut state = self.state.lock().unwrap();
        state.input_frontier = Some(frontier);
        Ok(())
    }

    fn input_frontier(&self) -> Option<rockstream_types::frontier::FreshnessToken> {
        let state = self.state.lock().unwrap();
        state.input_frontier.clone()
    }
}

/// Hopping time-window operator (v0.50).
///
/// Assigns each input row to every overlapping fixed-size window that contains
/// its event timestamp. Windows close when the watermark advances past the
/// window end. Late rows are handled with the same semantics as tumble windows.
pub struct HopWindowOp {
    /// Output schema: [window_id: i64, ...input_cols...]
    pub schema: SchemaRef,
    n_input_cols: usize,
    time_col: usize,
    window_size_ms: i64,
    slide_ms: i64,
    late_data_policy: LateDataPolicy,
    state: Mutex<HopWindowState>,
    fill_level: Arc<AtomicUsize>,
}

impl HopWindowOp {
    pub fn output_schema(input_schema: &Schema) -> SchemaRef {
        TumbleWindowOp::output_schema(input_schema)
    }

    pub fn new(
        input_schema: SchemaRef,
        time_col: usize,
        window_size_ms: i64,
        slide_ms: i64,
        late_data_policy: LateDataPolicy,
    ) -> Self {
        let schema = Self::output_schema(&input_schema);
        let n_input_cols = input_schema.fields().len();
        Self {
            schema,
            n_input_cols,
            time_col,
            window_size_ms,
            slide_ms,
            late_data_policy,
            state: Mutex::new(HopWindowState::new()),
            fill_level: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn fill_level(&self) -> usize {
        self.fill_level.load(Ordering::Relaxed)
    }

    pub fn watermark_ms(&self) -> i64 {
        let state = self.state.lock().unwrap();
        state
            .input_frontier
            .as_ref()
            .and_then(|f| f.watermark_ms())
            .unwrap_or(state.watermark.watermark_ms)
    }

    pub fn process_epoch(&self, delta: ArrowZSet, _epoch: u64) -> Result<ArrowZSet, OpError> {
        if delta.is_empty() && delta.frontier.is_none() {
            return Ok(ArrowZSet::empty(self.schema.clone()));
        }

        let mut state = self.state.lock().unwrap();
        let mut next = state.clone();
        if let Some(ref frontier) = delta.frontier {
            next.input_frontier = Some(frontier.clone());
        }

        // See `TumbleWindowOp::process_epoch`'s identical fix for the full
        // rationale: freeze the watermark once, as of this epoch's start,
        // rather than re-reading it per-row as the loop mutates it — a
        // later-timestamped row processed earlier in the same batched
        // commit must not cause an earlier-timestamped (but still current)
        // row later in the same batch to be wrongly judged "late".
        let epoch_start_watermark = next
            .input_frontier
            .as_ref()
            .and_then(|f| f.watermark_ms())
            .unwrap_or(next.watermark.watermark_ms);

        let mut dirty_windows: HashSet<i64> = HashSet::new();
        let mut live_rows = next.total_rows();
        // See `TumbleWindowOp::process_epoch`'s identical mechanism: windows
        // an earlier retraction in *this same* batch has already reopened.
        let mut reopened_this_epoch: HashSet<i64> = HashSet::new();

        for row_idx in 0..delta.num_rows() {
            let w = delta.weights[row_idx];
            if w == 0 {
                continue;
            }
            let row_vals = extract_row_vals(&delta.data, row_idx, self.n_input_cols);
            let event_time_ms = row_vals.get(self.time_col).copied().unwrap_or(0);

            next.watermark = next.watermark.merge(WatermarkState {
                watermark_ms: event_time_ms,
            });

            for window_id in hop_window_ids(event_time_ms, self.window_size_ms, self.slide_ms) {
                // See `TumbleWindowOp::process_epoch`'s identical check for
                // the full rationale.
                let window_closed = (epoch_start_watermark > window_id + self.window_size_ms
                    || next.finalized.contains(&window_id))
                    && !reopened_this_epoch.contains(&window_id);

                if window_closed {
                    if w > 0 {
                        if self.late_data_policy == LateDataPolicy::Drop {
                            continue;
                        }
                    } else {
                        next.finalized.remove(&window_id);
                        reopened_this_epoch.insert(window_id);
                    }
                }
                let group_key = encode_row(&row_vals);
                let window = next.windows.entry(window_id).or_default();
                let entry = window
                    .entry(group_key.clone())
                    .or_insert((row_vals.clone(), 0i64));
                let previous = entry.1;
                entry.1 += w;
                match (previous > 0, entry.1 > 0) {
                    (false, true) => {
                        live_rows += 1;
                        if live_rows > HOP_WINDOW_STATE_LIMIT {
                            entry.1 = previous;
                            if previous == 0 {
                                window.remove(&group_key);
                            }
                            return Err(OpError::hop_window_state_overflow(
                                live_rows,
                                HOP_WINDOW_STATE_LIMIT,
                            ));
                        }
                    }
                    (true, false) => {
                        live_rows = live_rows.saturating_sub(1);
                    }
                    _ => {}
                }
                dirty_windows.insert(window_id);
            }
        }

        let watermark_ms = next
            .input_frontier
            .as_ref()
            .and_then(|f| f.watermark_ms())
            .unwrap_or(next.watermark.watermark_ms);
        let all_windows: Vec<i64> = {
            let mut ws: HashSet<i64> = dirty_windows.clone();
            for &wid in next.windows.keys() {
                if !next.finalized.contains(&wid) && watermark_ms > wid + self.window_size_ms {
                    ws.insert(wid);
                }
            }
            ws.into_iter().collect()
        };

        let mut output_rows: Vec<(Vec<i64>, i64)> = Vec::new();
        for window_id in all_windows {
            let new_state: HashMap<Vec<u8>, Vec<i64>> = next
                .windows
                .get(&window_id)
                .map(|m| {
                    m.iter()
                        .filter(|(_, (_, w))| *w > 0)
                        .map(|(k, (vals, _))| (k.clone(), vals.clone()))
                        .collect()
                })
                .unwrap_or_default();
            let old_emitted: EmittedMap = next
                .prev_output
                .get(&window_id)
                .cloned()
                .unwrap_or_default();

            for (key, old_vals) in &old_emitted {
                if !new_state.contains_key(key) {
                    let mut out_vals = vec![window_id];
                    out_vals.extend_from_slice(old_vals);
                    output_rows.push((out_vals, -1));
                }
            }

            let mut new_emitted: EmittedMap = HashMap::new();
            for (key, vals) in &new_state {
                if !old_emitted.contains_key(key) {
                    let mut out_vals = vec![window_id];
                    out_vals.extend_from_slice(vals);
                    output_rows.push((out_vals, 1));
                }
                new_emitted.insert(key.clone(), vals.clone());
            }

            next.prev_output.insert(window_id, new_emitted);
            if watermark_ms > window_id + self.window_size_ms {
                next.finalized.insert(window_id);
            }
        }

        let total = next.total_rows();
        *state = next;
        drop(state);
        self.fill_level.store(total, Ordering::Relaxed);
        build_output(&self.schema, output_rows)
    }
}

impl Operator for HopWindowOp {
    fn name(&self) -> &str {
        "HopWindowOp"
    }

    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.process_epoch(delta, 0)
    }

    fn push_input_frontier(
        &self,
        frontier: rockstream_types::frontier::FreshnessToken,
    ) -> Result<(), OpError> {
        let mut state = self.state.lock().unwrap();
        state.input_frontier = Some(frontier);
        Ok(())
    }

    fn input_frontier(&self) -> Option<rockstream_types::frontier::FreshnessToken> {
        let state = self.state.lock().unwrap();
        state.input_frontier.clone()
    }
}

/// Session time-window operator (v0.50).
///
/// Partitions rows by all non-time columns, then assigns each live row to a
/// gap-delimited session `[session_start, session_end]` inside its partition.
/// When a newly inserted or retracted row changes session boundaries, the
/// operator retracts stale tagged rows and emits replacement rows carrying the
/// new boundaries.
pub struct SessionWindowOp {
    /// Output schema: [session_start: i64, session_end: i64, ...input_cols...]
    pub schema: SchemaRef,
    n_input_cols: usize,
    time_col: usize,
    gap_ms: i64,
    late_data_policy: LateDataPolicy,
    state: Mutex<SessionWindowState>,
    fill_level: Arc<AtomicUsize>,
}

impl SessionWindowOp {
    pub fn output_schema(input_schema: &Schema) -> SchemaRef {
        let mut fields = vec![
            Field::new("session_start", DataType::Int64, false),
            Field::new("session_end", DataType::Int64, false),
        ];
        for f in input_schema.fields() {
            fields.push(f.as_ref().clone());
        }
        Arc::new(Schema::new(fields))
    }

    pub fn new(
        input_schema: SchemaRef,
        time_col: usize,
        gap_ms: i64,
        late_data_policy: LateDataPolicy,
    ) -> Self {
        let schema = Self::output_schema(&input_schema);
        let n_input_cols = input_schema.fields().len();
        Self {
            schema,
            n_input_cols,
            time_col,
            gap_ms,
            late_data_policy,
            state: Mutex::new(SessionWindowState::new()),
            fill_level: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn fill_level(&self) -> usize {
        self.fill_level.load(Ordering::Relaxed)
    }

    /// Load persisted state from `db` into this already-constructed
    /// instance in place — see `TumbleWindowOp::restore_in_place` for why.
    pub async fn restore_in_place(&self, db: &ShardDb, op_id: OperatorId) -> Result<(), OpError> {
        let oid = op_id.0;
        let prefix = ShardKeyEncoder::tumble_window_op_prefix(oid);
        let entries = db.scan_prefix(&prefix).await.map_err(OpError::storage)?;
        let wm_key = ShardKeyEncoder::watermark_key(oid);
        let wm_bytes = db.get(&wm_key).await.ok().flatten();

        let mut st = self.state.lock().unwrap();
        for (_key, value) in entries {
            if let Some((row_vals, weight)) = decode_window_value(&value, self.n_input_cols) {
                let partition_key = encode_partition_key(&row_vals, self.time_col);
                let row_key = encode_row(&row_vals);
                st.partitions
                    .entry(partition_key)
                    .or_default()
                    .rows
                    .insert(row_key, (row_vals, weight));
            }
        }
        if let Some(bytes) = wm_bytes {
            if bytes.len() >= 8 {
                let wm = i64::from_be_bytes(bytes[0..8].try_into().unwrap_or([0; 8]));
                st.watermark = WatermarkState { watermark_ms: wm };
            }
        }
        for partition in st.partitions.values_mut() {
            let live_rows = partition
                .rows
                .values()
                .filter(|(_, weight)| *weight > 0)
                .map(|(vals, _)| vals.clone())
                .collect::<Vec<_>>();
            let sessions = derive_sessions(&live_rows, self.time_col, self.gap_ms);
            partition.live_session_count = sessions
                .iter()
                .map(|(start, end, _)| (*start, *end))
                .collect::<HashSet<_>>()
                .len();
            for (session_start, session_end, row_vals) in sessions {
                let mut tagged = vec![session_start, session_end];
                tagged.extend_from_slice(&row_vals);
                partition.prev_output.insert(encode_row(&row_vals), tagged);
            }
        }
        let total = st.total_sessions();
        drop(st);
        self.fill_level.store(total, Ordering::Relaxed);
        Ok(())
    }

    pub fn watermark_ms(&self) -> i64 {
        let state = self.state.lock().unwrap();
        state
            .input_frontier
            .as_ref()
            .and_then(|f| f.watermark_ms())
            .unwrap_or(state.watermark.watermark_ms)
    }

    pub fn process_epoch(&self, delta: ArrowZSet, _epoch: u64) -> Result<ArrowZSet, OpError> {
        if delta.is_empty() && delta.frontier.is_none() {
            return Ok(ArrowZSet::empty(self.schema.clone()));
        }

        let mut state = self.state.lock().unwrap();
        let mut next = state.clone();
        if let Some(ref frontier) = delta.frontier {
            next.input_frontier = Some(frontier.clone());
        }

        let mut dirty_partitions: HashSet<Vec<u8>> = HashSet::new();
        for row_idx in 0..delta.num_rows() {
            let weight = delta.weights[row_idx];
            if weight == 0 {
                continue;
            }
            let row_vals = extract_row_vals(&delta.data, row_idx, self.n_input_cols);
            let event_time_ms = row_vals.get(self.time_col).copied().unwrap_or(0);
            // Unlike TUMBLE/HOP, session boundaries are data-dependent: an
            // intervening event may legitimately arrive with a timestamp
            // earlier than the partition's already-observed watermark while
            // still being within `gap_ms` of an open session (e.g. the event
            // that merges two sessions). Only drop against an explicit
            // upstream frontier watermark, never against our own
            // accumulated watermark, or merge-triggering events would be
            // dropped as "late" and sessions would never merge/split
            // correctly.
            let current_watermark = next
                .input_frontier
                .as_ref()
                .and_then(|f| f.watermark_ms())
                .unwrap_or(i64::MIN);
            if weight > 0
                && event_time_ms < current_watermark
                && self.late_data_policy == LateDataPolicy::Drop
            {
                continue;
            }

            next.watermark = next.watermark.merge(WatermarkState {
                watermark_ms: event_time_ms,
            });

            let partition_key = encode_partition_key(&row_vals, self.time_col);
            let row_key = encode_row(&row_vals);
            let partition = next.partitions.entry(partition_key.clone()).or_default();
            let entry = partition
                .rows
                .entry(row_key.clone())
                .or_insert((row_vals.clone(), 0));
            entry.1 += weight;
            if entry.1 == 0 {
                partition.rows.remove(&row_key);
            }
            dirty_partitions.insert(partition_key);
        }

        let mut total_sessions = next.total_sessions();
        let mut output_rows = Vec::new();
        for partition_key in dirty_partitions {
            let partition = next
                .partitions
                .get_mut(&partition_key)
                .expect("dirty partition must exist");
            total_sessions = total_sessions.saturating_sub(partition.live_session_count);

            let live_rows = partition
                .rows
                .values()
                .filter(|(_, weight)| *weight > 0)
                .map(|(vals, _)| vals.clone())
                .collect::<Vec<_>>();
            let sessions = derive_sessions(&live_rows, self.time_col, self.gap_ms);
            let session_count = sessions
                .iter()
                .map(|(start, end, _)| (*start, *end))
                .collect::<HashSet<_>>()
                .len();
            total_sessions += session_count;
            if total_sessions > SESSION_WINDOW_STATE_LIMIT {
                return Err(OpError::session_window_state_overflow(
                    total_sessions,
                    SESSION_WINDOW_STATE_LIMIT,
                ));
            }

            let mut new_output = EmittedMap::new();
            for (session_start, session_end, row_vals) in sessions.iter().cloned() {
                let row_key = encode_row(&row_vals);
                let mut tagged = vec![session_start, session_end];
                tagged.extend_from_slice(&row_vals);
                new_output.insert(row_key, tagged);
            }

            for (key, old_vals) in &partition.prev_output {
                match new_output.get(key) {
                    Some(new_vals) if new_vals == old_vals => {}
                    _ => output_rows.push((old_vals.clone(), -1)),
                }
            }
            for (key, new_vals) in &new_output {
                match partition.prev_output.get(key) {
                    Some(old_vals) if old_vals == new_vals => {}
                    _ => output_rows.push((new_vals.clone(), 1)),
                }
            }

            partition.prev_output = new_output;
            partition.live_session_count = session_count;
        }

        *state = next;
        drop(state);
        self.fill_level.store(total_sessions, Ordering::Relaxed);
        build_output(&self.schema, output_rows)
    }
}

impl Operator for SessionWindowOp {
    fn name(&self) -> &str {
        "SessionWindowOp"
    }

    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.process_epoch(delta, 0)
    }

    fn push_input_frontier(
        &self,
        frontier: rockstream_types::frontier::FreshnessToken,
    ) -> Result<(), OpError> {
        let mut state = self.state.lock().unwrap();
        state.input_frontier = Some(frontier);
        Ok(())
    }

    fn input_frontier(&self) -> Option<rockstream_types::frontier::FreshnessToken> {
        let state = self.state.lock().unwrap();
        state.input_frontier.clone()
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Floor division that correctly handles negative dividends.
///
/// Returns `floor(a / b)` for any sign of `a`.
fn floor_div(a: i64, b: i64) -> i64 {
    let d = a / b;
    // If they have different signs and there's a remainder, subtract 1.
    if (a ^ b) < 0 && d * b != a {
        d - 1
    } else {
        d
    }
}

fn hop_window_ids(event_time_ms: i64, window_size_ms: i64, slide_ms: i64) -> Vec<i64> {
    let overlap = window_size_ms / slide_ms;
    let latest_start = floor_div(event_time_ms, slide_ms) * slide_ms;
    let mut window_ids = Vec::with_capacity(overlap.max(0) as usize);
    for offset in 0..overlap {
        let window_id = latest_start - offset * slide_ms;
        if event_time_ms >= window_id && event_time_ms < window_id + window_size_ms {
            window_ids.push(window_id);
        }
    }
    window_ids.sort();
    window_ids
}

fn encode_partition_key(vals: &[i64], time_col: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(vals.len().saturating_sub(1) * 8);
    for (idx, value) in vals.iter().enumerate() {
        if idx != time_col {
            key.extend_from_slice(&value.to_be_bytes());
        }
    }
    key
}

fn derive_sessions(rows: &[Vec<i64>], time_col: usize, gap_ms: i64) -> Vec<(i64, i64, Vec<i64>)> {
    if rows.is_empty() {
        return Vec::new();
    }
    let mut sorted = rows.to_vec();
    sorted.sort_by_key(|row| (row.get(time_col).copied().unwrap_or(0), row.clone()));

    let mut current_start = sorted[0].get(time_col).copied().unwrap_or(0);
    let mut current_end = current_start;
    let mut current_rows = vec![sorted[0].clone()];
    let mut sessions = Vec::new();

    for row in sorted.into_iter().skip(1) {
        let event_time_ms = row.get(time_col).copied().unwrap_or(0);
        if event_time_ms <= current_end.saturating_add(gap_ms) {
            current_end = current_end.max(event_time_ms);
            current_rows.push(row);
            continue;
        }
        for session_row in current_rows.drain(..) {
            sessions.push((current_start, current_end, session_row));
        }
        current_start = event_time_ms;
        current_end = event_time_ms;
        current_rows.push(row);
    }
    for session_row in current_rows {
        sessions.push((current_start, current_end, session_row));
    }
    sessions
}

fn extract_row_vals(batch: &RecordBatch, row_idx: usize, n_cols: usize) -> Vec<i64> {
    batch
        .columns()
        .iter()
        .take(n_cols)
        .map(|col| {
            col.as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(row_idx))
                .unwrap_or(0)
        })
        .collect()
}

fn encode_row(vals: &[i64]) -> Vec<u8> {
    let mut key = Vec::with_capacity(vals.len() * 8);
    for v in vals {
        key.extend_from_slice(&v.to_be_bytes());
    }
    key
}

fn build_output(schema: &SchemaRef, rows: Vec<(Vec<i64>, i64)>) -> Result<ArrowZSet, OpError> {
    if rows.is_empty() {
        return Ok(ArrowZSet::empty(schema.clone()));
    }

    // Aggregate by row key to cancel intra-epoch retractions.
    let mut agg: HashMap<Vec<u8>, (i64, Vec<i64>)> = HashMap::new();
    for (vals, w) in rows {
        let key = encode_row(&vals);
        let entry = agg.entry(key).or_insert((0i64, vals));
        entry.0 += w;
    }

    let num_cols = schema.fields().len();
    let mut col_builders: Vec<Vec<i64>> = vec![Vec::new(); num_cols];
    let mut weights: Vec<i64> = Vec::new();

    for (_, (net_w, vals)) in agg {
        if net_w == 0 {
            continue;
        }
        weights.push(net_w);
        for (ci, &val) in vals.iter().enumerate().take(num_cols) {
            col_builders[ci].push(val);
        }
    }

    if weights.is_empty() {
        return Ok(ArrowZSet::empty(schema.clone()));
    }

    let arrays: Vec<ArrayRef> = col_builders
        .into_iter()
        .map(|col| Arc::new(Int64Array::from(col)) as ArrayRef)
        .collect();

    let data = RecordBatch::try_new(schema.clone(), arrays).map_err(OpError::arrow)?;
    Ok(ArrowZSet::new(data, weights))
}

// ─── LFS persistence ─────────────────────────────────────────────────────────

/// Encode window state value: `[weight:8 BE][col0:8 BE]...[colN:8 BE]`
fn encode_window_value(row_vals: &[i64], weight: i64) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + row_vals.len() * 8);
    v.extend_from_slice(&weight.to_be_bytes());
    for val in row_vals {
        v.extend_from_slice(&val.to_be_bytes());
    }
    v
}

fn decode_window_value(bytes: &[u8], n_input_cols: usize) -> Option<(Vec<i64>, i64)> {
    if bytes.len() < 8 + n_input_cols * 8 {
        return None;
    }
    let weight = i64::from_be_bytes(bytes[0..8].try_into().ok()?);
    let mut vals = Vec::with_capacity(n_input_cols);
    for i in 0..n_input_cols {
        let off = 8 + i * 8;
        vals.push(i64::from_be_bytes(bytes[off..off + 8].try_into().ok()?));
    }
    Some((vals, weight))
}

/// Persist TumbleWindowOp state to a ShardDb.
///
/// Uses only point Put/Delete operations — never DeleteRange.
pub async fn persist_tumble_window_state(
    db: &ShardDb,
    op: &TumbleWindowOp,
    op_id: OperatorId,
) -> Result<(), OpError> {
    let batch = {
        let state = op.state.lock().unwrap();
        let mut batch = WriteBatch::new();
        let oid = op_id.0;

        // Write window entries.
        for (&window_id, window_map) in &state.windows {
            for (group_key, (row_vals, weight)) in window_map {
                let key = ShardKeyEncoder::tumble_window_key(oid, window_id, group_key);
                let value = encode_window_value(row_vals, *weight);
                batch.put(&key, &value);
            }
        }

        // Write watermark.
        let wm_key = ShardKeyEncoder::watermark_key(oid);
        batch.put(&wm_key, &state.watermark.watermark_ms.to_be_bytes());
        batch
    };

    db.write_batch(batch).await.map_err(OpError::storage)?;
    Ok(())
}

/// Load TumbleWindowOp state from a ShardDb.
pub async fn load_tumble_window_state(
    db: &ShardDb,
    input_schema: SchemaRef,
    time_col: usize,
    window_size_ms: i64,
    late_data_policy: LateDataPolicy,
    op_id: OperatorId,
) -> Result<TumbleWindowOp, OpError> {
    let op = TumbleWindowOp::new(input_schema, time_col, window_size_ms, late_data_policy);
    let n_input_cols = op.n_input_cols;
    let oid = op_id.0;

    // Load window entries and watermark before locking state.
    let prefix = ShardKeyEncoder::tumble_window_op_prefix(oid);
    let entries = db.scan_prefix(&prefix).await.map_err(OpError::storage)?;
    let prefix_len = prefix.len(); // 1 + 2 + 8 = 11

    let wm_key = ShardKeyEncoder::watermark_key(oid);
    let wm_bytes = db.get(&wm_key).await.ok().flatten();

    let mut st = op.state.lock().unwrap();

    for (key, value) in entries {
        if let Some((row_vals, weight)) = decode_window_value(&value, n_input_cols) {
            // key = prefix + window_id(8) + group_key(var)
            if key.len() < prefix_len + 8 {
                continue;
            }
            let window_id =
                i64::from_be_bytes(key[prefix_len..prefix_len + 8].try_into().unwrap_or([0; 8]));
            let group_key = key[prefix_len + 8..].to_vec();
            st.windows
                .entry(window_id)
                .or_default()
                .insert(group_key, (row_vals, weight));
        }
    }

    if let Some(bytes) = wm_bytes {
        if bytes.len() >= 8 {
            let wm = i64::from_be_bytes(bytes[0..8].try_into().unwrap_or([0; 8]));
            st.watermark = WatermarkState { watermark_ms: wm };
        }
    }

    let total = st.total_rows();
    drop(st);
    op.fill_level.store(total, Ordering::Relaxed);
    Ok(op)
}

/// Persist HopWindowOp state to a ShardDb using the shared TW keyspace.
pub async fn persist_hop_window_state(
    db: &ShardDb,
    op: &HopWindowOp,
    op_id: OperatorId,
) -> Result<(), OpError> {
    let batch = {
        let state = op.state.lock().unwrap();
        let mut batch = WriteBatch::new();
        let oid = op_id.0;
        for (&window_id, window_map) in &state.windows {
            for (group_key, (row_vals, weight)) in window_map {
                let key = ShardKeyEncoder::tumble_window_key(oid, window_id, group_key);
                let value = encode_window_value(row_vals, *weight);
                batch.put(&key, &value);
            }
        }
        let wm_key = ShardKeyEncoder::watermark_key(oid);
        batch.put(&wm_key, &state.watermark.watermark_ms.to_be_bytes());
        batch
    };

    db.write_batch(batch).await.map_err(OpError::storage)?;
    Ok(())
}

/// Load HopWindowOp state from a ShardDb using the shared TW keyspace.
pub async fn load_hop_window_state(
    db: &ShardDb,
    input_schema: SchemaRef,
    time_col: usize,
    window_size_ms: i64,
    slide_ms: i64,
    late_data_policy: LateDataPolicy,
    op_id: OperatorId,
) -> Result<HopWindowOp, OpError> {
    let op = HopWindowOp::new(
        input_schema,
        time_col,
        window_size_ms,
        slide_ms,
        late_data_policy,
    );
    let n_input_cols = op.n_input_cols;
    let oid = op_id.0;
    let prefix = ShardKeyEncoder::tumble_window_op_prefix(oid);
    let entries = db.scan_prefix(&prefix).await.map_err(OpError::storage)?;
    let prefix_len = prefix.len();
    let wm_key = ShardKeyEncoder::watermark_key(oid);
    let wm_bytes = db.get(&wm_key).await.ok().flatten();

    let mut st = op.state.lock().unwrap();
    for (key, value) in entries {
        if let Some((row_vals, weight)) = decode_window_value(&value, n_input_cols) {
            if key.len() < prefix_len + 8 {
                continue;
            }
            let window_id =
                i64::from_be_bytes(key[prefix_len..prefix_len + 8].try_into().unwrap_or([0; 8]));
            let group_key = key[prefix_len + 8..].to_vec();
            st.windows
                .entry(window_id)
                .or_default()
                .insert(group_key, (row_vals, weight));
        }
    }

    if let Some(bytes) = wm_bytes {
        if bytes.len() >= 8 {
            let wm = i64::from_be_bytes(bytes[0..8].try_into().unwrap_or([0; 8]));
            st.watermark = WatermarkState { watermark_ms: wm };
        }
    }

    let total = st.total_rows();
    drop(st);
    op.fill_level.store(total, Ordering::Relaxed);
    Ok(op)
}

/// Persist SessionWindowOp state to a ShardDb using the shared TW keyspace.
pub async fn persist_session_window_state(
    db: &ShardDb,
    op: &SessionWindowOp,
    op_id: OperatorId,
) -> Result<(), OpError> {
    let batch = {
        let state = op.state.lock().unwrap();
        let mut batch = WriteBatch::new();
        let oid = op_id.0;
        for partition in state.partitions.values() {
            for (row_key, (row_vals, weight)) in &partition.rows {
                let event_time_ms = row_vals.get(op.time_col).copied().unwrap_or(0);
                let key = ShardKeyEncoder::tumble_window_key(oid, event_time_ms, row_key);
                let value = encode_window_value(row_vals, *weight);
                batch.put(&key, &value);
            }
        }
        let wm_key = ShardKeyEncoder::watermark_key(oid);
        batch.put(&wm_key, &state.watermark.watermark_ms.to_be_bytes());
        batch
    };
    db.write_batch(batch).await.map_err(OpError::storage)?;
    Ok(())
}

/// Load SessionWindowOp state from a ShardDb using the shared TW keyspace.
pub async fn load_session_window_state(
    db: &ShardDb,
    input_schema: SchemaRef,
    time_col: usize,
    gap_ms: i64,
    late_data_policy: LateDataPolicy,
    op_id: OperatorId,
) -> Result<SessionWindowOp, OpError> {
    let op = SessionWindowOp::new(input_schema, time_col, gap_ms, late_data_policy);
    let n_input_cols = op.n_input_cols;
    let oid = op_id.0;
    let prefix = ShardKeyEncoder::tumble_window_op_prefix(oid);
    let entries = db.scan_prefix(&prefix).await.map_err(OpError::storage)?;
    let wm_key = ShardKeyEncoder::watermark_key(oid);
    let wm_bytes = db.get(&wm_key).await.ok().flatten();

    let mut st = op.state.lock().unwrap();
    for (_key, value) in entries {
        if let Some((row_vals, weight)) = decode_window_value(&value, n_input_cols) {
            let partition_key = encode_partition_key(&row_vals, time_col);
            let row_key = encode_row(&row_vals);
            st.partitions
                .entry(partition_key)
                .or_default()
                .rows
                .insert(row_key, (row_vals, weight));
        }
    }
    if let Some(bytes) = wm_bytes {
        if bytes.len() >= 8 {
            let wm = i64::from_be_bytes(bytes[0..8].try_into().unwrap_or([0; 8]));
            st.watermark = WatermarkState { watermark_ms: wm };
        }
    }
    for partition in st.partitions.values_mut() {
        let live_rows = partition
            .rows
            .values()
            .filter(|(_, weight)| *weight > 0)
            .map(|(vals, _)| vals.clone())
            .collect::<Vec<_>>();
        let sessions = derive_sessions(&live_rows, time_col, gap_ms);
        partition.live_session_count = sessions
            .iter()
            .map(|(start, end, _)| (*start, *end))
            .collect::<HashSet<_>>()
            .len();
        for (session_start, session_end, row_vals) in sessions {
            let mut tagged = vec![session_start, session_end];
            tagged.extend_from_slice(&row_vals);
            partition.prev_output.insert(encode_row(&row_vals), tagged);
        }
    }
    let total = st.total_sessions();
    drop(st);
    op.fill_level.store(total, Ordering::Relaxed);
    Ok(op)
}

/// Load the persisted watermark for an operator (used by compaction filters).
pub async fn load_watermark(db: &ShardDb, op_id: OperatorId) -> Result<WatermarkState, OpError> {
    let wm_key = ShardKeyEncoder::watermark_key(op_id.0);
    match db.get(&wm_key).await.map_err(OpError::storage)? {
        Some(bytes) if bytes.len() >= 8 => {
            let wm = i64::from_be_bytes(bytes[0..8].try_into().unwrap_or([0; 8]));
            Ok(WatermarkState { watermark_ms: wm })
        }
        _ => Ok(WatermarkState::new()),
    }
}

/// Persist the watermark for an operator.
pub async fn persist_watermark(
    db: &ShardDb,
    op_id: OperatorId,
    watermark: WatermarkState,
) -> Result<(), OpError> {
    let mut batch = WriteBatch::new();
    let wm_key = ShardKeyEncoder::watermark_key(op_id.0);
    batch.put(&wm_key, &watermark.watermark_ms.to_be_bytes());
    db.write_batch(batch).await.map_err(OpError::storage)?;
    Ok(())
}

// ─── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn input_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("t", DataType::Int64, false), // time_col = 0
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn make_input(rows: &[(i64, i64, i64)]) -> ArrowZSet {
        let t: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let v: Vec<i64> = rows.iter().map(|r| r.1).collect();
        let w: Vec<i64> = rows.iter().map(|r| r.2).collect();
        let data = RecordBatch::try_new(
            input_schema(),
            vec![
                Arc::new(Int64Array::from(t)) as ArrayRef,
                Arc::new(Int64Array::from(v)) as ArrayRef,
            ],
        )
        .unwrap();
        ArrowZSet::new(data, w)
    }

    fn accumulate(state: &mut HashMap<(i64, i64, i64), i64>, zset: &ArrowZSet) {
        if zset.is_empty() {
            return;
        }
        let wid_col = zset
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let t_col = zset
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let v_col = zset
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..zset.num_rows() {
            *state
                .entry((wid_col.value(i), t_col.value(i), v_col.value(i)))
                .or_insert(0) += zset.weights[i];
        }
    }

    fn live_rows(state: &HashMap<(i64, i64, i64), i64>) -> Vec<(i64, i64, i64)> {
        let mut rows: Vec<_> = state
            .iter()
            .filter(|(_, &w)| w > 0)
            .map(|(&k, _)| k)
            .collect();
        rows.sort();
        rows
    }

    /// Regression test for a v0.51.4 fix: a retraction of a row belonging to
    /// an already-finalized window must always be applied (never dropped by
    /// the late-data policy), and must not disturb an unrelated, untouched
    /// group sharing the same window.
    #[test]
    fn retraction_reopens_finalized_window_without_disturbing_other_groups() {
        // window_size_ms = 10; group A = (t=1, v=100), group B = (t=2, v=200),
        // both land in window 0. A later row (t=1000, v=999) advances the
        // watermark far enough to finalize window 0.
        let op = TumbleWindowOp::new(input_schema(), 0, 10, LateDataPolicy::Drop);
        let mut acc: HashMap<(i64, i64, i64), i64> = HashMap::new();

        let out1 = op
            .process_epoch(make_input(&[(1, 100, 1), (2, 200, 1), (1000, 999, 1)]), 1)
            .unwrap();
        accumulate(&mut acc, &out1);
        assert_eq!(
            live_rows(&acc),
            vec![(0, 1, 100), (0, 2, 200), (1000, 1000, 999)]
        );

        // Window 0 should now be finalized (watermark 1000 > window_end 10).
        {
            let state = op.state.lock().unwrap();
            assert!(
                state.finalized.contains(&0),
                "window 0 should be finalized after epoch 1"
            );
        }

        // Retract group B only. Group A must remain untouched/live.
        let out2 = op.process_epoch(make_input(&[(2, 200, -1)]), 2).unwrap();
        accumulate(&mut acc, &out2);
        let live = live_rows(&acc);
        assert_eq!(
            live,
            vec![(0, 1, 100), (1000, 1000, 999)],
            "group A (t=1,v=100) must survive an unrelated retraction of group B in the same window: {live:?}"
        );
    }

    fn accumulate_session(state: &mut HashMap<(i64, i64, i64, i64), i64>, zset: &ArrowZSet) {
        if zset.is_empty() {
            return;
        }
        let session_start = zset
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let session_end = zset
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let t_col = zset
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let v_col = zset
            .data
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..zset.num_rows() {
            *state
                .entry((
                    session_start.value(i),
                    session_end.value(i),
                    t_col.value(i),
                    v_col.value(i),
                ))
                .or_insert(0) += zset.weights[i];
        }
    }

    fn live_session_rows(state: &HashMap<(i64, i64, i64, i64), i64>) -> Vec<(i64, i64, i64, i64)> {
        let mut rows: Vec<_> = state
            .iter()
            .filter(|(_, &w)| w > 0)
            .map(|(&k, _)| k)
            .collect();
        rows.sort();
        rows
    }

    fn session_rows(zset: &ArrowZSet) -> Vec<(i64, i64, i64, i64, i64)> {
        if zset.is_empty() {
            return Vec::new();
        }
        let session_start = zset
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let session_end = zset
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let t_col = zset
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let v_col = zset
            .data
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut rows = Vec::new();
        for i in 0..zset.num_rows() {
            rows.push((
                session_start.value(i),
                session_end.value(i),
                t_col.value(i),
                v_col.value(i),
                zset.weights[i],
            ));
        }
        rows.sort();
        rows
    }

    fn batch_hop_rows(
        rows: &[(i64, i64, i64)],
        window_size_ms: i64,
        slide_ms: i64,
    ) -> Vec<(i64, i64, i64)> {
        let mut net: HashMap<(i64, i64, i64), i64> = HashMap::new();
        for &(t, v, w) in rows {
            if w == 0 {
                continue;
            }
            for window_id in hop_window_ids(t, window_size_ms, slide_ms) {
                *net.entry((window_id, t, v)).or_insert(0) += w;
            }
        }
        live_rows(&net)
    }

    fn batch_session_rows(rows: &[(i64, i64, i64)], gap_ms: i64) -> Vec<(i64, i64, i64, i64)> {
        let mut live_inputs: HashMap<(i64, i64), i64> = HashMap::new();
        for &(t, v, w) in rows {
            *live_inputs.entry((t, v)).or_insert(0) += w;
        }
        let mut by_partition: HashMap<i64, Vec<Vec<i64>>> = HashMap::new();
        for ((t, v), weight) in live_inputs {
            if weight > 0 {
                by_partition.entry(v).or_default().push(vec![t, v]);
            }
        }
        let mut out = Vec::new();
        for rows in by_partition.into_values() {
            for (session_start, session_end, row_vals) in derive_sessions(&rows, 0, gap_ms) {
                out.push((session_start, session_end, row_vals[0], row_vals[1]));
            }
        }
        out.sort();
        out
    }

    #[test]
    fn tumble_window_basic_assignment() {
        let window_size_ms = 1000i64;
        let op = TumbleWindowOp::new(input_schema(), 0, window_size_ms, LateDataPolicy::Drop);

        // Epoch 1: two rows in window 0 [0, 1000), one row in window 1 [1000, 2000).
        let out1 = op
            .process_epoch(make_input(&[(100, 10, 1), (500, 20, 1), (1500, 30, 1)]), 1)
            .unwrap();

        let mut net: HashMap<(i64, i64, i64), i64> = Default::default();
        accumulate(&mut net, &out1);

        let live = live_rows(&net);
        // window_id=0: (0, 100, 10), (0, 500, 20)
        // window_id=1000: (1000, 1500, 30)
        assert!(
            live.contains(&(0, 100, 10)),
            "expected (0, 100, 10) in output"
        );
        assert!(
            live.contains(&(0, 500, 20)),
            "expected (0, 500, 20) in output"
        );
        assert!(
            live.contains(&(1000, 1500, 30)),
            "expected (1000, 1500, 30) in output"
        );

        // Epoch 2: add one row in window 1 (t=1600 >= watermark=1500, not late).
        let out2 = op.process_epoch(make_input(&[(1600, 40, 1)]), 2).unwrap();

        accumulate(&mut net, &out2);
        let live2 = live_rows(&net);
        assert!(
            live2.contains(&(1000, 1600, 40)),
            "expected (1000, 1600, 40) in epoch 2"
        );
        assert_eq!(op.fill_level(), 4, "4 positive-weight rows total");
    }

    #[test]
    fn tumble_window_watermark_merge_idempotent() {
        let w1 = WatermarkState { watermark_ms: 1000 };
        let w2 = WatermarkState { watermark_ms: 2000 };

        // Idempotent: merge(w, w) = w.
        assert_eq!(w1.merge(w1), w1);
        assert_eq!(w2.merge(w2), w2);

        // merge(w1, w2) = max.
        assert_eq!(w1.merge(w2), WatermarkState { watermark_ms: 2000 });
        assert_eq!(w2.merge(w1), WatermarkState { watermark_ms: 2000 });

        // merge is commutative.
        assert_eq!(w1.merge(w2), w2.merge(w1));
    }

    #[test]
    fn tumble_window_late_data_dropped() {
        let window_size_ms = 1000i64;
        let op = TumbleWindowOp::new(input_schema(), 0, window_size_ms, LateDataPolicy::Drop);

        // Epoch 1: insert a row in window [0, 1000). Watermark advances to 100.
        let _out1 = op.process_epoch(make_input(&[(100, 10, 1)]), 1).unwrap();

        // Epoch 2: advance watermark past window end (2000 > 0 + 1000).
        let _out2 = op.process_epoch(make_input(&[(2000, 99, 1)]), 2).unwrap();

        assert!(
            op.watermark_ms() >= 2000,
            "watermark should be >= 2000 after epoch 2"
        );

        // Epoch 3: insert a late row in window [0, 1000) — event_time=50 < watermark=2000.
        let out3 = op.process_epoch(make_input(&[(50, 10, 1)]), 3).unwrap();

        // The late row must NOT appear in the output.
        assert!(
            out3.is_empty(),
            "late row must not appear in output; got {} rows",
            out3.num_rows()
        );
    }

    #[test]
    fn hop_window_assigns_row_to_all_overlapping_windows() {
        let op = HopWindowOp::new(input_schema(), 0, 1000, 500, LateDataPolicy::Drop);
        let out = op.process_epoch(make_input(&[(1250, 42, 1)]), 1).unwrap();
        let mut net: HashMap<(i64, i64, i64), i64> = Default::default();
        accumulate(&mut net, &out);
        assert_eq!(live_rows(&net), vec![(500, 1250, 42), (1000, 1250, 42)]);
        assert_eq!(op.fill_level(), 2);
    }

    #[test]
    fn hop_window_matches_batch_oracle_with_overlap() {
        let window_size_ms = 1000;
        let slide_ms = 500;
        let op = HopWindowOp::new(
            input_schema(),
            0,
            window_size_ms,
            slide_ms,
            LateDataPolicy::Drop,
        );
        let mut net: HashMap<(i64, i64, i64), i64> = Default::default();

        let out1 = op
            .process_epoch(make_input(&[(250, 10, 1), (1250, 20, 1)]), 1)
            .unwrap();
        accumulate(&mut net, &out1);

        let out2 = op
            .process_epoch(make_input(&[(1250, 20, -1), (1750, 30, 1)]), 2)
            .unwrap();
        accumulate(&mut net, &out2);

        let expected = batch_hop_rows(
            &[(250, 10, 1), (1250, 20, 1), (1750, 30, 1), (1250, 20, -1)],
            window_size_ms,
            slide_ms,
        );
        assert_eq!(live_rows(&net), expected);
    }

    #[test]
    fn hop_window_late_data_policy_matches_tumble_semantics() {
        let op = HopWindowOp::new(input_schema(), 0, 1000, 500, LateDataPolicy::Drop);
        op.process_epoch(make_input(&[(100, 10, 1)]), 1).unwrap();
        op.process_epoch(make_input(&[(2500, 99, 1)]), 2).unwrap();
        let out = op.process_epoch(make_input(&[(50, 10, 1)]), 3).unwrap();
        assert!(out.is_empty(), "late hop row must be dropped");
        assert!(op.watermark_ms() >= 2500);
    }

    #[test]
    fn hop_window_state_bound_scales_with_overlap_and_backpressures() {
        let op = HopWindowOp::new(
            input_schema(),
            0,
            ((HOP_WINDOW_STATE_LIMIT + 1) as i64) * 1000,
            1000,
            LateDataPolicy::Drop,
        );
        let err = op
            .process_epoch(make_input(&[(0, 7, 1)]), 1)
            .expect_err("overlap-aware bound must backpressure");
        match err {
            OpError::HopWindowStateOverflow { current, limit, .. } => {
                assert_eq!(current, HOP_WINDOW_STATE_LIMIT + 1);
                assert_eq!(limit, HOP_WINDOW_STATE_LIMIT);
            }
            other => panic!("expected HopWindowStateOverflow, got {other:?}"),
        }
        assert_eq!(op.fill_level(), 0, "overflow must not commit partial state");
    }

    #[test]
    fn session_window_extends_open_session_within_gap() {
        let op = SessionWindowOp::new(input_schema(), 0, 1000, LateDataPolicy::Drop);
        let mut net: HashMap<(i64, i64, i64, i64), i64> = Default::default();
        accumulate_session(
            &mut net,
            &op.process_epoch(make_input(&[(100, 7, 1)]), 1).unwrap(),
        );
        accumulate_session(
            &mut net,
            &op.process_epoch(make_input(&[(900, 7, 1)]), 2).unwrap(),
        );
        assert_eq!(
            live_session_rows(&net),
            vec![(100, 900, 100, 7), (100, 900, 900, 7)]
        );
    }

    #[test]
    fn session_window_starts_new_session_after_gap() {
        let op = SessionWindowOp::new(input_schema(), 0, 1000, LateDataPolicy::Drop);
        let mut net: HashMap<(i64, i64, i64, i64), i64> = Default::default();
        accumulate_session(
            &mut net,
            &op.process_epoch(make_input(&[(100, 7, 1)]), 1).unwrap(),
        );
        accumulate_session(
            &mut net,
            &op.process_epoch(make_input(&[(1500, 7, 1)]), 2).unwrap(),
        );
        assert_eq!(
            live_session_rows(&net),
            vec![(100, 100, 100, 7), (1500, 1500, 1500, 7)]
        );
    }

    #[test]
    fn session_window_merge_retracts_both_and_emits_replacement() {
        let op = SessionWindowOp::new(input_schema(), 0, 1000, LateDataPolicy::Drop);
        let _ = op
            .process_epoch(
                make_input(&[(100, 7, 1), (900, 7, 1), (2100, 7, 1), (2500, 7, 1)]),
                1,
            )
            .unwrap();
        {
            let state = op.state.lock().unwrap();
            let partition = state.partitions.values().next().unwrap();
            assert_eq!(partition.live_session_count, 2);
        }
        let out = op.process_epoch(make_input(&[(1500, 7, 1)]), 2).unwrap();
        let rows = session_rows(&out);
        {
            let state = op.state.lock().unwrap();
            let partition = state.partitions.values().next().unwrap();
            assert_eq!(
                partition.live_session_count,
                1,
                "rows={:?} prev={:?}",
                partition.rows.values().collect::<Vec<_>>(),
                partition.prev_output.values().collect::<Vec<_>>()
            );
        }
        assert!(rows.contains(&(100, 900, 100, 7, -1)), "{rows:?}");
        assert!(rows.contains(&(100, 900, 900, 7, -1)), "{rows:?}");
        assert!(rows.contains(&(2100, 2500, 2100, 7, -1)), "{rows:?}");
        assert!(rows.contains(&(2100, 2500, 2500, 7, -1)), "{rows:?}");
        assert!(rows.contains(&(100, 2500, 100, 7, 1)), "{rows:?}");
        assert!(rows.contains(&(100, 2500, 900, 7, 1)), "{rows:?}");
        assert!(rows.contains(&(100, 2500, 1500, 7, 1)), "{rows:?}");
        assert!(rows.contains(&(100, 2500, 2100, 7, 1)), "{rows:?}");
        assert!(rows.contains(&(100, 2500, 2500, 7, 1)), "{rows:?}");
    }

    #[test]
    fn session_window_matches_batch_oracle_under_retraction_storm() {
        let op = SessionWindowOp::new(input_schema(), 0, 1000, LateDataPolicy::Drop);
        let mut net: HashMap<(i64, i64, i64, i64), i64> = Default::default();
        let epochs = [
            vec![(100, 7, 1), (2500, 7, 1), (700, 9, 1)],
            vec![(1500, 7, 1), (700, 9, -1), (1200, 9, 1)],
            vec![(100, 7, -1), (2600, 7, 1)],
        ];
        let mut input_rows = Vec::new();
        for (epoch, rows) in epochs.into_iter().enumerate() {
            input_rows.extend(rows.iter().copied());
            accumulate_session(
                &mut net,
                &op.process_epoch(make_input(rows.as_slice()), epoch as u64 + 1)
                    .unwrap(),
            );
        }
        assert_eq!(
            live_session_rows(&net),
            batch_session_rows(&input_rows, 1000),
            "net={:?}",
            live_session_rows(&net)
        );
    }

    #[test]
    fn session_window_state_bound_backpressures() {
        let op = SessionWindowOp::new(input_schema(), 0, 0, LateDataPolicy::Drop);
        let rows: Vec<_> = (0..=SESSION_WINDOW_STATE_LIMIT as i64)
            .map(|idx| (idx * 10, idx, 1))
            .collect();
        let err = op
            .process_epoch(make_input(rows.as_slice()), 1)
            .expect_err("session bound must backpressure");
        match err {
            OpError::SessionWindowStateOverflow { current, limit, .. } => {
                assert_eq!(current, SESSION_WINDOW_STATE_LIMIT + 1);
                assert_eq!(limit, SESSION_WINDOW_STATE_LIMIT);
            }
            other => panic!("expected SessionWindowStateOverflow, got {other:?}"),
        }
        assert_eq!(op.fill_level(), 0);
    }

    #[test]
    fn tumble_window_compaction_filter_no_early_eviction() {
        // Build a TW key for window_id=1000 with some group_key.
        let op_id = 7u64;
        let window_id = 1000i64;
        let window_size_ms = 1000i64;
        let key = ShardKeyEncoder::tumble_window_key(op_id, window_id, b"gk");

        // Scenario: watermark past TTL, but frontier has NOT advanced past window_end.
        // window_end = window_id + window_size_ms = 2000
        // TTL condition: 2000 + 0 < 3000 = true (watermark=3000)
        // Frontier condition: frontier_ms=1500 > 2000 = FALSE
        let filter = CompactionFilter {
            watermark_ms: 3000,
            window_size_ms,
            allowed_lateness_ms: 0,
            frontier_ms: 1500, // NOT past window_end=2000
        };
        assert!(
            !filter.may_delete(&key),
            "must not delete when frontier has not advanced past window_end"
        );

        // Now advance frontier past window_end: frontier=2001 > 2000.
        let filter2 = CompactionFilter {
            watermark_ms: 3000,
            window_size_ms,
            allowed_lateness_ms: 0,
            frontier_ms: 2001,
        };
        assert!(
            filter2.may_delete(&key),
            "may delete when both TTL and frontier conditions are satisfied"
        );
    }

    #[test]
    fn floor_div_handles_negatives() {
        assert_eq!(floor_div(0, 1000), 0);
        assert_eq!(floor_div(999, 1000), 0);
        assert_eq!(floor_div(1000, 1000), 1);
        assert_eq!(floor_div(1999, 1000), 1);
        assert_eq!(floor_div(-1, 1000), -1);
        assert_eq!(floor_div(-1000, 1000), -1);
        assert_eq!(floor_div(-1001, 1000), -2);
    }

    #[test]
    fn tumble_window_frontier_advancement_closes_window() {
        use rockstream_types::frontier::{FreshnessToken, SourceProgress};
        use rockstream_types::ids::SourceId;
        use std::collections::BTreeMap;

        let window_size_ms = 1000i64;
        let op = TumbleWindowOp::new(input_schema(), 0, window_size_ms, LateDataPolicy::Drop);

        // Epoch 1: Add row at t=100, no frontier yet.
        let out1 = op.process_epoch(make_input(&[(100, 10, 1)]), 1).unwrap();
        assert_eq!(out1.num_rows(), 1);

        // Epoch 2: Empty input but with FreshnessToken frontier advancing watermark past window end (watermark=1500 > 1000).
        let mut source_progress = BTreeMap::new();
        source_progress.insert(SourceId(1), SourceProgress::new(1, Some(1500)));
        let token = FreshnessToken::new(source_progress, 999);

        let mut input_empty = make_input(&[]);
        input_empty = input_empty.with_frontier(token);

        let _out2 = op.process_epoch(input_empty, 2).unwrap();
        // The window [0, 1000) should be closed and finalized. Let's verify that watermark advanced.
        assert_eq!(op.watermark_ms(), 1500);

        // The state should now have finalized window_id = 0.
        let state = op.state.lock().unwrap();
        assert!(state.finalized.contains(&0i64));
    }
}
