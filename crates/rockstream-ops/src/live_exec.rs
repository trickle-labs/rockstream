//! `StatefulPipeline`: an executable operator chain that can host stateful
//! operators (Aggregate, Distinct, TumbleWindow/HopWindow/SessionWindow,
//! Window, TopK) alongside stateless ones (Filter/Project/Map), addressed by
//! a persisted `OperatorId`.
//!
//! Part of v0.51.4 Slices 1-6 ("True Incremental Maintenance on the Serving
//! Path"). `StatefulPipeline` generalizes `LinearPipeline` (`pipeline.rs`):
//! the synchronous `process` step threads a delta through every stage in
//! order (reusing each stateful operator's existing `Operator::process_delta`
//! or, for epoch-keyed operators (`WindowOp`/`TopKOp`), its
//! `process_epoch`), while the separate async `persist` step writes each
//! stateful stage's arrangement to storage (reusing each operator's existing
//! `persist_*_state` function), so callers can call `process` synchronously
//! and only pay the `.await` cost once per commit for persistence.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::ids::OperatorId;

use crate::aggregate::{append_agg_state, persist_agg_state, AggregateOp};
use crate::distinct::{persist_distinct_state, DistinctOp};
use crate::error::OpError;
use crate::join::JoinOp;
use crate::op::Operator;
use crate::outer_join::OuterJoinOp;
use crate::time_window::{
    persist_hop_window_state, persist_session_window_state, persist_tumble_window_state,
    HopWindowOp, SessionWindowOp, TumbleWindowOp,
};
use crate::topk::{persist_topk_state, TopKOp};
use crate::window::{persist_window_state, WindowOp};
use crate::zset::ArrowZSet;

/// Build a schema of `n` non-nullable `Int64` columns named `c0..c(n-1)`.
///
/// Every stateful operator wired here (`AggregateOp`, `DistinctOp`,
/// `TumbleWindowOp`, `HopWindowOp`, `WindowOp`, `TopKOp`) only supports
/// `Int64`-typed columns today (pre-existing operator constraint, not
/// introduced by this wiring) so a uniform column-count-only schema
/// (no real type inference) is sufficient to satisfy their constructors.
pub fn int64_schema(n: usize) -> SchemaRef {
    Arc::new(Schema::new(
        (0..n)
            .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ))
}

/// Allocate a fresh `OperatorId` for a stateful stage that has no
/// plan-carried id of its own (`Aggregate`, `TumbleWindow`, `HopWindow`,
/// `Window`, `TopK` PlanNodes carry no `OperatorId` field — unlike
/// `InnerJoin`/`OuterJoin`/`Distinct`, which do).
static NEXT_STATEFUL_OP_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// Set by `with_view_id_scope` while `compile_plan` is compiling a
    /// given view: `(base, next_offset)`. When present, `next_stateful_op_id`
    /// allocates `base + next_offset` (and increments `next_offset`) instead
    /// of drawing from the process-global `NEXT_STATEFUL_OP_ID` counter.
    static VIEW_ID_SCOPE: std::cell::Cell<Option<(u64, u64)>> = const { std::cell::Cell::new(None) };
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// A view's compiled-in internal stage ids (`AggregateOp`, `TumbleWindowOp`,
/// key packers, ...) must be reproducible across processes — a gateway
/// restart recompiles every catalog-registered view (`recover_compiled_views`)
/// and each recompiled stage's `OperatorId` must land on the exact storage
/// keys the pre-restart process wrote, or the restored arrangement would be
/// silently empty. `next_stateful_op_id`'s previous implementation drew from
/// a single process-lifetime atomic counter, so recompilation order (which
/// differs across processes) produced different ids for the same view.
///
/// `with_view_id_scope` fixes this: every stateful id requested by
/// `compile_plan` while compiling `view_name` is derived deterministically
/// from `view_name` plus a per-compilation sequence number, so compiling the
/// same view's SQL twice (same process or a fresh one) always yields the
/// same ids, independent of what other views were compiled before it.
pub fn with_view_id_scope<T>(view_name: &str, f: impl FnOnce() -> T) -> T {
    // Large stride keeps each view's id range far apart from the next
    // view's, and the additive offset keeps this range clear of the small,
    // hand-assigned literal `OperatorId`s used elsewhere in the codebase
    // (e.g. join `left_arr_id`/`right_arr_id` test fixtures).
    const STRIDE: u64 = 1_000_000;
    const BASE_OFFSET: u64 = 1_000_000_000_000;
    let base = BASE_OFFSET.wrapping_add(fnv1a64(view_name.as_bytes()).wrapping_mul(STRIDE));
    let prev = VIEW_ID_SCOPE.with(|c| c.replace(Some((base, 0))));
    let result = f();
    VIEW_ID_SCOPE.with(|c| c.set(prev));
    result
}

/// Storage key prefix tag for `GroupKeyPacker`'s persisted intern table
/// (distinct from any `ShardPrefix` variant used elsewhere — this table is
/// local to `rockstream-ops`'s live-exec wiring, not a core shard concept).
const GROUP_KEY_PACKER_PREFIX: &[u8] = &[0x01, 0x4B, 0x50];
const UTF8_PACKER_PREFIX: &[u8] = &[0x01, 0x55, 0x50];

fn append_utf8_packer_state(
    forward: &Mutex<HashMap<String, i64>>,
    op_id: OperatorId,
    target: &mut WriteBatch,
) {
    let forward = forward.lock().unwrap();
    for (value, surrogate) in forward.iter() {
        let mut key = Vec::with_capacity(UTF8_PACKER_PREFIX.len() + 8 + value.len());
        key.extend_from_slice(UTF8_PACKER_PREFIX);
        key.extend_from_slice(&op_id.0.to_be_bytes());
        key.extend_from_slice(value.as_bytes());
        target.put(&key, &surrogate.to_be_bytes());
    }
}

async fn restore_utf8_packer_state(
    db: &ShardDb,
    op_id: OperatorId,
    forward: &Mutex<HashMap<String, i64>>,
    reverse: &Mutex<HashMap<i64, String>>,
    next_id: &Mutex<i64>,
) -> Result<(), OpError> {
    let mut prefix = Vec::with_capacity(UTF8_PACKER_PREFIX.len() + 8);
    prefix.extend_from_slice(UTF8_PACKER_PREFIX);
    prefix.extend_from_slice(&op_id.0.to_be_bytes());
    let entries = db.scan_prefix(&prefix).await.map_err(OpError::storage)?;
    let mut forward = forward.lock().unwrap();
    let mut reverse = reverse.lock().unwrap();
    let mut max_id = -1;
    for (key, value) in entries {
        if value.len() != 8 {
            continue;
        }
        let Ok(text) = std::str::from_utf8(&key[prefix.len()..]) else {
            continue;
        };
        let mut raw = [0; 8];
        raw.copy_from_slice(&value);
        let surrogate = i64::from_be_bytes(raw);
        forward.insert(text.to_string(), surrogate);
        reverse.insert(surrogate, text.to_string());
        max_id = max_id.max(surrogate);
    }
    *next_id.lock().unwrap() = max_id + 1;
    Ok(())
}

pub fn next_stateful_op_id() -> OperatorId {
    let scoped = VIEW_ID_SCOPE.with(|c| match c.get() {
        Some((base, offset)) => {
            c.set(Some((base, offset + 1)));
            Some(OperatorId(base + offset))
        }
        None => None,
    });
    scoped.unwrap_or_else(|| OperatorId(NEXT_STATEFUL_OP_ID.fetch_add(1, Ordering::Relaxed)))
}

/// One stage of a `StatefulPipeline`.
pub enum Stage {
    /// A stateless operator (`FilterOp`/`ProjectOp`/`MapOp`) — no persisted
    /// identity, nothing to load/persist.
    Stateless(Arc<dyn Operator>),
    Aggregate(Arc<AggregateOp>),
    /// v0.51.4 gap-fix: `MIN`/`MAX` aggregates, via the pre-existing
    /// (v0.6), oracle-proven, retraction-safe `MinMaxOp` — `AggregateOp`
    /// itself only ever computes sum/count/avg.
    MinMax(Arc<crate::minmax::MinMaxOp>, OperatorId),
    Distinct(Arc<DistinctOp>, OperatorId),
    TumbleWindow(Arc<TumbleWindowOp>, OperatorId),
    HopWindow(Arc<HopWindowOp>, OperatorId),
    /// v0.51.4 Slice 6: `SessionWindow` (data-dependent, gap-delimited
    /// event-time sessions — `time_window.rs`'s pre-existing, oracle-proven
    /// `SessionWindowOp`).
    SessionWindow(Arc<SessionWindowOp>, OperatorId),
    Window(Arc<WindowOp>, OperatorId),
    TopK(Arc<TopKOp>, OperatorId),
    /// v0.51.4 Slice 6: packs N `Int64` group-by columns into the single
    /// surrogate `Int64` key `AggregateOp` requires — see `GroupKeyPacker`.
    /// Input rows: `(k0, .., k_{n-1}, v)`; output rows: `(surrogate_k, v)`.
    KeyPack(Arc<GroupKeyPacker>, OperatorId),
    /// The inverse of `KeyPack`, applied to `AggregateOp`'s output: input
    /// rows `(surrogate_k, sum, count, avg)`; output rows
    /// `(k0, .., k_{n-1}, sum, count, avg)`.
    KeyUnpack(Arc<GroupKeyPacker>, OperatorId),
    /// v0.51.4 Slice 8: packs a single non-`Int64` (`Utf8`) group-by column
    /// into `AggregateOp`'s single-`Int64`-key shape — see `Utf8KeyPacker`.
    Utf8KeyPack(Arc<Utf8KeyPacker>, OperatorId),
    /// The inverse of `Utf8KeyPack`.
    Utf8KeyUnpack(Arc<Utf8KeyPacker>, OperatorId),
    /// v0.51.4 Slice 8: packs one arbitrary `Utf8` passthrough column (e.g.
    /// a join side's non-key `TEXT` column) into an `Int64` surrogate —
    /// see `Utf8ColumnPacker`. The `usize` is the column index to pack.
    Utf8ColumnPack(Arc<Utf8ColumnPacker>, usize, OperatorId),
    /// The inverse of `Utf8ColumnPack`. The `usize` is the column index to
    /// unpack (may differ from the pack-time index — e.g. shifted by the
    /// left side's column count after a join concatenates both sides).
    Utf8ColumnUnpack(Arc<Utf8ColumnPacker>, usize, OperatorId),
    /// v0.51.4 Slice 8: multiple aggregates sharing one group-by key,
    /// composed from independent single-aggregate lanes cascade-joined by
    /// key — see `MultiAggregatePipeline`.
    MultiAggregate(Arc<MultiAggregatePipeline>),
}

impl Stage {
    fn process(&self, delta: ArrowZSet, epoch: u64) -> Result<ArrowZSet, OpError> {
        match self {
            Stage::Stateless(op) => op.process_delta(delta),
            Stage::Aggregate(op) => op.process_delta(delta),
            Stage::MinMax(op, _) => op.process_delta(delta),
            Stage::Distinct(op, _) => op.process_delta(delta),
            Stage::TumbleWindow(op, _) => op.process_delta(delta),
            Stage::HopWindow(op, _) => op.process_delta(delta),
            Stage::SessionWindow(op, _) => op.process_delta(delta),
            Stage::Window(op, _) => op.process_epoch(delta, epoch),
            Stage::TopK(op, _) => op.process_epoch(delta, epoch),
            Stage::KeyPack(op, _) => op.pack(delta),
            Stage::KeyUnpack(op, _) => op.unpack(delta),
            Stage::Utf8KeyPack(op, _) => op.pack(delta),
            Stage::Utf8KeyUnpack(op, _) => op.unpack(delta),
            Stage::Utf8ColumnPack(op, col_idx, _) => op.pack(delta, *col_idx),
            Stage::Utf8ColumnUnpack(op, col_idx, _) => op.unpack(delta, *col_idx),
            Stage::MultiAggregate(op) => op.process(delta),
        }
    }

    pub fn state_bytes(&self) -> u64 {
        match self {
            Stage::Stateless(op) => op.state_bytes(),
            Stage::Aggregate(op) => op.state_bytes(),
            Stage::MinMax(op, _) => op.state_bytes(),
            Stage::Distinct(op, _) => op.state_bytes(),
            Stage::TumbleWindow(op, _) => op.state_bytes(),
            Stage::HopWindow(op, _) => op.state_bytes(),
            Stage::SessionWindow(op, _) => op.state_bytes(),
            Stage::Window(op, _) => op.state_bytes(),
            Stage::TopK(op, _) => op.state_bytes(),
            Stage::KeyPack(_, _) | Stage::KeyUnpack(_, _) => 0,
            Stage::Utf8KeyPack(_, _) | Stage::Utf8KeyUnpack(_, _) => 0,
            Stage::Utf8ColumnPack(_, _, _) | Stage::Utf8ColumnUnpack(_, _, _) => 0,
            Stage::MultiAggregate(op) => op.state_bytes(),
        }
    }

    async fn persist(&self, db: &ShardDb) -> Result<(), OpError> {
        match self {
            Stage::Stateless(_) => Ok(()),
            Stage::Aggregate(op) => persist_agg_state(db, op).await,
            Stage::MinMax(op, _) => crate::minmax::persist_minmax_state(db, op).await,
            Stage::Distinct(op, id) => persist_distinct_state(db, op, *id).await,
            Stage::TumbleWindow(op, id) => persist_tumble_window_state(db, op, *id).await,
            Stage::HopWindow(op, id) => persist_hop_window_state(db, op, *id).await,
            Stage::SessionWindow(op, id) => persist_session_window_state(db, op, *id).await,
            Stage::Window(op, id) => persist_window_state(db, op, *id).await,
            Stage::TopK(op, id) => persist_topk_state(db, op, *id).await,
            // `GroupKeyPacker`'s intern table is in-process bookkeeping that
            // lets one long-lived `StatefulPipeline` instance reuse the
            // existing single-key `AggregateOp` for a composite group-by key
            // (surviving across commits within the same process). Restart
            // durability for this composite-key path is out of this slice's
            // scope (not among this slice's exit tests) — a future version
            // may add a persisted intern-table format if needed.
            // Persisted once via the `KeyPack` stage's id (the `KeyUnpack`
            // stage shares the exact same `Arc<GroupKeyPacker>` — see
            // `compile.rs`'s composite-key wiring — so persisting again
            // under `KeyUnpack`'s id would just be a redundant write of
            // the same table).
            Stage::KeyPack(packer, id) => packer.persist(db, *id).await,
            Stage::KeyUnpack(_, _) => Ok(()),
            Stage::Utf8KeyPack(packer, id) => packer.persist(db, *id).await,
            Stage::Utf8KeyUnpack(_, _) => Ok(()),
            Stage::Utf8ColumnPack(packer, _, id) => packer.persist(db, *id).await,
            Stage::Utf8ColumnUnpack(_, _, _) => Ok(()),
            // `MultiAggregatePipeline::persist` recurses back into
            // `StatefulPipeline::persist` (each lane) → `Stage::persist`,
            // which the compiler can't size without an explicit `Box::pin`
            // indirection somewhere in the cycle.
            Stage::MultiAggregate(op) => Box::pin(op.persist(db)).await,
        }
    }

    async fn append_state(&self, db: &ShardDb, target: &mut WriteBatch) -> Result<(), OpError> {
        match self {
            Stage::Stateless(_) => Ok(()),
            Stage::Aggregate(op) => append_agg_state(db, op, target).await,
            Stage::MinMax(op, _) => crate::minmax::append_minmax_state(db, op, target).await,
            Stage::Distinct(op, id) => crate::distinct::append_distinct_state(op, *id, target),
            Stage::TumbleWindow(op, id) => {
                crate::time_window::append_tumble_window_state(op, *id, target)
            }
            Stage::HopWindow(op, id) => {
                crate::time_window::append_hop_window_state(op, *id, target)
            }
            Stage::SessionWindow(op, id) => {
                crate::time_window::append_session_window_state(op, *id, target)
            }
            Stage::Window(op, id) => crate::window::append_window_state(op, *id, target),
            Stage::TopK(op, id) => crate::topk::append_topk_state(op, *id, target),
            Stage::KeyPack(packer, id) => {
                packer.append_state(*id, target);
                Ok(())
            }
            Stage::KeyUnpack(_, _)
            | Stage::Utf8KeyUnpack(_, _)
            | Stage::Utf8ColumnUnpack(_, _, _) => Ok(()),
            Stage::Utf8KeyPack(packer, id) => {
                packer.append_state(*id, target);
                Ok(())
            }
            Stage::Utf8ColumnPack(packer, _, id) => {
                packer.append_state(*id, target);
                Ok(())
            }
            Stage::MultiAggregate(op) => Box::pin(op.append_state(db, target)).await,
        }
    }

    /// Load each stateful stage's persisted arrangement from `db` into the
    /// stage's already-constructed op instance, in place (used by
    /// `GatewayHandler::recover_compiled_views` after a process restart —
    /// see `AggregateOp::restore_in_place` for why an in-place load is
    /// needed instead of just calling the existing `load_from_storage`
    /// constructors). Covers exactly the operator families the v0.51.4
    /// Durability Slices require (Aggregate, TumbleWindow, SessionWindow;
    /// Join is restored separately by `JoinPipeline::restore`, keyed off
    /// `JoinKind`, not `Stage`). `HopWindow`/`Window`/`TopK`/`Distinct`/
    /// `MultiAggregate` are out of this restore path's scope for now (not
    /// named by the Durability Slices plan) and remain a no-op here, same
    /// as the composite-key packers below.
    async fn restore(&self, db: &ShardDb) -> Result<(), OpError> {
        match self {
            Stage::Aggregate(op) => op.restore_in_place(db).await,
            Stage::TumbleWindow(op, id) => op.restore_in_place(db, *id).await,
            Stage::SessionWindow(op, id) => op.restore_in_place(db, *id).await,
            // Same pairing note as `Stage::persist`'s `KeyPack` arm — the
            // `KeyUnpack` stage shares the same `Arc<GroupKeyPacker>`, so
            // restoring once via `KeyPack`'s id is sufficient.
            Stage::KeyPack(packer, id) => packer.restore_in_place(db, *id).await,
            Stage::Utf8KeyPack(packer, id) => packer.restore_in_place(db, *id).await,
            Stage::Utf8ColumnPack(packer, _, id) => packer.restore_in_place(db, *id).await,
            Stage::MinMax(op, _) => op.restore_in_place(db).await,
            Stage::MultiAggregate(op) => Box::pin(op.restore_in_place(db)).await,
            _ => Ok(()),
        }
    }
}

/// Packs a tuple of `Int64` group-by columns into a single surrogate
/// `Int64` key, and unpacks it back — v0.51.4 Slice 6.
///
/// `AggregateOp` (pre-existing since v0.5, oracle-proven) is fixed to a
/// single `Int64` group key. A `SessionWindow`-shaped `GROUP BY` (e.g.
/// Nexmark q11's `GROUP BY bidder, SESSION(...)`, lowered by
/// `rockstream-sql` to `GROUP BY bidder, session_start, session_end` — see
/// `rockstream-sql/src/lower.rs`'s `try_lower_session_window_aggregate`)
/// needs a *composite* key. Rather than widen `AggregateOp` itself (a
/// pre-existing, broadly-depended-on operator, out of this slice's scope),
/// `GroupKeyPacker` sits immediately before and after it in the same
/// `StatefulPipeline`: it interns each distinct `(k0, .., k_{n-1})` tuple
/// this pipeline instance has ever seen to a small monotonically-assigned
/// surrogate `i64`, so the composite key survives `AggregateOp`'s
/// single-`Int64`-key round trip losslessly (an exact tuple lookup, never a
/// hash — no collision risk regardless of the key columns' value domain).
pub struct GroupKeyPacker {
    n_key_cols: usize,
    forward: Mutex<HashMap<Vec<u8>, i64>>,
    reverse: Mutex<HashMap<i64, Vec<i64>>>,
    reverse_slices: Mutex<HashMap<i64, Vec<ArrayRef>>>,
    next_id: Mutex<i64>,
}

impl GroupKeyPacker {
    pub fn new(n_key_cols: usize) -> Self {
        GroupKeyPacker {
            n_key_cols,
            forward: Mutex::new(HashMap::new()),
            reverse: Mutex::new(HashMap::new()),
            reverse_slices: Mutex::new(HashMap::new()),
            next_id: Mutex::new(0),
        }
    }

    /// Number of distinct key tuples interned so far (fill-level metric).
    pub fn entry_count(&self) -> usize {
        self.forward.lock().unwrap().len()
    }

    /// Persist the surrogate-key intern table to `db` (v0.51.4 durability
    /// follow-up: without this, a composite `GROUP BY` key's surrogate
    /// mapping would be lost on restart, causing the same logical key to
    /// be assigned a *new* surrogate id post-restart — silently duplicating
    /// output rows instead of merging into the pre-restart group/session).
    pub async fn persist(&self, db: &ShardDb, op_id: OperatorId) -> Result<(), OpError> {
        let mut batch = WriteBatch::new();
        self.append_state(op_id, &mut batch);
        if batch.is_empty() {
            return Ok(());
        }
        db.write_batch(batch).await.map_err(OpError::storage)
    }

    /// Add the surrogate-key intern table to a caller-owned M3 write.
    pub fn append_state(&self, op_id: OperatorId, target: &mut WriteBatch) {
        let forward = self.forward.lock().unwrap();
        for (encoded_key, &surrogate) in forward.iter() {
            let mut key = Vec::with_capacity(3 + 8 + encoded_key.len());
            key.extend_from_slice(GROUP_KEY_PACKER_PREFIX);
            key.extend_from_slice(&op_id.0.to_be_bytes());
            key.extend_from_slice(encoded_key);
            target.put(&key, &surrogate.to_be_bytes());
        }
    }

    /// Load the persisted surrogate-key intern table from `db` into this
    /// already-constructed instance in place.
    pub async fn restore_in_place(&self, db: &ShardDb, op_id: OperatorId) -> Result<(), OpError> {
        let mut prefix = Vec::with_capacity(3 + 8);
        prefix.extend_from_slice(GROUP_KEY_PACKER_PREFIX);
        prefix.extend_from_slice(&op_id.0.to_be_bytes());
        let entries = db.scan_prefix(&prefix).await.map_err(OpError::storage)?;
        let n = self.n_key_cols;
        let mut forward = self.forward.lock().unwrap();
        let mut reverse = self.reverse.lock().unwrap();
        let mut max_id: i64 = -1;
        for (key, value) in entries {
            let encoded_len = key.len().saturating_sub(prefix.len());
            if key.len() <= prefix.len() || value.len() < 8 || encoded_len != n * 9 {
                continue;
            }
            let encoded_key = key[prefix.len()..].to_vec();
            let surrogate = i64::from_be_bytes(value[0..8].try_into().unwrap_or([0; 8]));
            let mut key_vals = Vec::with_capacity(n);
            let mut valid = true;
            for i in 0..n {
                let start = i * 9;
                if encoded_key[start] != 1 {
                    valid = false;
                    break;
                }
                key_vals.push(i64::from_be_bytes(
                    encoded_key[start + 1..start + 9]
                        .try_into()
                        .unwrap_or([0; 8]),
                ));
            }
            if !valid {
                continue;
            }
            forward.insert(encoded_key, surrogate);
            reverse.insert(surrogate, key_vals);
            max_id = max_id.max(surrogate);
        }
        *self.next_id.lock().unwrap() = max_id + 1;
        Ok(())
    }

    #[allow(dead_code)]
    fn encode_tuple(vals: &[i64]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(vals.len() * 8);
        for v in vals {
            buf.extend_from_slice(&v.to_be_bytes());
        }
        buf
    }

    #[allow(dead_code)]
    fn surrogate_for(&self, key_vals: &[i64]) -> i64 {
        let encoded = Self::encode_tuple(key_vals);
        let mut forward = self.forward.lock().unwrap();
        if let Some(id) = forward.get(&encoded) {
            return *id;
        }
        let mut next_id = self.next_id.lock().unwrap();
        let id = *next_id;
        *next_id += 1;
        drop(next_id);
        forward.insert(encoded, id);
        self.reverse.lock().unwrap().insert(id, key_vals.to_vec());
        id
    }

    fn encode_array_row_key(col: &dyn arrow::array::Array, row: usize, buf: &mut Vec<u8>) {
        if col.is_null(row) {
            buf.push(0);
            return;
        }
        buf.push(1);
        use arrow::array::*;
        if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
            buf.extend_from_slice(&a.value(row).to_be_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
            buf.extend_from_slice(&a.value(row).to_be_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<Int16Array>() {
            buf.extend_from_slice(&a.value(row).to_be_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<Int8Array>() {
            buf.push(a.value(row) as u8);
        } else if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
            buf.extend_from_slice(&a.value(row).to_be_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
            buf.extend_from_slice(&a.value(row).to_be_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<UInt16Array>() {
            buf.extend_from_slice(&a.value(row).to_be_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<UInt8Array>() {
            buf.push(a.value(row));
        } else if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
            buf.extend_from_slice(&a.value(row).to_bits().to_be_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<Float32Array>() {
            buf.extend_from_slice(&a.value(row).to_bits().to_be_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
            let s = a.value(row);
            buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
            buf.extend_from_slice(s.as_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<Date32Array>() {
            buf.extend_from_slice(&a.value(row).to_be_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
            buf.push(if a.value(row) { 1 } else { 0 });
        } else if let Some(a) = col.as_any().downcast_ref::<Decimal128Array>() {
            buf.extend_from_slice(&a.value(row).to_be_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<TimestampSecondArray>() {
            buf.extend_from_slice(&a.value(row).to_be_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
            buf.extend_from_slice(&a.value(row).to_be_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            buf.extend_from_slice(&a.value(row).to_be_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
            buf.extend_from_slice(&a.value(row).to_be_bytes());
        } else {
            let s = format!("{:?}", col.as_any());
            buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
    }

    /// `(k0, .., k_{n-1}, v)` rows → `(surrogate_k, v)` rows, one-to-one,
    /// preserving row order and weight.
    pub fn pack(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let n = self.n_key_cols;
        let mut surrogate_keys: Vec<i64> = Vec::with_capacity(delta.num_rows());
        let mut forward = self.forward.lock().unwrap();
        let mut reverse = self.reverse.lock().unwrap();
        let mut reverse_slices = self.reverse_slices.lock().unwrap();
        let mut next_id = self.next_id.lock().unwrap();

        for row in 0..delta.num_rows() {
            let mut encoded = Vec::new();
            for i in 0..n {
                Self::encode_array_row_key(delta.data.column(i).as_ref(), row, &mut encoded);
            }
            let id = if let Some(&existing_id) = forward.get(&encoded) {
                existing_id
            } else {
                let id = *next_id;
                *next_id += 1;
                forward.insert(encoded, id);

                let key_slices: Vec<ArrayRef> =
                    (0..n).map(|i| delta.data.column(i).slice(row, 1)).collect();
                reverse_slices.insert(id, key_slices);

                let mut int64_vals = Vec::new();
                let mut all_int64 = true;
                for i in 0..n {
                    if let Some(col) = delta.data.column(i).as_any().downcast_ref::<Int64Array>() {
                        int64_vals.push(col.value(row));
                    } else {
                        all_int64 = false;
                        break;
                    }
                }
                if all_int64 {
                    reverse.insert(id, int64_vals);
                }
                id
            };
            surrogate_keys.push(id);
        }

        let val_col = if delta.data.num_columns() > n {
            delta.data.column(n).clone()
        } else {
            Arc::new(Int64Array::from(vec![0; delta.num_rows()])) as ArrayRef
        };

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", val_col.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(surrogate_keys)) as ArrayRef,
                val_col,
            ],
        )
        .map_err(OpError::arrow)?;
        Ok(ArrowZSet::new(batch, delta.weights.clone()))
    }

    /// `(surrogate_k, sum, count, avg)` rows → `(k0, .., k_{n-1}, sum,
    /// count, avg)` rows, one-to-one, preserving row order and weight.
    pub fn unpack(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let n = self.n_key_cols;
        let surrogate_col = delta
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 surrogate key column");
        let num_rows = delta.num_rows();

        let reverse_slices = self.reverse_slices.lock().unwrap();
        let reverse = self.reverse.lock().unwrap();

        let mut key_cols: Vec<ArrayRef> = Vec::with_capacity(n);
        let mut key_fields: Vec<Field> = Vec::with_capacity(n);

        if num_rows == 0 {
            for i in 0..n {
                let sample_slice = reverse_slices.values().find_map(|v| v.get(i).cloned());
                let dt = sample_slice
                    .as_ref()
                    .map(|s| s.data_type().clone())
                    .unwrap_or(DataType::Int64);
                key_fields.push(Field::new(format!("k{i}"), dt.clone(), false));
                key_cols.push(arrow::array::new_empty_array(&dt));
            }
        } else {
            for i in 0..n {
                let mut slices_col_i: Vec<ArrayRef> = Vec::with_capacity(num_rows);
                let mut fallback_dt: Option<DataType> = None;
                for row in 0..num_rows {
                    let surrogate = surrogate_col.value(row);
                    if let Some(slices) = reverse_slices.get(&surrogate) {
                        if fallback_dt.is_none() {
                            fallback_dt = Some(slices[i].data_type().clone());
                        }
                        slices_col_i.push(slices[i].clone());
                    } else if let Some(orig_i64) = reverse.get(&surrogate) {
                        let val = orig_i64.get(i).copied().unwrap_or(0);
                        let arr = Arc::new(Int64Array::from(vec![val])) as ArrayRef;
                        if fallback_dt.is_none() {
                            fallback_dt = Some(DataType::Int64);
                        }
                        slices_col_i.push(arr);
                    } else {
                        let dt = fallback_dt.clone().unwrap_or(DataType::Int64);
                        slices_col_i.push(arrow::array::new_empty_array(&dt));
                    }
                }
                let col_refs: Vec<&dyn arrow::array::Array> =
                    slices_col_i.iter().map(|s| s.as_ref()).collect();
                let concat_col = arrow::compute::concat(&col_refs).map_err(OpError::arrow)?;
                key_fields.push(Field::new(
                    format!("k{i}"),
                    concat_col.data_type().clone(),
                    false,
                ));
                key_cols.push(concat_col);
            }
        }

        let mut fields = key_fields;
        let mut arrays = key_cols;

        // Rest columns (everything after the surrogate key) are passed
        // through unchanged, preserving each column's own Arrow type.
        for (i, col) in delta.data.columns().iter().enumerate().skip(1) {
            fields.push(Field::new(
                format!("r{}", i - 1),
                col.data_type().clone(),
                false,
            ));
            arrays.push(col.clone());
        }
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays).map_err(OpError::arrow)?;
        Ok(ArrowZSet::new(batch, delta.weights.clone()))
    }
}

/// v0.51.4 Slice 8: packs a single non-`Int64` (currently only `Utf8`)
/// `GROUP BY` column into `AggregateOp`'s single-`Int64`-key shape, mirroring
/// `GroupKeyPacker`'s exact-tuple interning technique but for a value-type
/// mismatch rather than a column-count mismatch (e.g. Nexmark-adjacent
/// `SELECT url, COUNT(*) FROM clicks GROUP BY url`, `url` being `TEXT`).
pub struct Utf8KeyPacker {
    forward: Mutex<HashMap<String, i64>>,
    reverse: Mutex<HashMap<i64, String>>,
    next_id: Mutex<i64>,
}

impl Utf8KeyPacker {
    pub fn new() -> Self {
        Utf8KeyPacker {
            forward: Mutex::new(HashMap::new()),
            reverse: Mutex::new(HashMap::new()),
            next_id: Mutex::new(0),
        }
    }

    /// Number of distinct keys interned so far (fill-level metric).
    pub fn entry_count(&self) -> usize {
        self.forward.lock().unwrap().len()
    }

    pub async fn persist(&self, db: &ShardDb, op_id: OperatorId) -> Result<(), OpError> {
        let mut batch = WriteBatch::new();
        self.append_state(op_id, &mut batch);
        if batch.is_empty() {
            return Ok(());
        }
        db.write_batch(batch).await.map_err(OpError::storage)
    }

    pub fn append_state(&self, op_id: OperatorId, target: &mut WriteBatch) {
        append_utf8_packer_state(&self.forward, op_id, target);
    }

    pub async fn restore_in_place(&self, db: &ShardDb, op_id: OperatorId) -> Result<(), OpError> {
        restore_utf8_packer_state(db, op_id, &self.forward, &self.reverse, &self.next_id).await
    }

    fn surrogate_for(&self, key: &str) -> i64 {
        let mut forward = self.forward.lock().unwrap();
        if let Some(id) = forward.get(key) {
            return *id;
        }
        let mut next_id = self.next_id.lock().unwrap();
        let id = *next_id;
        *next_id += 1;
        drop(next_id);
        forward.insert(key.to_string(), id);
        self.reverse.lock().unwrap().insert(id, key.to_string());
        id
    }

    /// `(k: Utf8, v: Int64)` rows → `(surrogate_k: Int64, v: Int64)` rows.
    pub fn pack(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let key_col = delta
            .data
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| {
                OpError::unsupported_plan_node("Utf8KeyPacker::pack: column 0 is not Utf8")
            })?;
        let val_col = delta
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                OpError::unsupported_plan_node("Utf8KeyPacker::pack: column 1 is not Int64")
            })?;
        let mut surrogate_keys: Vec<i64> = Vec::with_capacity(delta.num_rows());
        let mut values: Vec<i64> = Vec::with_capacity(delta.num_rows());
        for row in 0..delta.num_rows() {
            surrogate_keys.push(self.surrogate_for(key_col.value(row)));
            values.push(val_col.value(row));
        }
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(surrogate_keys)) as ArrayRef,
                Arc::new(Int64Array::from(values)) as ArrayRef,
            ],
        )
        .map_err(OpError::arrow)?;
        Ok(ArrowZSet::new(batch, delta.weights.clone()))
    }

    /// `(surrogate_k, sum, count, avg)` rows → `(k: Utf8, sum, count, avg)`
    /// rows, one-to-one, preserving row order and weight.
    pub fn unpack(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let surrogate_col = delta
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 surrogate key column");
        let reverse = self.reverse.lock().unwrap();
        let mut keys: Vec<String> = Vec::with_capacity(delta.num_rows());
        for row in 0..delta.num_rows() {
            let surrogate = surrogate_col.value(row);
            keys.push(reverse.get(&surrogate).cloned().unwrap_or_default());
        }
        let mut fields: Vec<Field> = vec![Field::new("k", DataType::Utf8, false)];
        let mut arrays: Vec<ArrayRef> =
            vec![Arc::new(arrow::array::StringArray::from(keys)) as ArrayRef];
        // Rest columns (everything after the surrogate key) are passed
        // through unchanged, preserving each column's own Arrow type — e.g.
        // `AVG`'s genuinely `Float64` output (v0.51.6 Slice 4), not just
        // `Int64` (matches `GroupKeyPacker::unpack`'s identical fix above).
        for (i, col) in delta.data.columns().iter().enumerate().skip(1) {
            fields.push(Field::new(
                format!("r{}", i - 1),
                col.data_type().clone(),
                false,
            ));
            arrays.push(col.clone());
        }
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays).map_err(OpError::arrow)?;
        Ok(ArrowZSet::new(batch, delta.weights.clone()))
    }
}

impl Default for Utf8KeyPacker {
    fn default() -> Self {
        Self::new()
    }
}

/// v0.51.4 Slice 8: packs one arbitrary `Utf8` column (identified by
/// position, not necessarily the group/join key) at `col_idx` within an
/// N-column batch into an `Int64` surrogate, leaving every other column
/// unchanged — the same intern-table technique as `Utf8KeyPacker`, applied
/// so a non-Int64 *passthrough* column can travel through `JoinOp`
/// (Int64-only, by design, since v0.8/v0.9) and be restored afterward.
/// E.g. a view-of-view join like `campaigns c JOIN campaign_totals t ON
/// c.campaign_id = t.campaign_id` where `campaigns.name`/`campaigns.channel`
/// are `TEXT` and only ever pass through unchanged, never used as a join
/// key or arithmetic operand.
pub struct Utf8ColumnPacker {
    forward: Mutex<HashMap<String, i64>>,
    reverse: Mutex<HashMap<i64, String>>,
    next_id: Mutex<i64>,
}

impl Utf8ColumnPacker {
    pub fn new() -> Self {
        Utf8ColumnPacker {
            forward: Mutex::new(HashMap::new()),
            reverse: Mutex::new(HashMap::new()),
            next_id: Mutex::new(0),
        }
    }

    pub async fn persist(&self, db: &ShardDb, op_id: OperatorId) -> Result<(), OpError> {
        let mut batch = WriteBatch::new();
        self.append_state(op_id, &mut batch);
        if batch.is_empty() {
            return Ok(());
        }
        db.write_batch(batch).await.map_err(OpError::storage)
    }

    pub fn append_state(&self, op_id: OperatorId, target: &mut WriteBatch) {
        append_utf8_packer_state(&self.forward, op_id, target);
    }

    pub async fn restore_in_place(&self, db: &ShardDb, op_id: OperatorId) -> Result<(), OpError> {
        restore_utf8_packer_state(db, op_id, &self.forward, &self.reverse, &self.next_id).await
    }

    fn surrogate_for(&self, key: &str) -> i64 {
        let mut forward = self.forward.lock().unwrap();
        if let Some(id) = forward.get(key) {
            return *id;
        }
        let mut next_id = self.next_id.lock().unwrap();
        let id = *next_id;
        *next_id += 1;
        drop(next_id);
        forward.insert(key.to_string(), id);
        self.reverse.lock().unwrap().insert(id, key.to_string());
        id
    }

    /// Replace column `col_idx` (must be `Utf8`) with its `Int64` surrogate.
    /// `col_idx` is a parameter (not fixed at construction) because the
    /// same packer instance packs a column at its pre-join, side-local
    /// index and later unpacks it at its post-join, shifted output index —
    /// the intern table must be shared across both calls.
    pub fn pack(&self, delta: ArrowZSet, col_idx: usize) -> Result<ArrowZSet, OpError> {
        let batch = &delta.data;
        let col = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| {
                OpError::unsupported_plan_node("Utf8ColumnPacker::pack: column is not Utf8")
            })?;
        let surrogate: Vec<i64> = (0..batch.num_rows())
            .map(|r| self.surrogate_for(col.value(r)))
            .collect();
        let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();
        arrays[col_idx] = Arc::new(Int64Array::from(surrogate)) as ArrayRef;
        let fields: Vec<Field> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if i == col_idx {
                    Field::new(f.name(), DataType::Int64, false)
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        let new_batch = RecordBatch::try_new(schema, arrays).map_err(OpError::arrow)?;
        Ok(ArrowZSet::new(new_batch, delta.weights.clone()))
    }

    /// Inverse: replace column `col_idx` (the `Int64` surrogate) with the
    /// original `Utf8` value.
    pub fn unpack(&self, delta: ArrowZSet, col_idx: usize) -> Result<ArrowZSet, OpError> {
        let batch = &delta.data;
        let col = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                OpError::unsupported_plan_node("Utf8ColumnPacker::unpack: column is not Int64")
            })?;
        let reverse = self.reverse.lock().unwrap();
        let restored: Vec<Option<String>> = (0..batch.num_rows())
            .map(|r| {
                (!col.is_null(r))
                    .then(|| reverse.get(&col.value(r)).cloned())
                    .flatten()
            })
            .collect();
        let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();
        arrays[col_idx] = Arc::new(arrow::array::StringArray::from(restored)) as ArrayRef;
        let fields: Vec<Field> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if i == col_idx {
                    Field::new(f.name(), DataType::Utf8, f.is_nullable())
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        let new_batch = RecordBatch::try_new(schema, arrays).map_err(OpError::arrow)?;
        Ok(ArrowZSet::new(new_batch, delta.weights.clone()))
    }
}

impl Default for Utf8ColumnPacker {
    fn default() -> Self {
        Self::new()
    }
}

/// An executable chain mixing stateless and stateful operators.
///
/// `process` is synchronous (every operator's compute step is synchronous —
/// state lives behind an in-process `Mutex`); `persist` is the separate
/// async step that writes every stateful stage's arrangement to `db`,
/// called once per commit after `process` completes.
#[derive(Default)]
pub struct StatefulPipeline {
    stages: Vec<Stage>,
    epoch_ctr: AtomicU64,
}

impl StatefulPipeline {
    pub fn new() -> Self {
        StatefulPipeline {
            stages: Vec::new(),
            epoch_ctr: AtomicU64::new(0),
        }
    }

    pub fn push(mut self, stage: Stage) -> Self {
        self.stages.push(stage);
        self
    }

    pub fn is_empty_pipeline(&self) -> bool {
        self.stages.is_empty()
    }

    /// Thread `delta` through every stage in order, returning the final
    /// output delta.
    pub fn process(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let epoch = self.epoch_ctr.fetch_add(1, Ordering::SeqCst);
        let mut current = delta;
        for stage in &self.stages {
            current = stage.process(current, epoch)?;
        }
        Ok(current)
    }

    /// Persist every stateful stage's arrangement to `db`. A no-op for a
    /// pipeline with only stateless stages.
    pub async fn persist(&self, db: &ShardDb) -> Result<(), OpError> {
        for stage in &self.stages {
            stage.persist(db).await?;
        }
        Ok(())
    }

    /// Append state without committing it. Source backfill combines this with
    /// output, checkpoint, cursor, lifecycle, and frontier in one M3 batch.
    pub async fn append_state(&self, db: &ShardDb, target: &mut WriteBatch) -> Result<(), OpError> {
        for stage in &self.stages {
            stage.append_state(db, target).await?;
        }
        Ok(())
    }

    /// Load every stateful stage's persisted arrangement from `db` into
    /// this pipeline's already-constructed stages, in place. See
    /// `Stage::restore` for exactly which operator families this covers.
    pub async fn restore(&self, db: &ShardDb) -> Result<(), OpError> {
        for stage in &self.stages {
            stage.restore(db).await?;
        }
        Ok(())
    }

    /// Return sum of state bytes held by all stages in this pipeline.
    pub fn state_bytes(&self) -> u64 {
        self.stages.iter().map(|s| s.state_bytes()).sum()
    }
}

/// How `MultiAggregatePipeline::process` extracts one final output column
/// from the cascade-joined accumulator's payload columns (0-indexed,
/// counting from the first payload column after `k`) — see
/// `compile_multi_aggregate_lanes` in `compile.rs`.
#[derive(Debug, Clone, Copy)]
pub enum FinalizeCol {
    /// Pass the payload column at this index through unchanged.
    Direct(usize),
    /// v0.51.6 Slice 4: combine a `(sum, count)` `Int64` pair — forwarded
    /// through the join cascade instead of `AggregateOp`'s already-`Float64`
    /// `avg_v` (see the `AggregateFunc::Avg` arm in `compile.rs`) because
    /// `OuterJoinOp`'s persisted arrangement is `Int64`-only — into one true
    /// `Float64` avg column via `avg_from_sum_count`.
    Avg { sum_idx: usize, count_idx: usize },
}

/// v0.51.4 Slice 8: composes `N` independent single-aggregate "lanes" —
/// each its own `StatefulPipeline` producing `(k, payload..)` from the same
/// shared input delta — into one `(k, agg_0, .., agg_{N-1})` row per group,
/// by cascade-joining the lanes' outputs on `k` via `OuterJoinOp` (`Left`),
/// then finalizing each lane's payload into its aggregate's true output
/// value via `finalize` (see `FinalizeCol`).
///
/// Built for Nexmark q15's `SUM(...)`, `COUNT(DISTINCT ...)` x2 all `GROUP
/// BY date_bin(...)` shape (`compile_multi_aggregate_lanes` in `compile.rs`)
/// — reuses `OuterJoinOp` (pre-existing, oracle-proven since v0.9) rather
/// than a bespoke "zip by key" primitive. A *left* outer join (not inner) is
/// required: a group with e.g. a nonzero `SUM` but zero rows matching a
/// later `COUNT(DISTINCT CASE WHEN ...)` lane must still appear with that
/// aggregate reported as `0` (SQL's `GROUP BY` semantics — every group
/// contributing to *any* aggregate appears in the result), not be dropped
/// the way an inner join would. `OuterJoinOp` NULL-pads an unmatched side
/// with `0i64` (see its module doc), which is exactly `COUNT`/`SUM`'s
/// identity element for "no matching rows" (and, for `AVG`'s forwarded
/// `sum`/`count` pair, `avg_from_sum_count(0, 0)` — a genuinely
/// no-matching-rows group — is defined to be `0.0`, matching `AggregateOp`'s
/// own convention).
pub struct MultiAggregatePipeline {
    lanes: Vec<StatefulPipeline>,
    /// Payload width (columns beyond `k`) of each lane's output, in the same
    /// order as `lanes` — 1 for every lane except `AVG`'s (2: `sum`, `count`).
    lane_widths: Vec<usize>,
    /// `joins.len() == lanes.len() - 1`; `joins[i]` combines the running
    /// accumulator (lanes `0..=i`, already joined) with `lanes[i + 1]`'s
    /// output.
    joins: Vec<Arc<OuterJoinOp>>,
    /// One entry per *original* aggregate (`aggregates` order in
    /// `compile_multi_aggregate_lanes`), describing how to build that
    /// aggregate's final output column from the fully joined accumulator's
    /// payload columns.
    finalize: Vec<FinalizeCol>,
}

impl MultiAggregatePipeline {
    pub fn new(
        lanes: Vec<StatefulPipeline>,
        lane_widths: Vec<usize>,
        joins: Vec<Arc<OuterJoinOp>>,
        finalize: Vec<FinalizeCol>,
    ) -> Self {
        MultiAggregatePipeline {
            lanes,
            lane_widths,
            joins,
            finalize,
        }
    }

    pub fn state_bytes(&self) -> u64 {
        let lane_bytes: u64 = self.lanes.iter().map(|l| l.state_bytes()).sum();
        let join_bytes: u64 = self.joins.iter().map(|j| j.state_bytes()).sum();
        lane_bytes + join_bytes
    }

    /// Drop column `drop_idx` from `zset`, keeping every other column in
    /// order — used to remove a join's duplicate right-side key column.
    fn drop_column(zset: ArrowZSet, drop_idx: usize) -> Result<ArrowZSet, OpError> {
        let n = zset.data.num_columns();
        let exprs: Vec<crate::project::NamedExpr> = (0..n)
            .filter(|&i| i != drop_idx)
            .map(|i| {
                crate::project::NamedExpr::new(format!("c{i}"), rockstream_plan::Expr::Column(i))
            })
            .collect();
        crate::project::ProjectOp::new(exprs).process_delta(zset)
    }

    /// Build the final `(k, agg_0, .., agg_{N-1})` row from the fully
    /// cascade-joined accumulator's `(k, payload_0, .., payload_{M-1})`
    /// columns, per `self.finalize` — see `FinalizeCol`.
    fn finalize_row(&self, acc: ArrowZSet) -> Result<ArrowZSet, OpError> {
        use arrow::array::{Float64Array, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};

        let k_col = acc.data.column(0).clone();
        let mut fields: Vec<Field> = vec![Field::new("k", DataType::Int64, false)];
        let mut cols: Vec<ArrayRef> = vec![k_col];
        for (i, col_kind) in self.finalize.iter().enumerate() {
            match *col_kind {
                FinalizeCol::Direct(idx) => {
                    let col = acc.data.column(1 + idx).clone();
                    fields.push(Field::new(
                        format!("agg{i}"),
                        col.data_type().clone(),
                        false,
                    ));
                    cols.push(col);
                }
                FinalizeCol::Avg { sum_idx, count_idx } => {
                    let sum_col = acc
                        .data
                        .column(1 + sum_idx)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("AVG lane sum column must be Int64");
                    let count_col = acc
                        .data
                        .column(1 + count_idx)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("AVG lane count column must be Int64");
                    let avgs: Vec<f64> = (0..acc.data.num_rows())
                        .map(|row| {
                            rockstream_types::laws::sum_count::avg_from_sum_count(
                                sum_col.value(row),
                                count_col.value(row),
                            )
                            .unwrap_or(0.0)
                        })
                        .collect();
                    fields.push(Field::new(format!("agg{i}"), DataType::Float64, false));
                    cols.push(Arc::new(Float64Array::from(avgs)) as ArrayRef);
                }
            }
        }
        let schema = Arc::new(Schema::new(fields));
        let data = RecordBatch::try_new(schema, cols).map_err(OpError::arrow)?;
        Ok(ArrowZSet::new(data, acc.weights))
    }

    pub fn process(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let mut lane_outputs: Vec<ArrowZSet> = Vec::with_capacity(self.lanes.len());
        for lane in &self.lanes {
            lane_outputs.push(lane.process(delta.clone())?);
        }
        let mut lane_iter = lane_outputs.into_iter();
        let mut acc = lane_iter.next().ok_or_else(|| {
            OpError::unsupported_plan_node("MultiAggregatePipeline requires at least one lane")
        })?;
        // `acc_n_cols` tracks the running accumulator's column count (`k`
        // plus every lane payload joined so far).
        let mut acc_n_cols = 1 + self.lane_widths[0];
        for (join, (right, &w)) in self
            .joins
            .iter()
            .zip(lane_iter.by_ref().zip(self.lane_widths[1..].iter()))
        {
            let joined = join.process_epoch(acc, right)?;
            // joined = (acc_cols.., right_k, right_payload..) — drop the
            // duplicate right-side key column at position acc_n_cols.
            acc = Self::drop_column(joined, acc_n_cols)?;
            acc_n_cols += w;
        }
        self.finalize_row(acc)
    }

    pub async fn persist(&self, db: &ShardDb) -> Result<(), OpError> {
        for lane in &self.lanes {
            lane.persist(db).await?;
        }
        for join in &self.joins {
            join.persist_state(db).await?;
        }
        Ok(())
    }

    pub async fn append_state(&self, db: &ShardDb, target: &mut WriteBatch) -> Result<(), OpError> {
        for lane in &self.lanes {
            lane.append_state(db, target).await?;
        }
        for join in &self.joins {
            join.append_state(target)?;
        }
        Ok(())
    }

    pub async fn restore_in_place(&self, db: &ShardDb) -> Result<(), OpError> {
        for lane in &self.lanes {
            lane.restore(db).await?;
        }
        for join in &self.joins {
            join.restore_in_place(db).await?;
        }
        Ok(())
    }
}

/// The join operator hosted by a `JoinPipeline` — either an inner
/// (`JoinOp`) or outer/semi/anti (`OuterJoinOp`) equi-join.
pub enum JoinKind {
    Inner(Arc<JoinOp>),
    Outer(Arc<OuterJoinOp>),
}

impl JoinKind {
    fn process_epoch(&self, left: ArrowZSet, right: ArrowZSet) -> Result<ArrowZSet, OpError> {
        match self {
            JoinKind::Inner(op) => op.process_epoch(left, right),
            JoinKind::Outer(op) => op.process_epoch(left, right),
        }
    }

    async fn persist(&self, db: &ShardDb) -> Result<(), OpError> {
        match self {
            JoinKind::Inner(op) => op.persist_state(db).await,
            JoinKind::Outer(op) => op.persist_state(db).await,
        }
    }

    fn append_state(&self, target: &mut WriteBatch) -> Result<(), OpError> {
        match self {
            JoinKind::Inner(op) => op.append_state(target),
            JoinKind::Outer(op) => op.append_state(target),
        }
    }

    /// Load the join's persisted arrangement(s) from `db` in place.
    /// `OuterJoinOp` (semi/anti/outer joins) is out of scope for now — no
    /// Durability Slice in the v0.51.4 plan exercises an outer join, and
    /// its extra `left_key_weight`/`right_key_weight` tables would need
    /// their own in-place restore method; `JoinOp` (inner join, what the
    /// plan's join durability test actually exercises) is fully restored.
    async fn restore(&self, db: &ShardDb) -> Result<(), OpError> {
        match self {
            JoinKind::Inner(op) => op.restore_in_place(db).await,
            JoinKind::Outer(_) => Ok(()),
        }
    }
}

/// v0.51.4 Slice 3: a two-input pipeline for `InnerJoin`/`OuterJoin`-shaped
/// compiled views.
///
/// Unlike `StatefulPipeline` (one delta source), a join has two independent
/// delta sources — a commit to the left source table and a commit to the
/// right source table are two separate events, each of which must reach the
/// *same* join arrangement. `process_left`/`process_right` let a caller feed
/// whichever side actually changed (the other side's delta is simply empty)
/// while still going through `JoinOp`/`OuterJoinOp`'s single atomic
/// `process_epoch(left, right)` call, which correctly handles the DBSP
/// bilinear rule `Δ(L⋈R) = ΔL⋈R₀ + L₀⋈ΔR + ΔL⋈ΔR` regardless of which side
/// (or both, if a single commit's `WriteBatch` touched both source tables)
/// is non-empty.
pub struct JoinPipeline {
    /// Stateless stages (e.g. a residual `Filter`) applied to the left
    /// source's delta before it reaches the join.
    left_pre: Vec<Stage>,
    /// Stateless stages applied to the right source's delta before the join.
    right_pre: Vec<Stage>,
    join: JoinKind,
    /// Stateless stages applied to the join's output (e.g. a `Project` down
    /// to the view's declared output columns).
    post: Vec<Stage>,
}

impl JoinPipeline {
    pub fn new(
        left_pre: Vec<Stage>,
        right_pre: Vec<Stage>,
        join: JoinKind,
        post: Vec<Stage>,
    ) -> Self {
        JoinPipeline {
            left_pre,
            right_pre,
            join,
            post,
        }
    }

    /// Process one commit's left-source delta and right-source delta
    /// (either or both may be empty) through the full join pipeline,
    /// returning the combined output delta.
    pub fn process(
        &self,
        left_delta: ArrowZSet,
        right_delta: ArrowZSet,
    ) -> Result<ArrowZSet, OpError> {
        let mut left = left_delta;
        for stage in &self.left_pre {
            left = stage.process(left, 0)?;
        }
        let mut right = right_delta;
        for stage in &self.right_pre {
            right = stage.process(right, 0)?;
        }
        let mut out = self.join.process_epoch(left, right)?;
        for stage in &self.post {
            out = stage.process(out, 0)?;
        }
        Ok(out)
    }

    /// Persist the join's arrangement(s) to `db`. `left_pre`/`right_pre`/
    /// `post` are stateless in every shape this version compiles, so only
    /// the join itself has state to persist.
    pub async fn persist(&self, db: &ShardDb) -> Result<(), OpError> {
        self.join.persist(db).await?;
        for stage in &self.left_pre {
            stage.persist(db).await?;
        }
        for stage in &self.right_pre {
            stage.persist(db).await?;
        }
        for stage in &self.post {
            stage.persist(db).await?;
        }
        Ok(())
    }

    pub async fn append_state(&self, db: &ShardDb, target: &mut WriteBatch) -> Result<(), OpError> {
        self.join.append_state(target)?;
        for stage in self
            .left_pre
            .iter()
            .chain(&self.right_pre)
            .chain(&self.post)
        {
            stage.append_state(db, target).await?;
        }
        Ok(())
    }

    /// Load the join's persisted arrangement(s), and any stateful pre/post
    /// stage's state, from `db` in place. See `JoinKind::restore` for the
    /// `OuterJoinOp` scope note.
    pub async fn restore(&self, db: &ShardDb) -> Result<(), OpError> {
        self.join.restore(db).await?;
        for stage in &self.left_pre {
            stage.restore(db).await?;
        }
        for stage in &self.right_pre {
            stage.restore(db).await?;
        }
        for stage in &self.post {
            stage.restore(db).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::lit;
    use crate::filter::FilterOp;
    use crate::project::{NamedExpr, ProjectOp};
    use object_store::local::LocalFileSystem;
    use rockstream_plan::{BinaryOp, Expr};
    use tempfile::TempDir;

    async fn make_db() -> (TempDir, Arc<ShardDb>) {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let db = Arc::new(ShardDb::builder("db", store).build().await.unwrap());
        (dir, db)
    }

    #[tokio::test]
    async fn stateless_only_pipeline_matches_linear_pipeline_behavior() {
        let predicate = Expr::BinaryOp {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column(0)),
            right: Box::new(lit(1)),
        };
        let pipeline = StatefulPipeline::new()
            .push(Stage::Stateless(Arc::new(FilterOp::new(predicate))))
            .push(Stage::Stateless(Arc::new(ProjectOp::new(vec![
                NamedExpr::new("a", Expr::Column(0)),
            ]))));
        let batch = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20), (3, 30)], 1);
        let out = pipeline.process(batch).unwrap();
        assert_eq!(out.num_rows(), 2);

        let (_dir, db) = make_db().await;
        pipeline.persist(&db).await.unwrap();
    }

    #[tokio::test]
    async fn aggregate_stage_accumulates_across_process_calls() {
        let op_id = next_stateful_op_id();
        let pipeline =
            StatefulPipeline::new().push(Stage::Aggregate(Arc::new(AggregateOp::new(op_id))));
        let batch1 = ArrowZSet::from_ab_rows(&[(1, 10)], 1);
        let out1 = pipeline.process(batch1).unwrap();
        assert_eq!(out1.num_rows(), 1); // insert of new group

        let batch2 = ArrowZSet::from_ab_rows(&[(1, 5)], 1);
        let out2 = pipeline.process(batch2).unwrap();
        // retract old group row + insert updated group row
        assert_eq!(out2.num_rows(), 2);

        let (_dir, db) = make_db().await;
        pipeline.persist(&db).await.unwrap();
    }

    #[tokio::test]
    async fn group_key_packer_restores_type_tagged_int64_keys() {
        let (_dir, db) = make_db().await;
        let op_id = OperatorId(42);
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, false),
            Field::new("k1", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1000, 2000])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef,
            ],
        )
        .unwrap();

        let original = GroupKeyPacker::new(2);
        original.pack(ArrowZSet::new(batch, vec![1, 1])).unwrap();
        original.persist(&db, op_id).await.unwrap();

        let restored = GroupKeyPacker::new(2);
        restored.restore_in_place(&db, op_id).await.unwrap();
        let reordered = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![9, 1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![9000, 1000])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef,
            ],
        )
        .unwrap();
        let packed = restored
            .pack(ArrowZSet::new(reordered, vec![1, 1]))
            .unwrap();
        let keys = packed
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(keys.values().as_ref(), &[2, 0]);
    }

    #[tokio::test]
    async fn utf8_packer_state_restores_from_the_caller_owned_batch() {
        let (_dir, db) = make_db().await;
        let op_id = OperatorId(43);
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["beta", "alpha"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            ],
        )
        .unwrap();
        let original = Utf8KeyPacker::new();
        original.pack(ArrowZSet::new(source, vec![1, 1])).unwrap();
        let mut m3 = WriteBatch::new();
        original.append_state(op_id, &mut m3);
        db.write_batch(m3).await.unwrap();

        let restored = Utf8KeyPacker::new();
        restored.restore_in_place(&db, op_id).await.unwrap();
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["alpha", "gamma"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![3, 4])) as ArrayRef,
            ],
        )
        .unwrap();
        let packed = restored.pack(ArrowZSet::new(source, vec![1, 1])).unwrap();
        let keys = packed
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(keys.values().as_ref(), &[1, 2]);

        let unpacked = restored.unpack(packed).unwrap();
        let keys = unpacked
            .data
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(
            keys.iter().collect::<Vec<_>>(),
            vec![Some("alpha"), Some("gamma")]
        );
        assert_eq!(unpacked.weights, vec![1, 1]);
    }

    /// Regression test for a v0.51.4 fix: `TumbleWindowOp` used to drop
    /// *retractions* of already-finalized/late windows using the same
    /// late-data policy meant only for new arrivals, permanently freezing
    /// stale state after the first commit that closed a window. This
    /// exercises the full `TumbleWindow -> KeyPack -> Aggregate -> KeyUnpack`
    /// pipeline (mirrors how a composite `GROUP BY bidder, date_bin(...)`
    /// view compiles) to confirm a retraction targeting one group in an
    /// already-closed window is applied, while an unrelated group sharing
    /// that same window is left untouched.
    #[tokio::test]
    async fn full_pipeline_retraction_reopens_finalized_window_without_disturbing_other_groups() {
        use crate::time_window::TumbleWindowOp;
        let input_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("t", DataType::Int64, false),
            Field::new("bidder", DataType::Int64, false),
        ]));
        let tumble_op = Arc::new(TumbleWindowOp::new(
            input_schema,
            0,
            10,
            rockstream_plan::LateDataPolicy::Drop,
        ));
        let tumble_id = next_stateful_op_id();
        let packer = Arc::new(GroupKeyPacker::new(2));
        let agg_id = next_stateful_op_id();
        let pipeline = StatefulPipeline::new()
            .push(Stage::TumbleWindow(tumble_op, tumble_id))
            .push(Stage::Stateless(Arc::new(ProjectOp::new(vec![
                NamedExpr::new("k0", Expr::Column(0)), // window_id
                NamedExpr::new("k1", Expr::Column(2)), // bidder
                NamedExpr::new("v", lit(1)),
            ]))))
            .push(Stage::KeyPack(packer.clone(), next_stateful_op_id()))
            .push(Stage::Aggregate(Arc::new(AggregateOp::new(agg_id))))
            .push(Stage::KeyUnpack(packer, next_stateful_op_id()));

        // Epoch 1: bidder 100 and bidder 200 both bid at t=1/t=2 (window 0),
        // plus a t=1000 bid that advances the watermark far enough to
        // finalize window 0.
        let in1 = ArrowZSet::new(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("t", DataType::Int64, false),
                    Field::new("bidder", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 1000])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![100, 200, 999])) as ArrayRef,
                ],
            )
            .unwrap(),
            vec![1, 1, 1],
        );
        let out1 = pipeline.process(in1).unwrap();
        assert_eq!(out1.num_rows(), 3);

        // Epoch 2: retract bidder=200's bid only. bidder=100's count=1 group
        // must remain visible.
        let in2 = ArrowZSet::new(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("t", DataType::Int64, false),
                    Field::new("bidder", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![2])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![200])) as ArrayRef,
                ],
            )
            .unwrap(),
            vec![-1],
        );
        let out2 = pipeline.process(in2).unwrap();
        // Exactly one output row: the retraction of bidder=200's
        // (window=0, bidder=200, count=1) group. bidder=100's group must
        // not be touched at all (no spurious retract/insert pair).
        assert_eq!(out2.num_rows(), 1);
        assert_eq!(out2.weights, vec![-1]);
        let k1 = out2
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(k1.value(0), 200);
    }

    /// Same fix, but with several windows finalized at once and a single
    /// batch retracting one group from *each* window simultaneously
    /// (mirrors a multi-statement transaction touching many groups across
    /// many windows in one commit) — every untouched group in every
    /// affected window must survive.
    #[tokio::test]
    async fn full_pipeline_many_windows_multi_retraction_leaves_untouched_groups_intact() {
        let input_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("t", DataType::Int64, false),
            Field::new("bidder", DataType::Int64, false),
        ]));
        let tumble_op = Arc::new(crate::time_window::TumbleWindowOp::new(
            input_schema,
            0,
            10,
            rockstream_plan::LateDataPolicy::Drop,
        ));
        let tumble_id = next_stateful_op_id();
        let packer = Arc::new(GroupKeyPacker::new(2));
        let agg_id = next_stateful_op_id();
        let pipeline = StatefulPipeline::new()
            .push(Stage::TumbleWindow(tumble_op, tumble_id))
            .push(Stage::Stateless(Arc::new(ProjectOp::new(vec![
                NamedExpr::new("k0", Expr::Column(0)),
                NamedExpr::new("k1", Expr::Column(2)),
                NamedExpr::new("v", lit(1)),
            ]))))
            .push(Stage::KeyPack(packer.clone(), next_stateful_op_id()))
            .push(Stage::Aggregate(Arc::new(AggregateOp::new(agg_id))))
            .push(Stage::KeyUnpack(packer, next_stateful_op_id()));

        fn make_batch(rows: &[(i64, i64)], weights: Vec<i64>) -> ArrowZSet {
            let schema: SchemaRef = Arc::new(Schema::new(vec![
                Field::new("t", DataType::Int64, false),
                Field::new("bidder", DataType::Int64, false),
            ]));
            let t: Vec<i64> = rows.iter().map(|r| r.0).collect();
            let b: Vec<i64> = rows.iter().map(|r| r.1).collect();
            ArrowZSet::new(
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int64Array::from(t)) as ArrayRef,
                        Arc::new(Int64Array::from(b)) as ArrayRef,
                    ],
                )
                .unwrap(),
                weights,
            )
        }

        // 5 windows (0,10,20,30,40), 4 distinct bidders each (20 groups
        // total), all inserted in epoch 1, plus a very-late row that closes
        // every one of those windows.
        let mut rows: Vec<(i64, i64)> = Vec::new();
        for w in 0..5i64 {
            for b in 0..4i64 {
                rows.push((w * 10 + 1, 1000 + w * 100 + b));
            }
        }
        rows.push((10_000, 999_999));
        let weights = vec![1; rows.len()];
        let out1 = pipeline.process(make_batch(&rows, weights)).unwrap();
        assert_eq!(out1.num_rows(), 21);

        // Epoch 2: retract ONE bidder from each of the 5 windows (bidder
        // offset 0 only), all in a single batch/epoch — mirrors a single
        // multi-statement transaction touching many different windows at
        // once. The other 3 bidders per window (15 groups total) must
        // survive untouched.
        let mut retract_rows: Vec<(i64, i64)> = Vec::new();
        for w in 0..5i64 {
            retract_rows.push((w * 10 + 1, 1000 + w * 100));
        }
        let retract_weights = vec![-1; retract_rows.len()];
        let out2 = pipeline
            .process(make_batch(&retract_rows, retract_weights))
            .unwrap();

        // Collect final live (k0, k1) pairs across both epochs.
        use std::collections::HashMap;
        let mut net: HashMap<(i64, i64), i64> = HashMap::new();
        for out in [&out1, &out2] {
            let k0 = out
                .data
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let k1 = out
                .data
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..out.num_rows() {
                *net.entry((k0.value(i), k1.value(i))).or_insert(0) += out.weights[i];
            }
        }
        let mut live: Vec<(i64, i64)> = net
            .into_iter()
            .filter(|(_, w)| *w > 0)
            .map(|(k, _)| k)
            .collect();
        live.sort();
        let mut expected: Vec<(i64, i64)> = Vec::new();
        for w in 0..5i64 {
            for b in 1..4i64 {
                expected.push((w * 10, 1000 + w * 100 + b));
            }
        }
        expected.push((10_000, 999_999));
        expected.sort();
        assert_eq!(live, expected);
    }
}
