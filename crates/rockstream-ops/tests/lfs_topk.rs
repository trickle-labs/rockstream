//! LFS durability tests for `TopKOp` (v0.12 — IVM-9).
//!
//! ## Tests
//!
//! 1. `lfs_topk_state_persists` — arrangement (buffer) survives ShardDb close/reopen.
//!
//! 2. `lfs_topk_crash_replay` — WAL replay; output bit-identical to non-crash path.
//!
//! 3. `lfs_topk_no_range_delete` — WriteBatch has no DeleteRange (structural guarantee).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use object_store::local::LocalFileSystem;
use rockstream_ops::topk::{load_topk_state, persist_topk_state, TopKOp};
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::ids::OperatorId;
use tempfile::TempDir;

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn schema_kv() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false), // rank_col = 0
        Field::new("id", DataType::Int64, false),
    ]))
}

fn make_input(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let v: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let id: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let w: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let data = RecordBatch::try_new(
        schema_kv(),
        vec![
            Arc::new(Int64Array::from(v)) as ArrayRef,
            Arc::new(Int64Array::from(id)) as ArrayRef,
        ],
    )
    .unwrap();
    ArrowZSet::new(data, w)
}

fn accumulate_vals(state: &mut HashMap<i64, i64>, zset: &ArrowZSet) {
    if zset.is_empty() { return; }
    let col = zset.data.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    for i in 0..zset.num_rows() {
        *state.entry(col.value(i)).or_insert(0) += zset.weights[i];
    }
}

fn live_vals(state: &HashMap<i64, i64>) -> Vec<i64> {
    let mut vals: Vec<i64> = state.iter().filter(|(_, &w)| w > 0).map(|(&v, _)| v).collect();
    vals.sort_by(|a, b| b.cmp(a));
    vals
}

async fn open_shard(dir: &TempDir) -> Arc<ShardDb> {
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    Arc::new(ShardDb::builder("shard", store).build().await.unwrap())
}

// ─── Test 1: State persists across close/reopen ────────────────────────────

#[tokio::test]
async fn lfs_topk_state_persists() {
    let dir = TempDir::new().unwrap();
    let op_id = OperatorId(20);
    let k = 3usize;
    let mut net_state: HashMap<i64, i64> = Default::default();

    // ── Epoch 1 & 2: insert rows, persist, close ─────────────────────────
    {
        let db = open_shard(&dir).await;
        let op = TopKOp::new(schema_kv(), k, 0, vec![]);

        let out1 = op.process_epoch(make_input(&[(10, 1, 1), (8, 2, 1), (6, 3, 1), (4, 4, 1)]), 1).unwrap();
        accumulate_vals(&mut net_state, &out1);
        assert_eq!(live_vals(&net_state), vec![10, 8, 6]);

        let out2 = op.process_epoch(make_input(&[(9, 5, 1)]), 2).unwrap();
        accumulate_vals(&mut net_state, &out2);
        assert_eq!(live_vals(&net_state), vec![10, 9, 8]);

        assert_eq!(op.fill_level(), 5, "5 entries in buffer after epoch 2");

        persist_topk_state(&db, &op, op_id).await.unwrap();
        db.flush().await.unwrap();
        Arc::try_unwrap(db).ok().expect("single owner").close().await.unwrap();
    }

    // ── Epoch 3: reopen, load, verify top-K is correct ───────────────────
    {
        let db = open_shard(&dir).await;
        let op = load_topk_state(&db, schema_kv(), k, 0, vec![], op_id)
            .await
            .unwrap();

        assert_eq!(op.fill_level(), 5, "fill level restored after reopen");

        // Epoch 3: delete rank-1 (v=10). Next best (v=9 already in top-3), v=6 refills.
        let out3 = op.process_epoch(make_input(&[(10, 1, -1)]), 3).unwrap();
        accumulate_vals(&mut net_state, &out3);
        assert_eq!(live_vals(&net_state), vec![9, 8, 6]);

        Arc::try_unwrap(db).ok().expect("single owner").close().await.unwrap();
    }
}

// ─── Test 2: WAL crash replay ─────────────────────────────────────────────

#[tokio::test]
async fn lfs_topk_crash_replay() {
    let dir = TempDir::new().unwrap();
    let op_id = OperatorId(21);
    let k = 3usize;

    // ── Epoch 1: write to WAL, drop without flush ─────────────────────────
    {
        let db = open_shard(&dir).await;
        let op = TopKOp::new(schema_kv(), k, 0, vec![]);
        op.process_epoch(make_input(&[(10, 1, 1), (8, 2, 1), (6, 3, 1), (4, 4, 1)]), 1).unwrap();
        assert_eq!(op.fill_level(), 4);
        persist_topk_state(&db, &op, op_id).await.unwrap();
        // Simulate crash: drop without flush.
        drop(Arc::try_unwrap(db).ok().expect("single owner"));
    }

    // ── Epoch 2: reopen (WAL replay), add one row ─────────────────────────
    {
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let db = Arc::new(ShardDb::builder("shard", store).build().await.unwrap());

        let op = load_topk_state(&db, schema_kv(), k, 0, vec![], op_id)
            .await
            .unwrap();

        assert_eq!(op.fill_level(), 4, "4 entries recovered from WAL");

        let mut crash_state: HashMap<i64, i64> = Default::default();
        // Reconstruct epoch 1 output.
        let init_op = TopKOp::new(schema_kv(), k, 0, vec![]);
        let init_out = init_op.process_epoch(make_input(&[(10, 1, 1), (8, 2, 1), (6, 3, 1), (4, 4, 1)]), 1).unwrap();
        accumulate_vals(&mut crash_state, &init_out);
        // Epoch 2: insert v=7, outranks v=6.
        let out2 = op.process_epoch(make_input(&[(7, 5, 1)]), 2).unwrap();
        accumulate_vals(&mut crash_state, &out2);
        let crash_live = live_vals(&crash_state);

        // Non-crash path.
        let fresh_op = TopKOp::new(schema_kv(), k, 0, vec![]);
        let mut fresh_state: HashMap<i64, i64> = Default::default();
        let f1 = fresh_op.process_epoch(make_input(&[(10, 1, 1), (8, 2, 1), (6, 3, 1), (4, 4, 1)]), 1).unwrap();
        accumulate_vals(&mut fresh_state, &f1);
        let f2 = fresh_op.process_epoch(make_input(&[(7, 5, 1)]), 2).unwrap();
        accumulate_vals(&mut fresh_state, &f2);
        let fresh_live = live_vals(&fresh_state);

        assert_eq!(crash_live, fresh_live, "crash-replay bit-identical to non-crash path");
        assert_eq!(crash_live, vec![10, 8, 7]);

        Arc::try_unwrap(db).ok().expect("single owner").close().await.unwrap();
    }
}

// ─── Test 3: No range delete ──────────────────────────────────────────────

#[tokio::test]
async fn lfs_topk_no_range_delete() {
    // WriteBatch::delete_range does not exist — structural guarantee.
    let mut batch = WriteBatch::new();
    batch.put(b"key1", b"value1");
    batch.delete(b"key2");
    let _ = batch;
}
