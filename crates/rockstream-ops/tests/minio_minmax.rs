//! MinIO (S3) backend integration tests for v0.6 MinMax operator.
//!
//! Tests:
//! 1. `minio_minmax_writes_and_persists` — MinMaxOp state survives ShardDb
//!    close/reopen on the MinIO (S3-compatible) backend.
//! 2. `minio_minmax_crash_replay_bit_identical` — simulated crash before epoch
//!    commit; on restart the shard replays from its persisted frontier to
//!    **bit-identical** output on the S3 backend.
//!
//! Docker must be running. Tests skip gracefully if Docker is unavailable.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use rockstream_ops::aggregate::{load_frontier, persist_frontier};
use rockstream_ops::minmax::{persist_minmax_state, MinMaxKind, MinMaxOp};
use rockstream_ops::op::Operator;
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
const MINIO_BUCKET: &str = "rockstream-test-minmax";

async fn start_minio() -> Option<(
    testcontainers::ContainerAsync<rockstream_test_support::minio::MinIO2024>,
    u16,
)> {
    rockstream_test_support::minio::start_minio(MINIO_BUCKET).await
}

fn minio_object_store(port: u16) -> Arc<dyn ObjectStore> {
    Arc::new(rockstream_test_support::minio::minio_object_store(
        port,
        MINIO_BUCKET,
    ))
}

async fn open_minio_shard_db(port: u16, path: &str) -> Arc<ShardDb> {
    let store = minio_object_store(port);
    Arc::new(
        ShardDb::builder(path, store)
            .build()
            .await
            .expect("failed to open ShardDb on MinIO"),
    )
}

fn make_kv_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let k_vals: Vec<i64> = rows.iter().map(|(k, _, _)| *k).collect();
    let v_vals: Vec<i64> = rows.iter().map(|(_, v, _)| *v).collect();
    let weights: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
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

fn extract_output(batch: &ArrowZSet) -> Vec<(i64, i64, i64)> {
    let k_col = batch
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let e_col = batch
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut rows: Vec<(i64, i64, i64)> = (0..batch.num_rows())
        .map(|i| (k_col.value(i), e_col.value(i), batch.weights[i]))
        .collect();
    rows.sort();
    rows
}

// ─── Test 1: State persists across close/reopen on MinIO ─────────────────────

/// Proof: MinMaxOp state (multiset + extremum cache + frontier) survives
/// close/reopen on the S3-compatible MinIO backend.
#[tokio::test]
async fn minio_minmax_writes_and_persists() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!("SKIP minio_minmax_writes_and_persists: Docker not available");
            return;
        }
    };

    let db = open_minio_shard_db(port, "shard-mm-persist").await;
    let op = MinMaxOp::new_min(OperatorId(10));

    // Epoch 1: insert groups.
    let delta1 = make_kv_batch(&[(1, 10, 1), (1, 5, 1), (2, 20, 1)]);
    let _ = op.process_delta(delta1).unwrap();
    persist_minmax_state(&db, &op).await.unwrap();
    persist_frontier(&db, 1).await.unwrap();

    assert_eq!(op.cached_extremum(1), Some(5));
    assert_eq!(op.cached_extremum(2), Some(20));

    drop(op);
    drop(db);

    // ── Reopen on same MinIO path ────────────────────────────────────────────
    let db2 = open_minio_shard_db(port, "shard-mm-persist").await;
    let op2 = MinMaxOp::load_from_storage(&db2, OperatorId(10), MinMaxKind::Min)
        .await
        .unwrap();
    let frontier = load_frontier(&db2).await.unwrap();

    assert_eq!(frontier, Some(1));
    assert_eq!(op2.live_groups(), 2);
    assert_eq!(op2.cached_extremum(1), Some(5));
    assert_eq!(op2.cached_extremum(2), Some(20));

    // Epoch 2: retract min of k=1.
    let delta2 = make_kv_batch(&[(1, 5, -1)]);
    let out = op2.process_delta(delta2).unwrap();
    let rows = extract_output(&out);
    assert!(rows.contains(&(1, 5, -1)), "missing retraction: {rows:?}");
    assert!(rows.contains(&(1, 10, 1)), "missing insertion: {rows:?}");
    assert_eq!(op2.cached_extremum(1), Some(10));

    persist_minmax_state(&db2, &op2).await.unwrap();
    persist_frontier(&db2, 2).await.unwrap();
}

// ─── Test 2: Crash-replay on MinIO ───────────────────────────────────────────

/// Proof: simulated crash on the S3 backend; on restart the shard replays
/// epoch 2 from persisted frontier (epoch 1) to bit-identical output.
#[tokio::test]
async fn minio_minmax_crash_replay_bit_identical() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!("SKIP minio_minmax_crash_replay_bit_identical: Docker not available");
            return;
        }
    };

    let delta1 = make_kv_batch(&[(1, 10, 1), (1, 5, 1), (2, 20, 1)]);
    let delta2 = make_kv_batch(&[(1, 5, -1), (2, 15, 1)]);

    // ── Reference run ────────────────────────────────────────────────────────
    let db_ref = open_minio_shard_db(port, "shard-mm-ref").await;
    let op_ref = MinMaxOp::new_min(OperatorId(11));
    let _ = op_ref.process_delta(delta1.clone()).unwrap();
    persist_minmax_state(&db_ref, &op_ref).await.unwrap();
    persist_frontier(&db_ref, 1).await.unwrap();
    let reference_output = extract_output(&op_ref.process_delta(delta2.clone()).unwrap());
    drop(op_ref);
    drop(db_ref);

    // ── Crash run ────────────────────────────────────────────────────────────
    let db_crash = open_minio_shard_db(port, "shard-mm-crash").await;
    let op_crash = MinMaxOp::new_min(OperatorId(11));
    let _ = op_crash.process_delta(delta1).unwrap();
    persist_minmax_state(&db_crash, &op_crash).await.unwrap();
    persist_frontier(&db_crash, 1).await.unwrap();
    // Simulate crash: process epoch 2 in memory, never persist.
    let _ = op_crash.process_delta(delta2.clone()).unwrap();
    drop(op_crash);
    drop(db_crash);

    // ── Recovery ─────────────────────────────────────────────────────────────
    let db_recovery = open_minio_shard_db(port, "shard-mm-crash").await;
    let frontier = load_frontier(&db_recovery).await.unwrap();
    assert_eq!(
        frontier,
        Some(1),
        "frontier must point to last committed epoch"
    );

    let op_recovery = MinMaxOp::load_from_storage(&db_recovery, OperatorId(11), MinMaxKind::Min)
        .await
        .unwrap();
    let replay_output = extract_output(&op_recovery.process_delta(delta2).unwrap());

    assert_eq!(
        replay_output,
        reference_output,
        "crash-replay (MinIO) not bit-identical:\n  replay:    {replay_output:?}\n  reference: {reference_output:?}"
    );
}
