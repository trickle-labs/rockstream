//! MinIO (S3) backend integration tests for v0.5 operators.
//!
//! Tests:
//! 1. `minio_aggregate_writes_and_persists` — AggregateOp state and frontier
//!    survive a close/reopen cycle on the MinIO (S3) backend.
//! 2. `minio_group_commit_reduces_durability_events` — GroupCommit reduces
//!    write count ≥5× vs. individual commits on the S3 backend.
//!
//! Docker must be running.  Tests skip gracefully if Docker is unavailable.

use std::sync::Arc;

use object_store::ObjectStore;
use rockstream_ops::aggregate::{load_frontier, persist_agg_state, persist_frontier, AggregateOp};
use rockstream_ops::group_commit::GroupCommit;
use rockstream_ops::zset::ArrowZSet;
use rockstream_ops::{FactorizedAggregateKind, FactorizedJoinAggregateOp};
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::ids::OperatorId;
const MINIO_BUCKET: &str = "rockstream-test-ops";

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

async fn open_shard_db_minio(port: u16, path: &str) -> Arc<ShardDb> {
    let store = minio_object_store(port);
    Arc::new(
        ShardDb::builder(path, store)
            .build()
            .await
            .expect("failed to open ShardDb on MinIO"),
    )
}

fn make_kv_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
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

// ─── Test 1: AggregateOp state + frontier survive on MinIO ───────────────────

/// Proof: AggregateOp state and frontier persist across close/reopen on the
/// S3 (MinIO) backend — proving S3-semantics correctness of the op_state and
/// shard_meta namespaces.
#[tokio::test]
async fn minio_aggregate_writes_and_persists() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!("SKIP minio_aggregate_writes_and_persists: Docker not available");
            return;
        }
    };

    // Phase 1: write state.
    {
        let db = open_shard_db_minio(port, "agg-test").await;
        let op = AggregateOp::new(OperatorId(1));
        let _ = op
            .process_delta(make_kv_batch(&[(1, 10, 1), (2, 20, 1)]))
            .unwrap();
        persist_agg_state(&db, &op).await.unwrap();
        persist_frontier(&db, 5).await.unwrap();
        db.flush().await.unwrap();
        Arc::try_unwrap(db)
            .ok()
            .expect("single owner")
            .close()
            .await
            .unwrap();
    }

    // Phase 2: reopen and verify.
    {
        let db = open_shard_db_minio(port, "agg-test").await;
        let frontier = load_frontier(&db).await.unwrap();
        assert_eq!(frontier, Some(5u64), "frontier must survive on MinIO");
        let op = AggregateOp::load_from_storage(&db, OperatorId(1))
            .await
            .unwrap();
        assert_eq!(op.live_groups(), 2, "2 live groups must survive on MinIO");
    }
}

#[tokio::test]
async fn factorized_join_replays_and_retracts_on_minio() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!("SKIP factorized_join_replays_and_retracts_on_minio: Docker not available");
            return;
        }
    };
    {
        let db = open_shard_db_minio(port, "factorized-join").await;
        let op = FactorizedJoinAggregateOp::new(
            OperatorId(59_701),
            vec![0],
            vec![0],
            2,
            2,
            0,
            3,
            FactorizedAggregateKind::Sum,
        );
        assert!(op
            .process_epoch(
                make_kv_batch(&[(1, 2, 1)]),
                ArrowZSet::empty(make_kv_batch(&[]).schema())
            )
            .unwrap()
            .is_empty());
        let mut writes = WriteBatch::new();
        op.append_state_with_db(&db, &mut writes).await.unwrap();
        db.write_batch(writes).await.unwrap();
        db.flush().await.unwrap();
        Arc::try_unwrap(db)
            .ok()
            .expect("single owner")
            .close()
            .await
            .unwrap();
    }
    let db = open_shard_db_minio(port, "factorized-join").await;
    let restored = FactorizedJoinAggregateOp::new(
        OperatorId(59_701),
        vec![0],
        vec![0],
        2,
        2,
        0,
        3,
        FactorizedAggregateKind::Sum,
    );
    restored.restore_in_place(&db).await.unwrap();
    let inserted = restored
        .process_epoch(
            ArrowZSet::empty(make_kv_batch(&[]).schema()),
            make_kv_batch(&[(1, 5, 1)]),
        )
        .unwrap();
    assert_eq!(inserted.weights, vec![1]);
    assert_eq!(
        inserted
            .data
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .values(),
        &[5]
    );
    let retracted = restored
        .process_epoch(
            ArrowZSet::empty(make_kv_batch(&[]).schema()),
            make_kv_batch(&[(1, 5, -1)]),
        )
        .unwrap();
    assert_eq!(retracted.weights, vec![-1]);
    assert_eq!(
        retracted
            .data
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .values(),
        &[5]
    );
}

// ─── Test 2: GroupCommit reduces durability events ≥5× on MinIO ──────────────

/// Proof: GroupCommit issues exactly 1 Db::write() for N=6 operator batches
/// on the S3 (MinIO) backend, proving ≥5× reduction in durability events under
/// S3 semantics (list/get/put latency, conditional writes, multipart paths).
#[tokio::test]
async fn minio_group_commit_reduces_durability_events() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!("SKIP minio_group_commit_reduces_durability_events: Docker not available");
            return;
        }
    };
    let db = open_shard_db_minio(port, "gc-test").await;

    const NUM_OPERATORS: usize = 6;
    let gc = GroupCommit::new(db.clone());

    for i in 0..NUM_OPERATORS {
        let mut wb = WriteBatch::new();
        wb.put(&[0x01, i as u8], &(i as u64).to_be_bytes());
        gc.add_batch(wb).unwrap();
    }
    assert_eq!(gc.fill_level(), NUM_OPERATORS);

    let merged = gc.flush().await.unwrap();
    assert_eq!(gc.commit_count(), 1, "exactly 1 Db::write() on MinIO");
    assert_eq!(merged, NUM_OPERATORS);

    let reduction = NUM_OPERATORS as u64 / gc.commit_count();
    assert!(reduction >= 5, "≥5× reduction required; got {reduction}×");

    // Spot-check: all keys visible after commit.
    for i in 0..NUM_OPERATORS {
        let val = db.get(&[0x01, i as u8]).await.unwrap();
        assert!(val.is_some(), "key {i} not found after MinIO group commit");
    }
}
