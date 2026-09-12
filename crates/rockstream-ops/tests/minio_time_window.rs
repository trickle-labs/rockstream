//! MinIO (S3) backend integration tests for `TumbleWindowOp` (v0.12 — IVM-8).
//!
//! Tests skip gracefully if Docker is unavailable.
//!
//! 1. `minio_tumble_window_late_data_and_ttl` — late rows dropped; partial state not
//!    evicted on MinIO backend until both TTL and frontier gate are satisfied.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use rockstream_ops::time_window::{
    load_hop_window_state, load_session_window_state, load_tumble_window_state,
    persist_hop_window_state, persist_session_window_state, persist_tumble_window_state,
    CompactionFilter, HopWindowOp, SessionWindowOp, TumbleWindowOp,
};
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::LateDataPolicy;
use rockstream_storage::{keys::ShardKeyEncoder, ShardDb};
use rockstream_types::ids::OperatorId;

const MINIO_BUCKET: &str = "rockstream-test-tw";

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

async fn open_shard_minio(port: u16, path: &str) -> Arc<ShardDb> {
    let store = minio_object_store(port);
    Arc::new(
        ShardDb::builder(path, store)
            .build()
            .await
            .expect("failed to open ShardDb on MinIO"),
    )
}

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("t", DataType::Int64, false),
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

fn accumulate_session(
    state: &mut std::collections::HashMap<(i64, i64, i64, i64), i64>,
    zset: &ArrowZSet,
) {
    if zset.is_empty() {
        return;
    }
    let start = zset
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let end = zset
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let t = zset
        .data
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let v = zset
        .data
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..zset.num_rows() {
        *state
            .entry((start.value(i), end.value(i), t.value(i), v.value(i)))
            .or_insert(0) += zset.weights[i];
    }
}

fn live_session_rows(
    state: &std::collections::HashMap<(i64, i64, i64, i64), i64>,
) -> Vec<(i64, i64, i64, i64)> {
    let mut rows: Vec<_> = state
        .iter()
        .filter(|(_, &w)| w > 0)
        .map(|(&k, _)| k)
        .collect();
    rows.sort();
    rows
}

// ─── Test 1: Late data + TTL on MinIO backend ─────────────────────────────

#[tokio::test]
async fn minio_tumble_window_late_data_and_ttl() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!("Docker not available — skipping minio_tumble_window_late_data_and_ttl");
            return;
        }
    };
    let db = open_shard_minio(port, "tw-test").await;
    let op_id = OperatorId(30);
    let window_size_ms = 1000i64;

    let op = TumbleWindowOp::new(input_schema(), 0, window_size_ms, LateDataPolicy::Drop);

    // Epoch 1: rows in window [0, 1000).
    let out1 = op
        .process_epoch(make_input(&[(100, 10, 1), (500, 20, 1)]), 1)
        .unwrap();
    assert!(!out1.is_empty(), "epoch 1 must produce output");

    // Epoch 2: advance watermark past window end (t=5000 > window_end=1000).
    let _out2 = op.process_epoch(make_input(&[(5000, 99, 1)]), 2).unwrap();
    assert!(
        op.watermark_ms() >= 5000,
        "watermark must be >= 5000 after epoch 2"
    );

    // Persist to MinIO.
    persist_tumble_window_state(&db, &op, op_id).await.unwrap();

    // Epoch 3: late row for window [0, 1000) — t=50 < watermark=5000 → dropped.
    let out3 = op.process_epoch(make_input(&[(50, 77, 1)]), 3).unwrap();
    assert!(
        out3.is_empty(),
        "late row must be dropped, got {} rows",
        out3.num_rows()
    );

    // Reload from MinIO and verify state.
    let op2 = load_tumble_window_state(
        &db,
        input_schema(),
        0,
        window_size_ms,
        LateDataPolicy::Drop,
        op_id,
    )
    .await
    .unwrap();

    assert_eq!(
        op2.fill_level(),
        op.fill_level(),
        "fill level matches after reload"
    );

    // Verify compaction filter refuses early deletion of window [0, 1000) state
    // when frontier has NOT advanced past window_end.
    let sample_key = ShardKeyEncoder::tumble_window_key(op_id.0, 0i64, b"gk");
    let filter = CompactionFilter {
        watermark_ms: 5000,
        window_size_ms,
        allowed_lateness_ms: 0,
        frontier_ms: 500, // NOT past window_end=1000
    };
    assert!(
        !filter.may_delete(&sample_key),
        "must not evict window state when frontier < window_end"
    );
}

#[tokio::test]
async fn hop_window_late_data_and_ttl_on_minio() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!("Docker not available — skipping hop_window_late_data_and_ttl_on_minio");
            return;
        }
    };

    let db = open_shard_minio(port, "hop-test").await;
    let op_id = OperatorId(31);
    let op = HopWindowOp::new(input_schema(), 0, 1000, 500, LateDataPolicy::Drop);

    let out1 = op
        .process_epoch(make_input(&[(750, 10, 1), (1250, 20, 1)]), 1)
        .unwrap();
    assert_eq!(
        out1.num_rows(),
        4,
        "each row should fan out to two overlapping windows"
    );

    let _out2 = op.process_epoch(make_input(&[(5000, 99, 1)]), 2).unwrap();
    assert!(op.watermark_ms() >= 5000);

    persist_hop_window_state(&db, &op, op_id).await.unwrap();

    let late = op.process_epoch(make_input(&[(50, 77, 1)]), 3).unwrap();
    assert!(
        late.is_empty(),
        "late hop row must be dropped on MinIO path"
    );

    let op2 = load_hop_window_state(
        &db,
        input_schema(),
        0,
        1000,
        500,
        LateDataPolicy::Drop,
        op_id,
    )
    .await
    .unwrap();
    assert_eq!(
        op2.fill_level(),
        op.fill_level(),
        "hop fill level matches after reload"
    );

    let sample_key = ShardKeyEncoder::tumble_window_key(op_id.0, 500i64, b"gk");
    let filter = CompactionFilter {
        watermark_ms: 5000,
        window_size_ms: 1000,
        allowed_lateness_ms: 0,
        frontier_ms: 1200,
    };
    assert!(
        !filter.may_delete(&sample_key),
        "hop state must not evict before frontier passes the window end"
    );
}

#[tokio::test]
async fn session_window_merge_survives_minio_restart() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!(
                "Docker not available — skipping session_window_merge_survives_minio_restart"
            );
            return;
        }
    };

    let db = open_shard_minio(port, "session-test").await;
    let op_id = OperatorId(32);
    let mut net_state: std::collections::HashMap<(i64, i64, i64, i64), i64> = Default::default();

    let op = SessionWindowOp::new(input_schema(), 0, 1000, LateDataPolicy::Drop);
    let out1 = op
        .process_epoch(
            make_input(&[(100, 7, 1), (900, 7, 1), (2100, 7, 1), (2500, 7, 1)]),
            1,
        )
        .unwrap();
    accumulate_session(&mut net_state, &out1);
    persist_session_window_state(&db, &op, op_id).await.unwrap();

    let op2 = load_session_window_state(&db, input_schema(), 0, 1000, LateDataPolicy::Drop, op_id)
        .await
        .unwrap();
    let out2 = op2.process_epoch(make_input(&[(1500, 7, 1)]), 2).unwrap();
    accumulate_session(&mut net_state, &out2);

    assert_eq!(op2.fill_level(), 1, "merged session survives MinIO reload");
    assert_eq!(
        live_session_rows(&net_state),
        vec![
            (100, 2500, 100, 7),
            (100, 2500, 900, 7),
            (100, 2500, 1500, 7),
            (100, 2500, 2100, 7),
            (100, 2500, 2500, 7),
        ]
    );
}
