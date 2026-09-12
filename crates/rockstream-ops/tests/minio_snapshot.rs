//! MinIO (S3) backend integration tests for `SnapshotOp` (v0.13 — Slice 2).

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use object_store::ObjectStore;
use rockstream_ops::op::Operator;
use rockstream_ops::sink::ViewSinkOp;
use rockstream_ops::snapshot::SnapshotOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
const MINIO_BUCKET: &str = "rockstream-test-snapshot";

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

fn schema_kv() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

#[tokio::test]
async fn minio_snapshot_bootstrap_restart_resilience() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!("SKIP minio_snapshot_bootstrap_restart_resilience: Docker not available");
            return;
        }
    };
    let op_id = OperatorId(101);
    let schema = schema_kv();
    let db_path = "snap-resilience";

    // ── Phase 1: Write initial epochs, verify first chunk, and close ─────
    {
        let db = open_shard_db_minio(port, db_path).await;
        let sink = ViewSinkOp::new(db.clone(), op_id);

        // Epoch 0: 6 rows
        let batch0 =
            ArrowZSet::from_ab_rows(&[(1, 10), (2, 20), (2, 20), (3, 30), (4, 40), (5, 50)], 1);
        sink.write_epoch(&batch0, 0).await.unwrap();

        // Epoch 1: retract (2, 20) and (3, 30), and add new (6, 60)
        let batch1_retract_2 = ArrowZSet::from_ab_rows(&[(2, 20)], -1);
        let batch1_retract_3 = ArrowZSet::from_ab_rows(&[(3, 30)], -1);
        let batch1_add_6 = ArrowZSet::from_ab_rows(&[(6, 60)], 1);

        sink.write_epoch(&batch1_retract_2, 1).await.unwrap();
        sink.write_epoch(&batch1_retract_3, 2).await.unwrap();
        sink.write_epoch(&batch1_add_6, 3).await.unwrap();

        db.flush().await.unwrap();

        let snap_op = SnapshotOp::load_and_initialize(db.clone(), op_id, 2, schema.clone(), 0)
            .await
            .unwrap();

        assert!(!snap_op.is_complete());

        // Emit first chunk: (1,10), (2,20)
        let out1 = snap_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert_eq!(out1.num_rows(), 2);
        let k0 = out1
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let k1 = out1
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(1);
        assert_eq!(k0, 1);
        assert_eq!(k1, 2);
        assert!(!snap_op.is_complete());

        drop(sink);
        drop(snap_op);

        Arc::try_unwrap(db)
            .ok()
            .expect("single owner")
            .close()
            .await
            .unwrap();
    }

    // ── Phase 2: Reopen, resume from offset 2, process next chunk, and close ──
    {
        let db = open_shard_db_minio(port, db_path).await;

        let snap_op = SnapshotOp::load_and_initialize(db.clone(), op_id, 2, schema.clone(), 2)
            .await
            .unwrap();

        assert!(!snap_op.is_complete());

        let out2 = snap_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert_eq!(out2.num_rows(), 2);
        let k0 = out2
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let k1 = out2
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(1);
        assert_eq!(k0, 4);
        assert_eq!(k1, 5);
        assert!(!snap_op.is_complete());

        drop(snap_op);

        Arc::try_unwrap(db)
            .ok()
            .expect("single owner")
            .close()
            .await
            .unwrap();
    }

    // ── Phase 3: Reopen, resume from offset 4, process last chunk, and complete ──
    {
        let db = open_shard_db_minio(port, db_path).await;

        let snap_op = SnapshotOp::load_and_initialize(db.clone(), op_id, 2, schema.clone(), 4)
            .await
            .unwrap();

        assert!(!snap_op.is_complete());

        let out3 = snap_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert_eq!(out3.num_rows(), 1);
        let k0 = out3
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(k0, 6);
        assert!(snap_op.is_complete());

        let out4 = snap_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert!(out4.is_empty());
        assert!(snap_op.is_complete());

        drop(snap_op);

        Arc::try_unwrap(db)
            .ok()
            .expect("single owner")
            .close()
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn minio_snapshot_bootstrap_large_scale() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!("SKIP minio_snapshot_bootstrap_large_scale: Docker not available");
            return;
        }
    };
    let op_id = OperatorId(102);
    let schema = schema_kv();
    let db = open_shard_db_minio(port, "snap-large").await;

    let mut wb = rockstream_storage::WriteBatch::new();
    let limit = rockstream_ops::snapshot::SNAPSHOT_BUFFER_LIMIT;

    for i in 0..=limit {
        let mut key = Vec::with_capacity(1 + 8 + 8 + 8);
        key.push(rockstream_storage::ShardPrefix::ViewOutput.as_byte());
        key.extend_from_slice(&op_id.0.to_be_bytes());
        key.extend_from_slice(&0u64.to_be_bytes());
        key.extend_from_slice(&(i as u64).to_be_bytes());

        let mut value = Vec::with_capacity(26);
        value.push(0u8); // TAG_INT64
        value.extend_from_slice(&(i as i64).to_be_bytes());
        value.push(0u8); // TAG_INT64
        value.extend_from_slice(&(i as i64).to_be_bytes());
        value.extend_from_slice(&1i64.to_be_bytes());
        wb.put(&key, &value);

        if i > 0 && i % 100_000 == 0 {
            db.write_batch(wb).await.unwrap();
            wb = rockstream_storage::WriteBatch::new();
        }
    }
    if !wb.is_empty() {
        db.write_batch(wb).await.unwrap();
    }

    db.flush().await.unwrap();

    let res = SnapshotOp::load_and_initialize(db.clone(), op_id, 100, schema.clone(), 0).await;
    assert!(
        res.is_err(),
        "Expected error due to SNAPSHOT_BUFFER_LIMIT violation"
    );

    match res {
        Err(rockstream_ops::error::OpError::Storage { source, .. }) => match source {
            rockstream_storage::StorageError::Unsupported(msg) => {
                assert!(msg.contains("exceeds SNAPSHOT_BUFFER_LIMIT"));
            }
            other => panic!("Expected StorageError::Unsupported, got {:?}", other),
        },
        Err(other) => panic!(
            "Expected StorageError::Unsupported, got Err variant: {:?}",
            other
        ),
        Ok(_) => panic!("Expected StorageError::Unsupported, got Ok"),
    }

    Arc::try_unwrap(db)
        .ok()
        .expect("single owner")
        .close()
        .await
        .unwrap();
}
