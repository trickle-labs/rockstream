//! LFS durability and integration tests for `SnapshotOp` (v0.13 — Slice 2).
//!
//! ## Tests
//!
//! 1. `lfs_snapshot_bootstrap_restart_resilience` — verifying that snapshot
//!    bootstrapping can resume correctly across ShardDb restarts, retaining
//!    correct offset and processing the rest of the stream.
//!
//! 2. `lfs_snapshot_bootstrap_large_scale` — verifying that snapshot size
//!    exceeding `SNAPSHOT_BUFFER_LIMIT` is correctly rejected with a
//!    `StorageError::Unsupported` error.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use object_store::local::LocalFileSystem;
use rockstream_ops::op::Operator;
use rockstream_ops::sink::ViewSinkOp;
use rockstream_ops::snapshot::SnapshotOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use tempfile::TempDir;

fn schema_kv() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

async fn open_shard(dir: &TempDir) -> Arc<ShardDb> {
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    Arc::new(ShardDb::builder("shard", store).build().await.unwrap())
}

#[tokio::test]
async fn lfs_snapshot_bootstrap_restart_resilience() {
    let dir = TempDir::new().unwrap();
    let op_id = OperatorId(101);
    let schema = schema_kv();

    // ── Phase 1: Write initial epochs, verify first chunk, and close ─────
    {
        let db = open_shard(&dir).await;
        let sink = ViewSinkOp::new(db.clone(), op_id);

        // Epoch 0: 5 rows
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

        // Expected positive consolidated state:
        // (1, 10) weight 1
        // (2, 20) weight 2 - 1 = 1
        // (3, 30) weight 1 - 1 = 0 (retracted, excluded)
        // (4, 40) weight 1
        // (5, 50) weight 1
        // (6, 60) weight 1
        // Consolidated list: (1, 10), (2, 20), (4, 40), (5, 50), (6, 60)
        // Batch size = 2.
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

        // Close db simulating a checkpoint/restart
        Arc::try_unwrap(db)
            .ok()
            .expect("single owner")
            .close()
            .await
            .unwrap();
    }

    // ── Phase 2: Reopen, resume from offset 2, process next chunk, and close ──
    {
        let db = open_shard(&dir).await;

        // Initialize with resume_offset = 2
        let snap_op = SnapshotOp::load_and_initialize(db.clone(), op_id, 2, schema.clone(), 2)
            .await
            .unwrap();

        assert!(!snap_op.is_complete());

        // Emit second chunk: (4,40), (5,50)
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
        let db = open_shard(&dir).await;

        // Initialize with resume_offset = 4
        let snap_op = SnapshotOp::load_and_initialize(db.clone(), op_id, 2, schema.clone(), 4)
            .await
            .unwrap();

        assert!(!snap_op.is_complete());

        // Emit final chunk: (6,60)
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

        // Next delta is empty
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
async fn lfs_snapshot_bootstrap_large_scale() {
    let dir = TempDir::new().unwrap();
    let op_id = OperatorId(102);
    let schema = schema_kv();
    let db = open_shard(&dir).await;

    // Write 1,000,001 rows directly via WriteBatch to storage to exceed SNAPSHOT_BUFFER_LIMIT (1,000,000)
    let mut wb = rockstream_storage::WriteBatch::new();
    let limit = rockstream_ops::snapshot::SNAPSHOT_BUFFER_LIMIT;

    for i in 0..=limit {
        let mut key = Vec::with_capacity(1 + 8 + 8 + 8);
        key.push(rockstream_storage::ShardPrefix::ViewOutput.as_byte());
        key.extend_from_slice(&op_id.0.to_be_bytes());
        key.extend_from_slice(&0u64.to_be_bytes()); // epoch 0
        key.extend_from_slice(&(i as u64).to_be_bytes()); // row index

        let mut value = Vec::with_capacity(24);
        value.extend_from_slice(&(i as i64).to_be_bytes()); // col k
        value.extend_from_slice(&(i as i64).to_be_bytes()); // col v
        value.extend_from_slice(&1i64.to_be_bytes()); // weight 1
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

    // Try to load snapshot
    let res = SnapshotOp::load_and_initialize(db.clone(), op_id, 100, schema.clone(), 0).await;
    assert!(
        res.is_err(),
        "Expected error due to SNAPSHOT_BUFFER_LIMIT violation"
    );

    match res {
        Err(rockstream_ops::error::OpError::Storage { source, .. }) => match source {
            rockstream_storage::StorageError::Unsupported(msg) => {
                assert!(
                    msg.contains("exceeds SNAPSHOT_BUFFER_LIMIT"),
                    "Unexpected error message: {}",
                    msg
                );
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
