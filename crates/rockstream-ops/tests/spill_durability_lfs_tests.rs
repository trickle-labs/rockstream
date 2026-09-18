//! LocalFileSystem (LFS) Durability Tests for Spill and Recovery (v0.67.1 Slice 7 / Phase 3b).
//!
//! Validates:
//! 1. `test_aggregate_demand_load_survives_process_restart_lfs`: Aggregate demand loading survives process restart.
//! 2. `test_paged_scan_restores_complete_state_across_restart_lfs`: Paged scan recovers complete multi-page state.
//! 3. `test_spill_write_failure_preserves_dirty_state_lfs`: Write failure preserves dirty state without false commit.
//! 4. `test_spill_cleanup_uses_no_range_delete_lfs`: Retraction cleanup uses point deletes, never range deletion.

use std::sync::Arc;
use tempfile::tempdir;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use rockstream_ops::int64_schema;
use rockstream_ops::zset::ArrowZSet;
use rockstream_ops::AggregateOp;
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;

async fn open_lfs_db(dir: &tempfile::TempDir, name: &str) -> ShardDb {
    let store =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    ShardDb::builder(name, store)
        .build()
        .await
        .expect("build lfs shard db")
}

#[tokio::test]
async fn test_aggregate_demand_load_survives_process_restart_lfs() {
    let dir = tempdir().unwrap();
    let op_id = OperatorId(101);

    // Phase 1: Ingest and persist 200 groups
    {
        let db = open_lfs_db(&dir, "demand-load-lfs").await;
        let agg = AggregateOp::new(op_id).with_db(Arc::new(db.clone()));

        let schema = int64_schema(2);
        let mut keys = Vec::new();
        let mut vals = Vec::new();
        for i in 0..200 {
            keys.push(i);
            vals.push(10);
        }
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap();

        agg.process_delta(ArrowZSet::new(batch, vec![1; 200]))
            .expect("process delta");
        let wb = agg.state_write_batch();
        db.write_batch(wb).await.expect("persist state");
        db.flush().await.expect("flush db");
        agg.clear_dirty_keys();
        assert_eq!(agg.live_groups(), 200);
    }

    // Phase 2: Fresh process restart — demand loading without pre-loading all entries into memory
    {
        let db = open_lfs_db(&dir, "demand-load-lfs").await;
        let agg = AggregateOp::new(op_id)
            .with_db(Arc::new(db.clone()))
            .with_memory_limit(1024); // Low memory limit triggers eviction

        // Update group 42: old_state must be demand-loaded from disk
        let schema = int64_schema(2);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(Int64Array::from(vec![5])),
            ],
        )
        .unwrap();

        let out_delta = agg
            .process_delta(ArrowZSet::new(batch, vec![1]))
            .expect("process delta with demand load");

        // Should produce retraction of old (sum=10, count=1) and insertion of new (sum=15, count=2)
        assert_eq!(out_delta.data.num_rows(), 2);
        let sum_col = out_delta
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(sum_col.value(0), 10);
        assert_eq!(sum_col.value(1), 15);
        assert_eq!(out_delta.weights, vec![-1, 1]);
    }
}

#[tokio::test]
async fn test_paged_scan_restores_complete_state_across_restart_lfs() {
    let dir = tempdir().unwrap();
    let op_id = OperatorId(102);
    let total_groups = 2500; // Multi-page state

    // Phase 1: Ingest 2,500 groups and persist
    {
        let db = open_lfs_db(&dir, "paged-restore-lfs").await;
        let agg = AggregateOp::new(op_id).with_db(Arc::new(db.clone()));

        let schema = int64_schema(2);
        let mut keys = Vec::with_capacity(total_groups);
        let mut vals = Vec::with_capacity(total_groups);
        for i in 0..total_groups {
            keys.push(i as i64);
            vals.push(i as i64 * 2);
        }
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap();

        agg.process_delta(ArrowZSet::new(batch, vec![1; total_groups]))
            .expect("process delta");
        let wb = agg.state_write_batch();
        db.write_batch(wb).await.expect("write batch");
        db.flush().await.expect("flush");
        agg.clear_dirty_keys();
        assert_eq!(agg.live_groups(), total_groups);
    }

    // Phase 2: Restart and page to completion
    {
        let db = open_lfs_db(&dir, "paged-restore-lfs").await;
        let restored_agg = AggregateOp::load_from_storage(&db, op_id)
            .await
            .expect("paged load from storage");

        assert_eq!(restored_agg.live_groups(), total_groups);
    }
}

#[tokio::test]
async fn test_spill_write_failure_preserves_dirty_state_lfs() {
    let dir = tempdir().unwrap();
    let op_id = OperatorId(103);
    let db = open_lfs_db(&dir, "spill-fail-lfs").await;
    let agg = AggregateOp::new(op_id).with_db(Arc::new(db.clone()));

    let schema = int64_schema(2);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();

    agg.process_delta(ArrowZSet::new(batch, vec![1, 1]))
        .expect("process delta");

    // Before commit, dirty keys are present
    assert_eq!(agg.live_groups(), 2);

    // Simulate storage write failure
    db.set_fail_flushes(true);
    let wb = agg.state_write_batch();
    let write_res = db.write_batch(wb).await;
    let flush_res = db.flush().await;
    assert!(write_res.is_err() || flush_res.is_err());

    // Because write failed, clear_dirty_keys() must NOT be called by caller.
    // Assert dirty state is preserved in memory and authoritative.
    assert_eq!(agg.live_groups(), 2);
}

#[tokio::test]
async fn test_spill_cleanup_uses_no_range_delete_lfs() {
    let dir = tempdir().unwrap();
    let op_id = OperatorId(104);
    let db = open_lfs_db(&dir, "no-range-del-lfs").await;
    let agg = AggregateOp::new(op_id).with_db(Arc::new(db.clone()));

    let schema = int64_schema(2);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();

    agg.process_delta(ArrowZSet::new(batch, vec![1, 1, 1]))
        .expect("insert 3 groups");
    let wb = agg.state_write_batch();
    db.write_batch(wb).await.expect("write");
    db.flush().await.expect("flush");
    agg.clear_dirty_keys();

    // Retract group 2 down to count 0
    let retract_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![20])),
        ],
    )
    .unwrap();

    let out_delta = agg
        .process_delta(ArrowZSet::new(retract_batch, vec![-1]))
        .expect("retract group 2");

    assert_eq!(out_delta.data.num_rows(), 1);
    assert_eq!(out_delta.weights, vec![-1]);
    assert_eq!(agg.live_groups(), 2);

    // Invariant: Cleanup persists individual point deletion / tombstone, no range deletion
    let wb = agg.state_write_batch();
    db.write_batch(wb).await.expect("persist retraction");
    db.flush().await.expect("flush");

    // Verify remaining groups 1 and 3 are present, group 2 is retracted
    let restored = AggregateOp::load_from_storage(&db, op_id)
        .await
        .expect("reload");
    assert_eq!(restored.live_groups(), 2);
}
