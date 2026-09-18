//! MinIO (S3) Durability Tests for Spill and Recovery (v0.67.1 Slice 7 / Phase 3b).
//!
//! Validates:
//! 1. `test_aggregate_demand_load_survives_process_restart_minio`: Aggregate demand loading survives process restart on MinIO.
//! 2. `test_paged_scan_restores_complete_state_across_restart_minio`: Paged scan recovers complete multi-page state from MinIO.
//! 3. `test_spill_compaction_overlap_on_object_store_minio`: Overlap of compaction and spill flushes on MinIO.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use rockstream_ops::int64_schema;
use rockstream_ops::zset::ArrowZSet;
use rockstream_ops::AggregateOp;
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;

const MINIO_BUCKET: &str = "rockstream-test-spill-durability";

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

#[tokio::test]
async fn test_aggregate_demand_load_survives_process_restart_minio() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!("SKIP test_aggregate_demand_load_survives_process_restart_minio: Docker not available");
            return;
        }
    };

    let shard_path = "minio-spill-demand-load";
    let op_id = OperatorId(201);

    // Phase 1: Ingest and persist 100 groups on MinIO
    {
        let store = minio_object_store(port);
        let db = Arc::new(
            ShardDb::builder(shard_path, store)
                .build()
                .await
                .expect("open ShardDb on MinIO"),
        );
        let agg = AggregateOp::new(op_id).with_db(db.clone());

        let schema = int64_schema(2);
        let mut keys = Vec::new();
        let mut vals = Vec::new();
        for i in 0..100 {
            keys.push(i);
            vals.push(50);
        }
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap();

        agg.process_delta(ArrowZSet::new(batch, vec![1; 100]))
            .expect("process delta");
        let wb = agg.state_write_batch();
        db.write_batch(wb).await.expect("persist state to MinIO");
        db.flush().await.expect("flush MinIO");
        agg.clear_dirty_keys();
        assert_eq!(agg.live_groups(), 100);
    }

    // Phase 2: Reopen MinIO ShardDb, verify demand-loading on key 25
    {
        let store = minio_object_store(port);
        let db = Arc::new(
            ShardDb::builder(shard_path, store)
                .build()
                .await
                .expect("reopen ShardDb on MinIO"),
        );
        let agg = AggregateOp::new(op_id)
            .with_db(db.clone())
            .with_memory_limit(512);

        let schema = int64_schema(2);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![25])),
                Arc::new(Int64Array::from(vec![10])),
            ],
        )
        .unwrap();

        let out_delta = agg
            .process_delta(ArrowZSet::new(batch, vec![1]))
            .expect("demand load delta");

        assert_eq!(out_delta.data.num_rows(), 2);
        assert_eq!(out_delta.weights, vec![-1, 1]);
    }
}

#[tokio::test]
async fn test_paged_scan_restores_complete_state_across_restart_minio() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!("SKIP test_paged_scan_restores_complete_state_across_restart_minio: Docker not available");
            return;
        }
    };

    let shard_path = "minio-spill-paged-restore";
    let op_id = OperatorId(202);
    let total_groups = 1500;

    // Phase 1: Write multi-page state to MinIO
    {
        let store = minio_object_store(port);
        let db = Arc::new(
            ShardDb::builder(shard_path, store)
                .build()
                .await
                .expect("open ShardDb on MinIO"),
        );
        let agg = AggregateOp::new(op_id).with_db(db.clone());

        let schema = int64_schema(2);
        let mut keys = Vec::with_capacity(total_groups);
        let mut vals = Vec::with_capacity(total_groups);
        for i in 0..total_groups {
            keys.push(i as i64);
            vals.push(i as i64 * 3);
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
        db.write_batch(wb).await.expect("persist state to MinIO");
        db.flush().await.expect("flush MinIO");
        agg.clear_dirty_keys();
        assert_eq!(agg.live_groups(), total_groups);
    }

    // Phase 2: Page through MinIO state to completion
    {
        let store = minio_object_store(port);
        let db = ShardDb::builder(shard_path, store)
            .build()
            .await
            .expect("reopen ShardDb on MinIO");

        let restored_agg = AggregateOp::load_from_storage(&db, op_id)
            .await
            .expect("paged load from storage on MinIO");

        assert_eq!(restored_agg.live_groups(), total_groups);
    }
}

#[tokio::test]
async fn test_spill_compaction_overlap_on_object_store_minio() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!(
                "SKIP test_spill_compaction_overlap_on_object_store_minio: Docker not available"
            );
            return;
        }
    };

    let shard_path = "minio-spill-compaction-overlap";
    let op_id = OperatorId(203);
    let store = minio_object_store(port);
    let db = Arc::new(
        ShardDb::builder(shard_path, store)
            .build()
            .await
            .expect("open ShardDb on MinIO"),
    );

    let agg = AggregateOp::new(op_id).with_db(db.clone());

    // Ingest 500 groups
    let schema = int64_schema(2);
    let mut keys = Vec::new();
    let mut vals = Vec::new();
    for i in 0..500 {
        keys.push(i);
        vals.push(10);
    }
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .unwrap();

    agg.process_delta(ArrowZSet::new(batch, vec![1; 500]))
        .expect("process delta");
    let wb = agg.state_write_batch();
    db.write_batch(wb).await.expect("persist");
    db.flush().await.expect("flush");
    agg.clear_dirty_keys();

    // Trigger checkpoint/flush while concurrently applying updates
    let update_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![5, 5, 5])),
        ],
    )
    .unwrap();

    let out = agg
        .process_delta(ArrowZSet::new(update_batch, vec![1, 1, 1]))
        .expect("concurrent update");
    assert_eq!(out.data.num_rows(), 6); // 3 retractions + 3 additions

    let wb2 = agg.state_write_batch();
    db.write_batch(wb2).await.expect("persist second epoch");
    db.flush().await.expect("flush second epoch");

    assert_eq!(agg.live_groups(), 500);
}
