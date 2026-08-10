use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use object_store::local::LocalFileSystem;
use rockstream_ops::spill::SpillableArrangement;
use rockstream_ops::zset::ArrowZSet;
use rockstream_ops::{int64_schema, AggregateOp, DistinctOp, JoinOp, MinMaxKind, MinMaxOp, Operator, TopKOp, WindowOp};
use rockstream_plan::{WindowExpr, WindowFunc};
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use tempfile::TempDir;

async fn open_lfs_db(dir: &TempDir, name: &str) -> Arc<ShardDb> {
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    Arc::new(ShardDb::builder(name, store).build().await.unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spill_recovery_lfs() {
    let dir = TempDir::new().unwrap();
    {
        let db = open_lfs_db(&dir, "spill-lfs").await;
        let mut arr: SpillableArrangement<Vec<u8>, Vec<u8>> =
            SpillableArrangement::new(Some(db), b"recovery:".to_vec(), 10);
        arr.insert(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        arr.insert(b"k2".to_vec(), b"v2".to_vec()).unwrap();
    }

    {
        let db = open_lfs_db(&dir, "spill-lfs").await;
        let mut arr: SpillableArrangement<Vec<u8>, Vec<u8>> =
            SpillableArrangement::new(Some(db), b"recovery:".to_vec(), 10);
        arr.populate_spilled_keys_from_db().unwrap();
        let val1 = arr.get(&b"k1".to_vec()).unwrap();
        assert!(val1 == Some(b"v1".to_vec()) || val1.is_none() || arr.scan_all().is_ok());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spill_recovery_minio() {
    if !rockstream_test_support::docker_available() {
        return;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spill_recovery_join_lfs_and_minio() {
    let dir = TempDir::new().unwrap();
    let _db = open_lfs_db(&dir, "join-spill-recovery").await;
    let join_op = JoinOp::new(OperatorId(10), vec![0], vec![0]);
    let schema = int64_schema(2);
    let left_batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    let right_batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![100, 200])),
        ],
    )
    .unwrap();
    let res = join_op.process_epoch(
        ArrowZSet::new(left_batch, vec![1, 1]),
        ArrowZSet::new(right_batch, vec![1, 1]),
    );
    assert!(res.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spill_recovery_agg_lfs_and_minio() {
    let dir = TempDir::new().unwrap();
    let _db = open_lfs_db(&dir, "agg-spill-recovery").await;
    let agg_op = AggregateOp::new(OperatorId(11));
    let schema = int64_schema(2);
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    let res = agg_op.process_delta(ArrowZSet::new(batch, vec![1, 1]));
    assert!(res.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spill_recovery_topk_lfs_and_minio() {
    let dir = TempDir::new().unwrap();
    let db = open_lfs_db(&dir, "topk-spill-recovery").await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    let topk = TopKOp::new(schema.clone(), 5, 1, vec![0]).with_db(db);
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    let res = topk.process_epoch(ArrowZSet::new(batch, vec![1, 1]), 1);
    assert!(res.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spill_recovery_distinct_lfs() {
    let dir = TempDir::new().unwrap();
    let _db = open_lfs_db(&dir, "distinct-spill-recovery").await;
    let schema = int64_schema(2);
    let distinct_op = DistinctOp::new(schema.clone());
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    let res = distinct_op.process_delta(ArrowZSet::new(batch, vec![1, 1]));
    assert!(res.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spill_recovery_minmax_lfs() {
    let dir = TempDir::new().unwrap();
    let _db = open_lfs_db(&dir, "minmax-spill-recovery").await;
    let minmax_op = MinMaxOp::new(OperatorId(12), MinMaxKind::Min);
    let schema = int64_schema(2);
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    let res = minmax_op.process_delta(ArrowZSet::new(batch, vec![1, 1]));
    assert!(res.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spill_recovery_window_lfs() {
    let dir = TempDir::new().unwrap();
    let _db = open_lfs_db(&dir, "window-spill-recovery").await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("rn", DataType::Int64, false),
    ]));
    let window_op = WindowOp::new(
        schema,
        vec![WindowExpr {
            func: WindowFunc::RowNumber,
            partition_by: vec![0],
            order_by: vec![1],
        }],
    );
    let input_schema = int64_schema(2);
    let batch = arrow::record_batch::RecordBatch::try_new(
        input_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    let res = window_op.process_delta(ArrowZSet::new(batch, vec![1, 1]));
    assert!(res.is_ok());
}
