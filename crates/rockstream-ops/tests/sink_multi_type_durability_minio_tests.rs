//! v0.51.3 Slice 1 durability: `ViewSinkOp`'s generalized multi-type
//! (`Int64`/`Utf8`/`Boolean`/`Float64`) row encoding must persist and decode
//! correctly across a reconnect / new `ShardDb` handle against the same
//! MinIO (S3) backend.

use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use rockstream_ops::sink::{read_view_output, ColumnValue, ViewSinkOp};
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
const MINIO_BUCKET: &str = "rockstream-test-sink-multi-type";

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

#[tokio::test]
async fn mixed_type_view_output_persists_across_reconnect_minio() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!(
                "SKIP mixed_type_view_output_persists_across_reconnect_minio: Docker not available"
            );
            return;
        }
    };
    let op_id = OperatorId(7);
    let db_path = "sink-multi-type-reconnect";

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("score", DataType::Float64, false),
    ]));

    // ── Phase 1: write, flush ────────────────────────────────────────────
    {
        let db = open_shard_db_minio(port, db_path).await;
        let sink = ViewSinkOp::new(db.clone(), op_id);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(Float64Array::from(vec![1.5, -2.5])),
            ],
        )
        .unwrap();
        sink.write_next_epoch(&ArrowZSet::new(batch, vec![1, 1]))
            .await
            .unwrap();
        db.flush().await.unwrap();
    }

    // ── Phase 2: reopen against the same backend, read back ─────────────
    let db2 = open_shard_db_minio(port, db_path).await;
    let stored = read_view_output(db2.as_ref(), op_id, 4).await.unwrap();
    assert_eq!(
        stored.len(),
        2,
        "expected 2 rows to survive reconnect, got: {stored:?}"
    );

    let mut rows: Vec<(i64, String, bool, f64)> = stored
        .iter()
        .map(|(_, _, cols, _)| {
            (
                cols[0].as_i64().unwrap(),
                cols[1].as_utf8().unwrap().to_string(),
                cols[2].as_bool().unwrap(),
                cols[3].as_f64().unwrap(),
            )
        })
        .collect();
    rows.sort_by_key(|a| a.0);

    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string(), true, 1.5),
            (2, "bob".to_string(), false, -2.5),
        ],
        "mixed-type row content did not survive reconnect"
    );

    let (_, _, cols0, w0) = &stored[0];
    assert_eq!(cols0[0], ColumnValue::Int64(1));
    assert_eq!(cols0[1], ColumnValue::Utf8("alice".to_string()));
    assert_eq!(cols0[2], ColumnValue::Boolean(true));
    assert_eq!(cols0[3], ColumnValue::Float64(1.5));
    assert_eq!(*w0, 1);
}
