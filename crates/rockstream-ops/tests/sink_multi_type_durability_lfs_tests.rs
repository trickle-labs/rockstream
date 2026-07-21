//! v0.51.3 Slice 1 durability: `ViewSinkOp`'s generalized multi-type
//! (`Int64`/`Utf8`/`Boolean`/`Float64`) row encoding must persist and decode
//! correctly across a reconnect / new `ShardDb` handle against the same LFS
//! backend.

use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::local::LocalFileSystem;
use rockstream_ops::sink::{read_view_output, ColumnValue, ViewSinkOp};
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use tempfile::TempDir;

async fn open_shard_db(dir: &TempDir) -> Arc<ShardDb> {
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    Arc::new(ShardDb::builder("shard", store).build().await.unwrap())
}

#[tokio::test]
async fn mixed_type_view_output_persists_across_reconnect_lfs() {
    let dir = TempDir::new().unwrap();
    let op_id = OperatorId(7);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("score", DataType::Float64, false),
    ]));

    // ── Phase 1: write, flush, close ────────────────────────────────────
    {
        let db = open_shard_db(&dir).await;
        let sink = ViewSinkOp::new(db.clone(), op_id);

        let batch = RecordBatch::try_new(
            schema.clone(),
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

        drop(sink);
        Arc::try_unwrap(db)
            .ok()
            .expect("db has a single owner after dropping sink")
            .close()
            .await
            .unwrap();
    }

    // ── Phase 2: reopen against the same backend, read back ─────────────
    let db2 = open_shard_db(&dir).await;
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

    // Also verify decoded ColumnValue equality directly (byte-identical
    // round trip, not just field-by-field comparison).
    let (_, _, cols0, w0) = &stored[0];
    assert_eq!(cols0[0], ColumnValue::Int64(1));
    assert_eq!(cols0[1], ColumnValue::Utf8("alice".to_string()));
    assert_eq!(cols0[2], ColumnValue::Boolean(true));
    assert_eq!(cols0[3], ColumnValue::Float64(1.5));
    assert_eq!(*w0, 1);
}
