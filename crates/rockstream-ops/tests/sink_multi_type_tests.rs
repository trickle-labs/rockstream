//! v0.51.3 Slice 1: `ViewSinkOp`'s row encoding must serve mixed-type
//! columns (`Int64`, `Utf8`, `Boolean`, `Float64`), not just `Int64`.
//!
//! Every existing gateway view test mixes `BIGINT` and `TEXT` columns in one
//! view (e.g. `mv_immediate_population_durability_lfs_tests.rs`), so this is
//! a hard, in-Scope blocker for the rest of the plan, not an enhancement.

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
async fn write_and_read_back_mixed_int64_utf8_row() {
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let sink = ViewSinkOp::new(db.clone(), OperatorId(1));

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let id_col = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let name_col = Arc::new(StringArray::from(vec!["alice", "bob", "carol"]));
    let batch = RecordBatch::try_new(schema, vec![id_col, name_col]).unwrap();
    let zset = ArrowZSet::new(batch, vec![1, 1, 1]);

    sink.write_next_epoch(&zset).await.unwrap();
    db.flush().await.unwrap();

    let stored = read_view_output(db.as_ref(), OperatorId(1), 2)
        .await
        .unwrap();
    assert_eq!(stored.len(), 3, "expected 3 rows, got: {stored:?}");

    let mut rows: Vec<(i64, String, i64)> = stored
        .iter()
        .map(|(_, _, cols, w)| {
            (
                cols[0].as_i64().expect("column 0 must decode as Int64"),
                cols[1]
                    .as_utf8()
                    .expect("column 1 must decode as Utf8")
                    .to_string(),
                *w,
            )
        })
        .collect();
    rows.sort();

    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string(), 1),
            (2, "bob".to_string(), 1),
            (3, "carol".to_string(), 1),
        ],
        "round-tripped mixed Int64/Utf8 row content mismatch"
    );
}

#[tokio::test]
async fn write_and_read_back_boolean_and_float64_row() {
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let sink = ViewSinkOp::new(db.clone(), OperatorId(2));

    let schema = Arc::new(Schema::new(vec![
        Field::new("active", DataType::Boolean, false),
        Field::new("score", DataType::Float64, false),
    ]));
    let active_col = Arc::new(BooleanArray::from(vec![true, false]));
    let score_col = Arc::new(Float64Array::from(vec![3.5, -1.25]));
    let batch = RecordBatch::try_new(schema, vec![active_col, score_col]).unwrap();
    let zset = ArrowZSet::new(batch, vec![1, -1]);

    sink.write_next_epoch(&zset).await.unwrap();
    db.flush().await.unwrap();

    let stored = read_view_output(db.as_ref(), OperatorId(2), 2)
        .await
        .unwrap();
    assert_eq!(stored.len(), 2, "expected 2 rows, got: {stored:?}");

    let mut rows: Vec<(bool, f64, i64)> = stored
        .iter()
        .map(|(_, _, cols, w)| {
            (
                cols[0].as_bool().expect("column 0 must decode as Boolean"),
                cols[1].as_f64().expect("column 1 must decode as Float64"),
                *w,
            )
        })
        .collect();
    rows.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

    assert_eq!(
        rows,
        vec![(false, -1.25, -1), (true, 3.5, 1)],
        "round-tripped Boolean/Float64 row content mismatch"
    );
}

#[tokio::test]
async fn mixed_type_row_is_byte_identical_after_multiple_epochs() {
    // Guards against cross-epoch corruption: write two epochs with
    // different mixed-type content to the same op_id and verify both are
    // recovered exactly, keyed by (epoch, row_idx).
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let sink = ViewSinkOp::new(db.clone(), OperatorId(3));

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("ok", DataType::Boolean, false),
        Field::new("weight", DataType::Float64, false),
    ]));

    let batch0 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(StringArray::from(vec!["first"])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(Float64Array::from(vec![1.5])),
        ],
    )
    .unwrap();
    sink.write_next_epoch(&ArrowZSet::new(batch0, vec![1]))
        .await
        .unwrap();

    let batch1 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![20])),
            Arc::new(StringArray::from(vec!["second, with, commas"])),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(Float64Array::from(vec![-2.25])),
        ],
    )
    .unwrap();
    sink.write_next_epoch(&ArrowZSet::new(batch1, vec![1]))
        .await
        .unwrap();

    db.flush().await.unwrap();

    let stored = read_view_output(db.as_ref(), OperatorId(3), 4)
        .await
        .unwrap();
    assert_eq!(stored.len(), 2, "expected 2 rows, got: {stored:?}");

    let (epoch0, _, cols0, w0) = &stored[0];
    assert_eq!(*epoch0, 0);
    assert_eq!(cols0[0], ColumnValue::Int64(10));
    assert_eq!(cols0[1], ColumnValue::Utf8("first".to_string()));
    assert_eq!(cols0[2], ColumnValue::Boolean(true));
    assert_eq!(cols0[3], ColumnValue::Float64(1.5));
    assert_eq!(*w0, 1);

    let (epoch1, _, cols1, w1) = &stored[1];
    assert_eq!(*epoch1, 1);
    assert_eq!(cols1[0], ColumnValue::Int64(20));
    assert_eq!(
        cols1[1],
        ColumnValue::Utf8("second, with, commas".to_string())
    );
    assert_eq!(cols1[2], ColumnValue::Boolean(false));
    assert_eq!(cols1[3], ColumnValue::Float64(-2.25));
    assert_eq!(*w1, 1);
}

#[tokio::test]
async fn read_view_output_returns_every_row_beyond_ten_mebibytes() {
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let sink = ViewSinkOp::new(db.clone(), OperatorId(4));
    let count = 250_000_i64;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(0..count)),
            Arc::new(Int64Array::from_iter_values((0..count).map(|id| -id))),
        ],
    )
    .unwrap();
    sink.write_next_epoch(&ArrowZSet::new(batch, vec![1; count as usize]))
        .await
        .unwrap();
    db.flush().await.unwrap();

    let actual = read_view_output(db.as_ref(), OperatorId(4), 2)
        .await
        .unwrap()
        .into_iter()
        .map(|(_, _, row, weight)| (row[0].as_i64(), row[1].as_i64(), weight))
        .collect::<Vec<_>>();
    let expected = (0..count)
        .map(|id| (Some(id), Some(-id), 1))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}
