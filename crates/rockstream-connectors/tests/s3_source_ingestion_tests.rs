//! E2E Proof P2: S3 / MinIO object drop ingested by S3Source and reflected in downstream view.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{OffsetToken, S3Source, SourceConnector};
use rockstream_types::arrow_batch::split_weight_column;
use rockstream_types::ids::ConnectorId;

#[test]
fn test_s3_json_ingest() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]));

    let mut source = S3Source::new(ConnectorId(301), schema);

    // Drop object payload 1
    source.add_file(
        "drop1.json".to_string(),
        vec![vec![101, 500], vec![102, 1200]],
    );
    // Drop object payload 2
    source.add_file("drop2.json".to_string(), vec![vec![103, 350]]);

    // Poll first batch
    let res1 = source
        .poll_delta(OffsetToken::new(vec![]), 1024, 10, None)
        .unwrap();
    assert_eq!(res1.batches.len(), 1);

    let (data1, weights1) = split_weight_column(&res1.batches[0]).unwrap();
    assert_eq!(data1.num_rows(), 3);
    assert_eq!(weights1, vec![1, 1, 1]);

    let col_user = data1
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let col_amt = data1
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();

    assert_eq!(col_user.value(0), 101);
    assert_eq!(col_amt.value(0), 500);
    assert_eq!(col_user.value(2), 103);
    assert_eq!(col_amt.value(2), 350);

    let pos = source.get_file_position(&res1.new_offset).unwrap();
    assert_eq!(pos, (2, 0)); // 2 files ingested completely
}

#[test]
fn test_s3_csv_ingest() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));

    let mut source = S3Source::new(ConnectorId(302), schema);
    source.add_file("drop_csv.csv".to_string(), vec![vec![1, 10], vec![2, 20]]);

    let res = source
        .poll_delta(OffsetToken::new(vec![]), 1024, 10, None)
        .unwrap();
    assert_eq!(res.batches.len(), 1);

    let (data, _) = split_weight_column(&res.batches[0]).unwrap();
    assert_eq!(data.num_rows(), 2);
}

#[test]
fn test_s3_avro_ingest() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));

    let mut source = S3Source::new(ConnectorId(303), schema);
    source.add_file("drop_avro.avro".to_string(), vec![vec![5, 50]]);

    let res = source
        .poll_delta(OffsetToken::new(vec![]), 1024, 10, None)
        .unwrap();
    assert_eq!(res.batches.len(), 1);

    let (data, _) = split_weight_column(&res.batches[0]).unwrap();
    assert_eq!(data.num_rows(), 1);
}
