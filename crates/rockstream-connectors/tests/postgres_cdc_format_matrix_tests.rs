use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{CdcChange, CdcWireFormat, PgLsn, PostgresCdcSource, SourceConnector};
use rockstream_types::arrow_batch::split_weight_column;
use rockstream_types::ids::ConnectorId;

fn source(format: CdcWireFormat) -> PostgresCdcSource {
    PostgresCdcSource::new(
        ConnectorId(514),
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ])),
        format,
    )
}

fn exact_rows(source: &mut PostgresCdcSource) -> (Vec<Vec<i64>>, Vec<i64>) {
    let result = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 1024, 1024, None)
        .expect("poll succeeds");
    let (batch, weights) = split_weight_column(&result.batches[0]).expect("weighted batch");
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            column
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("int columns")
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    let rows = (0..batch.num_rows())
        .map(|row| columns.iter().map(|column| column[row]).collect())
        .collect();
    (rows, weights)
}

#[test]
fn pgoutput_insert_matches_batch_oracle() {
    let mut source = source(CdcWireFormat::PgOutput);
    source
        .decode_and_enqueue(b"B|0/10|7|I|order-1|1,100")
        .expect("pgoutput insert decodes");
    assert_eq!(exact_rows(&mut source), (vec![vec![1, 100]], vec![1]));
}

#[test]
fn pgoutput_update_retracts_and_reinserts_same_row_id() {
    let mut source = source(CdcWireFormat::PgOutput);
    source
        .decode_and_enqueue(b"B|0/20|7|U|order-1|1,100|1,125")
        .expect("pgoutput update decodes");
    let expected_id = CdcChange::row_id_for(7, b"order-1");
    let change = source.queued_changes().next().expect("queued change");
    assert_eq!(change.row_id, expected_id);
    assert_eq!(
        exact_rows(&mut source),
        (vec![vec![1, 100], vec![1, 125]], vec![-1, 1])
    );
}

#[test]
fn pgoutput_delete_retracts_keyed_row() {
    let mut source = source(CdcWireFormat::PgOutput);
    source
        .decode_and_enqueue(b"B|0/30|7|D|order-1|1,125")
        .expect("pgoutput delete decodes");
    assert_eq!(exact_rows(&mut source), (vec![vec![1, 125]], vec![-1]));
}

#[test]
fn wal2json_insert_matches_batch_oracle() {
    let mut source = source(CdcWireFormat::Wal2Json);
    source
        .decode_and_enqueue(
            br#"{"lsn":"0/10","table_id":7,"op":"insert","key":"order-1","new":[1,100]}"#,
        )
        .expect("wal2json insert decodes");
    assert_eq!(exact_rows(&mut source), (vec![vec![1, 100]], vec![1]));
}

#[test]
fn wal2json_update_retracts_and_reinserts_same_row_id() {
    let mut source = source(CdcWireFormat::Wal2Json);
    source
        .decode_and_enqueue(br#"{"lsn":"0/20","table_id":7,"op":"update","key":"order-1","old":[1,100],"new":[1,125]}"#)
        .expect("wal2json update decodes");
    assert_eq!(
        exact_rows(&mut source),
        (vec![vec![1, 100], vec![1, 125]], vec![-1, 1])
    );
}

#[test]
fn wal2json_delete_retracts_keyed_row() {
    let mut source = source(CdcWireFormat::Wal2Json);
    source
        .decode_and_enqueue(
            br#"{"lsn":"0/30","table_id":7,"op":"delete","key":"order-1","old":[1,125]}"#,
        )
        .expect("wal2json delete decodes");
    assert_eq!(exact_rows(&mut source), (vec![vec![1, 125]], vec![-1]));
}

#[test]
fn pgoutput_insert_produces_exact_positive_zset_delta() {
    pgoutput_insert_matches_batch_oracle();
}

#[test]
fn pgoutput_update_produces_retract_and_insert_zset_delta() {
    pgoutput_update_retracts_and_reinserts_same_row_id();
}

#[test]
fn pgoutput_delete_produces_exact_negative_zset_delta() {
    pgoutput_delete_retracts_keyed_row();
}

#[test]
fn wal2json_insert_produces_exact_positive_zset_delta() {
    wal2json_insert_matches_batch_oracle();
}

#[test]
fn wal2json_update_produces_retract_and_insert_zset_delta() {
    wal2json_update_retracts_and_reinserts_same_row_id();
}

#[test]
fn wal2json_delete_produces_exact_negative_zset_delta() {
    wal2json_delete_retracts_keyed_row();
}

#[test]
fn zero_credit_poll_does_not_consume_socket_buffer() {
    let mut source = source(CdcWireFormat::PgOutput);
    source
        .decode_and_enqueue(b"B|0/10|7|I|order-1|1,100")
        .expect("record queues");
    let result = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 1024, 0, None)
        .expect("zero-credit poll pauses socket reads");
    assert!(result.batches.is_empty());
    assert_eq!(source.buffered_records(), 1);
}
