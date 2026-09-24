use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{
    decode_pgoutput_event, CdcChange, CdcWireFormat, PgLsn, PgOutputEvent, PostgresCdcSource,
    SourceConnector,
};
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

async fn exact_rows(source: &mut PostgresCdcSource) -> (Vec<Vec<i64>>, Vec<i64>) {
    let result = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 1024, 1024, None)
        .await
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

async fn do_pgoutput_insert_matches_batch_oracle() {
    let mut source = source(CdcWireFormat::PgOutput);
    source
        .decode_and_enqueue(b"B|0/10|7|I|order-1|1,100")
        .expect("pgoutput insert decodes");
    assert_eq!(exact_rows(&mut source).await, (vec![vec![1, 100]], vec![1]));
}

#[tokio::test]
async fn pgoutput_insert_matches_batch_oracle() {
    do_pgoutput_insert_matches_batch_oracle().await;
}

async fn do_pgoutput_update_retracts_and_reinserts_same_row_id() {
    let mut source = source(CdcWireFormat::PgOutput);
    source
        .decode_and_enqueue(b"B|0/20|7|U|order-1|1,100|1,125")
        .expect("pgoutput update decodes");
    let expected_id = CdcChange::row_id_for(7, b"order-1");
    let change = source.queued_changes().next().expect("queued change");
    assert_eq!(change.row_id, expected_id);
    assert_eq!(
        exact_rows(&mut source).await,
        (vec![vec![1, 100], vec![1, 125]], vec![-1, 1])
    );
}

#[tokio::test]
async fn pgoutput_update_retracts_and_reinserts_same_row_id() {
    do_pgoutput_update_retracts_and_reinserts_same_row_id().await;
}

async fn do_pgoutput_delete_retracts_keyed_row() {
    let mut source = source(CdcWireFormat::PgOutput);
    source
        .decode_and_enqueue(b"B|0/30|7|D|order-1|1,125")
        .expect("pgoutput delete decodes");
    assert_eq!(
        exact_rows(&mut source).await,
        (vec![vec![1, 125]], vec![-1])
    );
}

#[tokio::test]
async fn pgoutput_delete_retracts_keyed_row() {
    do_pgoutput_delete_retracts_keyed_row().await;
}

async fn do_wal2json_insert_matches_batch_oracle() {
    let mut source = source(CdcWireFormat::Wal2Json);
    source
        .decode_and_enqueue(
            br#"{"lsn":"0/10","table_id":7,"op":"insert","key":"order-1","new":[1,100]}"#,
        )
        .expect("wal2json insert decodes");
    assert_eq!(exact_rows(&mut source).await, (vec![vec![1, 100]], vec![1]));
}

#[tokio::test]
async fn wal2json_insert_matches_batch_oracle() {
    do_wal2json_insert_matches_batch_oracle().await;
}

async fn do_wal2json_update_retracts_and_reinserts_same_row_id() {
    let mut source = source(CdcWireFormat::Wal2Json);
    source
        .decode_and_enqueue(br#"{"lsn":"0/20","table_id":7,"op":"update","key":"order-1","old":[1,100],"new":[1,125]}"#)
        .expect("wal2json update decodes");
    assert_eq!(
        exact_rows(&mut source).await,
        (vec![vec![1, 100], vec![1, 125]], vec![-1, 1])
    );
}

#[tokio::test]
async fn wal2json_update_retracts_and_reinserts_same_row_id() {
    do_wal2json_update_retracts_and_reinserts_same_row_id().await;
}

async fn do_wal2json_delete_retracts_keyed_row() {
    let mut source = source(CdcWireFormat::Wal2Json);
    source
        .decode_and_enqueue(
            br#"{"lsn":"0/30","table_id":7,"op":"delete","key":"order-1","old":[1,125]}"#,
        )
        .expect("wal2json delete decodes");
    assert_eq!(
        exact_rows(&mut source).await,
        (vec![vec![1, 125]], vec![-1])
    );
}

#[tokio::test]
async fn wal2json_delete_retracts_keyed_row() {
    do_wal2json_delete_retracts_keyed_row().await;
}

#[tokio::test]
async fn pgoutput_insert_produces_exact_positive_zset_delta() {
    do_pgoutput_insert_matches_batch_oracle().await;
}

#[tokio::test]
async fn pgoutput_update_produces_retract_and_insert_zset_delta() {
    do_pgoutput_update_retracts_and_reinserts_same_row_id().await;
}

#[tokio::test]
async fn pgoutput_delete_produces_exact_negative_zset_delta() {
    do_pgoutput_delete_retracts_keyed_row().await;
}

#[tokio::test]
async fn wal2json_insert_produces_exact_positive_zset_delta() {
    do_wal2json_insert_matches_batch_oracle().await;
}

#[tokio::test]
async fn wal2json_update_produces_retract_and_insert_zset_delta() {
    do_wal2json_update_retracts_and_reinserts_same_row_id().await;
}

#[tokio::test]
async fn wal2json_delete_produces_exact_negative_zset_delta() {
    do_wal2json_delete_retracts_keyed_row().await;
}

#[tokio::test]
async fn zero_credit_poll_does_not_consume_socket_buffer() {
    let mut source = source(CdcWireFormat::PgOutput);
    source
        .decode_and_enqueue(b"B|0/10|7|I|order-1|1,100")
        .expect("record queues");
    let result = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 1024, 0, None)
        .await
        .expect("zero-credit poll pauses socket reads");
    assert!(result.batches.is_empty());
    assert_eq!(source.buffered_records(), 1);
}

fn make_begin_frame(xid: u32) -> Vec<u8> {
    let mut frame = vec![b'B'];
    frame.extend_from_slice(&[0u8; 16]);
    frame.extend_from_slice(&xid.to_be_bytes());
    frame
}

fn make_commit_frame(end_lsn: u64) -> Vec<u8> {
    let mut frame = vec![b'C'];
    frame.push(0);
    frame.extend_from_slice(&[0u8; 8]);
    frame.extend_from_slice(&end_lsn.to_be_bytes());
    frame.extend_from_slice(&[0u8; 8]);
    frame
}

fn make_relation_frame(
    relation_id: u32,
    namespace: &str,
    name: &str,
    replica_identity: u8,
    columns: &[(&str, u32, i32, u8)],
) -> Vec<u8> {
    let mut frame = vec![b'R'];
    frame.extend_from_slice(&relation_id.to_be_bytes());
    frame.extend_from_slice(namespace.as_bytes());
    frame.push(0);
    frame.extend_from_slice(name.as_bytes());
    frame.push(0);
    frame.push(replica_identity);
    frame.extend_from_slice(&(columns.len() as u16).to_be_bytes());
    for (col_name, oid, modifier, flags) in columns {
        frame.push(*flags);
        frame.extend_from_slice(col_name.as_bytes());
        frame.push(0);
        frame.extend_from_slice(&oid.to_be_bytes());
        frame.extend_from_slice(&(*modifier as u32).to_be_bytes());
    }
    frame
}

fn make_insert_frame(relation_id: u32, values: &[Option<&str>]) -> Vec<u8> {
    let mut frame = vec![b'I'];
    frame.extend_from_slice(&relation_id.to_be_bytes());
    frame.push(b'N');
    frame.extend_from_slice(&(values.len() as u16).to_be_bytes());
    for val in values {
        match val {
            None => frame.push(b'n'),
            Some(s) => {
                frame.push(b't');
                frame.extend_from_slice(&(s.len() as i32).to_be_bytes());
                frame.extend_from_slice(s.as_bytes());
            }
        }
    }
    frame
}

fn decode_test_tuple(values: &[Option<&str>]) -> Vec<Option<String>> {
    let mut xid = None;
    let begin = make_begin_frame(100);
    decode_pgoutput_event(&mut xid, &begin).expect("begin decodes");
    assert_eq!(xid, Some(100));

    let insert = make_insert_frame(42, values);
    let event = decode_pgoutput_event(&mut xid, &insert).expect("insert decodes");
    let decoded_values = match event {
        PgOutputEvent::Insert { new_values, .. } => new_values,
        other => panic!("expected Insert event, got {:?}", other),
    };

    let commit = make_commit_frame(2048);
    decode_pgoutput_event(&mut xid, &commit).expect("commit decodes");
    assert_eq!(xid, None);

    decoded_values
}

#[test]
fn test_pgoutput_decoder_preserves_exact_column_types_and_nulls() {
    let mut xid = None;
    let begin = make_begin_frame(42);
    let begin_event = decode_pgoutput_event(&mut xid, &begin).expect("begin decodes");
    assert_eq!(begin_event, PgOutputEvent::Begin { xid: 42 });

    let rel_frame = make_relation_frame(
        101,
        "public",
        "orders",
        b'f',
        &[
            ("id", 23, -1, 1),
            ("name", 25, -1, 0),
            ("price", 1700, -1, 0),
        ],
    );
    let rel_event = decode_pgoutput_event(&mut xid, &rel_frame).expect("relation decodes");
    match rel_event {
        PgOutputEvent::Relation {
            xid: rxid,
            relation,
        } => {
            assert_eq!(rxid, 42);
            assert_eq!(relation.relation_id, 101);
            assert_eq!(relation.namespace, "public");
            assert_eq!(relation.name, "orders");
            assert_eq!(relation.columns.len(), 3);
            assert_eq!(relation.columns[0].type_oid, 23);
            assert_eq!(relation.columns[1].type_oid, 25);
            assert_eq!(relation.columns[2].type_oid, 1700);
        }
        other => panic!("expected Relation event, got {:?}", other),
    }

    let insert_frame = make_insert_frame(101, &[Some("1"), Some("laptop"), None]);
    let insert_event = decode_pgoutput_event(&mut xid, &insert_frame).expect("insert decodes");
    match insert_event {
        PgOutputEvent::Insert {
            xid: ixid,
            relation_id,
            new_values,
        } => {
            assert_eq!(ixid, 42);
            assert_eq!(relation_id, 101);
            assert_eq!(
                new_values,
                vec![Some("1".to_string()), Some("laptop".to_string()), None]
            );
        }
        other => panic!("expected Insert event, got {:?}", other),
    }

    let commit_frame = make_commit_frame(1024);
    let commit_event = decode_pgoutput_event(&mut xid, &commit_frame).expect("commit decodes");
    assert_eq!(
        commit_event,
        PgOutputEvent::Commit {
            xid: 42,
            commit_lsn: PgLsn(1024)
        }
    );
    assert_eq!(xid, None);
}

#[test]
fn test_cdc_type_int2_exact() {
    let values = decode_test_tuple(&[Some("-32768"), Some("32767"), None]);
    assert_eq!(
        values,
        vec![Some("-32768".to_string()), Some("32767".to_string()), None]
    );
}

#[test]
fn test_cdc_type_int4_exact() {
    let values = decode_test_tuple(&[Some("-2147483648"), Some("2147483647"), None]);
    assert_eq!(
        values,
        vec![
            Some("-2147483648".to_string()),
            Some("2147483647".to_string()),
            None
        ]
    );
}

#[test]
fn test_cdc_type_int8_exact() {
    let values = decode_test_tuple(&[
        Some("-9223372036854775808"),
        Some("9223372036854775807"),
        Some("0"),
        None,
    ]);
    assert_eq!(
        values,
        vec![
            Some("-9223372036854775808".to_string()),
            Some("9223372036854775807".to_string()),
            Some("0".to_string()),
            None,
        ]
    );
}

#[test]
fn test_cdc_type_float4_exact() {
    let values = decode_test_tuple(&[
        Some("3.14159"),
        Some("-0.0"),
        Some("NaN"),
        Some("+Inf"),
        None,
    ]);
    assert_eq!(
        values,
        vec![
            Some("3.14159".to_string()),
            Some("-0.0".to_string()),
            Some("NaN".to_string()),
            Some("+Inf".to_string()),
            None,
        ]
    );
}

#[test]
fn test_cdc_type_float8_exact() {
    let values = decode_test_tuple(&[Some("1.7976931348623157e308"), None]);
    assert_eq!(
        values,
        vec![Some("1.7976931348623157e308".to_string()), None]
    );
}

#[test]
fn test_cdc_type_numeric_exact() {
    let values = decode_test_tuple(&[Some("123456789.9876"), Some("-0.0001"), None]);
    assert_eq!(
        values,
        vec![
            Some("123456789.9876".to_string()),
            Some("-0.0001".to_string()),
            None,
        ]
    );
}

#[test]
fn test_cdc_type_text_exact() {
    let values = decode_test_tuple(&[Some(""), Some("RockStream 🚀 数据库"), None]);
    assert_eq!(
        values,
        vec![
            Some("".to_string()),
            Some("RockStream 🚀 数据库".to_string()),
            None,
        ]
    );
}

#[test]
fn test_cdc_type_boolean_exact() {
    let values = decode_test_tuple(&[Some("t"), Some("f"), None]);
    assert_eq!(
        values,
        vec![Some("t".to_string()), Some("f".to_string()), None]
    );
}

#[test]
fn test_cdc_type_date_exact() {
    let values = decode_test_tuple(&[Some("1970-01-01"), Some("2026-09-23"), None]);
    assert_eq!(
        values,
        vec![
            Some("1970-01-01".to_string()),
            Some("2026-09-23".to_string()),
            None,
        ]
    );
}

#[test]
fn test_cdc_type_timestamp_exact() {
    let values = decode_test_tuple(&[Some("2026-09-24 00:07:48.123456+00"), None]);
    assert_eq!(
        values,
        vec![Some("2026-09-24 00:07:48.123456+00".to_string()), None]
    );
}

#[test]
fn test_cdc_type_bytea_exact() {
    let values = decode_test_tuple(&[Some(r"\x00010203ff"), Some(r"\x00"), None]);
    assert_eq!(
        values,
        vec![
            Some(r"\x00010203ff".to_string()),
            Some(r"\x00".to_string()),
            None,
        ]
    );
}
