//! v0.52 Slice 2 — Connector Decode Failure Quarantine & DLQ Entry Tests.

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use rockstream_connectors::postgres_cdc::{CdcWireFormat, PostgresCdcSource};
use rockstream_types::dlq::get_global_dlq;
use rockstream_types::ids::ConnectorId;

#[test]
fn test_postgres_cdc_decode_failure_quarantines_to_dlq() {
    get_global_dlq().lock().clear();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Utf8, true),
    ]));

    let mut source = PostgresCdcSource::new(ConnectorId(1), schema, CdcWireFormat::Wal2Json);

    let invalid_payload = b"not_valid_wal2json_json";
    let res = source.decode_and_enqueue(invalid_payload);
    assert!(
        res.is_ok(),
        "decode failure should be caught and quarantined"
    );

    let dlq = get_global_dlq().lock();
    assert_eq!(dlq.len(), 1);
    let entry = &dlq[0];
    assert_eq!(entry.source_name, "postgres_cdc");
    assert_eq!(entry.error_code, "RS-1003");
    assert!(!entry.raw_bytes_hex.is_empty());

    drop(dlq);
    get_global_dlq().lock().clear();
}
