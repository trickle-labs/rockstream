use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{
    CdcWireFormat, PgLsn, PostgresCdcSource, SourceConnector, SourceError,
};
use rockstream_types::ids::ConnectorId;

#[tokio::test]
async fn pgoutput_begin_commit_envelope_has_exact_xid_lsn_and_rows() {
    let mut source = PostgresCdcSource::new(
        ConnectorId(522),
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        CdcWireFormat::PgOutput,
    );
    source.decode_and_enqueue(b"BEGIN|52").unwrap();
    source.decode_and_enqueue(b"B|0/10|7|I|one|1").unwrap();
    assert_eq!(source.buffered_records(), 0);
    source.decode_and_enqueue(b"COMMIT|0/20").unwrap();
    assert_eq!(
        source.last_decoded_envelope(),
        Some(&rockstream_connectors::CdcTransactionEnvelope {
            xid: 52,
            end_lsn: PgLsn(0x20),
            changes: vec![rockstream_connectors::CdcChange {
                lsn: PgLsn(0x10),
                table_id: 7,
                primary_key: b"one".to_vec(),
                row_id: rockstream_connectors::CdcChange::row_id_for(7, b"one"),
                operation: rockstream_connectors::CdcOperation::Insert,
                old_values: None,
                new_values: Some(vec![1]),
            }],
        })
    );
    let delta = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 1024, 1024, None)
        .await
        .unwrap();
    assert_eq!(delta.new_offset, PgLsn(0x20).to_offset_token());
    assert_eq!(
        delta.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1],
    );
}

#[tokio::test]
async fn cdc_tx_mixed_two_tables_exact_atomic_batch_oracle() {
    let mut source = PostgresCdcSource::new(
        ConnectorId(523),
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        CdcWireFormat::PgOutput,
    );
    source.decode_and_enqueue(b"BEGIN|53").unwrap();
    source.decode_and_enqueue(b"B|0/30|7|I|one|1").unwrap();
    source.decode_and_enqueue(b"B|0/31|8|I|two|2").unwrap();
    source.decode_and_enqueue(b"COMMIT|0/32").unwrap();
    assert_eq!(
        source
            .last_decoded_envelope()
            .unwrap()
            .changes
            .iter()
            .map(|change| (change.table_id, change.new_values.clone()))
            .collect::<Vec<_>>(),
        vec![(7, Some(vec![1])), (8, Some(vec![2]))],
    );
    let error = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 1024, 1, None)
        .await
        .unwrap_err();
    assert_eq!(
        error,
        SourceError::PollDeltaFailed {
            reason: "[RS-4014] pgoutput transaction exceeds poll credits or byte budget; replication remains paused. Next steps: raise the source epoch budget".to_string(),
        }
    );
    let delta = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 1024, 2, None)
        .await
        .unwrap();
    assert_eq!(delta.new_offset, PgLsn(0x32).to_offset_token());
    assert_eq!(
        delta.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 2],
    );
}
