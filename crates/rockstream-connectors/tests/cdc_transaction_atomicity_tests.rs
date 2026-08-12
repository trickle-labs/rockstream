use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{
    CdcChange, CdcOperation, CdcTransactionEnvelope, CdcWireFormat, PgLsn, PostgresCdcSource,
    SourceConnector, SourceError, POSTGRES_CDC_MAX_TRANSACTION_BYTES,
};
use rockstream_types::arrow_batch::split_weight_column;
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

#[test]
fn cdc_tx_transaction_buffer_full_returns_coded_backpressure() {
    let mut source = PostgresCdcSource::new(
        ConnectorId(524),
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        CdcWireFormat::PgOutput,
    );
    let error = source
        .enqueue_envelope(
            CdcTransactionEnvelope {
                xid: 54,
                end_lsn: PgLsn(0x40),
                changes: vec![CdcChange {
                    lsn: PgLsn(0x40),
                    table_id: 7,
                    primary_key: b"one".to_vec(),
                    row_id: CdcChange::row_id_for(7, b"one"),
                    operation: CdcOperation::Insert,
                    old_values: None,
                    new_values: Some(vec![1]),
                }],
            },
            POSTGRES_CDC_MAX_TRANSACTION_BYTES + 1,
        )
        .unwrap_err();
    assert_eq!(
        error,
        SourceError::PollDeltaFailed {
            reason: "[RS-4014] pgoutput transaction exceeds POSTGRES_CDC_MAX_TRANSACTION_BYTES; replication is paused. Next steps: increase the bound or reduce upstream transaction size".to_string(),
        }
    );
    assert_eq!(
        (source.buffered_records(), source.replication_read_paused()),
        (0, true),
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

async fn assert_complete_transaction(
    xid: u32,
    frames: &[&[u8]],
    expected_changes: Vec<(u32, CdcOperation)>,
    expected_values: Vec<i64>,
    expected_weights: Vec<i64>,
) {
    let mut source = PostgresCdcSource::new(
        ConnectorId(52_200 + xid as u64),
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        CdcWireFormat::PgOutput,
    );
    source
        .decode_and_enqueue(format!("BEGIN|{xid}").as_bytes())
        .unwrap();
    for frame in frames {
        source.decode_and_enqueue(frame).unwrap();
    }
    source.decode_and_enqueue(b"COMMIT|0/52").unwrap();
    let delta = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 1024, 1024, None)
        .await
        .unwrap();
    let (batch, weights) = split_weight_column(&delta.batches[0]).unwrap();
    assert_eq!(
        (
            source
                .last_decoded_envelope()
                .unwrap()
                .changes
                .iter()
                .map(|change| (change.table_id, change.operation))
                .collect::<Vec<_>>(),
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec(),
            weights,
            delta.new_offset,
        ),
        (
            expected_changes,
            expected_values,
            expected_weights,
            PgLsn(0x52).to_offset_token(),
        )
    );
}

#[tokio::test]
async fn cdc_tx_insert_two_tables_exact_atomic_batch_oracle() {
    assert_complete_transaction(
        201,
        &[b"B|0/10|7|I|one|1", b"B|0/11|8|I|two|2"],
        vec![(7, CdcOperation::Insert), (8, CdcOperation::Insert)],
        vec![1, 2],
        vec![1, 1],
    )
    .await;
}

#[tokio::test]
async fn cdc_tx_update_two_tables_exact_atomic_batch_oracle() {
    assert_complete_transaction(
        202,
        &[b"B|0/20|7|U|one|1|2", b"B|0/21|8|U|two|3|4"],
        vec![(7, CdcOperation::Update), (8, CdcOperation::Update)],
        vec![1, 2, 3, 4],
        vec![-1, 1, -1, 1],
    )
    .await;
}

#[tokio::test]
async fn cdc_tx_delete_two_tables_exact_atomic_batch_oracle() {
    assert_complete_transaction(
        203,
        &[b"B|0/30|7|D|one|1", b"B|0/31|8|D|two|2"],
        vec![(7, CdcOperation::Delete), (8, CdcOperation::Delete)],
        vec![1, 2],
        vec![-1, -1],
    )
    .await;
}

#[tokio::test]
async fn cdc_tx_mixed_three_tables_exact_atomic_batch_oracle() {
    assert_complete_transaction(
        204,
        &[
            b"B|0/40|7|I|one|1",
            b"B|0/41|8|U|two|2|3",
            b"B|0/42|9|D|three|4",
        ],
        vec![
            (7, CdcOperation::Insert),
            (8, CdcOperation::Update),
            (9, CdcOperation::Delete),
        ],
        vec![1, 2, 3, 4],
        vec![1, -1, 1, -1],
    )
    .await;
}
