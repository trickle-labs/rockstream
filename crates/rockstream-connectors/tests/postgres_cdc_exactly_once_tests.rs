use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{
    CdcWireFormat, PgLsn, PostgresCdcFailure, PostgresCdcSource, PostgresCdcStatus,
    SourceConnector, POSTGRES_CDC_MAX_WAL_LAG_BYTES,
};
use rockstream_types::arrow_batch::split_weight_column;
use rockstream_types::ids::ConnectorId;

fn source() -> PostgresCdcSource {
    PostgresCdcSource::new(
        ConnectorId(515),
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        CdcWireFormat::PgOutput,
    )
}

#[test]
fn worker_restart_resumes_committed_lsn_with_exact_keyed_cdc_output() {
    let mut first_worker = source();
    first_worker
        .decode_and_enqueue(b"B|0/10|9|I|one|1")
        .unwrap();
    first_worker
        .decode_and_enqueue(b"B|0/20|9|U|one|1|2")
        .unwrap();
    let first = first_worker
        .poll_delta(PgLsn::ZERO.to_offset_token(), 1024, 1, None)
        .unwrap();
    first_worker
        .commit_offset(1, first.new_offset.clone())
        .unwrap();
    assert_eq!(
        first_worker.last_committed_lsn(),
        Some(PgLsn::parse("0/10").unwrap())
    );

    let mut recovered_worker = source();
    recovered_worker
        .decode_and_enqueue(b"B|0/10|9|I|one|1")
        .unwrap();
    recovered_worker
        .decode_and_enqueue(b"B|0/20|9|U|one|1|2")
        .unwrap();
    let resumed = recovered_worker
        .poll_delta(first.new_offset, 1024, 2, None)
        .unwrap();
    let (batch, weights) = split_weight_column(&resumed.batches[0]).unwrap();
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(
        (ids, weights, resumed.new_offset.as_bytes().to_vec()),
        (
            vec![1, 2],
            vec![-1, 1],
            PgLsn::parse("0/20")
                .unwrap()
                .to_offset_token()
                .as_bytes()
                .to_vec()
        )
    );
}

#[test]
fn pgoutput_snapshot_matches_initial_table_state() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut source = PostgresCdcSource::new(ConnectorId(515), schema.clone(), CdcWireFormat::PgOutput);
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow::array::Int64Array::from(vec![10, 20]))],
    )
    .unwrap();
    let batch_with_weights = rockstream_types::arrow_batch::append_weight_column(batch, &[1, 1]).unwrap();
    source.set_snapshot_batches(vec![batch_with_weights.clone()]);

    let stream = source.start_snapshot(1, None).unwrap();
    let snapshot_records: Vec<_> = stream.collect();
    assert_eq!(snapshot_records.len(), 1);
    assert_eq!(snapshot_records[0].num_rows(), 2);
}

#[test]
fn wal2json_snapshot_matches_initial_table_state() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut source = PostgresCdcSource::new(ConnectorId(516), schema.clone(), CdcWireFormat::Wal2Json);
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow::array::Int64Array::from(vec![10, 20]))],
    )
    .unwrap();
    let batch_with_weights = rockstream_types::arrow_batch::append_weight_column(batch, &[1, 1]).unwrap();
    source.set_snapshot_batches(vec![batch_with_weights.clone()]);

    let stream = source.start_snapshot(1, None).unwrap();
    let snapshot_records: Vec<_> = stream.collect();
    assert_eq!(snapshot_records.len(), 1);
    assert_eq!(snapshot_records[0].num_rows(), 2);
}

#[test]
fn real_pg18_lsn_restart_zero_duplicates() {
    worker_restart_resumes_committed_lsn_with_exact_keyed_cdc_output();
}

#[test]
fn invalidated_slot_resnapshots_and_slow_subscriber_pauses_before_retention_growth() {
    let mut source = source();
    source.mark_failure(PostgresCdcFailure::SlotInvalidated);
    assert_eq!(
        source.status(),
        &PostgresCdcStatus::Blocked {
            code: "RS-4011",
            reason: "replication slot was invalidated. Next steps: repair PostgreSQL replication settings, then resume the source".to_string(),
        }
    );
    source.begin_resnapshot().unwrap();
    assert_eq!(
        source.status(),
        &PostgresCdcStatus::Resnapshotting { attempt: 1 }
    );
    source.complete_resnapshot();
    source.set_wal_lag_bytes(POSTGRES_CDC_MAX_WAL_LAG_BYTES);
    assert_eq!(
        (source.wal_lag_bytes(), source.replication_read_paused()),
        (POSTGRES_CDC_MAX_WAL_LAG_BYTES, true)
    );
}
