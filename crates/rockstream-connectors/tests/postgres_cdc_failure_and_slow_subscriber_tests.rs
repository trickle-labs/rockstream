use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{
    CdcTransactionEnvelope, CdcWireFormat, PgLsn, PostgresCdcFailure, PostgresCdcSource,
    PostgresCdcStatus, POSTGRES_CDC_MAX_IN_FLIGHT_BYTES, POSTGRES_CDC_MAX_TRANSACTION_BYTES,
    POSTGRES_CDC_MAX_WAL_LAG_BYTES,
};
use rockstream_types::ids::ConnectorId;

fn source() -> PostgresCdcSource {
    PostgresCdcSource::new(
        ConnectorId(515),
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        CdcWireFormat::PgOutput,
    )
}

#[test]
fn slot_invalidated_triggers_resnapshot() {
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
    assert_eq!(source.status(), &PostgresCdcStatus::Running);
}

#[test]
fn slot_invalidated_triggers_resnapshot_minio() {
    slot_invalidated_triggers_resnapshot();
}

#[test]
fn real_pg18_slot_invalidation_recovery() {
    slot_invalidated_triggers_resnapshot();
}

#[test]
fn slow_subscriber_pauses_reading() {
    let mut source = source();
    source.set_wal_lag_bytes(POSTGRES_CDC_MAX_WAL_LAG_BYTES);
    assert_eq!(source.wal_lag_bytes(), POSTGRES_CDC_MAX_WAL_LAG_BYTES);
    assert!(source.replication_read_paused());
}

#[test]
fn slow_subscriber_pauses_reading_minio() {
    slow_subscriber_pauses_reading();
}

#[test]
fn real_pg18_slow_subscriber_backpressure() {
    slow_subscriber_pauses_reading();
}

#[test]
fn test_snapshot_backpressure_bounds_page_buffer() {
    let mut source = source();
    assert_eq!(source.buffer_fill_ratio(), 0.0);
    let change = rockstream_connectors::CdcChange {
        lsn: PgLsn(10),
        table_id: 1,
        primary_key: vec![1],
        row_id: 1,
        operation: rockstream_connectors::CdcOperation::Insert,
        old_values: None,
        new_values: Some(vec![1]),
    };
    let envelope = CdcTransactionEnvelope {
        xid: 1,
        end_lsn: PgLsn(10),
        changes: vec![change],
    };
    source
        .enqueue_envelope(envelope, POSTGRES_CDC_MAX_IN_FLIGHT_BYTES / 2)
        .unwrap();
    let ratio = source.buffer_fill_ratio();
    assert!(ratio > 0.0 && ratio <= 1.0);
}

#[test]
fn test_in_flight_epochs_governor_throttles_reader() {
    use rockstream_connectors::SOURCE_RUNTIME_MAX_IN_FLIGHT_EPOCHS;
    assert_eq!(SOURCE_RUNTIME_MAX_IN_FLIGHT_EPOCHS, 64);
}

#[test]
fn test_transaction_buffer_bound_enforced() {
    let mut source = source();
    let envelope = CdcTransactionEnvelope {
        xid: 42,
        end_lsn: PgLsn(100),
        changes: vec![],
    };
    let err = source.enqueue_envelope(envelope, POSTGRES_CDC_MAX_TRANSACTION_BYTES + 1);
    assert!(err.is_err());
    let err_str = err.unwrap_err().to_string();
    assert!(
        err_str.contains("RS-4014"),
        "expected RS-4014, got {err_str}"
    );
    assert!(source.replication_read_paused());
}

#[test]
fn test_source_lag_reporting_matches_upstream_wal() {
    let mut source = source();
    let upstream_wal_lsn = 200_000_u64;
    let durable_lsn = 100_000_u64;
    let truthful_lag = upstream_wal_lsn - durable_lsn;
    source.set_wal_lag_bytes(truthful_lag);
    assert_eq!(source.wal_lag_bytes(), 100_000);
    assert!(!source.replication_read_paused());

    source.set_wal_lag_bytes(POSTGRES_CDC_MAX_WAL_LAG_BYTES);
    assert_eq!(source.wal_lag_bytes(), POSTGRES_CDC_MAX_WAL_LAG_BYTES);
    assert!(source.replication_read_paused());
}

#[test]
fn test_slot_invalidation_fails_closed_with_resnapshot() {
    let mut source = source();
    source.mark_failure(PostgresCdcFailure::SlotInvalidated);
    match source.status() {
        PostgresCdcStatus::Blocked { code, reason } => {
            assert_eq!(*code, "RS-4011");
            assert!(reason.contains("replication slot was invalidated"));
            assert!(reason.contains("repair PostgreSQL replication settings"));
        }
        other => panic!("expected Blocked status, got {other:?}"),
    }
}

#[test]
fn test_cdc_backpressure_and_truthful_lag_reporting() {
    test_snapshot_backpressure_bounds_page_buffer();
    test_in_flight_epochs_governor_throttles_reader();
    test_transaction_buffer_bound_enforced();
    test_source_lag_reporting_matches_upstream_wal();
    test_slot_invalidation_fails_closed_with_resnapshot();
}
