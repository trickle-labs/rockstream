use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{
    CdcWireFormat, PostgresCdcFailure, PostgresCdcSource, PostgresCdcStatus,
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
