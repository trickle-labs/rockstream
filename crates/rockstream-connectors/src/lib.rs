//! Source and sink connector implementations for RockStream.
//!
//! This crate holds the source and sink connector contracts and the
//! built-in connectors (v0.21: sink 2PC protocol, source-epoch registry,
//! Kafka sink and M3 paired runtime assertions).

pub mod fault_injecting_store;
pub mod kafka_sink;
pub mod kafka_source;
pub mod postgres_cdc;
pub mod sink_connector;
pub mod source_connector;
pub mod source_epoch;
mod source_json;
pub mod source_runtime;

pub use fault_injecting_store::FaultInjectingObjectStore;
pub use kafka_sink::KafkaSink;
pub use kafka_source::KafkaSource;
pub use postgres_cdc::{
    CdcChange, CdcOperation, CdcTransactionEnvelope, CdcWireFormat, PgLsn, PgOutputColumn,
    PgOutputConfig, PgOutputEvent, PgOutputRelationMetadata, PgOutputSnapshotRelation,
    PgOutputSourceSnapshot, PostgresCdcFailure, PostgresCdcSource, PostgresCdcStatus,
    POSTGRES_CDC_MAX_IN_FLIGHT_BYTES, POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS,
    POSTGRES_CDC_MAX_RESNAPSHOT_ATTEMPTS, POSTGRES_CDC_MAX_TRANSACTION_BYTES,
    POSTGRES_CDC_MAX_WAL_LAG_BYTES, POSTGRES_CDC_TRANSACTION_MEMORY_BYTES,
};
pub use sink_connector::{
    assert_epoch_committed_only_after_cluster_checkpoint, assert_no_duplicate_delivery,
    assert_no_lost_delivery_after_checkpoint, assert_recovery_dispatch_idempotent, SinkConnector,
    SinkError,
};
pub use source_connector::{
    validate_window_watermark, PollDeltaResult, SnapshotStream, SourceConnector, SourceError,
    SourcePollLifecycle, WatermarkCapability, WindowWatermarkPolicy,
};
pub use source_epoch::{
    BackfillCursor, BackfillLifecycle, BackfillPhase, OffsetToken, SnapshotDeltaFence,
    SourceCheckpoint, SourceCheckpointState, SourceCheckpointStore, SourceEpochEntry,
    SourceEpochRegistry, SOURCE_CHECKPOINT_HISTORY_MAX_ENTRIES,
};
pub use source_runtime::{
    SourceOwnerLease, SourceRuntimeCoordinator, SourceRuntimeMetrics,
    SOURCE_RUNTIME_MAX_IN_FLIGHT_EPOCHS,
};

#[cfg(test)]
mod tests {
    #[test]
    fn connectors_crate_compiles() {}
}
