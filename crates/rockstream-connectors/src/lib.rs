//! Source and sink connector implementations for RockStream.
//!
//! This crate holds the source and sink connector contracts and the
//! built-in connectors (v0.21: sink 2PC protocol, source-epoch registry,
//! Kafka sink, object-store sink, M3 paired runtime assertions).

pub mod catalog_registrar;
pub mod cold_gc;
pub mod cold_tier_sink;
pub mod delta_sink;
pub mod fault_injecting_store;
pub mod iceberg_sink;
pub mod kafka_sink;
pub mod kafka_source;
pub mod object_store_sink;
pub mod partition_spec;
pub mod s3_source;
pub mod sink_connector;
pub mod source_connector;
pub mod source_epoch;

pub use catalog_registrar::{
    CatalogRegistrar, CatalogRegistrationError, DuckLakeCatalogRegistrar,
    FilesystemCatalogRegistrar, GlueCatalogRegistrar, HiveCatalogRegistrar, RegistrationOutcome,
    RestCatalogRegistrar,
};
pub use cold_gc::{ColdGc, ColdGcConfig, ColdGcMetrics, ColdGcResult, RetainedSnapshot};
pub use cold_tier_sink::{ColdTierSink, COLD_TIER_SINK_MAX_PENDING_EPOCHS};
pub use delta_sink::DeltaSink;
pub use fault_injecting_store::FaultInjectingObjectStore;
pub use iceberg_sink::IcebergSink;
pub use kafka_sink::KafkaSink;
pub use kafka_source::KafkaSource;
pub use object_store_sink::{ObjectStoreSink, OBJECT_STORE_SINK_MAX_PENDING_EPOCHS};
pub use s3_source::S3Source;
pub use sink_connector::{
    assert_commit_pointer_atomic, assert_epoch_committed_only_after_cluster_checkpoint,
    assert_no_duplicate_delivery, assert_no_lost_delivery_after_checkpoint,
    assert_recovery_dispatch_idempotent, SinkConnector, SinkError,
};
pub use source_connector::{
    validate_window_watermark, PollDeltaResult, SnapshotStream, SourceConnector, SourceError,
    SourcePollLifecycle, WatermarkCapability, WindowWatermarkPolicy,
};
pub use source_epoch::{OffsetToken, SourceEpochEntry, SourceEpochRegistry};

#[cfg(test)]
mod tests {
    #[test]
    fn connectors_crate_compiles() {}
}
