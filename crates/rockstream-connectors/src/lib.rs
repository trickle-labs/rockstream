//! Source and sink connector implementations for RockStream.
//!
//! This crate holds the source and sink connector contracts and the
//! built-in connectors (v0.21: sink 2PC protocol, source-epoch registry,
//! Kafka sink, object-store sink, M3 paired runtime assertions).

pub mod kafka_sink;
pub mod kafka_source;
pub mod object_store_sink;
pub mod s3_source;
pub mod sink_connector;
pub mod source_connector;
pub mod source_epoch;

pub use kafka_sink::KafkaSink;
pub use kafka_source::KafkaSource;
pub use object_store_sink::ObjectStoreSink;
pub use s3_source::S3Source;
pub use sink_connector::{
    assert_commit_pointer_atomic, assert_epoch_committed_only_after_cluster_checkpoint,
    assert_no_duplicate_delivery, assert_no_lost_delivery_after_checkpoint,
    assert_recovery_dispatch_idempotent, SinkConnector, SinkError,
};
pub use source_connector::{
    PollDeltaResult, SnapshotStream, SourceConnector, SourceError, WatermarkCapability,
};
pub use source_epoch::{OffsetToken, SourceEpochEntry, SourceEpochRegistry};

#[cfg(test)]
mod tests {
    #[test]
    fn connectors_crate_compiles() {}
}
