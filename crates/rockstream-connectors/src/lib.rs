//! Source and sink connector implementations for RockStream.
//!
//! This crate holds the source and sink connector contracts and the
//! built-in connectors (v0.21: sink 2PC protocol, source-epoch registry,
//! Kafka sink, object-store sink, M3 paired runtime assertions).

pub mod kafka_sink;
pub mod object_store_sink;
pub mod sink_connector;
pub mod source_epoch;

pub use kafka_sink::KafkaSink;
pub use object_store_sink::ObjectStoreSink;
pub use sink_connector::{
    SinkConnector, SinkError,
    assert_epoch_committed_only_after_cluster_checkpoint,
    assert_no_duplicate_delivery,
    assert_no_lost_delivery_after_checkpoint,
    assert_recovery_dispatch_idempotent,
};
pub use source_epoch::{OffsetToken, SourceEpochEntry, SourceEpochRegistry};

#[cfg(test)]
mod tests {
    #[test]
    fn connectors_crate_compiles() {}
}
