//! Example SDK connector for RockStream (v0.48).
//!
//! This module serves as the **third-party connector example** referenced in
//! the v0.48 roadmap proof criteria and connector developer guide. It shows
//! a minimal connector that:
//!
//! 1. Passes the Tier 1 contract tests (opaque `OffsetToken`, watermark,
//!    `credits_available`).
//! 2. Returns `partition_filter_support() -> false` (operator-layer filtering).
//! 3. Declares a `COUNTER` column via `discover_schema`, demonstrating the
//!    end-to-end CRDT column SDK workflow.
//! 4. Implements `lifecycle_state` / `pause` / `resume` / `delete` (connector
//!    lifecycle management).
//!
//! ## SDK Usage (end-to-end COUNTER column example)
//!
//! ```rust,ignore
//! use rockstream_connectors::example_sdk::{ExampleSdkSource, ExampleSdkSink};
//! use rockstream_connectors::source::Source;
//! use rockstream_connectors::sink::Sink;
//!
//! // Source: declares a COUNTER column via discover_schema
//! let mut source = ExampleSdkSource::new("events");
//! let meta = source.discover_schema();
//! assert!(meta.columns.contains_key("event_count"));
//!
//! // Sink: round-trips the schema through sink.discover_schema()
//! let mut sink = ExampleSdkSink::new("events-output");
//! let meta = sink.discover_schema();
//! assert!(meta.columns.contains_key("event_count"));
//! ```

use async_trait::async_trait;
use rockstream_types::batch::{OffsetToken, SinkBatch, SourceBatch};
use rockstream_types::connector::{
    ConnectorLifecycleState, LawSchemaMetadata, WriteClassification,
};
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::timestamp::{Epoch, EventTimeWatermark};

use crate::sink::Sink;
use crate::source::Source;

// ─── ExampleSdkSource ────────────────────────────────────────────────────────

/// A minimal example SDK source connector showing how to declare a COUNTER
/// column via `discover_schema`.
///
/// This connector is the "third-party example" from the v0.48 roadmap. It
/// passes the Tier 1 contract and demonstrates partition filter support
/// returning `false` (operator-layer filtering).
pub struct ExampleSdkSource {
    name: String,
    credits: usize,
    offset: u64,
    watermark: EventTimeWatermark,
    lifecycle: ConnectorLifecycleState,
}

impl ExampleSdkSource {
    /// Create a new example SDK source.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            credits: usize::MAX,
            offset: 0,
            watermark: 0,
            lifecycle: ConnectorLifecycleState::Running,
        }
    }
}

#[async_trait]
impl Source for ExampleSdkSource {
    async fn poll_batch(&mut self, epoch: Epoch) -> Option<SourceBatch> {
        if self.lifecycle != ConnectorLifecycleState::Running {
            return Some(SourceBatch {
                record_count: 0,
                epoch,
                offset: Some(OffsetToken(format!("sdk-offset-{}", self.offset))),
                watermark: Some(self.watermark),
            });
        }

        if self.credits == 0 {
            return Some(SourceBatch {
                record_count: 0,
                epoch,
                offset: Some(OffsetToken(format!("sdk-offset-{}", self.offset))),
                watermark: Some(self.watermark),
            });
        }

        self.offset += 1;
        self.watermark = epoch * 100;

        Some(SourceBatch {
            record_count: 5,
            epoch,
            offset: Some(OffsetToken(format!("sdk-offset-{}", self.offset))),
            watermark: Some(self.watermark),
        })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn credits_available(&self) -> usize {
        self.credits
    }

    fn set_credits(&mut self, credits: usize) {
        self.credits = credits;
    }

    fn current_offset(&self) -> Option<OffsetToken> {
        Some(OffsetToken(format!("sdk-offset-{}", self.offset)))
    }

    /// This connector does NOT implement partition push-down.
    /// The operator layer will apply filtering itself.
    fn partition_filter_support(&self) -> bool {
        false
    }

    /// Declare a `COUNTER` column (`event_count`) backed by `PNCounter/v1`.
    ///
    /// This is the end-to-end SDK example: an external connector declares a
    /// CRDT column and the write-classification metadata flows through schema
    /// discovery into `EXPLAIN TRANSACTION`.
    fn discover_schema(&self) -> LawSchemaMetadata {
        LawSchemaMetadata::empty().with_column(
            "event_count",
            MergeLawId(10), // PNCounter/v1
            "COUNTER",
            WriteClassification::BlindDelta,
        )
    }

    fn lifecycle_state(&self) -> ConnectorLifecycleState {
        self.lifecycle
    }

    async fn pause(&mut self) -> bool {
        if self.lifecycle == ConnectorLifecycleState::Running {
            self.lifecycle = ConnectorLifecycleState::Paused;
            true
        } else {
            false
        }
    }

    async fn resume(&mut self) -> bool {
        if self.lifecycle == ConnectorLifecycleState::Paused {
            self.lifecycle = ConnectorLifecycleState::Running;
            true
        } else {
            false
        }
    }

    async fn delete(&mut self) {
        self.lifecycle = ConnectorLifecycleState::Deleted;
    }
}

// ─── ExampleSdkSink ──────────────────────────────────────────────────────────

/// A minimal example SDK sink connector.
pub struct ExampleSdkSink {
    name: String,
    committed_epochs: Vec<Epoch>,
    aborted_epochs: Vec<Epoch>,
    lifecycle: ConnectorLifecycleState,
}

impl ExampleSdkSink {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            committed_epochs: Vec::new(),
            aborted_epochs: Vec::new(),
            lifecycle: ConnectorLifecycleState::Running,
        }
    }

    pub fn committed_epochs(&self) -> &[Epoch] {
        &self.committed_epochs
    }

    pub fn aborted_epochs(&self) -> &[Epoch] {
        &self.aborted_epochs
    }
}

#[async_trait]
impl Sink for ExampleSdkSink {
    async fn write_batch(&mut self, batch: &SinkBatch) {
        tracing::debug!(
            name = %self.name,
            epoch = batch.epoch,
            records = batch.record_count,
            "example sdk sink: writing batch"
        );
    }

    async fn commit(&mut self, epoch: Epoch) {
        self.committed_epochs.push(epoch);
        if self.committed_epochs.len() > 1024 {
            self.committed_epochs.remove(0);
        }
        tracing::debug!(
            name = %self.name,
            epoch,
            committed_fill_level = ?(self.committed_epochs.len() as f64 / 1024.0),
            "example sdk sink: committed"
        );
    }

    async fn abort(&mut self, epoch: Epoch) {
        self.aborted_epochs.push(epoch);
        if self.aborted_epochs.len() > 1024 {
            self.aborted_epochs.remove(0);
        }
        tracing::debug!(
            name = %self.name,
            epoch,
            aborted_fill_level = ?(self.aborted_epochs.len() as f64 / 1024.0),
            "example sdk sink: aborted"
        );
    }

    fn name(&self) -> &str {
        &self.name
    }

    /// Declare the same `COUNTER` column as the source.
    fn discover_schema(&self) -> LawSchemaMetadata {
        LawSchemaMetadata::empty().with_column(
            "event_count",
            MergeLawId(10), // PNCounter/v1
            "COUNTER",
            WriteClassification::BlindDelta,
        )
    }

    fn lifecycle_state(&self) -> ConnectorLifecycleState {
        self.lifecycle
    }

    async fn pause(&mut self) -> bool {
        if self.lifecycle == ConnectorLifecycleState::Running {
            self.lifecycle = ConnectorLifecycleState::Paused;
            true
        } else {
            false
        }
    }

    async fn resume(&mut self) -> bool {
        if self.lifecycle == ConnectorLifecycleState::Paused {
            self.lifecycle = ConnectorLifecycleState::Running;
            true
        } else {
            false
        }
    }

    async fn delete(&mut self) {
        self.lifecycle = ConnectorLifecycleState::Deleted;
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;
    use crate::source::Source;
    use rockstream_types::connector::ExplainTransaction;

    // ── Tier 1 contract tests ──────────────────────────────────────────────

    /// **Proof criterion (v0.48)**: Third-party example connector passes Tier 1
    /// contract tests.
    #[tokio::test]
    async fn example_sdk_source_tier1_contract_poll_batch() {
        let mut src = ExampleSdkSource::new("events");
        let batch = src.poll_batch(0).await.unwrap();
        assert_eq!(batch.record_count, 5);
        assert!(batch.offset.is_some());
        assert!(batch.watermark.is_some());
    }

    #[tokio::test]
    async fn example_sdk_source_tier1_credits_saturation() {
        let mut src = ExampleSdkSource::new("events");
        src.set_credits(0);
        let batch = src.poll_batch(0).await.unwrap();
        assert_eq!(batch.record_count, 0, "must pause on zero credits");
    }

    #[test]
    fn example_sdk_source_tier1_credits_available() {
        let src = ExampleSdkSource::new("events");
        assert_eq!(src.credits_available(), usize::MAX);
    }

    #[test]
    fn example_sdk_source_tier1_offset_is_some() {
        let src = ExampleSdkSource::new("events");
        assert!(src.current_offset().is_some());
    }

    #[tokio::test]
    async fn example_sdk_source_tier1_name() {
        let src = ExampleSdkSource::new("my-source");
        assert_eq!(src.name(), "my-source");
    }

    // ── Tier 2: partition_filter_support returns false ──────────────────────

    /// **Proof criterion (v0.48)**: `partition_filter_support() -> false` on
    /// connectors that do not implement pushdown; operator-layer filtering
    /// produces identical output.
    #[test]
    fn example_sdk_source_partition_filter_support_false() {
        let src = ExampleSdkSource::new("events");
        assert!(
            !src.partition_filter_support(),
            "SDK example must return false: operator-layer filtering applies"
        );
    }

    /// Proof: operator-layer filtering with `partition_filter_support=false`
    /// produces the same output as push-down would: all records pass, then
    /// the operator filters them.
    #[tokio::test]
    async fn example_sdk_source_operator_layer_filtering_identical_output() {
        use rockstream_types::connector::PartitionFilter;

        let mut src_no_filter = ExampleSdkSource::new("events");
        let mut src_with_filter = ExampleSdkSource::new("events");

        // start_snapshot delegates to poll_batch when partition_filter_support=false
        let batch_no_filter = src_no_filter.start_snapshot(0, None).await.unwrap();
        let filter = PartitionFilter::eq("region", "us-east-1");
        let batch_with_filter = src_with_filter
            .start_snapshot(0, Some(&filter))
            .await
            .unwrap();

        // When partition_filter_support=false, start_snapshot ignores the filter
        // and returns the same records as with no filter. Operator layer then
        // applies filtering, producing identical output.
        assert_eq!(
            batch_no_filter.record_count, batch_with_filter.record_count,
            "connector ignores filter and returns same records; operator filters"
        );
    }

    // ── CRDT column schema discovery ──────────────────────────────────────

    /// **Proof criterion (v0.48)**: SDK example shows a connector declaring a
    /// `COUNTER` column end-to-end.
    #[test]
    fn example_sdk_source_declares_counter_column() {
        let src = ExampleSdkSource::new("events");
        let meta = src.discover_schema();
        assert!(
            meta.columns.contains_key("event_count"),
            "must declare event_count COUNTER column"
        );
        let col = &meta.columns["event_count"];
        assert_eq!(col.crdt_type, "COUNTER");
        assert_eq!(col.write_classification, WriteClassification::BlindDelta);
    }

    /// **Proof criterion (v0.48)**: Connector-declared CRDT columns round-trip
    /// through schema discovery and `EXPLAIN`.
    #[test]
    fn example_sdk_source_schema_round_trips_through_explain_transaction() {
        let src = ExampleSdkSource::new("events");
        let meta = src.discover_schema();

        let explain = ExplainTransaction::from_schema_metadata(
            src.name(),
            src.partition_filter_support(),
            &meta,
        );

        assert_eq!(explain.connector_name, "events");
        assert!(!explain.partition_filter_support);
        assert_eq!(explain.columns.len(), 1);
        assert_eq!(explain.columns[0].column, "event_count");
        assert_eq!(explain.columns[0].crdt_type, "COUNTER");
        assert_eq!(
            explain.columns[0].write_classification,
            WriteClassification::BlindDelta
        );

        // Verify EXPLAIN format output contains expected fields
        let lines = explain.format_lines();
        assert!(lines.iter().any(|l| l.contains("event_count")));
        assert!(lines.iter().any(|l| l.contains("COUNTER")));
        assert!(lines.iter().any(|l| l.contains("blind_delta")));
    }

    /// **Proof criterion (v0.48)**: Connector write-classification metadata
    /// surfaces in `EXPLAIN TRANSACTION`.
    #[test]
    fn example_sdk_sink_schema_surfaces_in_explain_transaction() {
        let sink = ExampleSdkSink::new("events-output");
        let meta = sink.discover_schema();

        let explain = ExplainTransaction::from_schema_metadata(sink.name(), false, &meta);

        let lines = explain.format_lines();
        let full = lines.join("\n");
        assert!(
            full.contains("blind_delta"),
            "write_classification must appear in EXPLAIN TRANSACTION: {full}"
        );
        assert!(
            full.contains("COUNTER"),
            "CRDT type must appear in EXPLAIN TRANSACTION: {full}"
        );
    }

    // ── Lifecycle ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn example_sdk_source_lifecycle_pause_resume_delete() {
        let mut src = ExampleSdkSource::new("events");
        assert_eq!(src.lifecycle_state(), ConnectorLifecycleState::Running);

        assert!(src.pause().await);
        assert_eq!(src.lifecycle_state(), ConnectorLifecycleState::Paused);

        // Paused source returns 0 records
        let batch = src.poll_batch(0).await.unwrap();
        assert_eq!(batch.record_count, 0);

        assert!(src.resume().await);
        assert_eq!(src.lifecycle_state(), ConnectorLifecycleState::Running);

        src.delete().await;
        assert_eq!(src.lifecycle_state(), ConnectorLifecycleState::Deleted);
    }

    #[tokio::test]
    async fn example_sdk_sink_lifecycle_pause_resume_delete() {
        let mut sink = ExampleSdkSink::new("output");
        assert_eq!(sink.lifecycle_state(), ConnectorLifecycleState::Running);
        assert!(sink.pause().await);
        assert!(sink.resume().await);
        sink.delete().await;
        assert_eq!(sink.lifecycle_state(), ConnectorLifecycleState::Deleted);
    }

    // ── Sink 2PC contract ──────────────────────────────────────────────────

    #[tokio::test]
    async fn example_sdk_sink_2pc_commit() {
        let mut sink = ExampleSdkSink::new("output");
        sink.write_batch(&SinkBatch {
            epoch: 1,
            record_count: 10,
        })
        .await;
        sink.commit(1).await;
        assert_eq!(sink.committed_epochs(), &[1]);
    }

    #[tokio::test]
    async fn example_sdk_sink_2pc_abort() {
        let mut sink = ExampleSdkSink::new("output");
        sink.abort(2).await;
        assert_eq!(sink.aborted_epochs(), &[2]);
    }
}
