//! Source connector trait and support types (§13.3).
//!
//! Every source connector implements [`SourceConnector`] which provides:
//! - `discover_schema`: returns the schema of records produced by the source.
//! - `start_snapshot`: returns a stream of records for the initial snapshot.
//! - `poll_delta`: polls new delta records from the source starting from a given offset.
//! - `commit_offset`: commits the source offset associated with a given epoch.
//! - `pause` / `resume`: controls lifecycle state.
//! - `partition_filter_support`: declares if the connector supports partition pushdown.

use crate::source_epoch::OffsetToken;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use rockstream_types::connector::PartitionFilter;
use rockstream_types::timestamp::{Epoch, EventTimeWatermark};

/// Error from a source connector operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceError {
    /// Schema discovery failed.
    DiscoverSchemaFailed { reason: String },
    /// Starting the snapshot failed.
    StartSnapshotFailed { reason: String },
    /// Polling delta failed.
    PollDeltaFailed { reason: String },
    /// Committing the offset failed.
    CommitOffsetFailed { epoch: Epoch, reason: String },
    /// Generic source I/O error.
    Io(String),
}

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DiscoverSchemaFailed { reason } => {
                write!(f, "RS-4001: source schema discovery failed: {reason}")
            }
            Self::StartSnapshotFailed { reason } => {
                write!(f, "RS-4001: source start snapshot failed: {reason}")
            }
            Self::PollDeltaFailed { reason } => {
                write!(f, "RS-4001: source poll delta failed: {reason}")
            }
            Self::CommitOffsetFailed { epoch, reason } => {
                write!(
                    f,
                    "RS-4001: source commit offset failed for epoch {epoch}: {reason}"
                )
            }
            Self::Io(msg) => write!(f, "RS-4001: source I/O error: {msg}"),
        }
    }
}

impl std::error::Error for SourceError {}

/// Watermark capability of a source connector (§13.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatermarkCapability {
    /// Connector produces a watermark on every `poll_delta` from the underlying source.
    Native,
    /// Connector accepts watermarks from an out-of-band control channel.
    ExternalHint,
    /// Connector cannot produce a watermark under any conditions.
    None,
}

/// A stream of record batches for the initial snapshot (§13.3).
pub struct SnapshotStream {
    batches: Vec<RecordBatch>,
    index: usize,
}

impl SnapshotStream {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches, index: 0 }
    }
}

impl Iterator for SnapshotStream {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.batches.len() {
            let batch = self.batches[self.index].clone();
            self.index += 1;
            Some(batch)
        } else {
            None
        }
    }
}

/// The result of polling new delta records from a source connector (§13.3).
#[derive(Debug, Clone)]
pub struct PollDeltaResult {
    /// Batches of records retrieved from the source.
    pub batches: Vec<RecordBatch>,
    /// Opaque offset representing the position after these batches.
    pub new_offset: OffsetToken,
    /// Optional event-time watermark associated with the poll progress.
    pub watermark: Option<EventTimeWatermark>,
}

/// Trait implemented by every source connector (§13.3).
pub trait SourceConnector: Send + Sync {
    /// Returns the Arrow schema of the source data.
    fn discover_schema(&self) -> Result<SchemaRef, SourceError>;

    /// Start the initial snapshot stream from a given checkpoint frontier.
    fn start_snapshot(
        &mut self,
        frontier: Epoch,
        partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError>;

    /// Poll new delta records starting after the given offset.
    fn poll_delta(
        &mut self,
        after: OffsetToken,
        max_bytes: usize,
        credits_available: usize,
        partition_filter: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError>;

    /// Commit the progress offset for a given epoch.
    fn commit_offset(&mut self, epoch: Epoch, offset: OffsetToken) -> Result<(), SourceError>;

    /// Pause the source connector polling.
    fn pause(&mut self, reason: String) -> Result<(), SourceError>;

    /// Resume the source connector polling.
    fn resume(&mut self) -> Result<(), SourceError>;

    /// Returns whether this source connector supports partition filter pushdown.
    fn partition_filter_support(&self) -> bool {
        false
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    struct DummySource {
        schema: SchemaRef,
        paused: bool,
    }

    impl SourceConnector for DummySource {
        fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
            Ok(self.schema.clone())
        }

        fn start_snapshot(
            &mut self,
            _frontier: Epoch,
            _partition_filter: Option<PartitionFilter>,
        ) -> Result<SnapshotStream, SourceError> {
            Ok(SnapshotStream::new(vec![]))
        }

        fn poll_delta(
            &mut self,
            after: OffsetToken,
            _max_bytes: usize,
            _credits_available: usize,
            _partition_filter: Option<PartitionFilter>,
        ) -> Result<PollDeltaResult, SourceError> {
            Ok(PollDeltaResult {
                batches: vec![],
                new_offset: after,
                watermark: None,
            })
        }

        fn commit_offset(
            &mut self,
            _epoch: Epoch,
            _offset: OffsetToken,
        ) -> Result<(), SourceError> {
            Ok(())
        }

        fn pause(&mut self, _reason: String) -> Result<(), SourceError> {
            self.paused = true;
            Ok(())
        }

        fn resume(&mut self) -> Result<(), SourceError> {
            self.paused = false;
            Ok(())
        }

        fn partition_filter_support(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_source_trait_compiles() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut source = DummySource {
            schema,
            paused: false,
        };
        assert_eq!(source.discover_schema().unwrap().fields().len(), 1);
        assert!(!source.partition_filter_support());
        source.pause("test".to_string()).unwrap();
        assert!(source.paused);
        source.resume().unwrap();
        assert!(!source.paused);
    }
}
