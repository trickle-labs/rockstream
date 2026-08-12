//! Source connector trait and support types (§13.3).
//!
//! Every source connector implements [`SourceConnector`] which provides:
//! - `discover_schema`: returns the schema of records produced by the source.
//! - `start_snapshot`: returns a stream of records for the initial snapshot.
//! - `poll_delta`: polls new delta records from the source starting from a given offset.
//! - `commit_offset`: commits the source offset associated with a given epoch.
//! - `pause` / `resume`: controls lifecycle state.
//! - `partition_filter_support`: declares if the connector supports partition pushdown.

use crate::source_epoch::{OffsetToken, SnapshotDeltaFence, SourceEpochRegistry};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use rockstream_types::connector::PartitionFilter;
use rockstream_types::ids::ConnectorId;
use rockstream_types::timestamp::{Epoch, EventTimeWatermark};
use std::collections::BTreeMap;

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
    /// A windowed view requires an explicit compatible watermark policy.
    WatermarkRequired { reason: String },
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
                write!(f, "RS-4004: source poll failed: {reason}")
            }
            Self::CommitOffsetFailed { epoch, reason } => {
                write!(
                    f,
                    "RS-4001: source commit offset failed for epoch {epoch}: {reason}"
                )
            }
            Self::Io(msg) => write!(f, "RS-4001: source I/O error: {msg}"),
            Self::WatermarkRequired { reason } => write!(
                f,
                "RS-1005: connector.watermark_required: {reason}. Next steps: declare a compatible WATERMARK policy before registering the windowed view"
            ),
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

/// Explicit window-closing policy selected for a source-backed window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowWatermarkPolicy {
    Native,
    ProcessingTime,
    External,
    Disabled,
}

/// Reject a window configuration before source registration or worker allocation.
pub fn validate_window_watermark(
    capability: WatermarkCapability,
    policy: Option<WindowWatermarkPolicy>,
) -> Result<(), SourceError> {
    match (capability, policy) {
        (_, None) => Err(SourceError::WatermarkRequired {
            reason: "windowed sources cannot omit WATERMARK".to_string(),
        }),
        (WatermarkCapability::Native, Some(WindowWatermarkPolicy::Native))
        | (_, Some(WindowWatermarkPolicy::ProcessingTime | WindowWatermarkPolicy::Disabled))
        | (WatermarkCapability::ExternalHint, Some(WindowWatermarkPolicy::External)) => Ok(()),
        (WatermarkCapability::None, Some(WindowWatermarkPolicy::Native))
        | (WatermarkCapability::None, Some(WindowWatermarkPolicy::External))
        | (WatermarkCapability::ExternalHint, Some(WindowWatermarkPolicy::Native))
        | (WatermarkCapability::Native, Some(WindowWatermarkPolicy::External)) => {
            Err(SourceError::WatermarkRequired {
                reason: format!("source capability {capability:?} is incompatible with {policy:?}"),
            })
        }
    }
}

/// A stream of record batches for the initial snapshot (§13.3).
pub struct SnapshotStream {
    batches: Vec<SnapshotBatch>,
    index: usize,
    remaining_rows: usize,
}

/// One bounded snapshot chunk together with the exact resume position after it.
#[derive(Debug, Clone)]
pub struct SnapshotBatch {
    pub batch: RecordBatch,
    pub resume_offset: OffsetToken,
}

impl SnapshotStream {
    pub fn new(batches: Vec<SnapshotBatch>) -> Self {
        let remaining_rows = batches.iter().map(|chunk| chunk.batch.num_rows()).sum();
        Self {
            batches,
            index: 0,
            remaining_rows,
        }
    }

    /// A connector may return just its next bounded chunk while reporting the
    /// complete fenced tail so lifecycle status remains accurate.
    pub fn with_remaining(batches: Vec<SnapshotBatch>, remaining_rows: usize) -> Self {
        Self {
            batches,
            index: 0,
            remaining_rows,
        }
    }

    pub fn remaining_rows(&self) -> usize {
        self.remaining_rows
    }
}

impl Iterator for SnapshotStream {
    type Item = SnapshotBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.batches.len() {
            let batch = self.batches[self.index].clone();
            self.index += 1;
            self.remaining_rows = self.remaining_rows.saturating_sub(batch.batch.num_rows());
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
#[async_trait]
pub trait SourceConnector: Send + Sync {
    /// Returns the Arrow schema of the source data.
    fn discover_schema(&self) -> Result<SchemaRef, SourceError>;

    /// Capture the immutable snapshot/delta boundary without consuming input.
    async fn capture_snapshot_delta_fence(
        &mut self,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotDeltaFence, SourceError> {
        Err(SourceError::Io(
            "RS-4019: backfill.cursor_limit_exceeded: connector does not implement atomic snapshot/delta fence capture; next_steps: upgrade the connector before creating a materialized view".to_string(),
        ))
    }

    /// Start the initial snapshot stream bound to the captured fence.
    async fn start_snapshot(
        &mut self,
        fence: &SnapshotDeltaFence,
        after: Option<OffsetToken>,
        partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError>;

    /// Start the snapshot in chunks no larger than `max_rows`. Connectors
    /// whose native snapshot is already bounded can use the default.
    async fn start_snapshot_bounded(
        &mut self,
        fence: &SnapshotDeltaFence,
        after: Option<OffsetToken>,
        partition_filter: Option<PartitionFilter>,
        _max_rows: usize,
    ) -> Result<SnapshotStream, SourceError> {
        self.start_snapshot(fence, after, partition_filter).await
    }

    /// Poll new delta records starting after the given offset.
    async fn poll_delta(
        &mut self,
        after: OffsetToken,
        max_bytes: usize,
        credits_available: usize,
        partition_filter: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError>;

    /// Commit the progress offset for a given epoch.
    async fn commit_offset(&mut self, epoch: Epoch, offset: OffsetToken)
        -> Result<(), SourceError>;

    /// Pause the source connector polling.
    async fn pause(&mut self, reason: String) -> Result<(), SourceError>;

    /// Resume the source connector polling.
    async fn resume(&mut self) -> Result<(), SourceError>;

    /// Returns whether this source connector supports partition filter pushdown.
    fn partition_filter_support(&self) -> bool {
        false
    }

    /// Declares the source's event-time watermark capability.
    fn watermark_capability(&self) -> WatermarkCapability {
        WatermarkCapability::None
    }
}

#[async_trait]
impl<T: SourceConnector + ?Sized> SourceConnector for Box<T> {
    fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
        (**self).discover_schema()
    }

    async fn capture_snapshot_delta_fence(
        &mut self,
        partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotDeltaFence, SourceError> {
        (**self)
            .capture_snapshot_delta_fence(partition_filter)
            .await
    }

    async fn start_snapshot(
        &mut self,
        fence: &SnapshotDeltaFence,
        after: Option<OffsetToken>,
        partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError> {
        (**self)
            .start_snapshot(fence, after, partition_filter)
            .await
    }

    async fn start_snapshot_bounded(
        &mut self,
        fence: &SnapshotDeltaFence,
        after: Option<OffsetToken>,
        partition_filter: Option<PartitionFilter>,
        max_rows: usize,
    ) -> Result<SnapshotStream, SourceError> {
        (**self)
            .start_snapshot_bounded(fence, after, partition_filter, max_rows)
            .await
    }

    async fn poll_delta(
        &mut self,
        after: OffsetToken,
        max_bytes: usize,
        credits_available: usize,
        partition_filter: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        (**self)
            .poll_delta(after, max_bytes, credits_available, partition_filter)
            .await
    }

    async fn commit_offset(
        &mut self,
        epoch: Epoch,
        offset: OffsetToken,
    ) -> Result<(), SourceError> {
        (**self).commit_offset(epoch, offset).await
    }

    async fn pause(&mut self, reason: String) -> Result<(), SourceError> {
        (**self).pause(reason).await
    }

    async fn resume(&mut self) -> Result<(), SourceError> {
        (**self).resume().await
    }

    fn partition_filter_support(&self) -> bool {
        (**self).partition_filter_support()
    }

    fn watermark_capability(&self) -> WatermarkCapability {
        (**self).watermark_capability()
    }
}

/// Owns source polling lifecycle state so failed polls cannot advance a
/// committed offset. The registry records the durable recovery token per epoch.
pub struct SourcePollLifecycle<S: SourceConnector> {
    source: S,
    committed_offset: OffsetToken,
    source_epochs: SourceEpochRegistry,
    paused: bool,
}

impl<S: SourceConnector> SourcePollLifecycle<S> {
    /// Start polling from a previously committed source token.
    pub fn new(source: S, connector_id: ConnectorId, committed_offset: OffsetToken) -> Self {
        Self {
            source,
            committed_offset,
            source_epochs: SourceEpochRegistry::new(connector_id),
            paused: false,
        }
    }

    /// Poll without committing the returned offset. On failure the source is
    /// paused before the error is returned, preserving the recovery token.
    pub async fn poll(
        &mut self,
        max_bytes: usize,
        credits_available: usize,
        partition_filter: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        if self.paused {
            return Err(SourceError::Io(
                "source is paused after a failed poll; call resume before polling again"
                    .to_string(),
            ));
        }
        match self
            .source
            .poll_delta(
                self.committed_offset.clone(),
                max_bytes,
                credits_available,
                partition_filter,
            )
            .await
        {
            Ok(result) => Ok(result),
            Err(error) => {
                self.source.pause(error.to_string()).await?;
                self.paused = true;
                assert!(
                    self.paused,
                    "EDGE-SOURCEFAIL: a failed poll must pause before any offset can commit"
                );
                Err(error)
            }
        }
    }

    /// Durably commit a successful poll's token and source epoch together.
    pub async fn commit(&mut self, epoch: Epoch, offset: OffsetToken) -> Result<(), SourceError> {
        if self.paused {
            return Err(SourceError::Io(
                "source is paused after a failed poll; call resume before committing".to_string(),
            ));
        }
        let entry = self
            .source_epochs
            .prepare_commit(BTreeMap::from([(0, offset.clone())]))
            .map_err(|error| SourceError::Io(error.to_string()))?;
        assert_eq!(
            entry.source_epoch, epoch,
            "EDGE-SOURCEFAIL: a recovered offset must be committed at its prepared source epoch"
        );
        self.source_epochs
            .commit_epoch(entry)
            .map_err(|error| SourceError::Io(error.to_string()))?;
        self.committed_offset = offset;
        self.source
            .commit_offset(epoch, self.committed_offset.clone())
            .await?;
        Ok(())
    }

    /// Resume a source only after its failure has been handled.
    pub async fn resume(&mut self) -> Result<(), SourceError> {
        self.source.resume().await?;
        self.paused = false;
        Ok(())
    }

    /// The last durable recovery token.
    pub fn committed_offset(&self) -> &OffsetToken {
        &self.committed_offset
    }

    /// Whether a failed poll currently prevents further normal operation.
    pub fn is_paused(&self) -> bool {
        self.paused
    }

    /// Access the bounded source-epoch registry for recovery inspection.
    pub fn source_epochs(&self) -> &SourceEpochRegistry {
        &self.source_epochs
    }

    /// Return the owned connector after the lifecycle is no longer needed.
    pub fn into_inner(self) -> S {
        self.source
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

    #[async_trait]
    impl SourceConnector for DummySource {
        fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
            Ok(self.schema.clone())
        }

        async fn capture_snapshot_delta_fence(
            &mut self,
            _partition_filter: Option<PartitionFilter>,
        ) -> Result<SnapshotDeltaFence, SourceError> {
            Ok(SnapshotDeltaFence::new(
                OffsetToken::new(b"snapshot".to_vec()),
                OffsetToken::new(b"live".to_vec()),
            ))
        }

        async fn start_snapshot(
            &mut self,
            _fence: &SnapshotDeltaFence,
            _after: Option<OffsetToken>,
            _partition_filter: Option<PartitionFilter>,
        ) -> Result<SnapshotStream, SourceError> {
            Ok(SnapshotStream::new(vec![]))
        }

        async fn poll_delta(
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

        async fn commit_offset(
            &mut self,
            _epoch: Epoch,
            _offset: OffsetToken,
        ) -> Result<(), SourceError> {
            Ok(())
        }

        async fn pause(&mut self, _reason: String) -> Result<(), SourceError> {
            self.paused = true;
            Ok(())
        }

        async fn resume(&mut self) -> Result<(), SourceError> {
            self.paused = false;
            Ok(())
        }

        fn partition_filter_support(&self) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn test_source_trait_compiles() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut source = DummySource {
            schema,
            paused: false,
        };
        assert_eq!(source.discover_schema().unwrap().fields().len(), 1);
        assert!(!source.partition_filter_support());
        assert_eq!(
            source.capture_snapshot_delta_fence(None).await.unwrap(),
            SnapshotDeltaFence::new(
                OffsetToken::new(b"snapshot".to_vec()),
                OffsetToken::new(b"live".to_vec()),
            )
        );
        source.pause("test".to_string()).await.unwrap();
        assert!(source.paused);
        source.resume().await.unwrap();
        assert!(!source.paused);
    }
}
