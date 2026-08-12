//! Fenced, checkpoint-coupled source runtime coordination.

use std::collections::BTreeMap;

use rockstream_storage::WriteBatch;
use rockstream_types::ids::ConnectorId;
use rockstream_types::timestamp::Epoch;

use crate::source_connector::{PollDeltaResult, SnapshotStream, SourceConnector, SourceError};
use crate::source_epoch::SnapshotDeltaFence;
use crate::source_epoch::{
    BackfillLifecycle, OffsetToken, SourceCheckpoint, SourceCheckpointStore, SourceEpochRegistry,
};

/// Maximum epochs that may have durable input pending upstream acknowledgement.
pub const SOURCE_RUNTIME_MAX_IN_FLIGHT_EPOCHS: usize = 64;

/// A monotonically fenced ownership lease for one source runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceOwnerLease {
    pub owner_id: String,
    pub fence_token: u64,
}

/// Fill-level metrics for the bounded source runtime state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceRuntimeMetrics {
    pub source_runtime_in_flight_epochs: usize,
    pub source_checkpoint_history_entries: usize,
    pub source_cleanup_scan_pages: usize,
}

/// Coordinates source input such that upstream acknowledgement happens strictly
/// after the M3 input transaction and its committed checkpoint are durable.
pub struct SourceRuntimeCoordinator<S: SourceConnector> {
    source: S,
    connector_id: ConnectorId,
    checkpoint_store: SourceCheckpointStore,
    source_epochs: SourceEpochRegistry,
    committed_offset: OffsetToken,
    recovered: bool,
    active_lease: Option<SourceOwnerLease>,
    next_fence_token: u64,
    in_flight_epochs: usize,
    blocked_reason: Option<String>,
}

impl<S: SourceConnector> SourceRuntimeCoordinator<S> {
    pub fn new(
        source: S,
        connector_id: ConnectorId,
        committed_offset: OffsetToken,
        checkpoint_store: SourceCheckpointStore,
    ) -> Self {
        Self {
            source,
            connector_id,
            checkpoint_store,
            source_epochs: SourceEpochRegistry::new(connector_id),
            committed_offset,
            recovered: false,
            active_lease: None,
            next_fence_token: 0,
            in_flight_epochs: 0,
            blocked_reason: None,
        }
    }

    /// Capture the source's snapshot/delta fence through the recovered owner.
    /// The gateway owns scheduling and turns the returned batches into its M3
    /// write; this coordinator remains the sole checkpoint/ack authority.
    pub async fn capture_snapshot_delta_fence(
        &mut self,
    ) -> Result<SnapshotDeltaFence, SourceError> {
        self.source.capture_snapshot_delta_fence(None).await
    }

    /// Begin the bounded snapshot stream at the supplied frontier.
    pub async fn start_snapshot(
        &mut self,
        fence: &SnapshotDeltaFence,
        after: Option<OffsetToken>,
        max_rows: usize,
    ) -> Result<SnapshotStream, SourceError> {
        self.source
            .start_snapshot_bounded(fence, after, None, max_rows)
            .await
    }

    /// Poll one bounded live-delta batch after the durable checkpoint.
    pub async fn poll_delta(
        &mut self,
        max_bytes: usize,
        credits_available: usize,
    ) -> Result<PollDeltaResult, SourceError> {
        self.poll_delta_after(self.committed_offset.clone(), max_bytes, credits_available)
            .await
    }

    /// Poll after an explicit fence position. Snapshot cursors and live
    /// offsets are distinct, so catch-up starts at the captured live fence.
    pub async fn poll_delta_after(
        &mut self,
        after: OffsetToken,
        max_bytes: usize,
        credits_available: usize,
    ) -> Result<PollDeltaResult, SourceError> {
        self.source
            .poll_delta(after, max_bytes, credits_available, None)
            .await
    }

    /// The next source epoch that can be committed under the active lease.
    pub fn next_epoch(&self) -> Result<Epoch, SourceError> {
        self.source_epochs.current_epoch().checked_add(1).ok_or_else(|| {
            SourceError::Io(
                "RS-4018: source epoch exhausted; next_steps: create a new connector before retrying"
                    .to_string(),
            )
        })
    }

    /// Recover the exact highest committed token before permitting an owner to
    /// become active. Prepared records are deliberately ignored.
    pub async fn recover(&mut self) -> Result<Option<SourceCheckpoint>, SourceError> {
        let checkpoint = self
            .checkpoint_store
            .highest_committed()
            .await
            .map_err(storage_error)?;
        if let Some(checkpoint) = &checkpoint {
            self.committed_offset = checkpoint.token.clone();
            self.source_epochs = SourceEpochRegistry::restore(
                self.connector_id,
                checkpoint.source_epoch,
                BTreeMap::from([(0, checkpoint.token.clone())]),
            );
        }
        self.recovered = true;
        Ok(checkpoint)
    }

    /// Recover the view-specific cursor/lifecycle paired with the checkpoint.
    pub async fn backfill_lifecycle(
        &self,
        view_name: &str,
    ) -> Result<Option<BackfillLifecycle>, SourceError> {
        self.checkpoint_store
            .backfill_lifecycle(view_name)
            .await
            .map_err(storage_error)
    }

    /// Durably record a captured-but-not-yet-consumed backfill fence. This is
    /// intentionally before the first input M3: it makes an interrupted empty
    /// snapshot resumable without advancing the source checkpoint.
    pub async fn persist_backfill_intent(
        &self,
        lifecycle: &BackfillLifecycle,
    ) -> Result<(), SourceError> {
        let mut batch = WriteBatch::new();
        self.checkpoint_store
            .append_backfill_lifecycle(&mut batch, lifecycle)
            .map_err(storage_error)?;
        self.checkpoint_store
            .commit_m3(batch)
            .await
            .map_err(storage_error)
    }

    /// Register a new owner only after recovery; replacing an owner fences all
    /// older leases through a larger token.
    pub fn acquire_owner(
        &mut self,
        owner_id: impl Into<String>,
    ) -> Result<SourceOwnerLease, SourceError> {
        if !self.recovered {
            return Err(SourceError::Io(
                "RS-4012: source owner cannot become active before checkpoint recovery; next steps: run recovery and retry owner registration".to_string(),
            ));
        }
        self.next_fence_token += 1;
        let lease = SourceOwnerLease {
            owner_id: owner_id.into(),
            fence_token: self.next_fence_token,
        };
        self.active_lease = Some(lease.clone());
        self.blocked_reason = None;
        Ok(lease)
    }

    /// Fence a current owner. A stale lease cannot fence its successor.
    pub fn fence_owner(&mut self, lease: &SourceOwnerLease) -> bool {
        if self.active_lease.as_ref() == Some(lease) {
            self.active_lease = None;
            true
        } else {
            false
        }
    }

    /// Fence the owner and stop upstream work while retaining the exact
    /// committed checkpoint for a later resume.
    pub async fn pause(&mut self, reason: impl Into<String>) -> Result<(), SourceError> {
        self.active_lease = None;
        let reason = reason.into();
        self.source.pause(reason.clone()).await?;
        self.blocked_reason = Some(reason);
        Ok(())
    }

    /// Rebuild the committed checkpoint before allowing a new owner lease.
    pub async fn resume(&mut self) -> Result<Option<SourceCheckpoint>, SourceError> {
        let checkpoint = self.recover().await?;
        self.source.resume().await?;
        self.blocked_reason = None;
        Ok(checkpoint)
    }

    /// Fence and stop the source, then delete only this source's durable
    /// checkpoint keys with bounded scans and point deletes.
    pub async fn drop_source(&mut self) -> Result<usize, SourceError> {
        self.active_lease = None;
        self.source
            .pause("source dropped; owner fenced before cleanup".to_string())
            .await?;
        let removed = self
            .checkpoint_store
            .cleanup()
            .await
            .map_err(storage_error)?;
        self.blocked_reason = Some("source dropped".to_string());
        Ok(removed)
    }

    /// Prepare input and its checkpoint, commit both in one M3 batch, then
    /// acknowledge the upstream connector. Any pre-ack failure blocks polling
    /// and never acknowledges uncommitted input.
    pub async fn commit_epoch(
        &mut self,
        lease: &SourceOwnerLease,
        epoch: Epoch,
        offset: OffsetToken,
        mut m3_input: WriteBatch,
    ) -> Result<(), SourceError> {
        self.require_active_lease(lease)?;
        if self.in_flight_epochs >= SOURCE_RUNTIME_MAX_IN_FLIGHT_EPOCHS {
            return self.block(
                "RS-4014: source_runtime_in_flight_epochs reached SOURCE_RUNTIME_MAX_IN_FLIGHT_EPOCHS; next steps: wait for upstream acknowledgements before polling more input",
            );
        }
        let Some(next_epoch) = self.source_epochs.current_epoch().checked_add(1) else {
            return self.block("RS-4018: source epoch exhausted; next_steps: create a new connector before retrying");
        };
        if epoch != next_epoch {
            return self.block(&format!(
                "RS-4015: source epoch {epoch} is not the next fenced epoch {}; next steps: recover the committed checkpoint and retry",
                next_epoch
            ));
        }

        let prepared = SourceCheckpoint::prepared(self.connector_id, epoch, offset.clone());
        self.checkpoint_store
            .prepare(&prepared)
            .await
            .map_err(storage_error)?;
        self.in_flight_epochs += 1;

        let committed = match self
            .checkpoint_store
            .append_committed(&mut m3_input, &prepared)
        {
            Ok(committed) => committed,
            Err(error) => {
                self.in_flight_epochs -= 1;
                return self.block(&storage_error(error).to_string());
            }
        };
        if let Err(error) = self.checkpoint_store.commit_m3(m3_input).await {
            self.in_flight_epochs -= 1;
            return self.block(&storage_error(error).to_string());
        }

        self.source_epochs
            .commit_epoch(
                self.source_epochs
                    .prepare_commit(BTreeMap::from([(0, committed.token.clone())]))
                    .map_err(|error| SourceError::Io(error.to_string()))?,
            )
            .map_err(|error| SourceError::Io(error.to_string()))?;
        self.committed_offset = committed.token.clone();

        if let Err(error) = self.source.commit_offset(epoch, committed.token).await {
            return self.block(&format!(
                "RS-4016: M3 source input committed but upstream acknowledgement failed: {error}; next steps: recover the checkpoint and retry acknowledgement"
            ));
        }
        self.in_flight_epochs -= 1;
        Ok(())
    }

    /// Commit a source-backed backfill step atomically with its source
    /// checkpoint. Callers add output, operator state, and frontier writes to
    /// `m3_input`; this method adds the committed checkpoint, fence cursor,
    /// and lifecycle record before the one durable M3 commit and upstream ack.
    pub async fn commit_backfill_epoch(
        &mut self,
        lease: &SourceOwnerLease,
        epoch: Epoch,
        offset: OffsetToken,
        lifecycle: BackfillLifecycle,
        mut m3_input: WriteBatch,
    ) -> Result<(), SourceError> {
        self.require_active_lease(lease)?;
        if lifecycle.cursor.committed_epoch != epoch
            || lifecycle.cursor.fence.version != 1
            || lifecycle.view_name != lifecycle.cursor.view_name
            || (matches!(lifecycle.phase, crate::source_epoch::BackfillPhase::Running)
                && lifecycle.published_frontier != Some(epoch))
        {
            return self.block(
                "RS-4019: backfill.cursor_limit_exceeded: lifecycle does not match its M3 epoch; next_steps: recover the committed cursor and retry",
            );
        }
        if self.in_flight_epochs >= SOURCE_RUNTIME_MAX_IN_FLIGHT_EPOCHS {
            return self.block(
                "RS-4014: source_runtime_in_flight_epochs reached SOURCE_RUNTIME_MAX_IN_FLIGHT_EPOCHS; next_steps: wait for upstream acknowledgements before polling more input",
            );
        }
        let Some(next_epoch) = self.source_epochs.current_epoch().checked_add(1) else {
            return self.block("RS-4018: source epoch exhausted; next_steps: create a new connector before retrying");
        };
        if epoch != next_epoch {
            return self.block(&format!(
                "RS-4015: source epoch {epoch} is not the next fenced epoch {}; next steps: recover the committed checkpoint and retry",
                next_epoch
            ));
        }

        let prepared = SourceCheckpoint::prepared(self.connector_id, epoch, offset);
        self.checkpoint_store
            .prepare(&prepared)
            .await
            .map_err(storage_error)?;
        self.in_flight_epochs += 1;
        let committed = match self
            .checkpoint_store
            .append_committed(&mut m3_input, &prepared)
        {
            Ok(committed) => committed,
            Err(error) => {
                self.in_flight_epochs -= 1;
                return self.block(&storage_error(error).to_string());
            }
        };
        if let Err(error) = self
            .checkpoint_store
            .append_backfill_lifecycle(&mut m3_input, &lifecycle)
        {
            self.in_flight_epochs -= 1;
            return self.block(&storage_error(error).to_string());
        }
        if let Err(error) = self.checkpoint_store.commit_m3(m3_input).await {
            self.in_flight_epochs -= 1;
            return self.block(&storage_error(error).to_string());
        }

        self.source_epochs
            .commit_epoch(
                self.source_epochs
                    .prepare_commit(BTreeMap::from([(0, committed.token.clone())]))
                    .map_err(|error| SourceError::Io(error.to_string()))?,
            )
            .map_err(|error| SourceError::Io(error.to_string()))?;
        self.committed_offset = committed.token.clone();
        if let Err(error) = self.source.commit_offset(epoch, committed.token).await {
            return self.block(&format!(
                "RS-4016: M3 source input committed but upstream acknowledgement failed: {error}; next_steps: recover the checkpoint and retry acknowledgement"
            ));
        }
        self.in_flight_epochs -= 1;
        Ok(())
    }

    /// Retry the upstream acknowledgement for the checkpoint recovered after a
    /// crash between M3 commit and acknowledgement. This never writes input.
    pub async fn acknowledge_recovered(
        &mut self,
        lease: &SourceOwnerLease,
    ) -> Result<(), SourceError> {
        self.require_active_lease(lease)?;
        let epoch = self.source_epochs.current_epoch();
        if epoch == 0 {
            return Ok(());
        }
        if let Err(error) = self
            .source
            .commit_offset(epoch, self.committed_offset.clone())
            .await
        {
            return self.block(&format!(
                "RS-4016: recovered source checkpoint acknowledgement failed: {error}; next steps: retain ownership and retry acknowledgement"
            ));
        }
        self.in_flight_epochs = 0;
        Ok(())
    }

    pub fn committed_offset(&self) -> &OffsetToken {
        &self.committed_offset
    }

    pub fn blocked_reason(&self) -> Option<&str> {
        self.blocked_reason.as_deref()
    }

    pub async fn metrics(&self) -> Result<SourceRuntimeMetrics, SourceError> {
        Ok(SourceRuntimeMetrics {
            source_runtime_in_flight_epochs: self.in_flight_epochs,
            source_checkpoint_history_entries: self
                .checkpoint_store
                .history_entries()
                .await
                .map_err(storage_error)?,
            source_cleanup_scan_pages: self.checkpoint_store.cleanup_scan_pages(),
        })
    }

    pub fn into_inner(self) -> S {
        self.source
    }

    fn require_active_lease(&self, lease: &SourceOwnerLease) -> Result<(), SourceError> {
        if self.active_lease.as_ref() != Some(lease) {
            return Err(SourceError::Io(
                "RS-4013: source owner lease is fenced or inactive; next steps: recover the checkpoint and acquire a new owner lease".to_string(),
            ));
        }
        Ok(())
    }

    fn block<T>(&mut self, reason: &str) -> Result<T, SourceError> {
        self.blocked_reason = Some(reason.to_string());
        drop(self.source.pause(reason.to_string()));
        Err(SourceError::Io(reason.to_string()))
    }
}

fn storage_error(error: rockstream_storage::StorageError) -> SourceError {
    SourceError::Io(format!(
        "RS-4010: durable source checkpoint operation failed: {error}; next steps: keep the source paused, recover the highest committed checkpoint, then retry"
    ))
}
