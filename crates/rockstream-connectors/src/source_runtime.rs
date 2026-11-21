//! Fenced, checkpoint-coupled source runtime coordination.

use std::collections::{BTreeMap, HashSet};

use rockstream_storage::WriteBatch;
use rockstream_types::ids::ConnectorId;
use rockstream_types::timestamp::Epoch;
use rockstream_verified::persistence;

use crate::source_connector::{PollDeltaResult, SnapshotStream, SourceConnector, SourceError};
use crate::source_epoch::SnapshotDeltaFence;
use crate::source_epoch::{
    BackfillLifecycle, CoupledBatchDescriptor, OffsetToken, SourceCheckpoint,
    SourceCheckpointStore, SourceEpochRegistry,
};
use crate::{KafkaSource, PgOutputEvent, PostgresCdcSource};

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
    /// The latest durable checkpoint must be ACKed before accepting more input.
    pending_ack: bool,
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
            pending_ack: false,
            blocked_reason: None,
        }
    }

    /// Capture the source's snapshot/delta fence through the recovered owner.
    /// The gateway owns scheduling and turns the returned batches into its M3
    /// write; this coordinator remains the sole checkpoint/ack authority.
    pub async fn capture_snapshot_delta_fence(
        &mut self,
    ) -> Result<SnapshotDeltaFence, SourceError> {
        self.require_unblocked()?;
        self.source.capture_snapshot_delta_fence(None).await
    }

    /// Begin the bounded snapshot stream at the supplied frontier.
    pub async fn start_snapshot(
        &mut self,
        fence: &SnapshotDeltaFence,
        after: Option<OffsetToken>,
        max_rows: usize,
    ) -> Result<SnapshotStream, SourceError> {
        self.require_unblocked()?;
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
        self.require_unblocked()?;
        self.source
            .poll_delta(after, max_bytes, credits_available, None)
            .await
    }

    /// The next source epoch that can be committed under the active lease.
    pub fn next_epoch(&self) -> Result<Epoch, SourceError> {
        persistence::next_epoch(self.source_epochs.current_epoch()).ok_or_else(|| {
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
        // Even a previously ACKed checkpoint is safe to acknowledge again; on
        // recovery we cannot infer whether the broker observed the old ACK.
        self.pending_ack = checkpoint.is_some();
        self.in_flight_epochs = usize::from(self.pending_ack);
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
        self.require_unblocked()?;
        let mut batch = WriteBatch::new();
        self.checkpoint_store
            .append_backfill_lifecycle(&mut batch, lifecycle)
            .map_err(storage_error)?;
        self.checkpoint_store
            .commit_raw_batch(batch)
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
        // Acquiring another lease must not silently clear a pending ACK or a
        // blocked reason; only explicit recovery can unblock this source.
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
        self.blocked_reason = Some(reason.clone());
        self.source.pause(reason).await?;
        Ok(())
    }

    /// Rebuild the committed checkpoint before allowing a new owner lease.
    pub async fn resume(&mut self) -> Result<Option<SourceCheckpoint>, SourceError> {
        let checkpoint = self.recover().await?;
        self.source.resume().await?;
        if !self.pending_ack {
            self.blocked_reason = None;
        }
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
            ).await;
        }
        let Some(next_epoch) = persistence::next_epoch(self.source_epochs.current_epoch()) else {
            return self.block("RS-4018: source epoch exhausted; next_steps: create a new connector before retrying").await;
        };
        if !persistence::epoch_is_admissible(
            self.source_epochs.current_epoch(),
            epoch,
            false,
            true,
            false,
        ) {
            return self.block(&format!(
                "RS-4015: source epoch {epoch} is not the next fenced epoch {}; next steps: recover the committed checkpoint and retry",
                next_epoch
            )).await;
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
                return self.block(&storage_error(error).to_string()).await;
            }
        };
        self.add_progress_state(&mut m3_input, epoch);
        if let Err(error) = self.checkpoint_store.commit_m3(m3_input).await {
            self.in_flight_epochs -= 1;
            return self.block(&storage_error(error).to_string()).await;
        }

        self.source_epochs
            .commit_epoch_monotonic(
                self.source_epochs
                    .prepare_commit_at(epoch, BTreeMap::from([(0, committed.token.clone())]))
                    .map_err(|error| SourceError::Io(error.to_string()))?,
            )
            .map_err(|error| SourceError::Io(error.to_string()))?;
        self.committed_offset = committed.token.clone();
        self.pending_ack = true;

        if let Err(error) = self.source.commit_offset(epoch, committed.token).await {
            return self.block(&format!(
                "RS-4016: M3 source input committed but upstream acknowledgement failed: {error}; next steps: recover the checkpoint and retry acknowledgement"
            )).await;
        }
        self.in_flight_epochs = 0;
        self.pending_ack = false;
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
            ).await;
        }
        if self.in_flight_epochs >= SOURCE_RUNTIME_MAX_IN_FLIGHT_EPOCHS {
            return self.block(
                "RS-4014: source_runtime_in_flight_epochs reached SOURCE_RUNTIME_MAX_IN_FLIGHT_EPOCHS; next_steps: wait for upstream acknowledgements before polling more input",
            ).await;
        }
        let Some(next_epoch) = persistence::next_epoch(self.source_epochs.current_epoch()) else {
            return self.block("RS-4018: source epoch exhausted; next_steps: create a new connector before retrying").await;
        };
        if !persistence::epoch_is_admissible(
            self.source_epochs.current_epoch(),
            epoch,
            false,
            true,
            false,
        ) {
            return self.block(&format!(
                "RS-4015: source epoch {epoch} is not the next fenced epoch {}; next steps: recover the committed checkpoint and retry",
                next_epoch
            )).await;
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
                return self.block(&storage_error(error).to_string()).await;
            }
        };
        if let Err(error) = self
            .checkpoint_store
            .append_backfill_lifecycle(&mut m3_input, &lifecycle)
        {
            self.in_flight_epochs -= 1;
            return self.block(&storage_error(error).to_string()).await;
        }
        self.add_progress_state(&mut m3_input, epoch);
        if let Err(error) = self.checkpoint_store.commit_m3(m3_input).await {
            self.in_flight_epochs -= 1;
            return self.block(&storage_error(error).to_string()).await;
        }

        self.source_epochs
            .commit_epoch_monotonic(
                self.source_epochs
                    .prepare_commit_at(epoch, BTreeMap::from([(0, committed.token.clone())]))
                    .map_err(|error| SourceError::Io(error.to_string()))?,
            )
            .map_err(|error| SourceError::Io(error.to_string()))?;
        self.committed_offset = committed.token.clone();
        self.pending_ack = true;
        if let Err(error) = self.source.commit_offset(epoch, committed.token).await {
            return self.block(&format!(
                "RS-4016: M3 source input committed but upstream acknowledgement failed: {error}; next_steps: recover the checkpoint and retry acknowledgement"
            )).await;
        }
        self.in_flight_epochs = 0;
        self.pending_ack = false;
        Ok(())
    }

    /// Commit one replayable upstream transaction as one caller-built M3
    /// batch, then acknowledge it. Unlike generic sources, PostgreSQL's slot
    /// is the replay log, so no durable `Prepared` checkpoint is written.
    pub async fn commit_replayable_epoch(
        &mut self,
        lease: &SourceOwnerLease,
        epoch: Epoch,
        offset: OffsetToken,
        lifecycles: &[BackfillLifecycle],
        mut m3_input: WriteBatch,
    ) -> Result<(), SourceError> {
        self.require_active_lease(lease)?;
        if self.in_flight_epochs != 0 {
            return self.block(
                "RS-4014: replayable source already has an unacknowledged M3 epoch; next steps: recover and acknowledge it before polling",
            ).await;
        }
        let entry = self
            .source_epochs
            .prepare_commit_at(epoch, BTreeMap::from([(0, offset.clone())]))
            .map_err(|error| SourceError::Io(error.to_string()))?;
        let mut views = HashSet::with_capacity(lifecycles.len());
        for lifecycle in lifecycles {
            if lifecycle.phase != crate::source_epoch::BackfillPhase::Running
                || lifecycle.cursor.committed_epoch != epoch
                || lifecycle.cursor.last_key != offset.as_bytes()
                || lifecycle.published_frontier != Some(epoch)
                || !views.insert(lifecycle.view_name.as_str())
            {
                return self.block(
                    "RS-4019: replayable source lifecycle coverage is duplicate or mismatched; next_steps: restore attachments from the committed source checkpoint",
                ).await;
            }
            self.checkpoint_store
                .append_backfill_lifecycle(&mut m3_input, lifecycle)
                .map_err(storage_error)?;
        }
        let committed = self
            .checkpoint_store
            .append_replayable_committed(&mut m3_input, epoch, offset)
            .map_err(storage_error)?;
        self.add_progress_state(&mut m3_input, epoch);
        self.in_flight_epochs = 1;
        if let Err(error) = self.checkpoint_store.commit_m3(m3_input).await {
            self.in_flight_epochs = 0;
            return self.block(&storage_error(error).to_string()).await;
        }

        self.source_epochs
            .commit_epoch_monotonic(entry)
            .map_err(|error| SourceError::Io(error.to_string()))?;
        self.committed_offset = committed.token.clone();
        self.pending_ack = true;
        if let Err(error) = self.source.commit_offset(epoch, committed.token).await {
            return self.block(&format!(
                "RS-4016: M3 source transaction committed but upstream acknowledgement failed: {error}; next_steps: recover and acknowledge the durable checkpoint"
            )).await;
        }
        self.in_flight_epochs = 0;
        self.pending_ack = false;
        Ok(())
    }

    /// Atomically publish a newly attached view at the current source
    /// checkpoint without advancing or acknowledging the upstream source.
    pub async fn commit_attachment(
        &mut self,
        lease: &SourceOwnerLease,
        lifecycle: &BackfillLifecycle,
        mut m3_input: WriteBatch,
    ) -> Result<(), SourceError> {
        self.require_active_lease(lease)?;
        if lifecycle.phase != crate::source_epoch::BackfillPhase::Running
            || lifecycle.cursor.last_key != self.committed_offset.as_bytes()
            || lifecycle.published_frontier != Some(lifecycle.cursor.committed_epoch)
        {
            return self.block(
                "RS-4019: pgoutput attachment cursor does not match the current source checkpoint",
            ).await;
        }
        self.checkpoint_store
            .append_backfill_lifecycle(&mut m3_input, lifecycle)
            .map_err(storage_error)?;
        self.add_progress_state(&mut m3_input, lifecycle.cursor.committed_epoch);
        self.checkpoint_store
            .commit_raw_batch(m3_input)
            .await
            .map_err(storage_error)
    }

    /// Reconcile the latest durable M3 checkpoint with the upstream source.
    /// This is safe to call with no new records: it only ACKs the exact durable
    /// token, then resumes a paused source. It never replays view effects.
    pub async fn acknowledge_recovered(
        &mut self,
        lease: &SourceOwnerLease,
    ) -> Result<(), SourceError> {
        // Recovery is the one operation allowed through the blocked gate, but
        // the owner fence must still hold.
        self.require_owner_lease(lease)?;
        if !self.pending_ack {
            return Ok(());
        }
        let epoch = self.source_epochs.current_epoch();
        if epoch != 0 {
            if let Err(error) = self
                .source
                .commit_offset(epoch, self.committed_offset.clone())
                .await
            {
                return self.block(&format!(
                    "RS-4016: recovered source checkpoint acknowledgement failed: {error}; next steps: retain ownership and retry acknowledgement"
                )).await;
            }
        }
        if self.blocked_reason.is_some() {
            // A successful ACK is not enough when the connector is still
            // paused. Keep the gate closed if the asynchronous resume fails.
            self.source.resume().await.map_err(|error| {
                SourceError::Io(format!(
                    "RS-4016: durable source ACK succeeded but resume failed: {error}; next steps: retry recovery"
                ))
            })?;
        }
        self.in_flight_epochs = 0;
        self.pending_ack = false;
        self.blocked_reason = None;
        Ok(())
    }

    /// Whether polling must wait for the durable checkpoint's broker ACK.
    pub fn has_pending_ack(&self) -> bool {
        self.pending_ack
    }

    pub fn committed_offset(&self) -> &OffsetToken {
        &self.committed_offset
    }

    pub fn committed_epoch(&self) -> Epoch {
        self.source_epochs.current_epoch()
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

    fn require_owner_lease(&self, lease: &SourceOwnerLease) -> Result<(), SourceError> {
        if self.active_lease.as_ref() != Some(lease) {
            return Err(SourceError::Io(
                "RS-4013: source owner lease is fenced or inactive; next steps: recover the checkpoint and acquire a new owner lease".to_string(),
            ));
        }
        Ok(())
    }

    fn require_unblocked(&self) -> Result<(), SourceError> {
        if let Some(reason) = &self.blocked_reason {
            return Err(SourceError::Io(format!(
                "RS-4016: source is blocked pending recovery: {reason}"
            )));
        }
        if self.pending_ack {
            return Err(SourceError::Io(
                "RS-4016: durable checkpoint requires upstream acknowledgement before more input; next steps: acknowledge_recovered".to_string(),
            ));
        }
        Ok(())
    }

    fn require_active_lease(&self, lease: &SourceOwnerLease) -> Result<(), SourceError> {
        self.require_owner_lease(lease)?;
        self.require_unblocked()
    }

    async fn block<T>(&mut self, reason: &str) -> Result<T, SourceError> {
        self.blocked_reason = Some(reason.to_string());
        if let Err(error) = self.source.pause(reason.to_string()).await {
            // The coordinator gate remains closed even when the connector
            // cannot pause its partitions (e.g. during a Kafka rebalance).
            self.blocked_reason = Some(format!("{reason}; upstream pause failed: {error}"));
        }
        Err(SourceError::Io(reason.to_string()))
    }
}

impl SourceRuntimeCoordinator<KafkaSource> {
    pub fn kafka_secret_binding(
        &self,
        lease: &SourceOwnerLease,
    ) -> Result<(Option<&str>, u64), SourceError> {
        // Rotation remains possible while a failed ACK is blocking ingestion.
        self.require_owner_lease(lease)?;
        Ok(self.source.secret_binding())
    }

    pub fn rotate_kafka_secret_at_epoch(
        &mut self,
        lease: &SourceOwnerLease,
        secret_name: &str,
        version: u64,
        username: &str,
        password: &str,
    ) -> Result<bool, SourceError> {
        self.require_owner_lease(lease)?;
        self.source
            .rotate_sasl_plain_credentials(secret_name, version, username, password)
    }
}

impl SourceRuntimeCoordinator<PostgresCdcSource> {
    /// Open PostgreSQL only after recovery and the source owner fence.
    /// Opening or validating the slot before `acknowledge_recovered` is
    /// required during startup recovery while `pending_ack` is still set.
    pub async fn open_pgoutput(&mut self, lease: &SourceOwnerLease) -> Result<(), SourceError> {
        self.require_owner_lease(lease)?;
        if let Some(reason) = &self.blocked_reason {
            return Err(SourceError::Io(format!(
                "RS-4016: source is blocked pending recovery: {reason}"
            )));
        }
        if !self.recovered {
            return Err(SourceError::Io(
                "RS-4012: pgoutput cannot open before checkpoint recovery".to_string(),
            ));
        }
        self.source
            .open_pgoutput(self.source_epochs.current_epoch() != 0)
            .await
    }

    /// Poll one decoded event while retaining acknowledgement authority here.
    pub async fn poll_pgoutput_event(
        &mut self,
        lease: &SourceOwnerLease,
        max_messages: usize,
    ) -> Result<Option<PgOutputEvent>, SourceError> {
        self.require_active_lease(lease)?;
        self.source.poll_pgoutput_event(max_messages).await
    }

    pub async fn pgoutput_confirmed_lsn(
        &self,
        lease: &SourceOwnerLease,
    ) -> Result<Option<crate::PgLsn>, SourceError> {
        self.require_owner_lease(lease)?;
        self.source.confirmed_flush_lsn().await
    }

    pub async fn capture_pgoutput_source_snapshot(
        &mut self,
        lease: &SourceOwnerLease,
        relations: &[(String, arrow::datatypes::SchemaRef)],
    ) -> Result<crate::PgOutputSourceSnapshot, SourceError> {
        self.require_active_lease(lease)?;
        self.source.capture_source_snapshot(relations).await
    }

    /// Reject unsafe publication identities for every imported table under
    /// the active source lease, including attachments made after bootstrap.
    pub async fn validate_pgoutput_table(
        &self,
        lease: &SourceOwnerLease,
        table: &str,
    ) -> Result<(), SourceError> {
        self.require_owner_lease(lease)?;
        self.source.validate_pgoutput_table(table).await
    }

    pub async fn pgoutput_relation_column_policies(
        &self,
        lease: &SourceOwnerLease,
        relation_id: u32,
    ) -> Result<Vec<(bool, bool)>, SourceError> {
        self.require_owner_lease(lease)?;
        self.source.relation_column_policies(relation_id).await
    }

    pub fn pgoutput_secret_binding(
        &self,
        lease: &SourceOwnerLease,
    ) -> Result<(Option<&str>, u64), SourceError> {
        self.require_owner_lease(lease)?;
        Ok(self.source.secret_binding())
    }

    pub async fn rotate_pgoutput_secret_at_epoch(
        &mut self,
        lease: &SourceOwnerLease,
        secret_name: &str,
        version: u64,
        password: String,
    ) -> Result<bool, SourceError> {
        self.require_owner_lease(lease)?;
        let has_checkpoint = self.committed_epoch() != 0;
        self.source
            .rotate_pgoutput_secret_at_epoch(secret_name, version, password, has_checkpoint)
            .await
    }

    pub fn close_pgoutput(&mut self, lease: &SourceOwnerLease) -> bool {
        if self.active_lease.as_ref() != Some(lease) {
            return false;
        }
        self.source.close_pgoutput();
        self.active_lease = None;
        true
    }
}

fn storage_error(error: rockstream_storage::StorageError) -> SourceError {
    SourceError::Io(format!(
        "RS-4010: durable source checkpoint operation failed: {error}; next steps: keep the source paused, recover the highest committed checkpoint, then retry"
    ))
}

impl<S: SourceConnector> SourceRuntimeCoordinator<S> {
    fn add_progress_state(&self, batch: &mut WriteBatch, epoch: Epoch) {
        let key = format!("op_state/source_runtime/{}/{}", self.connector_id.0, epoch);
        if !CoupledBatchDescriptor::inspect(batch).has_state {
            batch.put(key.as_bytes(), &epoch.to_be_bytes());
        }
    }
}
