//! Minimal cold-tier sink scaffold for snapshot flush cadence.
//!
//! This type mirrors the in-memory staging style of [`crate::object_store_sink`]
//! while adding the v0.44 `should_flush` cadence hooks needed by the upcoming
//! Iceberg and Delta writers.

use std::collections::BTreeMap;

use rockstream_types::ids::ConnectorId;
use rockstream_types::sink::{RecoveryAction, SinkIdempotencyProfile, SinkState};
use rockstream_types::timestamp::Epoch;

use crate::sink_connector::{SinkConnector, SinkError};

pub const COLD_TIER_SINK_MAX_PENDING_EPOCHS: usize = 8;

#[derive(Debug)]
pub struct ColdTierSink {
    connector_id: ConnectorId,
    pending: BTreeMap<Epoch, Vec<u8>>,
    pending_epochs_count: usize,
    max_pending_epochs: usize,
    snapshot_interval_epochs: u64,
    snapshot_interval_ms: u64,
    now_ms: u64,
    last_flush_ms: u64,
}

impl ColdTierSink {
    pub fn new(
        connector_id: ConnectorId,
        snapshot_interval_epochs: u64,
        snapshot_interval_ms: u64,
    ) -> Self {
        Self {
            connector_id,
            pending: BTreeMap::new(),
            pending_epochs_count: 0,
            max_pending_epochs: COLD_TIER_SINK_MAX_PENDING_EPOCHS,
            snapshot_interval_epochs,
            snapshot_interval_ms,
            now_ms: 0,
            last_flush_ms: 0,
        }
    }

    pub fn cold_tier_pending_epochs_count(&self) -> usize {
        self.pending_epochs_count
    }

    pub fn backpressure_active(&self) -> bool {
        self.pending_epochs_count >= self.max_pending_epochs
    }

    pub fn pending_buffer_path(&self, epoch: Epoch) -> String {
        format!("connector/{}/pending_buffer/{epoch}", self.connector_id.0)
    }

    pub fn pending_exists(&self, epoch: Epoch) -> bool {
        self.pending.contains_key(&epoch)
    }

    pub fn advance_clock_ms(&mut self, delta_ms: u64) {
        self.now_ms = self.now_ms.saturating_add(delta_ms);
    }

    pub fn set_now_ms(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    pub fn mark_flushed(&mut self) {
        self.last_flush_ms = self.now_ms;
    }

    fn clear_pending_through(&mut self, epoch: Epoch) {
        let removed = self.pending.range(..=epoch).count();
        self.pending
            .retain(|pending_epoch, _| *pending_epoch > epoch);
        self.pending_epochs_count = self.pending_epochs_count.saturating_sub(removed);
    }
}

impl SinkConnector for ColdTierSink {
    fn idempotency_profile(&self) -> SinkIdempotencyProfile {
        SinkIdempotencyProfile::NativeIdempotent
    }

    fn should_flush(&self, _bytes_buffered: u64, epochs_buffered: u64) -> bool {
        let epoch_threshold_hit =
            self.snapshot_interval_epochs > 0 && epochs_buffered >= self.snapshot_interval_epochs;
        let time_threshold_hit = self.snapshot_interval_ms > 0
            && self.now_ms.saturating_sub(self.last_flush_ms) >= self.snapshot_interval_ms;
        epoch_threshold_hit || time_threshold_hit
    }

    fn pre_commit(&mut self, epoch: Epoch, row_count: usize) -> Result<SinkState, SinkError> {
        if self.backpressure_active() {
            return Err(SinkError::PreCommitFailed {
                epoch,
                reason: format!(
                    "backpressure: pending_epochs={} >= max={}",
                    self.pending_epochs_count, self.max_pending_epochs
                ),
            });
        }

        self.pending.insert(epoch, vec![0xCD; row_count.max(1)]);
        self.pending_epochs_count += 1;
        Ok(SinkState::PreCommitted {
            staged_rows: row_count,
            pending_handle: self.pending_buffer_path(epoch).into_bytes(),
        })
    }

    fn commit(&mut self, epoch: Epoch, _state: &SinkState) -> Result<(), SinkError> {
        self.clear_pending_through(epoch);
        self.mark_flushed();
        Ok(())
    }

    fn abort(&mut self, epoch: Epoch) -> Result<(), SinkError> {
        if self.pending.remove(&epoch).is_some() && self.pending_epochs_count > 0 {
            self.pending_epochs_count -= 1;
        }
        Ok(())
    }

    fn recover(&mut self, action: RecoveryAction) -> Result<(), SinkError> {
        match action {
            RecoveryAction::Noop => Ok(()),
            RecoveryAction::RerunCommit { epoch, .. } => {
                self.clear_pending_through(epoch);
                self.mark_flushed();
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_flush_every_third_epoch_and_stage_pending_buffer() {
        let mut sink = ColdTierSink::new(ConnectorId(3), 3, 0);
        let mut epochs_buffered = 0u64;
        let mut flushed_epochs = Vec::new();

        for epoch in 1..=9 {
            sink.pre_commit(epoch, 2).unwrap();
            epochs_buffered += 1;

            let should_flush = sink.should_flush(epochs_buffered * 2, epochs_buffered);
            if epoch % 3 == 0 {
                assert!(should_flush, "epoch {epoch} should flush");
                assert!(sink.pending_exists(epoch));
                sink.commit(epoch, &SinkState::Committed).unwrap();
                epochs_buffered = 0;
                flushed_epochs.push(epoch);
            } else {
                assert!(!should_flush, "epoch {epoch} should not flush");
            }
        }

        assert_eq!(flushed_epochs, vec![3, 6, 9]);
    }
}
