//! Kafka exactly-once sink (CheckBeforeCommit profile, v0.21).
//!
//! Implements the 2PC exactly-once sink protocol (DESIGN.md §11.4) for Kafka.
//! Uses `CheckBeforeCommit` idempotency profile: recovery queries the Kafka
//! topic for the epoch marker before deciding whether to commit.
//!
//! ## Bounded resources
//!
//! - `staged_epochs_count`: fill-level metric for staged (pre-committed) epochs.
//! - Backpressure applied when `staged_epochs_count >= max_staged_epochs`.
//!
//! ## Crash recovery paths
//!
//! | Crash point | Recovery action |
//! |---|---|
//! | Before pre-commit | Idle; epoch data reproduced from source. |
//! | Between pre-commit and commit | CheckBeforeCommit: query topic; if absent, new transaction. |
//! | During commit | CheckBeforeCommit: query topic; already present → Committed. |

use std::collections::BTreeSet;

use rockstream_types::ids::ConnectorId;
use rockstream_types::sink::{RecoveryAction, SinkIdempotencyProfile, SinkState};
use rockstream_types::timestamp::Epoch;

use crate::sink_connector::{
    assert_epoch_committed_only_after_cluster_checkpoint, assert_no_duplicate_delivery,
    assert_recovery_dispatch_idempotent, SinkConnector, SinkError,
};

/// Maximum number of epochs that may be in the staged (pre-committed) state
/// simultaneously. Exceeding this triggers backpressure.
pub const KAFKA_SINK_MAX_STAGED_EPOCHS: usize = 5;

/// In-memory Kafka sink implementing `CheckBeforeCommit` idempotency.
///
/// In production this would wrap a real Kafka producer (e.g. `rdkafka`).
/// Here it is a self-contained state machine that can be driven by tests.
pub struct KafkaSink {
    connector_id: ConnectorId,
    /// Epochs that have been delivered to the "external Kafka".
    delivered_epochs: BTreeSet<Epoch>,
    /// Epochs currently staged (pre-committed, awaiting cluster checkpoint).
    staged_epochs: BTreeSet<Epoch>,
    /// Maximum staged epochs before backpressure.
    max_staged_epochs: usize,
    /// Fill-level metric: current count of staged epochs.
    staged_epochs_count: usize,
    /// Simulated Kafka cluster_committed horizon (for checkpoint-coupling assertion).
    cluster_committed: Epoch,
}

impl KafkaSink {
    pub fn new(connector_id: ConnectorId) -> Self {
        Self {
            connector_id,
            delivered_epochs: BTreeSet::new(),
            staged_epochs: BTreeSet::new(),
            max_staged_epochs: KAFKA_SINK_MAX_STAGED_EPOCHS,
            staged_epochs_count: 0,
            cluster_committed: 0,
        }
    }

    /// Update the known `cluster_committed` horizon (called by the epoch-commit loop
    /// after a cluster checkpoint succeeds).
    pub fn set_cluster_committed(&mut self, epoch: Epoch) {
        self.cluster_committed = epoch;
    }

    /// Fill-level metric: current count of staged (pre-committed) epochs.
    ///
    /// Name: `kafka_sink_staged_epochs_count` (DESIGN.md §11.4).
    pub fn kafka_sink_staged_epochs_count(&self) -> usize {
        self.staged_epochs_count
    }

    /// Whether backpressure should be applied to the source connector.
    pub fn backpressure_active(&self) -> bool {
        self.staged_epochs_count >= self.max_staged_epochs
    }

    /// Simulate a "check" query to the Kafka topic for a given epoch.
    ///
    /// Returns `true` if the epoch has already been delivered (for
    /// `CheckBeforeCommit` recovery path).
    pub fn check_epoch_delivered(&self, epoch: Epoch) -> bool {
        self.delivered_epochs.contains(&epoch)
    }
}

impl SinkConnector for KafkaSink {
    fn idempotency_profile(&self) -> SinkIdempotencyProfile {
        SinkIdempotencyProfile::CheckBeforeCommit
    }

    fn pre_commit(&mut self, epoch: Epoch, row_count: usize) -> Result<SinkState, SinkError> {
        if self.backpressure_active() {
            return Err(SinkError::PreCommitFailed {
                epoch,
                reason: format!(
                    "backpressure: staged_epochs={} >= max={}",
                    self.staged_epochs_count, self.max_staged_epochs
                ),
            });
        }
        // Begin a Kafka producer transaction (simulated: record in staged set).
        let txn_id = format!("kafka-txn-{}-epoch-{}", self.connector_id.0, epoch);
        self.staged_epochs.insert(epoch);
        self.staged_epochs_count += 1;
        Ok(SinkState::PreCommitted {
            staged_rows: row_count,
            pending_handle: txn_id.into_bytes(),
        })
    }

    fn commit(&mut self, epoch: Epoch, state: &SinkState) -> Result<(), SinkError> {
        // M3-S3: must not commit before cluster checkpoint.
        assert_epoch_committed_only_after_cluster_checkpoint(
            self.connector_id,
            epoch,
            self.cluster_committed,
        );
        // M3-S1: must not deliver duplicate.
        assert_no_duplicate_delivery(self.connector_id, epoch, &self.delivered_epochs);

        match state {
            SinkState::PreCommitted { .. } | SinkState::Committed => {
                // Finalize: mark as delivered in Kafka.
                self.delivered_epochs.insert(epoch);
                self.staged_epochs.remove(&epoch);
                if self.staged_epochs_count > 0 {
                    self.staged_epochs_count -= 1;
                }
                Ok(())
            }
            SinkState::Idle => Err(SinkError::CommitFailed {
                epoch,
                reason: "commit called on Idle sink state".to_string(),
            }),
        }
    }

    fn abort(&mut self, epoch: Epoch) -> Result<(), SinkError> {
        self.staged_epochs.remove(&epoch);
        if self.staged_epochs_count > 0 {
            self.staged_epochs_count -= 1;
        }
        Ok(())
    }

    fn recover(&mut self, action: RecoveryAction) -> Result<(), SinkError> {
        match &action {
            RecoveryAction::Noop => Ok(()),
            RecoveryAction::RerunCommit {
                epoch,
                profile: _,
                pending_handle,
            } => {
                let epoch = *epoch;
                // CheckBeforeCommit: check the Kafka topic first.
                if self.check_epoch_delivered(epoch) {
                    // Already delivered; mark as committed (idempotent no-op).
                    let final_state = SinkState::Committed;
                    assert_recovery_dispatch_idempotent(self.connector_id, &action, &final_state);
                    return Ok(());
                }
                // Not yet delivered: begin a new transaction and commit.
                let state = SinkState::PreCommitted {
                    staged_rows: 0,
                    pending_handle: pending_handle.clone(),
                };
                // Recovery commit does not check cluster_committed (the
                // checkpoint already succeeded before recovery).
                self.delivered_epochs.insert(epoch);
                let final_state = SinkState::Committed;
                assert_recovery_dispatch_idempotent(self.connector_id, &action, &final_state);
                Ok(())
            }
        }
    }
}

impl KafkaSink {
    // ─── Test helpers ─────────────────────────────────────────────────────────

    /// Clear the staged epochs set (simulates ephemeral state loss on crash).
    pub fn staged_epochs_clear_for_test(&mut self) {
        self.staged_epochs.clear();
        self.staged_epochs_count = 0;
    }

    /// Inject a partial delivery (simulates crash-during-commit where the
    /// epoch was delivered but sink_state/ was not updated).
    pub fn inject_partial_delivery_for_test(&mut self, epoch: Epoch) {
        self.delivered_epochs.insert(epoch);
    }

    /// Return the number of delivered epochs (for assertion in tests).
    pub fn delivered_count_for_test(&self) -> usize {
        self.delivered_epochs.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sink() -> KafkaSink {
        let mut s = KafkaSink::new(ConnectorId(1));
        s.set_cluster_committed(100); // generous horizon for most tests
        s
    }

    // ── Happy path ────────────────────────────────────────────────────────────

    #[test]
    fn happy_path_pre_commit_then_commit() {
        let mut sink = make_sink();
        let state = sink.pre_commit(1, 50).unwrap();
        assert!(state.needs_recovery_commit());
        assert_eq!(sink.kafka_sink_staged_epochs_count(), 1);
        sink.commit(1, &state).unwrap();
        assert_eq!(sink.kafka_sink_staged_epochs_count(), 0);
        assert!(sink.check_epoch_delivered(1));
    }

    // ── Crash before pre-commit ───────────────────────────────────────────────

    #[test]
    fn crash_before_precommit_noop_recovery() {
        let mut sink = make_sink();
        // No pre-commit staged; Idle state.
        let action = RecoveryAction::Noop;
        sink.recover(action).unwrap();
        // Nothing delivered.
        assert!(!sink.check_epoch_delivered(1));
    }

    // ── Crash between pre-commit and commit ───────────────────────────────────

    #[test]
    fn crash_between_precommit_and_commit_recovery() {
        let mut sink = make_sink();
        let state = sink.pre_commit(2, 10).unwrap();
        // Simulate crash: ephemeral staged state lost (abort the in-memory set).
        sink.staged_epochs.clear();
        sink.staged_epochs_count = 0;
        // Recovery: CheckBeforeCommit — not yet delivered → re-run commit.
        let action = RecoveryAction::RerunCommit {
            epoch: 2,
            profile: SinkIdempotencyProfile::CheckBeforeCommit,
            pending_handle: state.pending_handle().to_vec(),
        };
        sink.recover(action).unwrap();
        assert!(sink.check_epoch_delivered(2));
    }

    // ── Crash during commit ───────────────────────────────────────────────────

    #[test]
    fn crash_during_commit_already_delivered_recovery() {
        let mut sink = make_sink();
        let state = sink.pre_commit(3, 5).unwrap();
        // Simulate: commit partially succeeded (delivered but not finalized).
        sink.delivered_epochs.insert(3);
        // Recovery: CheckBeforeCommit — already delivered → no-op.
        let action = RecoveryAction::RerunCommit {
            epoch: 3,
            profile: SinkIdempotencyProfile::CheckBeforeCommit,
            pending_handle: vec![],
        };
        sink.recover(action).unwrap();
        // Still exactly one delivery.
        assert_eq!(sink.delivered_epochs.len(), 1);
        assert!(sink.check_epoch_delivered(3));
    }

    // ── Backpressure ──────────────────────────────────────────────────────────

    #[test]
    fn backpressure_when_staged_epochs_full() {
        let mut sink = KafkaSink {
            connector_id: ConnectorId(1),
            delivered_epochs: BTreeSet::new(),
            staged_epochs: BTreeSet::new(),
            max_staged_epochs: 2,
            staged_epochs_count: 0,
            cluster_committed: 100,
        };
        sink.pre_commit(1, 10).unwrap();
        sink.pre_commit(2, 10).unwrap();
        // Now at capacity.
        assert!(sink.backpressure_active());
        let err = sink.pre_commit(3, 10);
        assert!(err.is_err());
    }

    // ── Abort ─────────────────────────────────────────────────────────────────

    #[test]
    fn abort_clears_staged_epoch() {
        let mut sink = make_sink();
        sink.pre_commit(10, 1).unwrap();
        assert_eq!(sink.kafka_sink_staged_epochs_count(), 1);
        sink.abort(10).unwrap();
        assert_eq!(sink.kafka_sink_staged_epochs_count(), 0);
        assert!(!sink.check_epoch_delivered(10));
    }
}

// ─── Extension trait for test access to pending_handle ───────────────────────

trait SinkStatePendingHandle {
    fn pending_handle(&self) -> &[u8];
}

impl SinkStatePendingHandle for SinkState {
    fn pending_handle(&self) -> &[u8] {
        match self {
            SinkState::PreCommitted { pending_handle, .. } => pending_handle,
            _ => &[],
        }
    }
}
