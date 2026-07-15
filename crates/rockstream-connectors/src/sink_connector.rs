//! Exactly-once sink connector trait and 2PC protocol (DESIGN.md §11.4, v0.21).
//!
//! Every sink connector implements [`SinkConnector`] which provides:
//! - `pre_commit`: stage rows in a sink-specific transactional buffer; record
//!   `sink_state/` entry in the shard's `WriteBatch`.
//! - `commit`: finalize the staged transaction after the cluster checkpoint.
//! - `abort`: discard a staged transaction.
//! - `recover`: re-run the commit path based on `SinkIdempotencyProfile`.
//!
//! ## Paired assertions (M3)
//!
//! | FizzBee invariant | Assertion |
//! |---|---|
//! | M3-S1 | [`assert_no_duplicate_delivery`] |
//! | M3-S2 | [`assert_no_lost_delivery_after_checkpoint`] |
//! | M3-S3 | [`assert_epoch_committed_only_after_cluster_checkpoint`] |
//! | M3-S4 | [`assert_recovery_dispatch_idempotent`] |
//!
//! ## Paired assertions (M5, v0.43)
//!
//! | FizzBee invariant | Assertion |
//! |---|---|
//! | M5-S1 / M5-S3 | [`assert_commit_pointer_atomic`] |

use std::collections::BTreeSet;

use rockstream_types::ids::ConnectorId;
use rockstream_types::sink::{RecoveryAction, SinkIdempotencyProfile, SinkState};
use rockstream_types::timestamp::Epoch;

// ─── SinkConnector trait ──────────────────────────────────────────────────────

/// Trait implemented by every exactly-once sink connector.
///
/// The concrete implementations (Kafka, object-store) live in the sub-modules
/// [`kafka_sink`] and [`object_store_sink`].
pub trait SinkConnector: Send + Sync {
    /// Returns the idempotency profile for this connector.
    fn idempotency_profile(&self) -> SinkIdempotencyProfile;

    /// Return whether the connector should flush its currently buffered rows.
    ///
    /// The default implementation preserves the existing behavior of sinks that
    /// flush every epoch.
    fn should_flush(&self, _bytes_buffered: u64, _epochs_buffered: u64) -> bool {
        true
    }

    /// Stage rows for the given epoch (pre-commit phase).
    ///
    /// Implementations must:
    /// 1. Begin a connector-specific transaction (Kafka: initTransaction;
    ///    S3: write to `_pending/{epoch}/`).
    /// 2. Write staged row count + opaque handle to the returned `SinkState`.
    ///
    /// The returned `SinkState` must be committed atomically with the shard's
    /// `WriteBatch` by the epoch-commit loop.
    fn pre_commit(&mut self, epoch: Epoch, row_count: usize) -> Result<SinkState, SinkError>;

    /// Finalize the commit after the cluster checkpoint succeeds.
    ///
    /// Implementations must be idempotent: calling `commit` twice for the
    /// same epoch (after a crash) must produce the same result.
    fn commit(&mut self, epoch: Epoch, state: &SinkState) -> Result<(), SinkError>;

    /// Abort the staged transaction (checkpoint aborted or source reset).
    fn abort(&mut self, epoch: Epoch) -> Result<(), SinkError>;

    /// Recover from a crash.
    ///
    /// The caller reads the durable `SinkState` and passes the appropriate
    /// `RecoveryAction`. The connector re-runs commit if needed, following
    /// its `SinkIdempotencyProfile`.
    fn recover(&mut self, action: RecoveryAction) -> Result<(), SinkError>;
}

// ─── SinkError ────────────────────────────────────────────────────────────────

/// Error from a sink connector operation.
#[derive(Debug)]
pub enum SinkError {
    /// Pre-commit failed.
    PreCommitFailed { epoch: Epoch, reason: String },
    /// Commit failed after pre-commit; recovery required.
    CommitFailed { epoch: Epoch, reason: String },
    /// Duplicate delivery detected and blocked (CheckBeforeCommit).
    DuplicateBlocked { epoch: Epoch },
    /// Generic sink I/O error.
    Io(String),
}

impl std::fmt::Display for SinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PreCommitFailed { epoch, reason } => {
                write!(
                    f,
                    "RS-4003: sink pre-commit failed for epoch {epoch}: {reason}"
                )
            }
            Self::CommitFailed { epoch, reason } => {
                write!(f, "RS-4004: sink commit failed for epoch {epoch}: {reason}")
            }
            Self::DuplicateBlocked { epoch } => {
                write!(f, "RS-4005: duplicate delivery blocked for epoch {epoch}")
            }
            Self::Io(msg) => write!(f, "RS-4002: sink I/O error: {msg}"),
        }
    }
}

impl std::error::Error for SinkError {}

// ─── M3 Paired Runtime Assertions ────────────────────────────────────────────

/// M3-S1 paired assertion: assert that an epoch has not already been delivered
/// to the external system before calling `commit`.
///
/// # Panics
///
/// Panics if `epoch` is already in `delivered_epochs`.
pub fn assert_no_duplicate_delivery(
    connector_id: ConnectorId,
    epoch: Epoch,
    delivered_epochs: &BTreeSet<Epoch>,
) {
    assert!(
        !delivered_epochs.contains(&epoch),
        "RS-4005: M3-S1 violation — duplicate delivery: \
         connector={connector_id}, epoch={epoch} is already in delivered_epochs. \
         next_steps: This is an exactly-once invariant violation; check sink idempotency profile \
         and recovery path."
    );
}

/// M3-S3 paired assertion: assert that a sink epoch transitions to Committed
/// only after the cluster has checkpointed that epoch.
///
/// # Panics
///
/// Panics if `cluster_committed < epoch`.
pub fn assert_epoch_committed_only_after_cluster_checkpoint(
    connector_id: ConnectorId,
    epoch: Epoch,
    cluster_committed: Epoch,
) {
    assert!(
        cluster_committed >= epoch,
        "RS-4004: M3-S3 violation — checkpoint-coupled commit: \
         connector={connector_id} attempted to finalize epoch={epoch} \
         but cluster_committed={cluster_committed}. \
         next_steps: The cluster checkpoint must complete before sink commit; \
         check CheckpointCoordinator and epoch-commit ordering."
    );
}

/// M3-S4 paired assertion: assert that the recovery dispatch for a given
/// idempotency profile produces a valid terminal state.
///
/// Specifically:
/// - `NativeIdempotent` / `FencingTokenRequired` / `CheckBeforeCommit` — after
///   recovery commit, `final_state` must be `Committed`.
/// - If `action` is `Noop`, `final_state` must be `Idle` or `Committed`.
///
/// # Panics
///
/// Panics if the terminal state is inconsistent with the recovery action.
pub fn assert_recovery_dispatch_idempotent(
    connector_id: ConnectorId,
    action: &RecoveryAction,
    final_state: &SinkState,
) {
    match action {
        RecoveryAction::Noop => {
            // Noop: final state must be Idle or Committed, never PreCommitted.
            assert!(
                !final_state.needs_recovery_commit(),
                "RS-4004: M3-S4 violation — recovery dispatch: \
                 connector={connector_id} Noop recovery left state in PreCommitted. \
                 next_steps: Recovery must fully resolve to Idle or Committed."
            );
        }
        RecoveryAction::RerunCommit { epoch, profile, .. } => {
            // RerunCommit: final state must be Committed.
            assert!(
                final_state.is_committed(),
                "RS-4004: M3-S4 violation — recovery dispatch: \
                 connector={connector_id} RerunCommit (epoch={epoch}, profile={profile}) \
                 did not result in Committed state (got {final_state:?}). \
                 next_steps: The commit path must be retried until successful."
            );
        }
    }
}

/// M3-S2 / M3-L1 paired assertion: assert that if a cluster checkpoint has
/// committed epoch `e`, the sink's durable state is not still `Idle` for `e`.
///
/// This is the safety prefix of M3-L1 (full liveness requires eventual
/// delivery; this checks that the durable staging step was not skipped).
///
/// # Panics
///
/// Panics if the cluster has committed `epoch` but the sink's durable state
/// for that epoch is `Idle` (indicating the pre-commit was skipped).
pub fn assert_no_lost_delivery_after_checkpoint(
    connector_id: ConnectorId,
    epoch: Epoch,
    cluster_committed: Epoch,
    sink_state: &SinkState,
) {
    if cluster_committed >= epoch {
        // The epoch has been checkpointed; the sink must have at least staged it.
        // M3-S2 / M3-L1: this is the safety prefix of M3-L1's liveness
        // property — checking the durable staging step was not skipped.
        assert!(
            !matches!(sink_state, SinkState::Idle),
            "RS-4004: M3-S2 violation — lost delivery: \
             connector={connector_id} epoch={epoch} was cluster-committed \
             (cluster_committed={cluster_committed}) but sink_state is Idle. \
             next_steps: The sink pre_commit must be called before the epoch \
             commit WriteBatch is flushed."
        );
    }
}

/// M5-S1 / M5-S3 paired assertion: assert that the final-prefix pointer
/// produced by the cold-tier sink's commit-time atomic rename is never
/// observed in a partially-written state (DESIGN.md §17.8 gap 1). A
/// truncated final-prefix object is both a duplicate-output risk (M5-S1: the
/// retried rename would otherwise re-append/re-fragment the same epoch's
/// output) and an atomicity violation (M5-S3), so this single check
/// implements both invariants per FIZZBEE_TEST_PLAN.md §3.7.
///
/// Real object stores (S3/GCS) do not implement the `_pending/{epoch}/` →
/// final-prefix rename as a single atomic operation; a crash mid-rename (or
/// a crashed/interrupted multi-part upload) can leave a truncated object
/// visible at the final key. This assertion compares the observed byte
/// length of the final-prefix object against the expected length staged
/// during `pre_commit`, so any such truncation is caught immediately rather
/// than being silently treated as a successful commit.
///
/// # Panics
///
/// Panics if `observed_len != expected_len`.
pub fn assert_commit_pointer_atomic(
    connector_id: ConnectorId,
    epoch: Epoch,
    observed_len: usize,
    expected_len: usize,
) {
    assert!(
        observed_len == expected_len,
        "RS-4006: M5-S1/M5-S3 violation — commit pointer not atomic: \
         connector={connector_id}, epoch={epoch} observed final-prefix object length={observed_len} \
         but expected={expected_len} (a partial/truncated write is visible at the final prefix). \
         next_steps: This indicates a crash mid-rename (DESIGN.md §17.8 gap 1). Recovery must \
         scan-and-delete the truncated object (never SlateDB range deletion) and retry the \
         rename with the full staged bytes before reporting the epoch as committed."
    );
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::sink::SinkState;

    // ── M3-S1: No duplicate delivery ──────────────────────────────────────────

    #[test]
    fn assert_no_duplicate_passes_when_not_delivered() {
        let delivered: BTreeSet<Epoch> = BTreeSet::new();
        // Must not panic.
        assert_no_duplicate_delivery(ConnectorId(1), 5, &delivered);
    }

    #[test]
    #[should_panic(expected = "RS-4005")]
    fn assert_no_duplicate_panics_when_already_delivered() {
        let mut delivered: BTreeSet<Epoch> = BTreeSet::new();
        delivered.insert(5);
        assert_no_duplicate_delivery(ConnectorId(1), 5, &delivered);
    }

    // ── M3-S3: Checkpoint-coupled commit ──────────────────────────────────────

    #[test]
    fn assert_checkpoint_coupled_passes_when_committed() {
        assert_epoch_committed_only_after_cluster_checkpoint(ConnectorId(1), 3, 3);
        assert_epoch_committed_only_after_cluster_checkpoint(ConnectorId(1), 2, 5);
    }

    #[test]
    #[should_panic(expected = "RS-4004")]
    fn assert_checkpoint_coupled_panics_when_not_committed() {
        assert_epoch_committed_only_after_cluster_checkpoint(ConnectorId(1), 5, 3);
    }

    // ── M3-S4: Recovery dispatch idempotency ──────────────────────────────────

    #[test]
    fn assert_recovery_noop_passes_with_idle() {
        assert_recovery_dispatch_idempotent(
            ConnectorId(1),
            &RecoveryAction::Noop,
            &SinkState::Idle,
        );
    }

    #[test]
    fn assert_recovery_noop_passes_with_committed() {
        assert_recovery_dispatch_idempotent(
            ConnectorId(1),
            &RecoveryAction::Noop,
            &SinkState::Committed,
        );
    }

    #[test]
    #[should_panic(expected = "RS-4004")]
    fn assert_recovery_noop_panics_with_pre_committed() {
        assert_recovery_dispatch_idempotent(
            ConnectorId(1),
            &RecoveryAction::Noop,
            &SinkState::PreCommitted {
                staged_rows: 5,
                pending_handle: vec![],
            },
        );
    }

    #[test]
    fn assert_recovery_rerun_passes_with_committed() {
        assert_recovery_dispatch_idempotent(
            ConnectorId(1),
            &RecoveryAction::RerunCommit {
                epoch: 3,
                profile: SinkIdempotencyProfile::NativeIdempotent,
                pending_handle: vec![],
            },
            &SinkState::Committed,
        );
    }

    #[test]
    #[should_panic(expected = "RS-4004")]
    fn assert_recovery_rerun_panics_with_idle() {
        assert_recovery_dispatch_idempotent(
            ConnectorId(1),
            &RecoveryAction::RerunCommit {
                epoch: 3,
                profile: SinkIdempotencyProfile::CheckBeforeCommit,
                pending_handle: vec![],
            },
            &SinkState::Idle,
        );
    }

    // ── M3-S2 / M3-L1 ─────────────────────────────────────────────────────────

    #[test]
    fn assert_no_lost_delivery_passes_when_staged() {
        assert_no_lost_delivery_after_checkpoint(
            ConnectorId(1),
            3,
            5,
            &SinkState::PreCommitted {
                staged_rows: 1,
                pending_handle: vec![],
            },
        );
        assert_no_lost_delivery_after_checkpoint(ConnectorId(1), 3, 5, &SinkState::Committed);
    }

    #[test]
    #[should_panic(expected = "RS-4004")]
    fn assert_no_lost_delivery_panics_when_idle_and_committed() {
        assert_no_lost_delivery_after_checkpoint(ConnectorId(1), 3, 5, &SinkState::Idle);
    }

    #[test]
    fn assert_no_lost_delivery_passes_when_epoch_not_yet_committed() {
        // cluster_committed=2 < epoch=5; no assertion needed.
        assert_no_lost_delivery_after_checkpoint(ConnectorId(1), 5, 2, &SinkState::Idle);
    }

    // ── M5-S1 / M5-S3: Commit pointer atomicity ───────────────────────────────

    #[test]
    fn assert_commit_pointer_atomic_passes_when_lengths_match() {
        assert_commit_pointer_atomic(ConnectorId(1), 3, 100, 100);
        assert_commit_pointer_atomic(ConnectorId(1), 3, 0, 0);
    }

    #[test]
    #[should_panic(expected = "RS-4006")]
    fn assert_commit_pointer_atomic_panics_on_truncated_object() {
        assert_commit_pointer_atomic(ConnectorId(1), 3, 42, 100);
    }

    // ── M3-S3 + test_m3_runtime_asserts (exhaustive paths) ───────────────────

    #[test]
    fn test_m3_runtime_asserts_all_paths() {
        let cid = ConnectorId(99);

        // M3-S1: fresh epoch not duplicate
        let delivered: BTreeSet<Epoch> = BTreeSet::new();
        assert_no_duplicate_delivery(cid, 1, &delivered);

        // M3-S3: cluster committed >= epoch
        assert_epoch_committed_only_after_cluster_checkpoint(cid, 1, 1);

        // M3-S4: noop → idle OK
        assert_recovery_dispatch_idempotent(cid, &RecoveryAction::Noop, &SinkState::Idle);

        // M3-S4: rerun → committed OK
        assert_recovery_dispatch_idempotent(
            cid,
            &RecoveryAction::RerunCommit {
                epoch: 1,
                profile: SinkIdempotencyProfile::NativeIdempotent,
                pending_handle: vec![],
            },
            &SinkState::Committed,
        );

        // M3-S2: epoch staged before cluster commit
        assert_no_lost_delivery_after_checkpoint(
            cid,
            1,
            1,
            &SinkState::PreCommitted {
                staged_rows: 5,
                pending_handle: vec![],
            },
        );
    }
}
