//! Exactly-once sink types for RockStream (v0.21).
//!
//! Defines the `SinkIdempotencyProfile` enum, `SinkState`, and `SinkEpochKey`
//! used by the 2PC sink protocol (DESIGN.md §11.4).
//!
//! ## Key design points
//!
//! - `SinkIdempotencyProfile` declares how a sink handles crash-recovery during
//!   the `PreCommitted` → `Committed` transition. The recovery driver reads
//!   the profile at recovery time and dispatches accordingly.
//! - `SinkState` mirrors the durable `sink_state/` entry in `shard_meta/`
//!   for a connector+epoch pair.
//! - `SinkEpochKey` provides the canonical key encoding for the `sink_state/`
//!   namespace (`shard_meta/sink: 0x06 0xSK connector_id(16) epoch(8 BE)`).

use serde::{Deserialize, Serialize};

use crate::ids::ConnectorId;
use crate::timestamp::Epoch;

// ─── SinkIdempotencyProfile ───────────────────────────────────────────────────

/// How a sink handles recovery from a crash during the 2PC commit phase.
///
/// Every sink implementation **must** declare one of these profiles. The
/// recovery driver reads the profile at recovery time and dispatches the
/// correct re-commit logic (DESIGN.md §11.4).
///
/// Paired with `SinkIdempotencyProfile` in `formal/m3_sink_2pc.fizz`
/// (FIZZBEE_TEST_PLAN.md §3.3, D6.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SinkIdempotencyProfile {
    /// The external system supports idempotent re-commit natively.
    ///
    /// Example: S3 conditional PUT using `If-None-Match`; Postgres named
    /// `PREPARE TRANSACTION`. Recovery can safely re-run `pre_commit → commit`
    /// without checking for prior delivery.
    NativeIdempotent,

    /// The external system requires a fencing token to prevent duplicate delivery.
    ///
    /// Recovery reads the last fencing token from durable state, re-issues it
    /// to the external system, and commits. The external system rejects stale
    /// tokens, preventing split-brain duplicates.
    FencingTokenRequired,

    /// The external system does not safely support re-commit; a check-before-act
    /// query is required.
    ///
    /// Example: Kafka — recovery queries the topic for the epoch marker; if
    /// absent, a new producer transaction is opened. If present, the epoch is
    /// already delivered.
    CheckBeforeCommit,
}

impl SinkIdempotencyProfile {
    /// Returns the canonical name used in metrics and audit events.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NativeIdempotent => "native_idempotent",
            Self::FencingTokenRequired => "fencing_token_required",
            Self::CheckBeforeCommit => "check_before_commit",
        }
    }
}

impl std::fmt::Display for SinkIdempotencyProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ─── SinkState ────────────────────────────────────────────────────────────────

/// Durable state of a 2PC sink for a specific connector+epoch pair.
///
/// Stored atomically in the shard's `WriteBatch` at key `sink_state/…`
/// (DESIGN.md §11.4). Recovery reads this state to determine what action is
/// required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SinkState {
    /// No active transaction for this epoch. The epoch's data will be
    /// reproduced from source on recovery; no sink action required.
    Idle,

    /// Pre-commit staged. The sink has buffered rows and recorded its pending
    /// position. Recovery must re-run the commit path according to the sink's
    /// `SinkIdempotencyProfile`.
    PreCommitted {
        /// Number of rows staged in the sink buffer.
        staged_rows: usize,
        /// Connector-specific opaque pending handle (serialised bytes).
        /// For Kafka: producer transaction ID. For S3: `_pending/` path prefix.
        pending_handle: Vec<u8>,
    },

    /// Transaction committed and finalized. The epoch has been delivered
    /// exactly once. Recovery is a no-op for this epoch.
    Committed,
}

impl SinkState {
    /// Returns `true` if recovery must re-run the commit path.
    pub fn needs_recovery_commit(&self) -> bool {
        matches!(self, Self::PreCommitted { .. })
    }

    /// Returns `true` if the epoch has been committed.
    pub fn is_committed(&self) -> bool {
        matches!(self, Self::Committed)
    }
}

// ─── SinkEpochKey ─────────────────────────────────────────────────────────────

/// Key for the `sink_state/` entry in `shard_meta/` namespace.
///
/// Encoding: `shard_meta/sink: 0x06 0xSK connector_id(16 BE) epoch(8 BE)`
/// (DESIGN.md §5.3, storage key table).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SinkEpochKey {
    pub connector_id: ConnectorId,
    pub epoch: Epoch,
}

impl SinkEpochKey {
    pub fn new(connector_id: ConnectorId, epoch: Epoch) -> Self {
        Self {
            connector_id,
            epoch,
        }
    }

    /// Encode the key to bytes: `[0x06, 0xSK, connector_id(8 BE), epoch(8 BE)]`.
    ///
    /// `0x06` is the `shard_meta` namespace prefix; `0xSK` is the sink-state
    /// sub-key discriminant.
    pub fn encode(&self) -> [u8; 18] {
        let mut buf = [0u8; 18];
        buf[0] = 0x06;
        buf[1] = 0x53; // 'S' for sink
        buf[2..10].copy_from_slice(&self.connector_id.0.to_be_bytes());
        buf[10..18].copy_from_slice(&self.epoch.to_be_bytes());
        buf
    }

    /// Decode from bytes produced by [`encode`].
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 18 || bytes[0] != 0x06 || bytes[1] != 0x53 {
            return None;
        }
        let connector_id = u64::from_be_bytes(bytes[2..10].try_into().ok()?);
        let epoch = u64::from_be_bytes(bytes[10..18].try_into().ok()?);
        Some(Self {
            connector_id: ConnectorId(connector_id),
            epoch,
        })
    }
}

// ─── SourceEpochKey ───────────────────────────────────────────────────────────

/// Key for the `connector/{id}/epoch_map/{source_epoch}` entry in the control
/// plane catalog. Maps a `source_epoch` to the committed partition→offset map.
///
/// Used by `SourceEpochRegistry` in `rockstream-connectors`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SourceEpochKey {
    pub connector_id: ConnectorId,
    pub source_epoch: Epoch,
}

impl SourceEpochKey {
    pub fn new(connector_id: ConnectorId, source_epoch: Epoch) -> Self {
        Self {
            connector_id,
            source_epoch,
        }
    }
}

// ─── RecoveryAction ───────────────────────────────────────────────────────────

/// The action a sink should take during recovery, determined by reading the
/// durable `SinkState` and dispatching based on `SinkIdempotencyProfile`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryAction {
    /// No action required. The epoch was not staged or is already committed.
    Noop,
    /// Re-run the commit path. The external system is safe to receive a
    /// duplicate commit (NativeIdempotent) or will use the fencing token
    /// (FencingTokenRequired) or will be checked first (CheckBeforeCommit).
    RerunCommit {
        epoch: Epoch,
        profile: SinkIdempotencyProfile,
        pending_handle: Vec<u8>,
    },
}

impl RecoveryAction {
    /// Determine the recovery action from durable state and the sink's profile.
    pub fn from_sink_state(
        state: &SinkState,
        epoch: Epoch,
        profile: SinkIdempotencyProfile,
    ) -> Self {
        match state {
            SinkState::Idle => RecoveryAction::Noop,
            SinkState::Committed => RecoveryAction::Noop,
            SinkState::PreCommitted { pending_handle, .. } => RecoveryAction::RerunCommit {
                epoch,
                profile,
                pending_handle: pending_handle.clone(),
            },
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_epoch_key_round_trips() {
        let key = SinkEpochKey::new(ConnectorId(42), 7);
        let encoded = key.encode();
        let decoded = SinkEpochKey::decode(&encoded).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn sink_epoch_key_namespace_prefix() {
        let key = SinkEpochKey::new(ConnectorId(1), 1);
        let encoded = key.encode();
        assert_eq!(encoded[0], 0x06, "shard_meta namespace prefix");
        assert_eq!(encoded[1], 0x53, "sink discriminant 'S'");
    }

    #[test]
    fn sink_epoch_key_decode_rejects_wrong_prefix() {
        let mut encoded = SinkEpochKey::new(ConnectorId(1), 1).encode();
        encoded[0] = 0xFF;
        assert!(SinkEpochKey::decode(&encoded).is_none());
    }

    #[test]
    fn sink_state_needs_recovery_commit() {
        assert!(SinkState::PreCommitted {
            staged_rows: 5,
            pending_handle: vec![]
        }
        .needs_recovery_commit());
        assert!(!SinkState::Idle.needs_recovery_commit());
        assert!(!SinkState::Committed.needs_recovery_commit());
    }

    #[test]
    fn recovery_action_idle_is_noop() {
        let action = RecoveryAction::from_sink_state(
            &SinkState::Idle,
            1,
            SinkIdempotencyProfile::NativeIdempotent,
        );
        assert_eq!(action, RecoveryAction::Noop);
    }

    #[test]
    fn recovery_action_committed_is_noop() {
        let action = RecoveryAction::from_sink_state(
            &SinkState::Committed,
            1,
            SinkIdempotencyProfile::CheckBeforeCommit,
        );
        assert_eq!(action, RecoveryAction::Noop);
    }

    #[test]
    fn recovery_action_pre_committed_returns_rerun() {
        let handle = b"kafka-txn-id-7".to_vec();
        let action = RecoveryAction::from_sink_state(
            &SinkState::PreCommitted {
                staged_rows: 10,
                pending_handle: handle.clone(),
            },
            3,
            SinkIdempotencyProfile::CheckBeforeCommit,
        );
        assert_eq!(
            action,
            RecoveryAction::RerunCommit {
                epoch: 3,
                profile: SinkIdempotencyProfile::CheckBeforeCommit,
                pending_handle: handle,
            }
        );
    }

    #[test]
    fn sink_idempotency_profile_display() {
        assert_eq!(
            SinkIdempotencyProfile::NativeIdempotent.to_string(),
            "native_idempotent"
        );
        assert_eq!(
            SinkIdempotencyProfile::CheckBeforeCommit.to_string(),
            "check_before_commit"
        );
        assert_eq!(
            SinkIdempotencyProfile::FencingTokenRequired.to_string(),
            "fencing_token_required"
        );
    }
}
