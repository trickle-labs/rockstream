//! Shard-migration types for distributed bucket handoff in RockStream.
//!
//! A shard migration moves a bounded set of virtual buckets from one or more
//! donor shards to a recipient shard without ever creating a dual-authoritative
//! window. The control plane persists a single [`MigrationRecord`] per
//! migration and advances it through the explicit state machine defined in the
//! v0.46 plan.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::ids::ShardId;
use crate::timestamp::Epoch;

/// The explicit migration state machine for online shard handoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationState {
    Planned,
    Snapshotting,
    Copying,
    DualWriting,
    CatchingUp,
    FencingOld,
    Cutover,
    Verifying,
    GcEligible,
    Done,
    Aborted,
}

impl std::fmt::Display for MigrationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Planned => "planned",
            Self::Snapshotting => "snapshotting",
            Self::Copying => "copying",
            Self::DualWriting => "dual_writing",
            Self::CatchingUp => "catching_up",
            Self::FencingOld => "fencing_old",
            Self::Cutover => "cutover",
            Self::Verifying => "verifying",
            Self::GcEligible => "gc_eligible",
            Self::Done => "done",
            Self::Aborted => "aborted",
        };
        write!(f, "{name}")
    }
}

/// A bounded logical set of migrated virtual buckets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BucketSet {
    /// Sorted logical bucket identifiers.
    pub buckets: BTreeSet<u64>,
}

impl BucketSet {
    /// Construct a `BucketSet` from explicit bucket ids.
    pub fn new<I>(buckets: I) -> Self
    where
        I: IntoIterator<Item = u64>,
    {
        Self {
            buckets: buckets.into_iter().collect(),
        }
    }

    /// Returns `true` if `bucket` is part of the migration.
    pub fn contains(&self, bucket: u64) -> bool {
        self.buckets.contains(&bucket)
    }

    /// Number of buckets in the set.
    pub fn len(&self) -> usize {
        self.buckets.len()
    }

    /// Returns `true` when the set is empty.
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }
}

/// Durable record for one online shard migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationRecord {
    /// Stable migration identifier.
    pub migration_id: String,
    /// Current state in the explicit migration state machine.
    pub state: MigrationState,
    /// Source shards whose buckets are moving.
    pub donor_shards: Vec<ShardId>,
    /// The shard that will own the buckets after cutover.
    pub recipient_shard: ShardId,
    /// Buckets included in this migration.
    pub buckets: BucketSet,
    /// Frontier captured at planning time (`F_plan`).
    pub planned_frontier: Epoch,
    /// The target bucket-map version used for dual-write / cutover.
    pub target_bucket_map_version: u64,
    /// Donor checkpoint ids keyed by donor shard.
    #[serde(default)]
    pub donor_checkpoints: BTreeMap<ShardId, u64>,
    /// Snapshot UUIDs for copying the exact donor checkpoint.
    #[serde(default)]
    pub donor_checkpoint_snapshots: BTreeMap<ShardId, String>,
    /// Logical epoch at which writes enter the migration routing policy.
    #[serde(default)]
    pub migration_epoch: Epoch,
    /// Epoch used to gate `GC_ELIGIBLE`.
    pub cutover_epoch: Option<Epoch>,
    /// Wall-clock timestamp (ms since Unix epoch) when the record was created.
    pub created_at_ms: u64,
    /// Wall-clock timestamp (ms since Unix epoch) when the record last changed.
    pub updated_at_ms: u64,
    /// Total bytes estimated/observed for the migration payload.
    #[serde(default)]
    pub total_bytes: Option<u64>,
    /// Bytes successfully copied to the recipient shard so far.
    #[serde(default)]
    pub copied_bytes: Option<u64>,
    /// Total logical rows estimated/observed for the migration payload.
    #[serde(default)]
    pub total_rows: Option<u64>,
    /// Rows successfully copied to the recipient shard so far.
    #[serde(default)]
    pub copied_rows: Option<u64>,
}

impl MigrationRecord {
    /// Construct a new `MigrationRecord` in [`MigrationState::Planned`].
    pub fn new(
        migration_id: impl Into<String>,
        donor_shards: Vec<ShardId>,
        recipient_shard: ShardId,
        buckets: BucketSet,
        planned_frontier: Epoch,
        target_bucket_map_version: u64,
    ) -> Self {
        let now = now_ms();
        Self {
            migration_id: migration_id.into(),
            state: MigrationState::Planned,
            donor_shards,
            recipient_shard,
            buckets,
            planned_frontier,
            target_bucket_map_version,
            donor_checkpoints: BTreeMap::new(),
            donor_checkpoint_snapshots: BTreeMap::new(),
            migration_epoch: planned_frontier,
            cutover_epoch: None,
            created_at_ms: now,
            updated_at_ms: now,
            total_bytes: None,
            copied_bytes: None,
            total_rows: None,
            copied_rows: None,
        }
    }

    /// Set the epoch used by dual routing without changing the migration plan.
    pub fn with_migration_epoch(mut self, migration_epoch: Epoch) -> Self {
        self.migration_epoch = migration_epoch;
        self
    }

    /// Attach work estimates to this record.
    pub fn with_work_estimates(
        mut self,
        total_bytes: Option<u64>,
        total_rows: Option<u64>,
    ) -> Self {
        self.total_bytes = total_bytes;
        self.total_rows = total_rows;
        self
    }

    /// Record observed progress during copying.
    pub fn record_progress(&mut self, copied_bytes: u64, copied_rows: u64) {
        self.copied_bytes = Some(
            self.copied_bytes
                .unwrap_or(0)
                .max(copied_bytes)
                .min(self.total_bytes.unwrap_or(u64::MAX)),
        );
        self.copied_rows = Some(
            self.copied_rows
                .unwrap_or(0)
                .max(copied_rows)
                .min(self.total_rows.unwrap_or(u64::MAX)),
        );
        self.updated_at_ms = now_ms();
    }

    /// Compute bytes remaining for active work.
    pub fn bytes_remaining(&self) -> Option<u64> {
        match self.state {
            MigrationState::Done => Some(0),
            MigrationState::DualWriting
            | MigrationState::CatchingUp
            | MigrationState::FencingOld
            | MigrationState::Cutover
            | MigrationState::Verifying
            | MigrationState::GcEligible => Some(0),
            MigrationState::Aborted => None,
            MigrationState::Planned | MigrationState::Snapshotting => self.total_bytes,
            MigrationState::Copying => match (self.total_bytes, self.copied_bytes) {
                (Some(total), Some(copied)) => Some(total.saturating_sub(copied)),
                (Some(total), None) => Some(total),
                _ => None,
            },
        }
    }

    /// Compute rows remaining for active work.
    pub fn rows_remaining(&self) -> Option<u64> {
        match self.state {
            MigrationState::Done => Some(0),
            MigrationState::DualWriting
            | MigrationState::CatchingUp
            | MigrationState::FencingOld
            | MigrationState::Cutover
            | MigrationState::Verifying
            | MigrationState::GcEligible => Some(0),
            MigrationState::Aborted => None,
            MigrationState::Planned | MigrationState::Snapshotting => self.total_rows,
            MigrationState::Copying => match (self.total_rows, self.copied_rows) {
                (Some(total), Some(copied)) => Some(total.saturating_sub(copied)),
                (Some(total), None) => Some(total),
                _ => None,
            },
        }
    }

    /// Returns the progress phase string.
    pub fn progress_phase(&self) -> String {
        self.state.to_string()
    }

    /// Compute a bounded estimate of remaining duration in milliseconds.
    pub fn estimated_remaining_ms(&self) -> Option<u64> {
        match self.state {
            MigrationState::Done => Some(0),
            MigrationState::Aborted => None,
            MigrationState::DualWriting
            | MigrationState::CatchingUp
            | MigrationState::FencingOld
            | MigrationState::Cutover
            | MigrationState::Verifying
            | MigrationState::GcEligible => Some(10),
            MigrationState::Planned | MigrationState::Snapshotting => self
                .total_bytes
                .map(|b| (b / (10 * 1024 * 1024) * 1000).clamp(50, 60_000)),
            MigrationState::Copying => {
                let remaining = self.bytes_remaining()?;
                if remaining == 0 {
                    return Some(0);
                }
                let elapsed_ms = self.updated_at_ms.saturating_sub(self.created_at_ms).max(1);
                let copied = self.copied_bytes.unwrap_or(0);
                if copied > 0 {
                    let rate = (copied as f64) / (elapsed_ms as f64);
                    if rate > 0.0 {
                        let ms = (remaining as f64 / rate) as u64;
                        return Some(ms.min(600_000));
                    }
                }
                Some((remaining / (10 * 1024 * 1024) * 1000).clamp(50, 60_000))
            }
        }
    }

    /// Returns `true` if the record may transition from `self.state` to `next`.
    pub fn can_transition_to(&self, next: MigrationState) -> bool {
        use MigrationState::*;
        if self.state == next {
            return true;
        }
        matches!(
            (self.state, next),
            (Planned, Snapshotting)
                | (Snapshotting, Copying)
                | (Copying, DualWriting)
                | (DualWriting, CatchingUp)
                | (CatchingUp, FencingOld)
                | (FencingOld, Cutover)
                | (Cutover, Verifying)
                | (Verifying, DualWriting)
                | (Verifying, GcEligible)
                | (GcEligible, Done)
                | (Planned, Aborted)
                | (Snapshotting, Aborted)
                | (Copying, Aborted)
                | (DualWriting, Aborted)
                | (CatchingUp, Aborted)
                | (FencingOld, Aborted)
                | (Cutover, Aborted)
                | (Verifying, Aborted)
                | (GcEligible, Aborted)
        )
    }

    /// Apply a validated state transition.
    ///
    /// Returns `true` if the state changed, or `false` when the transition was
    /// idempotently re-applied.
    pub fn apply_transition(
        &mut self,
        next: MigrationState,
    ) -> Result<bool, InvalidTransitionError> {
        if !self.can_transition_to(next) {
            return Err(InvalidTransitionError {
                from: self.state,
                to: next,
            });
        }
        if self.state == next {
            return Ok(false);
        }
        self.state = next;
        self.updated_at_ms = now_ms();
        if next == MigrationState::Cutover && self.cutover_epoch.is_none() {
            self.cutover_epoch = Some(self.planned_frontier);
        }
        Ok(true)
    }
}

/// Returned by [`MigrationRecord::apply_transition`] when the requested
/// transition is not permitted by the migration state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidTransitionError {
    pub from: MigrationState,
    pub to: MigrationState,
}

impl std::fmt::Display for InvalidTransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid migration transition: {:?} -> {:?}",
            self.from, self.to
        )
    }
}

impl std::error::Error for InvalidTransitionError {}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_state_roundtrips() {
        let states = [
            MigrationState::Planned,
            MigrationState::Snapshotting,
            MigrationState::Copying,
            MigrationState::DualWriting,
            MigrationState::CatchingUp,
            MigrationState::FencingOld,
            MigrationState::Cutover,
            MigrationState::Verifying,
            MigrationState::GcEligible,
            MigrationState::Done,
            MigrationState::Aborted,
        ];
        for state in states {
            let json = serde_json::to_string(&state).unwrap();
            let back: MigrationState = serde_json::from_str(&json).unwrap();
            assert_eq!(back, state);
        }
    }

    #[test]
    fn migration_record_allows_verify_rollback() {
        let mut record = MigrationRecord::new(
            "m1",
            vec![ShardId(1)],
            ShardId(2),
            BucketSet::new([1, 2]),
            7,
            9,
        );
        for state in [
            MigrationState::Snapshotting,
            MigrationState::Copying,
            MigrationState::DualWriting,
            MigrationState::CatchingUp,
            MigrationState::FencingOld,
            MigrationState::Cutover,
            MigrationState::Verifying,
        ] {
            record.apply_transition(state).unwrap();
        }
        assert!(record
            .apply_transition(MigrationState::DualWriting)
            .unwrap());
    }
}
