//! Topology types for the RockStream cluster control plane.
//!
//! These types are used by both the control-plane service and by worker nodes
//! to describe cluster membership, roles, and capacity.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::ids::{ShardId, WorkerId};
use crate::lease::{ShardLease, ShardRevokeReason};

/// The role a node is running.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    /// Runs all roles in a single process (embedded / development profile).
    All,
    /// Pure worker node: executes operator tasks and owns shards.
    Worker,
    /// Control-plane node: topology catalog, placement, lifecycle.
    Control,
    /// Gateway node: accepts SQL and pgwire connections.
    Gateway,
    /// Frontier coordinator node.
    Frontier,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRole::All => write!(f, "all"),
            NodeRole::Worker => write!(f, "worker"),
            NodeRole::Control => write!(f, "control"),
            NodeRole::Gateway => write!(f, "gateway"),
            NodeRole::Frontier => write!(f, "frontier"),
        }
    }
}

/// Fraction of available capacity on a worker node, in [0.0, 1.0].
///
/// 1.0 means the worker is completely idle; 0.0 means it is saturated.
/// The placement algorithm prefers workers with higher `capacity_headroom`
/// when assigning shards or operator instances.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct CapacityHeadroom(pub f64);

impl CapacityHeadroom {
    /// Fully available (no load).
    pub const FULL: Self = Self(1.0);

    /// Saturated (no headroom).
    pub const EMPTY: Self = Self(0.0);

    /// Create a new `CapacityHeadroom`, clamped to [0.0, 1.0].
    pub fn new(value: f64) -> Self {
        Self(value.clamp(0.0, 1.0))
    }

    /// Returns the raw fraction.
    pub fn fraction(&self) -> f64 {
        self.0
    }
}

impl Default for CapacityHeadroom {
    fn default() -> Self {
        Self::FULL
    }
}

impl std::fmt::Display for CapacityHeadroom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.2}", self.0)
    }
}

/// Registration request sent by a worker to the control plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegistration {
    /// The worker's proposed identity (may be overridden by control plane).
    pub worker_id: WorkerId,
    /// The role this worker is running.
    pub role: NodeRole,
    /// The advertised address for peer connections (`host:port`).
    pub address: String,
    /// Current capacity headroom at the time of registration.
    pub capacity_headroom: CapacityHeadroom,
    /// Wall-clock timestamp (ms since Unix epoch) when the registration was
    /// sent.
    pub registered_at_ms: u64,
}

impl WorkerRegistration {
    /// Build a registration for a worker joining at `address`.
    pub fn new(
        worker_id: WorkerId,
        role: NodeRole,
        address: impl Into<String>,
        capacity_headroom: CapacityHeadroom,
    ) -> Self {
        let registered_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            worker_id,
            role,
            address: address.into(),
            capacity_headroom,
            registered_at_ms,
        }
    }
}

/// A snapshot of a worker's state as known to the topology catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkerInfo {
    /// Unique identifier assigned / confirmed by the control plane.
    pub worker_id: WorkerId,
    /// Role flags for this node.
    pub role: NodeRole,
    /// Advertised network address (`host:port`).
    pub address: String,
    /// Most recently reported capacity headroom.
    pub capacity_headroom: CapacityHeadroom,
    /// When this worker registered (ms since Unix epoch).
    pub registered_at_ms: u64,
    /// Whether the worker is currently considered healthy.
    pub healthy: bool,
    /// Current lifecycle state of the worker (v0.38).
    #[serde(default)]
    pub lifecycle: WorkerLifecycleState,
}

impl WorkerInfo {
    /// Construct a `WorkerInfo` from a `WorkerRegistration`.
    pub fn from_registration(reg: &WorkerRegistration) -> Self {
        Self {
            worker_id: reg.worker_id,
            role: reg.role,
            address: reg.address.clone(),
            capacity_headroom: reg.capacity_headroom,
            registered_at_ms: reg.registered_at_ms,
            healthy: true,
            lifecycle: WorkerLifecycleState::Active,
        }
    }

    /// Update the capacity headroom from a heartbeat.
    pub fn update_capacity(&mut self, headroom: CapacityHeadroom) {
        self.capacity_headroom = headroom;
    }
}

/// A control-plane message sent from the control service to a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlMessage {
    /// Acknowledgement of successful worker registration.
    Registered {
        /// The canonical worker ID assigned by the control plane.
        worker_id: WorkerId,
    },
    /// The topology has changed; the worker should update its view.
    TopologyChanged {
        /// Current list of healthy workers.
        workers: Vec<WorkerInfo>,
    },
    /// Instructs the worker to stop gracefully.
    Shutdown,
    /// The control plane has assigned a shard lease to this worker.
    ///
    /// The worker must not write to `lease.shard_id` unless it holds the
    /// current [`LeaseToken`].
    ShardAssigned {
        /// The new lease (includes shard_id, worker_id, lease_token).
        lease: ShardLease,
    },
    /// The control plane has revoked a previously assigned shard lease.
    ///
    /// The worker must stop writing to `shard_id` immediately and discard
    /// any buffered writes associated with the old token.
    ShardRevoked {
        /// The shard whose lease was revoked.
        shard_id: ShardId,
        /// Why the lease was revoked.
        reason: ShardRevokeReason,
    },
    /// Response to a [`WorkerMessage::FenceWrite`] request: confirms whether
    /// the given [`LeaseToken`] is still the current writer for `shard_id`.
    FenceAck {
        /// The shard that was fenced.
        shard_id: ShardId,
        /// `true` if `lease_token` is the current active token.
        valid: bool,
    },
    /// Instructs the worker to begin the drain protocol (v0.46).
    ///
    /// The worker must transition to `WorkerLifecycleState::Draining`, stop
    /// accepting new shard assignments, and hand off all owned shards within
    /// the specified deadline.
    BeginDrain(DrainRequest),
    /// Operator-visible acknowledgement that a drain request was accepted.
    DrainStatus {
        /// The drained worker.
        worker_id: WorkerId,
        /// Current lifecycle state after applying the request.
        state: WorkerLifecycleState,
        /// Current drain-queue fill level.
        queue_fill: u32,
        /// Configured drain-queue bound.
        queue_capacity: u32,
    },
    /// Generic coded control-plane failure for operator actions.
    OperationFailed {
        /// Registered RS-XXXX code.
        code: String,
        /// Human-readable failure description.
        message: String,
        /// Actionable operator guidance.
        next_steps: String,
    },
    /// Published by the control plane after all workers have reported their
    /// pressure samples; consumers (HPA adapters) read this gauge (v0.38).
    ClusterPressureGauge(ClusterWorkerPressure),
    /// Notification that the global cluster frontier has advanced.
    ClusterFrontierAdvanced {
        /// The new cluster frontier epoch.
        epoch: u64,
    },
    /// A shard-lease request (or other leader-gated write) was rejected
    /// because this control node is not currently the Raft-elected
    /// control-plane leader (v0.45.2, M7-S2 leader-only write gating).
    ///
    /// RS-1731: the worker should re-resolve control-plane leadership and
    /// retry against the current leader.
    NotLeader {
        /// The current known leader's node id, if any is known.
        current_leader: Option<u64>,
    },
    /// Response to [`WorkerMessage::ClusterStatusQuery`] (v0.45.2 M7 S4):
    /// this control node's current Raft leadership status, used by
    /// operators/tests to locate the current elected leader without
    /// depending on any specific node already being it (e.g. after a
    /// leader-kill drill's failover).
    ClusterStatusReport {
        /// This node's own Raft node id, or `None` if this node is running
        /// without Raft attached (pre-v0.45.2 single-node control mode).
        node_id: Option<u64>,
        /// This node's current Raft role.
        role: RaftRoleWire,
        /// This node's current Raft term (`0` if Raft is not attached).
        term: u64,
    },
}

/// Wire-serializable mirror of `rockstream_control::raft::RaftRole`
/// (`rockstream-types` cannot depend on `rockstream-control`, so the
/// leadership status reported over the wire uses this small local copy;
/// `rockstream-control::service` is responsible for the conversion at the
/// boundary).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftRoleWire {
    Follower,
    Candidate,
    Leader,
    /// This control node is not running with Raft attached at all
    /// (pre-v0.45.2 single embedded control node — implicitly the sole
    /// writer, since there is no group to contend leadership with).
    NoRaft,
}

/// Lifecycle state of a worker node (v0.46 drain protocol).
///
/// Transitions: `Active` → `Draining` → `Decommissioned`.
///
/// Once `Decommissioned`, the control plane stops assigning new shards and
/// the worker is removed from the topology after a short grace period.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum WorkerLifecycleState {
    /// Normal operation — accepts new shard assignments.
    #[default]
    Active,
    /// Drain requested — the worker is handing off all owned shards.
    /// New shard assignments are rejected.  Transitions to `Decommissioned`
    /// once `shards_remaining == 0`.
    Draining {
        /// How many shards are still owned by this worker.
        shards_remaining: u32,
        /// Wall-clock time (ms since Unix epoch) when the drain was requested.
        started_at_ms: u64,
    },
    /// All shards have been migrated away; the worker is idle and may exit.
    Decommissioned {
        /// Wall-clock time (ms since Unix epoch) when decommission completed.
        completed_at_ms: u64,
    },
}

impl WorkerLifecycleState {
    /// Returns `true` if the worker is in the `Active` state.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }

    /// Returns `true` if the worker is draining or decommissioned
    /// (i.e., should not receive new shard assignments).
    pub fn is_draining_or_decommissioned(&self) -> bool {
        !self.is_active()
    }
}

/// Request from the control plane to a worker to begin draining.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrainRequest {
    /// The worker that should begin draining.
    pub worker_id: WorkerId,
    /// Hard deadline by which the drain must complete (ms since Unix epoch).
    /// Workers that exceed this deadline self-fence and stop committing epochs.
    pub deadline_ms: u64,
}

/// Configuration for the proactive splitter (v0.38).
///
/// The proactive splitter monitors per-shard state size and triggers a split
/// *before* the shard reaches the alert threshold, ensuring no freshness SLO
/// is missed due to an emergency reactive split.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProactiveSplitConfig {
    /// Target shard state size in bytes.  Once a shard's state exceeds
    /// `target_shard_state_bytes * split_trigger_fraction` the proactive
    /// splitter schedules a split.
    pub target_shard_state_bytes: u64,
    /// Fraction of `target_shard_state_bytes` at which proactive splitting is
    /// triggered.  Must be in `(0.0, 1.0]`.  Default: `0.80`.
    pub split_trigger_fraction: f64,
    /// Fraction of `target_shard_state_bytes` considered the "alert threshold".
    /// Proactive splits must start before this threshold to meet the SLO.
    /// Default: `0.90`.
    pub alert_threshold_fraction: f64,
}

impl Default for ProactiveSplitConfig {
    fn default() -> Self {
        Self {
            target_shard_state_bytes: 32 * 1024 * 1024 * 1024, // 32 GiB
            split_trigger_fraction: 0.80,
            alert_threshold_fraction: 0.90,
        }
    }
}

impl ProactiveSplitConfig {
    /// Byte threshold at which a proactive split should be scheduled.
    pub fn split_trigger_bytes(&self) -> u64 {
        (self.target_shard_state_bytes as f64 * self.split_trigger_fraction) as u64
    }

    /// Byte threshold considered the "alert threshold".
    pub fn alert_threshold_bytes(&self) -> u64 {
        (self.target_shard_state_bytes as f64 * self.alert_threshold_fraction) as u64
    }
}

/// Per-shard load sample used for skew detection (v0.38).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardLoadSample {
    /// Which shard this sample describes.
    pub shard_id: ShardId,
    /// Estimated state size in bytes (arrangement rows × avg row size).
    pub state_bytes: u64,
    /// Number of input rows processed in the most recent epoch.
    pub rows_per_epoch: u64,
}

/// Result of a skew-detection pass across all shards (v0.38).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkewReport {
    /// The shard carrying the heaviest load.
    pub worst_shard: ShardId,
    /// `worst_shard.state_bytes / median_state_bytes`.
    /// A ratio > `skew_threshold` means the cluster is skewed.
    pub load_factor: f64,
    /// Median shard state size in bytes.
    pub median_state_bytes: u64,
    /// Whether the load factor exceeds the configured threshold.
    pub skewed: bool,
}

/// Virtual-bucket configuration for hot-key splitting (v0.38).
///
/// When a single key accumulates disproportionate state (e.g. a viral hashtag
/// in a social-graph view), a virtual bucket sub-divides that key across
/// `bucket_count` logical sub-shards using a stable hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VirtualBucketConfig {
    /// The hot key prefix (first `prefix_len` bytes) that should be split.
    pub key_prefix: Vec<u8>,
    /// How many virtual sub-buckets to create for this prefix.
    /// Must be a power of two in `[2, 1024]`.
    pub bucket_count: u16,
}

/// The `cluster_worker_pressure` metric exposed for infrastructure autoscaling
/// (e.g. Kubernetes HPA) (v0.38).
///
/// Values:
/// - `< 1.0` — cluster has headroom; scale-in safe
/// - `1.0`   — ideal steady state
/// - `> 1.0` — overloaded; add workers
/// - `>= 2.0` — critical; emergency scale-out
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterWorkerPressure {
    /// Pressure value (dimensionless ratio).
    pub pressure: f64,
    /// Number of workers currently in `Active` state.
    pub active_workers: u32,
    /// Number of workers currently in `Draining` state.
    pub draining_workers: u32,
    /// Total shard count across the cluster.
    pub total_shards: u32,
    /// Timestamp when this sample was computed (ms since Unix epoch).
    pub sampled_at_ms: u64,
}

impl ClusterWorkerPressure {
    /// A freshly initialised gauge representing a single idle worker.
    pub fn idle() -> Self {
        Self {
            pressure: 0.0,
            active_workers: 1,
            draining_workers: 0,
            total_shards: 0,
            sampled_at_ms: 0,
        }
    }
}

/// A message sent from a worker to the control plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerMessage {
    /// Initial registration request.
    Register(WorkerRegistration),
    /// Periodic heartbeat with updated capacity.
    Heartbeat {
        worker_id: WorkerId,
        capacity_headroom: CapacityHeadroom,
    },
    /// Graceful deregistration.
    Deregister { worker_id: WorkerId },
    /// Request the control plane to acquire a shard lease on behalf of this
    /// worker. The control plane responds with [`ControlMessage::ShardAssigned`]
    /// or an error (connection close).
    RequestShard {
        worker_id: WorkerId,
        shard_id: ShardId,
    },
    /// Ask the control plane whether this token is still the active writer.
    /// Used by a worker before committing an epoch to double-check the fence.
    FenceWrite {
        shard_id: ShardId,
        lease_token: crate::ids::LeaseToken,
    },
    /// Worker acknowledges a drain request and reports how many shards it
    /// still owns (v0.38).
    DrainAck {
        worker_id: WorkerId,
        shards_remaining: u32,
    },
    /// Worker reports its updated lifecycle state (v0.38).
    LifecycleState {
        worker_id: WorkerId,
        state: WorkerLifecycleState,
    },
    /// Worker reports a per-shard load sample for skew detection (v0.38).
    ShardLoadReport {
        worker_id: WorkerId,
        samples: Vec<ShardLoadSample>,
    },
    /// Query this control node's current Raft leadership status (v0.45.2
    /// M7 S4). Any connected client (worker, ops tooling, or a test) may
    /// send this at any time without first registering; the control plane
    /// always answers with [`ControlMessage::ClusterStatusReport`].
    ClusterStatusQuery,
    /// Report a shard's current committed frontier epoch to the control
    /// plane's frontier aggregator (v0.45.2 M7 S4 — "frontier publication
    /// resumes within budget" after a control-leadership change). Only the
    /// current Raft leader publishes the resulting cluster frontier; a
    /// non-leader control node still ingests the report (so the meet
    /// computation carries over once it becomes leader) but replies
    /// [`ControlMessage::NotLeader`] instead of
    /// [`ControlMessage::ClusterFrontierAdvanced`].
    ReportShardFrontier {
        shard_id: ShardId,
        epoch: crate::timestamp::Epoch,
    },
    /// Operator-initiated request to begin draining a worker (v0.46).
    RequestDrain {
        /// The worker that should stop receiving new shard assignments and
        /// migrate away its current shard set.
        worker_id: WorkerId,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::WorkerId;

    #[test]
    fn capacity_headroom_clamps() {
        assert_eq!(CapacityHeadroom::new(1.5).0, 1.0);
        assert_eq!(CapacityHeadroom::new(-0.1).0, 0.0);
        assert_eq!(CapacityHeadroom::new(0.75).0, 0.75);
    }

    #[test]
    fn worker_registration_roundtrip() {
        let reg = WorkerRegistration::new(
            WorkerId(1),
            NodeRole::Worker,
            "127.0.0.1:7001",
            CapacityHeadroom::new(0.8),
        );
        let json = serde_json::to_string(&reg).unwrap();
        let decoded: WorkerRegistration = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.worker_id, WorkerId(1));
        assert_eq!(decoded.address, "127.0.0.1:7001");
    }

    #[test]
    fn worker_message_register_roundtrip() {
        let reg = WorkerRegistration::new(
            WorkerId(2),
            NodeRole::Worker,
            "127.0.0.1:7002",
            CapacityHeadroom::FULL,
        );
        let msg = WorkerMessage::Register(reg);
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: WorkerMessage = serde_json::from_str(&json).unwrap();
        match decoded {
            WorkerMessage::Register(r) => assert_eq!(r.worker_id, WorkerId(2)),
            _ => panic!("unexpected message variant"),
        }
    }

    #[test]
    fn control_message_registered_roundtrip() {
        let msg = ControlMessage::Registered {
            worker_id: WorkerId(3),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: ControlMessage = serde_json::from_str(&json).unwrap();
        match decoded {
            ControlMessage::Registered { worker_id } => {
                assert_eq!(worker_id, WorkerId(3))
            }
            _ => panic!("unexpected message variant"),
        }
    }

    #[test]
    fn node_role_display() {
        assert_eq!(NodeRole::Control.to_string(), "control");
        assert_eq!(NodeRole::Worker.to_string(), "worker");
        assert_eq!(NodeRole::All.to_string(), "all");
    }

    #[test]
    fn worker_info_from_registration() {
        let reg = WorkerRegistration::new(
            WorkerId(5),
            NodeRole::Worker,
            "10.0.0.1:7005",
            CapacityHeadroom::new(0.6),
        );
        let info = WorkerInfo::from_registration(&reg);
        assert_eq!(info.worker_id, WorkerId(5));
        assert!(info.healthy);
        assert_eq!(info.capacity_headroom.fraction(), 0.6);
    }
}
