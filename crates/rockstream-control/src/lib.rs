//! Control-plane service for RockStream.
//!
//! Manages cluster topology, pipeline lifecycle, shard scheduling, and
//! distributed coordination.
//!
//! ## Modules
//!
//! - [`audit`] — File-backed audit log (JSONL)
//! - [`frontier`] — Frontier aggregation and ClusterFrontier publishing (v0.18)
//! - [`topology`] — In-memory worker registry / topology catalog
//! - [`placement`] — Capacity-aware shard and operator placement
//! - [`raft`] — Control-plane Raft leader election (v0.45.2 M7 S1–S3)
//! - [`scheduler`] — Shard scheduling: distributes shards across workers
//! - [`service`] — TCP control service for worker registration and shard leasing
//! - [`shard`] — Shard lease management with fencing tokens
//! - [`tls`] — mTLS configuration scaffolding

pub mod acl;
pub mod audit;
pub mod checkpoint;
pub mod config_deriver;
pub mod frontier;
pub mod migration;
pub mod namespace;
pub mod placement;
pub mod raft;
pub mod scheduler;
pub mod service;
pub mod shard;
pub mod skew;
pub mod tls;
pub mod topology;

// Re-export commonly used top-level types.
pub use acl::{AclError, AclStore};
pub use checkpoint::{CheckpointCoordinator, CoordinatorError, DEFAULT_ALIGNMENT_MAX_CREDITS};
pub use frontier::{AggregatorError, FrontierAggregator};
pub use migration::{
    BucketMapVersionTracker, MigrationConsumerFrontierTracker, MigrationCoordinator,
    MigrationError, MigrationFillLevel, MigrationPersistentStore, MigrationShard, PhaseClocks,
};
pub use namespace::NamespaceCatalog;
pub use placement::PlacementAlgorithm;
pub use scheduler::{ShardAssignment, ShardScheduler};
pub use service::{ControlService, ControlServiceHandle};
pub use shard::{LeaseError, ShardManager, ShardManagerSnapshot, ShardPersistentStore};
pub use skew::{
    detect_hot_key, plan_hot_key_mitigation, HotKeyDetector, HotKeyDetectorError,
    HotKeyMitigationPlan, HotKeyReport, ProactiveSplitConfig, ProactiveSplitError,
    ProactiveSplitOutcome, ProactiveSplitter, ShardFootprintReport, SkewFillLevel,
    MAX_PROACTIVE_SPLIT_SAMPLE_KEYS, MAX_TRACKED_KEY_LOADS,
};
pub use tls::TlsConfig;
pub use topology::{TopologyCatalog, TopologyPersistentStore};

#[cfg(test)]
mod tests {
    #[test]
    fn control_crate_compiles() {}
}
