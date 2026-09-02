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
pub mod arrangement;
pub mod audit;
pub mod capacity_store;
pub mod checkpoint;
pub mod checkpoint_export;
pub mod checkpoint_store;
pub mod config_deriver;
pub mod freshness;
pub mod frontier;
pub mod kek;
pub mod migration;
pub mod namespace;
pub mod placement;
pub mod raft;
pub mod scheduler;
pub mod secret_store;
pub mod service;
pub mod shard;
pub mod shard_stats;
pub mod skew;
pub mod tls;
pub mod topology;

// Re-export commonly used top-level types.
pub use acl::{AclError, AclStore};
pub use capacity_store::CapacityThresholdStore;
pub use checkpoint::{
    ChangelogCheckpointContribution, CheckpointCoordinator, CoordinatorError,
    DEFAULT_ALIGNMENT_MAX_CREDITS,
};
pub use checkpoint_export::{
    CheckpointExportError, CheckpointExportOutcome, CheckpointExportService,
    CheckpointRestoreOutcome, MAX_CHECKPOINT_EXPORT_OBJECTS_IN_FLIGHT,
    MAX_CHECKPOINT_EXPORT_OBJECT_BYTES, MAX_CHECKPOINT_EXPORT_SCAN_WINDOW,
};
pub use checkpoint_store::CheckpointManifestStore;
pub use freshness::{
    AdmissionMode, CheckpointMode, FreshnessAction, FreshnessBounds, FreshnessController,
    FreshnessObservation,
};
pub use frontier::{AggregatorError, FrontierAggregator};
pub use kek::{AwsKmsKekProvider, EnvKekProvider, KekError, KekProvider};
pub use migration::{
    BucketMapVersionTracker, MigrationConsumerFrontierTracker, MigrationCoordinator,
    MigrationCopyStats, MigrationError, MigrationFillLevel, MigrationPersistentStore,
    MigrationShard, PhaseClocks, MAX_COPY_CHUNK_BYTES, MAX_COPY_CHUNK_ROWS,
};
pub use namespace::NamespaceCatalog;
pub use placement::PlacementAlgorithm;
pub use scheduler::{ShardAssignment, ShardScheduler};
pub use secret_store::{SecretListing, SecretStore, SecretStoreError};
pub use service::{ControlService, ControlServiceHandle};
pub use shard::{LeaseError, ShardManager, ShardManagerSnapshot, ShardPersistentStore};
pub use shard_stats::ShardStatsPersistentStore;
pub use skew::{
    compute_cluster_worker_pressure, detect_hot_key, plan_hot_key_mitigation,
    publish_cluster_worker_pressure, AdaptiveSkewSplitter, HotKeyDetector, HotKeyDetectorError,
    HotKeyMitigationPlan, HotKeyReport, PipelineShardPressureSample, ProactiveMergeOutcome,
    ProactiveSplitConfig, ProactiveSplitError, ProactiveSplitOutcome, ProactiveSplitter,
    ShardFootprintReport, SkewFillLevel, SkewSplitDecision, MAX_PROACTIVE_SPLIT_SAMPLE_KEYS,
    MAX_TRACKED_KEY_LOADS, SKEW_SPLIT_TRIGGER_WINDOW,
};
pub use tls::TlsConfig;
pub use topology::{TopologyCatalog, TopologyPersistentStore};

#[cfg(test)]
mod tests {
    #[test]
    fn control_crate_compiles() {}
}
