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
//! - [`scheduler`] — Shard scheduling: distributes shards across workers
//! - [`service`] — TCP control service for worker registration and shard leasing
//! - [`shard`] — Shard lease management with fencing tokens
//! - [`tls`] — mTLS configuration scaffolding

pub mod acl;
pub mod audit;
pub mod checkpoint;
pub mod config_deriver;
pub mod frontier;
pub mod namespace;
pub mod placement;
pub mod scheduler;
pub mod service;
pub mod shard;
pub mod tls;
pub mod topology;

// Re-export commonly used top-level types.
pub use acl::{AclError, AclStore};
pub use checkpoint::{CheckpointCoordinator, CoordinatorError, DEFAULT_ALIGNMENT_MAX_CREDITS};
pub use frontier::{AggregatorError, FrontierAggregator};
pub use namespace::NamespaceCatalog;
pub use placement::PlacementAlgorithm;
pub use scheduler::{ShardAssignment, ShardScheduler};
pub use service::{ControlService, ControlServiceHandle};
pub use shard::{LeaseError, ShardManager};
pub use tls::TlsConfig;
pub use topology::TopologyCatalog;

#[cfg(test)]
mod tests {
    #[test]
    fn control_crate_compiles() {}
}
