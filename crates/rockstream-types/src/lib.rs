//! Shared types for RockStream.
//!
//! This crate defines core types used across the RockStream system:
//! timestamps, frontiers, Z-set rows, schema definitions, identity types,
//! batch types, merge-law descriptors, law implementations, and audit events.

pub mod acl;
pub mod arrangement;
pub mod arrow_batch;
pub mod audit;
pub mod batch;
pub mod candidate_identity;
pub mod checkpoint;
pub mod compatibility;
pub mod config;
pub mod config_resolver;
pub mod config_validation;
pub mod connector;
pub mod cost;
pub mod dlq;
pub mod error_code;
pub mod evidence_manifest;
pub mod exchange;
pub mod explain;
pub mod frontier;
pub mod identity;
pub mod ids;
pub mod key_capsule;
pub mod laws;
pub mod lease;
pub mod merge_law;
pub mod metrics;
pub mod migration;
pub mod mutation_policy;
pub mod raft;
pub mod rendezvous;
pub mod schema_evolution;
pub mod secret;
pub mod sink;
pub mod state_budget;
pub mod state_mutation;
pub mod tiering;
pub mod topology;
pub mod view_lifecycle;
pub mod workload;

pub use arrangement::{
    ArrangementSpec, CanonicalBinaryOp, CanonicalExpr, CanonicalLiteral, CanonicalType,
    CanonicalUnaryOp, CollationId, CollationVersion, NullSemantics, PartitioningSpec,
    SourceIdentity, TimeDomainSemantics,
};
pub use candidate_identity::CandidateIdentity;
pub use compatibility::{
    ProtocolVersion, StorageFormatVersion, SupportedStorageFormatRange, SupportedVersionRange,
};
pub use evidence_manifest::{
    EvidenceIntegrityError, EvidenceManifest, RunnerEnvironment, SummaryMetric, TestSuiteResult,
    WorkflowRunInfo,
};
pub use ids::{ArrangementId, TenantId};
pub use key_capsule::{KeyCapsule, KeyCapsuleError, KeyValue};
pub use state_mutation::{EpochStateDelta, OperatorEpochMetrics, StateMutation};

/// Timestamp types.
pub mod timestamp {
    /// A logical epoch number.
    pub type Epoch = u64;

    /// Processing-time timestamp (wall-clock millis since Unix epoch).
    pub type ProcessingTime = u64;

    /// Event-time timestamp (application-defined millis since Unix epoch).
    pub type EventTime = u64;

    /// Event-time watermark (application-defined millis since Unix epoch).
    pub type EventTimeWatermark = u64;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_is_u64() {
        let e: timestamp::Epoch = 42;
        assert_eq!(e, 42);
    }
}
