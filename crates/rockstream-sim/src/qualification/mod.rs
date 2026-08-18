//! Automated End-to-End Release Qualification Framework.
//!
//! Provides orchestrators, workload generators, external batch oracle auditors,
//! fault injection, and recovery observers for release qualification.

pub mod env_check;
pub mod fault_injector;
pub mod metrics_collector;
pub mod oracle_auditor;
pub mod orchestrator;
pub mod recovery_observer;
pub mod workload;

pub use env_check::{
    check_prerequisites, PrerequisiteKind, PrerequisiteReport, PrerequisiteViolation,
    REQUIRED_CONTAINER_IMAGES, REQUIRED_PORTS,
};
pub use fault_injector::{FaultInjector, FaultType, InjectedFault};
pub use metrics_collector::{MetricSummary, QualificationMetricsCollector, RawMetricsData};
pub use oracle_auditor::{MultisetDiff, OracleAuditor};
pub use orchestrator::{
    ClusterHealth, NodeHandle, NodeRole, NodeStatus, QualificationCluster,
    QualificationClusterConfig,
};
pub use recovery_observer::{
    RecoveryObservation, RecoveryObservationType, RecoveryObserver, RecoveryTimingsReport,
};
pub use workload::{CdcTransaction, MutationOp, QualificationWorkloadGenerator, WorkloadRecord};
