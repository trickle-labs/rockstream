//! Identity types for RockStream.
//!
//! Strong type wrappers for all system identifiers to prevent accidental mixing.

use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! define_id {
    ($(#[$meta:meta])* $name:ident, $inner:ty, $prefix:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        pub struct $name(pub $inner);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}-{}", $prefix, self.0)
            }
        }

        impl From<$inner> for $name {
            fn from(v: $inner) -> Self {
                Self(v)
            }
        }
    };
}

define_id!(
    /// Identifies a shard within the cluster.
    ShardId, u64, "shard"
);

define_id!(
    /// Identifies an operator instance within a pipeline.
    OperatorId, u64, "op"
);

define_id!(
    /// Identifies a materialized view.
    ViewId, u64, "view"
);

define_id!(
    /// Identifies a namespace (tenant isolation boundary).
    NamespaceId, u64, "ns"
);

define_id!(
    /// Identifies an exchange (shuffle) channel.
    ExchangeId, u64, "xchg"
);

define_id!(
    /// A fencing token for distributed lease management.
    LeaseToken, u64, "lease"
);

define_id!(
    /// Identifies a workload (resource and SLO grouping).
    WorkloadId, u64, "workload"
);

define_id!(
    /// Identifies a worker node in the cluster.
    WorkerId, u64, "worker"
);

define_id!(
    /// Identifies a source connector/ingestion point.
    SourceId, u64, "src"
);

define_id!(
    /// Identifies a connector (source or sink) instance.
    ConnectorId, u64, "connector"
);

define_id!(
    /// Identifies a `FrontierAggregator` instance (v0.45.6 — frontier-lease
    /// publisher election).
    AggregatorId, u64, "aggregator"
);

define_id!(
    /// Identifies a tenant within the cluster (v0.59.6).
    TenantId, u64, "tenant"
);

define_id!(
    /// Identifies a physical shared arrangement (v0.59.6).
    ArrangementId, u64, "arr"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_id_display() {
        assert_eq!(ShardId(42).to_string(), "shard-42");
    }

    #[test]
    fn operator_id_display() {
        assert_eq!(OperatorId(7).to_string(), "op-7");
    }

    #[test]
    fn ids_are_distinct_types() {
        // This is a compile-time check — ShardId and OperatorId cannot be mixed.
        let _s: ShardId = ShardId(1);
        let _o: OperatorId = OperatorId(1);
        // These are different types despite same inner value.
    }
}
