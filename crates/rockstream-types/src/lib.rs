//! Shared types for RockStream.
//!
//! This crate defines core types used across the RockStream system:
//! timestamps, frontiers, Z-set rows, schema definitions, identity types,
//! batch types, merge-law descriptors, law implementations, and audit events.

pub mod arrow_batch;
pub mod audit;
pub mod batch;
pub mod error_code;
pub mod exchange;
pub mod explain;
pub mod frontier;
pub mod ids;
pub mod laws;
pub mod lease;
pub mod merge_law;
pub mod metrics;
pub mod schema_evolution;
pub mod state_budget;
pub mod topology;
pub mod view_lifecycle;
pub mod workload;

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
