//! `Operator` trait and `EpochOutput` type.
//!
//! Every stateless (and later stateful) operator implements `Operator`.
//! `EpochOutput` is the packet an operator emits after processing one
//! epoch's worth of input deltas.

use crate::error::OpError;
use crate::zset::ArrowZSet;
use rockstream_types::timestamp::Epoch;

/// The core operator trait.
///
/// Operators in the IVM engine consume delta batches and produce delta batches.
/// Stateless operators (Filter, Project, Map) implement `process_delta` as a
/// pure function — no mutable state, no arrangements.
///
/// # Thread safety
///
/// Operators must be `Send + Sync` so they can be owned by tokio tasks.
pub trait Operator: Send + Sync {
    /// Process one incoming Z-set delta batch and return the output delta.
    ///
    /// For stateless operators, this is a pure function.
    /// For stateful operators (v0.5+), this reads/writes arrangements.
    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError>;

    /// Human-readable name for logging and metrics.
    fn name(&self) -> &str;

    /// Whether this operator has finished its work (e.g. bootstrap/snapshot complete).
    fn is_complete(&self) -> bool {
        false
    }
}

/// Output produced by an operator for one epoch.
#[derive(Debug)]
pub struct EpochOutput {
    /// The epoch number this output belongs to.
    pub epoch: Epoch,
    /// The output delta batches (may be empty if all rows were filtered).
    pub batches: Vec<ArrowZSet>,
}

impl EpochOutput {
    /// Create an epoch output with a single batch.
    pub fn single(epoch: Epoch, batch: ArrowZSet) -> Self {
        EpochOutput {
            epoch,
            batches: vec![batch],
        }
    }

    /// Create an empty epoch output (no rows to emit).
    pub fn empty(epoch: Epoch) -> Self {
        EpochOutput {
            epoch,
            batches: Vec::new(),
        }
    }

    /// Total number of rows across all batches in this output.
    pub fn total_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}
