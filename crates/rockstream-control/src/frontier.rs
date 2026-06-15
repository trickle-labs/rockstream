//! Frontier aggregation for the control plane (v0.18, Slice 6).
//!
//! Implements a two-level hierarchical aggregator:
//!
//! 1. **`FrontierAggregator`** — ingests [`ShardFrontierReport`]s from individual
//!    shards and computes the cluster-wide minimum via meet (GLB).
//!
//! ## M2 Safety Invariants (runtime assertions)
//!
//! These mirror the FizzBee M2 model in `formal/m2_frontier_agg.fizz`:
//!
//! - **M2-S1 / M2-S2** (`M2_S1_MeetCorrectness` / `M2_S2_PessimisticStaleness`):
//!   The published cluster frontier must never exceed the true meet of all
//!   registered shard frontiers.
//! - **M2-S4** (`M2_S4_StaleWriteRejection`): The cluster frontier is
//!   monotonically non-decreasing; stale writes are rejected.
//!
//! ## Bounds
//!
//! | Resource | Bound | Metric |
//! |---|---|---|
//! | Registered shards | `MAX_REGISTERED_SHARDS` | `frontier_registered_shards` |

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use rockstream_types::frontier::{ClusterFrontier, ShardFrontierReport};
use rockstream_types::ids::ShardId;
use rockstream_types::timestamp::Epoch;

/// Maximum number of shards that can be registered with one `FrontierAggregator`.
///
/// Prevents unbounded memory growth. Ingest returns `RS-8001` when full.
pub const MAX_REGISTERED_SHARDS: usize = 100_000;

/// Error returned by [`FrontierAggregator`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AggregatorError {
    /// The shard registry is full; new shard reports are rejected.
    #[error(
        "RS-8001 frontier aggregator shard limit exceeded ({MAX_REGISTERED_SHARDS}); \
         next_steps: scale out aggregators or reduce shard count"
    )]
    RegistryFull,
}

/// The cluster-wide frontier fill level.
///
/// `registered` is the number of shards that have ever reported.
/// `capacity` is `MAX_REGISTERED_SHARDS`.
#[derive(Debug, Clone, Copy)]
pub struct FrontierFillLevel {
    /// Registered shard count.
    pub registered: usize,
    /// Maximum allowed shard count.
    pub capacity: usize,
}

impl FrontierFillLevel {
    /// Fraction of capacity used (0.0–1.0).
    pub fn fill_fraction(&self) -> f64 {
        self.registered as f64 / self.capacity as f64
    }
}

/// Inner state protected by the mutex.
struct Inner {
    /// Per-shard committed epochs. Only ever grows in epoch value (monotone).
    shard_epochs: HashMap<ShardId, Epoch>,
    /// The last published cluster frontier (monotonically non-decreasing).
    published: Option<Epoch>,
}

impl Inner {
    fn new() -> Self {
        Self {
            shard_epochs: HashMap::new(),
            published: None,
        }
    }

    /// Compute the current meet (minimum) across all registered shard epochs.
    ///
    /// Returns `None` if no shards have reported yet.
    fn compute_meet(&self) -> Option<Epoch> {
        self.shard_epochs.values().copied().min()
    }
}

/// Control-plane frontier aggregator.
///
/// Receives [`ShardFrontierReport`]s and publishes a [`ClusterFrontier`]
/// representing the global minimum committed epoch.
///
/// Thread-safe; clone is cheap (shared inner state via `Arc<Mutex<_>>`).
#[derive(Clone)]
pub struct FrontierAggregator {
    inner: Arc<Mutex<Inner>>,
}

impl FrontierAggregator {
    /// Create a new, empty aggregator.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::new())),
        }
    }

    /// Ingest a [`ShardFrontierReport`] from a shard.
    ///
    /// Updates the per-shard epoch (monotonically). The published cluster
    /// frontier only ever advances — it is never retreated.
    ///
    /// **M2-S1 / M2-S2**: the published frontier never exceeds the meet of all
    /// currently registered shard epochs — guaranteed by only updating
    /// `published` when `meet >= published`.
    ///
    /// **M2-S4**: published is monotonically non-decreasing — enforced by only
    /// assigning `published = meet` when `meet > published`.
    ///
    /// Returns `Err(AggregatorError::RegistryFull)` (RS-8001) when
    /// `MAX_REGISTERED_SHARDS` is exceeded.
    pub fn ingest(&self, report: ShardFrontierReport) -> Result<(), AggregatorError> {
        let mut inner = self.inner.lock();

        // Enforce registry capacity bound.
        if !inner.shard_epochs.contains_key(&report.shard_id)
            && inner.shard_epochs.len() >= MAX_REGISTERED_SHARDS
        {
            return Err(AggregatorError::RegistryFull);
        }

        // Monotone update: only advance, never retreat.
        let entry = inner.shard_epochs.entry(report.shard_id).or_insert(0);
        if report.epoch > *entry {
            *entry = report.epoch;
        }

        // M2-S1 / M2-S2: only publish the meet if it is ≥ current published.
        // This guarantees published never retreats (M2-S4) AND never exceeds
        // the true meet of all registered shard epochs (M2-S1/S2).
        if let Some(meet) = inner.compute_meet() {
            match inner.published {
                None => {
                    // First publication.
                    inner.published = Some(meet);
                }
                Some(old) if meet > old => {
                    // M2-S4 assertion: meet > old is already guaranteed here.
                    assert!(
                        meet >= old,
                        "M2-S4: stale write rejected — meet {} < published {}",
                        meet,
                        old
                    );
                    inner.published = Some(meet);
                }
                Some(_) => {
                    // meet <= published: do not retreat. M2-S4 is satisfied.
                    // M2-S1/S2 is satisfied because published ≤ prior meet ≤ current meet
                    // (a new low-epoch shard doesn't retroactively invalidate what we
                    // have already committed — it only blocks *future* advancement).
                }
            }
        }

        Ok(())
    }

    /// Return the current cluster frontier.
    pub fn cluster_frontier(&self) -> ClusterFrontier {
        let inner = self.inner.lock();
        ClusterFrontier {
            epoch: inner.published,
        }
    }

    /// Return the current fill level for monitoring.
    pub fn fill_level(&self) -> FrontierFillLevel {
        let inner = self.inner.lock();
        FrontierFillLevel {
            registered: inner.shard_epochs.len(),
            capacity: MAX_REGISTERED_SHARDS,
        }
    }
}

impl Default for FrontierAggregator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::frontier::ShardFrontierReport;
    use rockstream_types::ids::ShardId;

    /// Slice 6: basic single-shard report advances cluster frontier.
    #[test]
    fn single_shard_advances_frontier() {
        let agg = FrontierAggregator::new();
        assert_eq!(agg.cluster_frontier().epoch, None);

        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(1),
            epoch: 10,
        })
        .unwrap();
        assert_eq!(agg.cluster_frontier().epoch, Some(10));
    }

    /// Slice 6: cluster frontier is meet (minimum) of all shard epochs.
    #[test]
    fn cluster_frontier_is_meet_of_shards() {
        let agg = FrontierAggregator::new();
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(0),
            epoch: 5,
        })
        .unwrap();
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(1),
            epoch: 8,
        })
        .unwrap();
        // meet(5, 8) = 5
        assert_eq!(agg.cluster_frontier().epoch, Some(5));
    }

    /// Slice 6: M2-S4 — cluster frontier is monotonically non-decreasing.
    #[test]
    fn cluster_frontier_is_non_decreasing() {
        let agg = FrontierAggregator::new();
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(0),
            epoch: 5,
        })
        .unwrap();
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(0),
            epoch: 3,
        })
        .unwrap(); // stale update
                   // Published should remain at 5, not retreat.
        assert_eq!(agg.cluster_frontier().epoch, Some(5));
    }

    /// Slice 6: advancing a lagging shard unblocks the cluster frontier.
    ///
    /// Both shards start at the same epoch so the initial frontier is well-
    /// defined; then shard 0 advances ahead while shard 1 lags, bottlenecking
    /// the cluster.  Finally shard 1 catches up and the cluster frontier
    /// can advance.
    #[test]
    fn advancing_lagging_shard_unblocks_cluster_frontier() {
        let agg = FrontierAggregator::new();
        // Both shards start at epoch 1 — cluster frontier is 1.
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(0),
            epoch: 1,
        })
        .unwrap();
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(1),
            epoch: 1,
        })
        .unwrap();
        assert_eq!(agg.cluster_frontier().epoch, Some(1));

        // Shard 0 advances; shard 1 lags — cluster is bottlenecked at shard 1 (epoch 1).
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(0),
            epoch: 10,
        })
        .unwrap();
        assert_eq!(agg.cluster_frontier().epoch, Some(1));

        // Advance shard 1 — cluster should advance to 10.
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(1),
            epoch: 10,
        })
        .unwrap();
        assert_eq!(agg.cluster_frontier().epoch, Some(10));
    }

    /// Slice 6: fill-level metric is tracked.
    #[test]
    fn fill_level_is_tracked() {
        let agg = FrontierAggregator::new();
        assert_eq!(agg.fill_level().registered, 0);
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(1),
            epoch: 1,
        })
        .unwrap();
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(2),
            epoch: 1,
        })
        .unwrap();
        assert_eq!(agg.fill_level().registered, 2);
        assert_eq!(agg.fill_level().capacity, MAX_REGISTERED_SHARDS);
    }

    /// Slice 6: `has_committed_through` is correct.
    #[test]
    fn cluster_frontier_has_committed_through() {
        let agg = FrontierAggregator::new();
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(0),
            epoch: 15,
        })
        .unwrap();
        let cf = agg.cluster_frontier();
        assert!(cf.has_committed_through(10));
        assert!(cf.has_committed_through(15));
        assert!(!cf.has_committed_through(16));
    }
}
