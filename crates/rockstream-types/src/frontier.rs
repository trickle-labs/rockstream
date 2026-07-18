//! Frontier / antichain types for progress tracking (v0.32).
//!
//! A frontier represents the boundary of processed time — the set of
//! timestamps at which new data may still arrive.
//!
//! v0.32 adds the three-layer frontier protocol:
//!
//! - `ShardFrontierReport` — a single shard's current committed epoch.
//! - `WorkerFrontierSummary` — the minimum epoch across all shards on a worker.
//! - `ClusterFrontier` — the global minimum across all worker summaries.
//! - `CompleteThroughToken` — emitted by monotone (semilattice) laws to signal
//!   partial progress ahead of the cluster frontier.

use crate::ids::{OperatorId, ShardId, SourceId, WorkerId};
use crate::merge_law::MergeLawId;
use crate::timestamp::Epoch;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

pub use antichain::{Antichain, Frontier, Lattice, ProductTimestamp};

/// RockStream progress-merge algebra (NOT a mathematical lattice in general:
/// `FreshnessToken` is intentionally non-idempotent due to its hash field).
pub trait ProgressMerge {
    /// Greatest lower bound (or approximate GLB for non-idempotent types).
    fn meet(&self, other: &Self) -> Self;
    /// Least upper bound (or approximate LUB for non-idempotent types).
    fn join(&self, other: &Self) -> Self;
}

/// Progress details for a single source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SourceProgress {
    pub source_epoch: Epoch,
    pub event_time_watermark_ms: Option<i64>,
}

impl SourceProgress {
    pub fn new(source_epoch: Epoch, event_time_watermark_ms: Option<i64>) -> Self {
        Self {
            source_epoch,
            event_time_watermark_ms,
        }
    }
}

impl ProgressMerge for SourceProgress {
    fn meet(&self, other: &Self) -> Self {
        if self.source_epoch < other.source_epoch {
            *self
        } else if self.source_epoch > other.source_epoch {
            *other
        } else {
            Self {
                source_epoch: self.source_epoch,
                event_time_watermark_ms: match (
                    self.event_time_watermark_ms,
                    other.event_time_watermark_ms,
                ) {
                    (Some(w1), Some(w2)) => Some(std::cmp::min(w1, w2)),
                    (Some(w1), None) => Some(w1),
                    (None, Some(w2)) => Some(w2),
                    (None, None) => None,
                },
            }
        }
    }

    fn join(&self, other: &Self) -> Self {
        if self.source_epoch > other.source_epoch {
            *self
        } else if self.source_epoch < other.source_epoch {
            *other
        } else {
            Self {
                source_epoch: self.source_epoch,
                event_time_watermark_ms: match (
                    self.event_time_watermark_ms,
                    other.event_time_watermark_ms,
                ) {
                    (Some(w1), Some(w2)) => Some(std::cmp::max(w1, w2)),
                    (Some(w1), None) => Some(w1),
                    (None, Some(w2)) => Some(w2),
                    (None, None) => None,
                },
            }
        }
    }
}

/// A vector-valued progress token representing progress across multiple sources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreshnessToken {
    pub source_progress: BTreeMap<SourceId, SourceProgress>,
    pub cluster_frontier_hash: u64,
}

impl FreshnessToken {
    pub fn new(
        source_progress: BTreeMap<SourceId, SourceProgress>,
        cluster_frontier_hash: u64,
    ) -> Self {
        Self {
            source_progress,
            cluster_frontier_hash,
        }
    }

    /// Retrieve the minimum event_time_watermark_ms across all sources.
    pub fn watermark_ms(&self) -> Option<i64> {
        self.source_progress
            .values()
            .filter_map(|p| p.event_time_watermark_ms)
            .min()
    }
}

impl ProgressMerge for FreshnessToken {
    fn meet(&self, other: &Self) -> Self {
        let mut source_progress = BTreeMap::new();
        for (id, p1) in &self.source_progress {
            if let Some(p2) = other.source_progress.get(id) {
                source_progress.insert(*id, p1.meet(p2));
            }
        }
        let cluster_frontier_hash = self.cluster_frontier_hash ^ other.cluster_frontier_hash;
        Self {
            source_progress,
            cluster_frontier_hash,
        }
    }

    fn join(&self, other: &Self) -> Self {
        let mut source_progress = self.source_progress.clone();
        for (id, p2) in &other.source_progress {
            if let Some(p1) = source_progress.get(id) {
                source_progress.insert(*id, p1.join(p2));
            } else {
                source_progress.insert(*id, *p2);
            }
        }
        let cluster_frontier_hash = self.cluster_frontier_hash ^ other.cluster_frontier_hash;
        Self {
            source_progress,
            cluster_frontier_hash,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_freshness_token_lattice_properties() {
        use crate::ids::SourceId;
        use std::collections::BTreeMap;

        let s1 = SourceId(1);
        let s2 = SourceId(2);

        let p1 = SourceProgress::new(10, Some(100));
        let p2 = SourceProgress::new(20, Some(200));
        let p3 = SourceProgress::new(15, Some(150));

        let mut map_a = BTreeMap::new();
        map_a.insert(s1, p1);
        map_a.insert(s2, p2);
        let tok_a = FreshnessToken::new(map_a, 12345);

        let mut map_b = BTreeMap::new();
        map_b.insert(s1, p3);
        let tok_b = FreshnessToken::new(map_b, 67890);

        // Meet (intersection, element-wise min)
        let meet = tok_a.meet(&tok_b);
        assert_eq!(meet.source_progress.len(), 1);
        assert_eq!(meet.source_progress.get(&s1), Some(&p1)); // min of 10 and 15 is 10

        // Join (union, element-wise max)
        let join = tok_a.join(&tok_b);
        assert_eq!(join.source_progress.len(), 2);
        assert_eq!(join.source_progress.get(&s1), Some(&p3)); // max of 10 and 15 is 15
        assert_eq!(join.source_progress.get(&s2), Some(&p2));

        // Commutativity
        assert_eq!(
            tok_a.meet(&tok_b).source_progress,
            tok_b.meet(&tok_a).source_progress
        );
        assert_eq!(
            tok_a.join(&tok_b).source_progress,
            tok_b.join(&tok_a).source_progress
        );
    }
}

// ─── Three-layer frontier protocol (v0.32) ───────────────────────────────────

/// A single shard's report of its current committed epoch.
///
/// Emitted by `ShardFrontierReporter` after every successful `commit_epoch`
/// call. The `epoch` field is the *next* epoch to be processed (i.e., all
/// epochs strictly less than `epoch` are durably committed on this shard).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardFrontierReport {
    /// The shard that emitted this report.
    pub shard_id: ShardId,
    /// The current committed frontier epoch on this shard.
    pub epoch: Epoch,
}

/// A worker's summary of its per-shard frontiers.
///
/// Computed by `WorkerFrontierAggregator` as the minimum `epoch` across all
/// registered shards.  Consumers do not need per-shard subscriptions to track
/// global progress — subscribing to worker summaries is sufficient.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerFrontierSummary {
    /// The worker that produced this summary.
    pub worker_id: WorkerId,
    /// Minimum committed epoch across all shards on this worker.
    /// `None` if the worker has no registered shards yet.
    pub min_epoch: Option<Epoch>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardColumnStats {
    pub shard_id: ShardId,
    pub view_id: crate::ids::ViewId,
    pub checkpoint_epoch: u64,
    pub col_stats: Vec<ColumnStats>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnStats {
    pub col_idx: u16,
    pub min_bytes: Option<Bytes>,
    pub max_bytes: Option<Bytes>,
    pub bloom_filter: Option<Bytes>,
    pub null_count: u64,
    pub distinct_count_hll: Bytes,
}

impl ColumnStats {
    pub fn from_values(
        col_idx: u16,
        values: &[Option<Vec<u8>>],
        bloom_budget_bytes: usize,
    ) -> Self {
        let mut min_bytes: Option<Vec<u8>> = None;
        let mut max_bytes: Option<Vec<u8>> = None;
        let mut bloom_values = Vec::new();
        let mut null_count = 0_u64;
        let mut hll = [0_u8; 64];
        for value in values {
            match value {
                Some(bytes) => {
                    if min_bytes.as_ref().is_none_or(|min| bytes < min) {
                        min_bytes = Some(bytes.clone());
                    }
                    if max_bytes.as_ref().is_none_or(|max| bytes > max) {
                        max_bytes = Some(bytes.clone());
                    }
                    bloom_values.push(bytes.clone());
                    hll_add(&mut hll, bytes);
                }
                None => null_count += 1,
            }
        }
        Self {
            col_idx,
            min_bytes: min_bytes.map(Bytes::from),
            max_bytes: max_bytes.map(Bytes::from),
            bloom_filter: (!bloom_values.is_empty())
                .then(|| build_budget_capped_bloom_filter(&bloom_values, bloom_budget_bytes)),
            null_count,
            distinct_count_hll: Bytes::from(hll.to_vec()),
        }
    }
}

pub fn build_budget_capped_bloom_filter(values: &[Vec<u8>], budget_bytes: usize) -> Bytes {
    let total_bytes = budget_bytes.max(9);
    let bitset_bytes = total_bytes.saturating_sub(1);
    let mut filter = vec![3_u8; total_bytes];
    if bitset_bytes == 0 {
        return Bytes::from(filter);
    }
    for value in values {
        for bit in bloom_hashes(value, bitset_bytes * 8, filter[0]) {
            let idx = 1 + (bit / 8);
            let mask = 1_u8 << (bit % 8);
            filter[idx] |= mask;
        }
    }
    Bytes::from(filter)
}

pub fn build_exact_membership_filter(values: &[Vec<u8>]) -> Bytes {
    let mut encoded = vec![0_u8];
    for value in values {
        encoded.extend_from_slice(&(value.len() as u32).to_be_bytes());
        encoded.extend_from_slice(value);
    }
    Bytes::from(encoded)
}

pub fn bloom_filter_might_contain(filter: &[u8], value: &[u8]) -> bool {
    if filter.len() <= 1 {
        return true;
    }
    if filter[0] == 0 {
        let mut offset = 1;
        while offset + 4 <= filter.len() {
            let len = u32::from_be_bytes(filter[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + len > filter.len() {
                return true;
            }
            if &filter[offset..offset + len] == value {
                return true;
            }
            offset += len;
        }
        return false;
    }
    bloom_hashes(value, (filter.len() - 1) * 8, filter[0].max(1))
        .into_iter()
        .all(|bit| {
            let idx = 1 + (bit / 8);
            let mask = 1_u8 << (bit % 8);
            filter.get(idx).is_some_and(|byte| (*byte & mask) != 0)
        })
}

fn bloom_hashes(value: &[u8], modulus: usize, hashes: u8) -> Vec<usize> {
    (0..hashes.max(1))
        .map(|seed| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            seed.hash(&mut hasher);
            value.hash(&mut hasher);
            (hasher.finish() as usize) % modulus.max(1)
        })
        .collect()
}

fn hll_add(registers: &mut [u8; 64], value: &[u8]) {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    let hash = hasher.finish();
    let idx = (hash & 0x3f) as usize;
    let rho = ((hash >> 6).leading_zeros() + 1).min(u8::MAX as u32) as u8;
    registers[idx] = registers[idx].max(rho);
}

/// The cluster-wide committed frontier.
///
/// `ClusterFrontierPublisher` computes this as the minimum `min_epoch` across
/// all `WorkerFrontierSummary` values.  An epoch `e` being in the cluster
/// frontier means *every* shard in the cluster has durably committed all
/// epochs `< e`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterFrontier {
    /// The global minimum committed epoch.
    /// `None` if any worker has not yet reported a frontier.
    pub epoch: Option<Epoch>,
}

impl ClusterFrontier {
    /// Returns `true` if the cluster has committed all epochs strictly before
    /// `epoch`, i.e., `self.epoch >= Some(epoch)`.
    pub fn has_committed_through(&self, epoch: Epoch) -> bool {
        self.epoch.is_some_and(|f| f >= epoch)
    }
}

/// A partial-progress token emitted by a monotone (semilattice / idempotent)
/// law before the full cluster frontier has advanced.
///
/// Monotone laws satisfy `merge(a, merge(a, b)) = merge(a, b)` (idempotent),
/// so intermediate results can be published safely — re-processing the same
/// input is a no-op.  An operator that holds a `CompleteThroughToken` may
/// expose its current state to downstream consumers even while earlier shards
/// are still catching up.
///
/// Non-monotone laws (e.g. `WeightAdd/v1`, `SumCount/v1`) must **not** emit
/// these tokens because they accumulate retractions; premature output would
/// be incorrect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteThroughToken {
    /// The operator that produced this token.
    pub operator_id: OperatorId,
    /// The law governing this operator's state.
    pub law_id: MergeLawId,
    /// All epochs strictly before `complete_through` are reflected in the
    /// operator's current output, regardless of the cluster frontier.
    pub complete_through: Epoch,
}

#[cfg(test)]
mod protocol_tests {
    use super::*;
    use crate::ids::{OperatorId, ShardId, WorkerId};
    use crate::merge_law::MergeLawId;

    #[test]
    fn cluster_frontier_has_committed_through() {
        let cf = ClusterFrontier { epoch: Some(10) };
        assert!(cf.has_committed_through(10));
        assert!(cf.has_committed_through(5));
        assert!(!cf.has_committed_through(11));
    }

    #[test]
    fn cluster_frontier_none_never_committed() {
        let cf = ClusterFrontier { epoch: None };
        assert!(!cf.has_committed_through(0));
    }

    #[test]
    fn shard_frontier_report_fields() {
        let r = ShardFrontierReport {
            shard_id: ShardId(7),
            epoch: 42,
        };
        assert_eq!(r.shard_id, ShardId(7));
        assert_eq!(r.epoch, 42);
    }

    #[test]
    fn worker_frontier_summary_fields() {
        let s = WorkerFrontierSummary {
            worker_id: WorkerId(1),
            min_epoch: Some(5),
        };
        assert_eq!(s.min_epoch, Some(5));
    }

    #[test]
    fn shard_column_stats_roundtrip_tests() {
        let stats = ShardColumnStats {
            shard_id: ShardId(7),
            view_id: crate::ids::ViewId(11),
            checkpoint_epoch: 99,
            col_stats: vec![ColumnStats {
                col_idx: 2,
                min_bytes: Some(Bytes::from_static(b"a")),
                max_bytes: Some(Bytes::from_static(b"z")),
                bloom_filter: Some(Bytes::from_static(b"\x03abcdefghi")),
                null_count: 1,
                distinct_count_hll: Bytes::from(vec![0; 64]),
            }],
        };
        let json = serde_json::to_vec(&stats).unwrap();
        let decoded: ShardColumnStats = serde_json::from_slice(&json).unwrap();
        assert_eq!(decoded, stats);
    }

    #[test]
    fn bloom_filter_never_exceeds_budget_bytes_at_high_cardinality() {
        let values: Vec<Vec<u8>> = (0..10_000)
            .map(|idx| format!("customer-{idx:05}").into_bytes())
            .collect();
        let filter = build_budget_capped_bloom_filter(&values, 128);
        crate::metrics::set_shard_bloom_filter_bytes_used(1, 2, 3, filter.len() as u64);
        assert!(filter.len() <= 128);
        assert_eq!(
            crate::metrics::read_shard_bloom_filter_bytes_used(1, 2, 3),
            Some(filter.len() as u64)
        );
        assert!(bloom_filter_might_contain(&filter, b"customer-00042"));
    }

    #[test]
    fn complete_through_token_fields() {
        let tok = CompleteThroughToken {
            operator_id: OperatorId(3),
            law_id: MergeLawId(9),
            complete_through: 100,
        };
        assert_eq!(tok.complete_through, 100);
    }
}
