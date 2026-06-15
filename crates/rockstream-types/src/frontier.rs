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

use crate::ids::{OperatorId, ShardId, WorkerId};
use crate::merge_law::MergeLawId;
use crate::timestamp::Epoch;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Antichain<T> {
    elements: Vec<T>,
}

impl<T: PartialEq> PartialEq for Antichain<T> {
    fn eq(&self, other: &Self) -> bool {
        if self.elements.len() != other.elements.len() {
            return false;
        }
        self.elements.iter().all(|a| other.elements.contains(a))
    }
}

impl<T: Eq> Eq for Antichain<T> {}

impl<T: PartialOrd + Clone> Antichain<T> {
    /// Create an empty antichain (representing "no progress").
    pub fn empty() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    /// Create an antichain from a single element.
    pub fn from_elem(elem: T) -> Self {
        Self {
            elements: vec![elem],
        }
    }

    /// Returns the elements of the antichain.
    pub fn elements(&self) -> &[T] {
        &self.elements
    }

    /// Returns true if the antichain is empty.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Returns the number of elements in the antichain.
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Returns true if `time` is less than or equal to some element in the frontier.
    ///
    /// If this returns true, the time has NOT yet been completed.
    pub fn less_equal(&self, time: &T) -> bool {
        self.elements.iter().any(|e| e <= time)
    }

    /// Insert an element, maintaining the antichain invariant.
    pub fn insert(&mut self, elem: T) {
        if self.elements.iter().any(|e| e <= &elem) {
            return;
        }
        self.elements.retain(|e| {
            !matches!(
                elem.partial_cmp(e),
                Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal)
            )
        });
        self.elements.push(elem);
    }
}

impl<T: PartialOrd + Clone + fmt::Display> fmt::Display for Antichain<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, elem) in self.elements.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{elem}")?;
        }
        write!(f, "]")
    }
}

/// A mathematical lattice supporting meet (GLB) and join (LUB) operations.
pub trait Lattice {
    /// Greatest lower bound (GLB).
    fn meet(&self, other: &Self) -> Self;
    /// Least upper bound (LUB).
    fn join(&self, other: &Self) -> Self;
}

impl Lattice for u64 {
    fn meet(&self, other: &Self) -> Self {
        std::cmp::min(*self, *other)
    }

    fn join(&self, other: &Self) -> Self {
        std::cmp::max(*self, *other)
    }
}

/// A progress frontier represented as an antichain of timestamps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frontier<T> {
    antichain: Antichain<T>,
}

impl<T: PartialOrd + Clone> Frontier<T> {
    /// Create an empty frontier.
    pub fn empty() -> Self {
        Self {
            antichain: Antichain::empty(),
        }
    }

    /// Create a frontier from a single element.
    pub fn from_elem(elem: T) -> Self {
        Self {
            antichain: Antichain::from_elem(elem),
        }
    }

    /// Create a frontier from an iterator of elements.
    pub fn from_elements(elements: impl IntoIterator<Item = T>) -> Self {
        let mut antichain = Antichain::empty();
        for elem in elements {
            antichain.insert(elem);
        }
        Self { antichain }
    }

    /// Returns the elements of the frontier.
    pub fn elements(&self) -> &[T] {
        self.antichain.elements()
    }

    /// Returns true if the frontier is empty.
    pub fn is_empty(&self) -> bool {
        self.antichain.is_empty()
    }

    /// Returns true if `time` is less than or equal to some element in the frontier.
    pub fn less_equal(&self, time: &T) -> bool {
        self.antichain.less_equal(time)
    }

    /// Compute the meet (greatest lower bound) of two frontiers.
    pub fn meet(&self, other: &Self) -> Self {
        let mut result = self.antichain.clone();
        for elem in other.elements() {
            result.insert(elem.clone());
        }
        Self { antichain: result }
    }
}

impl<T: Lattice + PartialOrd + Clone> Frontier<T> {
    /// Compute the join (least upper bound) of two frontiers.
    pub fn join(&self, other: &Self) -> Self {
        let mut result = Antichain::empty();
        for a in self.elements() {
            for b in other.elements() {
                result.insert(a.join(b));
            }
        }
        Self { antichain: result }
    }

    /// Advance this frontier by joining it with another.
    pub fn advance(&mut self, other: &Self) {
        *self = self.join(other);
    }
}

impl<T: PartialOrd + Clone + fmt::Display> fmt::Display for Frontier<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, elem) in self.elements().iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{elem}")?;
        }
        write!(f, "]")
    }
}

/// A product of two timestamps, representing a multi-dimensional logical timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProductTimestamp<T1, T2> {
    pub outer: T1,
    pub inner: T2,
}

impl<T1, T2> ProductTimestamp<T1, T2> {
    /// Create a new product timestamp.
    pub fn new(outer: T1, inner: T2) -> Self {
        Self { outer, inner }
    }
}

impl<T1: fmt::Display, T2: fmt::Display> fmt::Display for ProductTimestamp<T1, T2> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.outer, self.inner)
    }
}

impl<T1: PartialOrd, T2: PartialOrd> PartialOrd for ProductTimestamp<T1, T2> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let outer_cmp = self.outer.partial_cmp(&other.outer)?;
        let inner_cmp = self.inner.partial_cmp(&other.inner)?;
        match (outer_cmp, inner_cmp) {
            (std::cmp::Ordering::Equal, std::cmp::Ordering::Equal) => {
                Some(std::cmp::Ordering::Equal)
            }
            (
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal,
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal,
            ) => Some(std::cmp::Ordering::Less),
            (
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal,
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal,
            ) => Some(std::cmp::Ordering::Greater),
            _ => None,
        }
    }
}

impl<T1: Lattice, T2: Lattice> Lattice for ProductTimestamp<T1, T2> {
    fn meet(&self, other: &Self) -> Self {
        Self::new(self.outer.meet(&other.outer), self.inner.meet(&other.inner))
    }

    fn join(&self, other: &Self) -> Self {
        Self::new(self.outer.join(&other.outer), self.inner.join(&other.inner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn antichain_from_elem() {
        let ac = Antichain::from_elem(5u64);
        assert_eq!(ac.elements(), &[5]);
    }

    #[test]
    fn antichain_empty() {
        let ac: Antichain<u64> = Antichain::empty();
        assert!(ac.is_empty());
        assert_eq!(ac.len(), 0);
    }

    #[test]
    fn antichain_less_equal() {
        let ac = Antichain::from_elem(5u64);
        assert!(ac.less_equal(&5));
        assert!(ac.less_equal(&6));
        assert!(!ac.less_equal(&4));
    }

    #[test]
    fn antichain_display() {
        let ac = Antichain::from_elem(42u64);
        assert_eq!(ac.to_string(), "[42]");
    }

    #[test]
    fn test_frontier_lattice_properties() {
        let f1 = Frontier::from_elements(vec![2u64, 5u64]);
        let f2 = Frontier::from_elements(vec![3u64, 4u64]);
        let f3 = Frontier::from_elements(vec![1u64, 6u64]);

        let pt = |o, i| ProductTimestamp::new(o, i);
        let v1 = Frontier::from_elements(vec![pt(2, 5), pt(5, 2)]);
        let v2 = Frontier::from_elements(vec![pt(3, 4), pt(4, 3)]);
        let v3 = Frontier::from_elements(vec![pt(1, 6), pt(6, 1)]);

        assert_eq!(f1.meet(&f2), f2.meet(&f1));
        assert_eq!(v1.meet(&v2), v2.meet(&v1));
        assert_eq!(f1.join(&f2), f2.join(&f1));
        assert_eq!(v1.join(&v2), v2.join(&v1));

        assert_eq!(f1.meet(&f2.meet(&f3)), f1.meet(&f2).meet(&f3));
        assert_eq!(v1.meet(&v2.meet(&v3)), v1.meet(&v2).meet(&v3));
        assert_eq!(f1.join(&f2.join(&f3)), f1.join(&f2).join(&f3));
        assert_eq!(v1.join(&v2.join(&v3)), v1.join(&v2).join(&v3));

        assert_eq!(f1.meet(&f1.join(&f2)), f1);
        assert_eq!(v1.meet(&v1.join(&v2)), v1);
        assert_eq!(f1.join(&f1.meet(&f2)), f1);
        assert_eq!(v1.join(&v1.meet(&v2)), v1);

        assert_eq!(f1.meet(&f2.join(&f3)), f1.meet(&f2).join(&f1.meet(&f3)));
        assert_eq!(v1.meet(&v2.join(&v3)), v1.meet(&v2).join(&v1.meet(&v3)));
        assert_eq!(f1.join(&f2.meet(&f3)), f1.join(&f2).meet(&f1.join(&f3)));
        assert_eq!(v1.join(&v2.meet(&v3)), v1.join(&v2).meet(&v1.join(&v3)));
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
    fn complete_through_token_fields() {
        let tok = CompleteThroughToken {
            operator_id: OperatorId(3),
            law_id: MergeLawId(9),
            complete_through: 100,
        };
        assert_eq!(tok.complete_through, 100);
    }
}
