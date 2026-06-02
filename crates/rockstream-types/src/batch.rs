//! Batch types shared across connectors and operators.
//!
//! These live in `rockstream-types` so that both the connector layer and the
//! operator layer can depend on them without creating a circular dependency.
//!
//! The core abstraction is `ZSet`: a multiset of `(key, value, weight)` tuples
//! representing insertions (+1) and deletions (-1) in the IVM delta model.

use crate::timestamp::Epoch;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Opaque token representing source offset position.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OffsetToken(pub String);

/// A batch of records produced by a source in one epoch.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceBatch {
    /// Number of records in this batch.
    pub record_count: usize,
    /// Epoch this batch belongs to.
    pub epoch: Epoch,
    /// Opaque source offset.
    pub offset: Option<OffsetToken>,
    /// Event-time watermark.
    pub watermark: Option<crate::timestamp::EventTimeWatermark>,
}

/// A batch of records to be written by a sink.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SinkBatch {
    /// Number of records in this batch.
    pub record_count: usize,
    /// Epoch this batch belongs to.
    pub epoch: Epoch,
}

/// A weight value for Z-set rows (IVM delta encoding).
/// Positive = insert, negative = delete, zero = no-op (retracted).
pub type Weight = i64;

/// A row in a Z-set: raw key bytes, raw value bytes, and a weight.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZSetRow {
    /// The row key (determines grouping/identity).
    pub key: Vec<u8>,
    /// The row value payload.
    pub value: Vec<u8>,
    /// The delta weight: +1 = insert, -1 = delete.
    pub weight: Weight,
}

/// A Z-set: a finite map from `(key, value)` to `Weight`.
///
/// This is the fundamental data structure of IVM. A Z-set represents a
/// *change* (delta) to a relation: positive weights are insertions,
/// negative weights are deletions, and zero-weight entries are no-ops
/// that can be garbage-collected.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZSet {
    /// Rows indexed by key for efficient lookup and merging.
    /// The inner map is value → weight.
    entries: BTreeMap<Vec<u8>, BTreeMap<Vec<u8>, Weight>>,
}

impl ZSet {
    /// Create an empty Z-set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a row into the Z-set. Weights are additive: inserting the same
    /// (key, value) with weight +1 twice yields weight +2.
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>, weight: Weight) {
        if weight == 0 {
            return;
        }
        let inner = self.entries.entry(key).or_default();
        let w = inner.entry(value).or_insert(0);
        *w += weight;
        // GC zero-weight entries
        if *w == 0 {
            let val_key = inner.keys().next().cloned();
            if let Some(k) = val_key {
                if inner.get(&k) == Some(&0) {
                    inner.remove(&k);
                }
            }
        }
    }

    /// Insert a row, cleaning up zero-weight entries.
    pub fn insert_row(&mut self, row: ZSetRow) {
        self.insert(row.key, row.value, row.weight);
    }

    /// Number of distinct (key, value) pairs with non-zero weight.
    pub fn len(&self) -> usize {
        self.entries.values().map(|inner| inner.len()).sum()
    }

    /// Returns true if the Z-set contains no entries with non-zero weight.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over all rows with non-zero weight.
    pub fn iter(&self) -> impl Iterator<Item = ZSetRow> + '_ {
        self.entries.iter().flat_map(|(key, inner)| {
            inner
                .iter()
                .filter(|(_, w)| **w != 0)
                .map(move |(value, weight)| ZSetRow {
                    key: key.clone(),
                    value: value.clone(),
                    weight: *weight,
                })
        })
    }

    /// Merge another Z-set into this one (additive union of weights).
    pub fn merge(&mut self, other: &ZSet) {
        for row in other.iter() {
            self.insert(row.key, row.value, row.weight);
        }
    }

    /// Negate all weights (produces the "undo" of this delta).
    pub fn negate(&self) -> ZSet {
        let mut result = ZSet::new();
        for row in self.iter() {
            result.insert(row.key, row.value, -row.weight);
        }
        result
    }

    /// Lookup the total weight for a given key across all values.
    pub fn weight_for_key(&self, key: &[u8]) -> Weight {
        self.entries
            .get(key)
            .map(|inner| inner.values().sum())
            .unwrap_or(0)
    }

    /// Consolidate: remove all zero-weight entries in place.
    pub fn consolidate(&mut self) {
        self.entries.retain(|_, inner| {
            inner.retain(|_, w| *w != 0);
            !inner.is_empty()
        });
    }
}

/// A Z-set batch tagged with its epoch — the unit of data flow between operators.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ZSetBatch {
    /// The delta for this epoch.
    pub zset: ZSet,
    /// Epoch this batch belongs to.
    pub epoch: Epoch,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_batch_default() {
        let b = SourceBatch::default();
        assert_eq!(b.record_count, 0);
        assert_eq!(b.epoch, 0);
    }

    #[test]
    fn sink_batch_default() {
        let b = SinkBatch::default();
        assert_eq!(b.record_count, 0);
        assert_eq!(b.epoch, 0);
    }

    #[test]
    fn zset_insert_and_lookup() {
        let mut zs = ZSet::new();
        zs.insert(b"k1".to_vec(), b"v1".to_vec(), 1);
        zs.insert(b"k1".to_vec(), b"v1".to_vec(), 1);
        assert_eq!(zs.weight_for_key(b"k1"), 2);
    }

    #[test]
    fn zset_insert_delete_cancels() {
        let mut zs = ZSet::new();
        zs.insert(b"k1".to_vec(), b"v1".to_vec(), 1);
        zs.insert(b"k1".to_vec(), b"v1".to_vec(), -1);
        zs.consolidate();
        assert!(zs.is_empty());
    }

    #[test]
    fn zset_merge() {
        let mut a = ZSet::new();
        a.insert(b"k1".to_vec(), b"v1".to_vec(), 1);

        let mut b = ZSet::new();
        b.insert(b"k1".to_vec(), b"v1".to_vec(), 2);
        b.insert(b"k2".to_vec(), b"v2".to_vec(), 1);

        a.merge(&b);
        assert_eq!(a.weight_for_key(b"k1"), 3);
        assert_eq!(a.weight_for_key(b"k2"), 1);
    }

    #[test]
    fn zset_negate() {
        let mut zs = ZSet::new();
        zs.insert(b"k1".to_vec(), b"v1".to_vec(), 3);
        let neg = zs.negate();
        assert_eq!(neg.weight_for_key(b"k1"), -3);
    }

    #[test]
    fn zset_iter_skips_zero() {
        let mut zs = ZSet::new();
        zs.insert(b"k1".to_vec(), b"v1".to_vec(), 1);
        zs.insert(b"k1".to_vec(), b"v1".to_vec(), -1);
        assert_eq!(zs.iter().count(), 0);
    }
}
