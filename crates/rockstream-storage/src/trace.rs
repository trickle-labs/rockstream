//! Immutable Consolidated Trace Batches & Multi-Consumer Compaction (v0.59.6).
//!
//! Implements differential-style shared arrangement traces:
//! - Compacted base snapshot through frontier $F$
//! - Immutable sequence of sorted delta batches
//! - Per-consumer read frontiers
//! - Compaction frontier derived strictly from the slowest live consumer

use crate::error::StorageError;
use rockstream_types::arrangement::ArrangementSpec;
use rockstream_types::batch::{Weight, ZSetRow};
use rockstream_types::compatibility::StorageFormatVersion;
use rockstream_types::ids::{ArrangementId, ViewId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// A descriptor of an immutable delta batch segment stored in an arrangement trace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceSegmentDescriptor {
    pub segment_id: u64,
    pub from_frontier: u64,
    pub to_frontier: u64,
    pub row_count: usize,
    pub byte_size: usize,
}

/// A versioned manifest header for durable shared arrangement traces (v0.59.6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceManifestHeader {
    pub format_version: StorageFormatVersion,
    pub spec: ArrangementSpec,
    pub arrangement_id: ArrangementId,
    pub base_frontier: u64,
    pub segments: Vec<TraceSegmentDescriptor>,
}

impl TraceManifestHeader {
    /// Create a new V3 shared trace manifest header for the given spec.
    pub fn new(spec: ArrangementSpec, base_frontier: u64) -> Self {
        let arrangement_id = spec.arrangement_id();
        Self {
            format_version: StorageFormatVersion::V3,
            spec,
            arrangement_id,
            base_frontier,
            segments: Vec::new(),
        }
    }

    /// Serializes the trace manifest header to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, StorageError> {
        serde_json::to_vec(self).map_err(|e| {
            StorageError::Unsupported(format!("Failed to serialize trace manifest: {}", e))
        })
    }

    /// Deserializes and validates a trace manifest header, enforcing fail-closed checks.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, StorageError> {
        let header: Self = serde_json::from_slice(bytes).map_err(|_| {
            StorageError::Unsupported(
                "RS-5003: corrupted trace manifest header: invalid json or corrupt state"
                    .to_string(),
            )
        })?;
        if header.format_version != StorageFormatVersion::V3 {
            return Err(StorageError::IncompatibleFormat {
                stored: header.format_version.0,
                min: StorageFormatVersion::V3.0,
                max: StorageFormatVersion::V3.0,
            });
        }
        if header.arrangement_id != header.spec.arrangement_id() {
            return Err(StorageError::Unsupported(
                "RS-5003: trace manifest arrangement_id does not match canonical spec hash"
                    .to_string(),
            ));
        }
        Ok(header)
    }
}

/// An immutable sorted batch of Z-set deltas covering a closed frontier interval `(from_frontier, to_frontier]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceBatch {
    pub from_frontier: u64,
    pub to_frontier: u64,
    pub rows: Vec<ZSetRow>,
}

/// Type alias for the materialized trace snapshot mapping keys to values and weights.
pub type TraceSnapshot = BTreeMap<Vec<u8>, (Vec<u8>, Weight)>;

/// A physical shared arrangement trace holding state accessible across multiple concurrent consumers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedArrangementTrace {
    pub arrangement_id: ArrangementId,
    pub spec: ArrangementSpec,
    /// Frontier through which all deltas have been consolidated into `base_snapshot`.
    pub base_frontier: u64,
    /// Base snapshot state: Map of key -> (value, net_weight).
    pub base_snapshot: TraceSnapshot,
    /// Chronological sequence of immutable delta batches above `base_frontier`.
    pub delta_batches: Vec<TraceBatch>,
    /// Active consumer read frontiers.
    pub consumer_frontiers: HashMap<ViewId, u64>,
}

impl SharedArrangementTrace {
    /// Create a new shared trace for the given spec.
    pub fn new(spec: ArrangementSpec) -> Self {
        let arrangement_id = spec.arrangement_id();
        Self {
            arrangement_id,
            spec,
            base_frontier: 0,
            base_snapshot: BTreeMap::new(),
            delta_batches: Vec::new(),
            consumer_frontiers: HashMap::new(),
        }
    }

    /// Register a consumer at an initial read frontier.
    pub fn register_consumer_frontier(&mut self, consumer_id: ViewId, initial_frontier: u64) {
        self.consumer_frontiers
            .insert(consumer_id, initial_frontier);
    }

    /// Advance a consumer's read frontier.
    pub fn advance_consumer_frontier(&mut self, consumer_id: ViewId, new_frontier: u64) {
        if let Some(f) = self.consumer_frontiers.get_mut(&consumer_id) {
            if new_frontier > *f {
                *f = new_frontier;
            }
        }
    }

    /// Deregister a consumer from tracking.
    pub fn deregister_consumer(&mut self, consumer_id: ViewId) {
        self.consumer_frontiers.remove(&consumer_id);
    }

    /// Append an immutable consolidated delta batch to the trace.
    pub fn commit_trace_batch(
        &mut self,
        from_frontier: u64,
        to_frontier: u64,
        mut rows: Vec<ZSetRow>,
    ) {
        assert!(
            from_frontier >= self.base_frontier,
            "Cannot commit batch starting before base frontier {}",
            self.base_frontier
        );
        assert!(
            to_frontier >= from_frontier,
            "to_frontier must be >= from_frontier"
        );

        // Sort deterministically by key and value
        rows.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then_with(|| a.value.cmp(&b.value))
                .then_with(|| a.weight.cmp(&b.weight))
        });

        self.delta_batches.push(TraceBatch {
            from_frontier,
            to_frontier,
            rows,
        });
    }

    /// Read the materialized arrangement snapshot as of `target_frontier`.
    ///
    /// Combines `base_snapshot` with all delta batches up to `target_frontier`.
    pub fn read_trace_snapshot(&self, target_frontier: u64) -> Result<TraceSnapshot, StorageError> {
        if target_frontier < self.base_frontier {
            return Err(StorageError::EpochPruned {
                requested_epoch: target_frontier,
                min_retention_epoch: self.base_frontier,
            });
        }

        let mut snapshot = self.base_snapshot.clone();

        for batch in &self.delta_batches {
            if batch.to_frontier <= target_frontier {
                for row in &batch.rows {
                    let entry = snapshot.entry(row.key.clone());
                    match entry {
                        std::collections::btree_map::Entry::Vacant(v) => {
                            if row.weight != 0 {
                                v.insert((row.value.clone(), row.weight));
                            }
                        }
                        std::collections::btree_map::Entry::Occupied(mut o) => {
                            let (val, weight) = o.get_mut();
                            *val = row.value.clone();
                            *weight += row.weight;
                            if *weight == 0 {
                                o.remove();
                            }
                        }
                    }
                }
            }
        }

        Ok(snapshot)
    }

    /// Compute the dynamic compaction frontier derived from the slowest live consumer:
    /// $F_{\text{compaction}} = \min_{c \in \text{live\_consumers}} F_c$.
    pub fn compute_compaction_frontier(&self) -> u64 {
        if self.consumer_frontiers.is_empty() {
            // If no active consumers, compaction can advance to the latest committed batch
            self.delta_batches
                .last()
                .map(|b| b.to_frontier)
                .unwrap_or(self.base_frontier)
        } else {
            *self
                .consumer_frontiers
                .values()
                .min()
                .unwrap_or(&self.base_frontier)
        }
    }

    /// Compact the trace up to $F_{\text{compaction}}$.
    ///
    /// Merges all delta batches with `to_frontier <= F_{\text{compaction}}` into `base_snapshot`,
    /// reclaims compacted batches, and advances `base_frontier`.
    ///
    /// Compaction is strictly never permitted to advance beyond the slowest live consumer!
    pub fn compact_trace(&mut self) -> u64 {
        let compaction_frontier = self.compute_compaction_frontier();
        if let Some(min_consumer) = self.consumer_frontiers.values().min() {
            assert!(
                compaction_frontier <= *min_consumer,
                "INVARIANT: M2-S5 - compaction frontier must never exceed slowest live consumer read frontier"
            );
        }
        if compaction_frontier <= self.base_frontier {
            return self.base_frontier;
        }

        let mut remaining_batches = Vec::new();

        for batch in std::mem::take(&mut self.delta_batches) {
            if batch.to_frontier <= compaction_frontier {
                // Merge into base snapshot
                for row in batch.rows {
                    let entry = self.base_snapshot.entry(row.key);
                    match entry {
                        std::collections::btree_map::Entry::Vacant(v) => {
                            if row.weight != 0 {
                                v.insert((row.value, row.weight));
                            }
                        }
                        std::collections::btree_map::Entry::Occupied(mut o) => {
                            let (val, weight) = o.get_mut();
                            *val = row.value;
                            *weight += row.weight;
                            if *weight == 0 {
                                o.remove();
                            }
                        }
                    }
                }
            } else {
                remaining_batches.push(batch);
            }
        }

        self.delta_batches = remaining_batches;
        self.base_frontier = compaction_frontier;
        self.base_frontier
    }

    /// Returns the approximate memory/storage byte size of this shared trace.
    pub fn byte_size(&self) -> usize {
        let base_bytes: usize = self
            .base_snapshot
            .iter()
            .map(|(k, (v, _))| k.len() + v.len() + 8)
            .sum();
        let delta_bytes: usize = self
            .delta_batches
            .iter()
            .map(|b| {
                b.rows
                    .iter()
                    .map(|r| r.key.len() + r.value.len() + 8)
                    .sum::<usize>()
            })
            .sum();
        base_bytes + delta_bytes
    }
}
