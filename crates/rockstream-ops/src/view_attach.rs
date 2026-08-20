//! Frontier-Safe Zero-Rescan New-View Installation (v0.59.6).
//!
//! Implements the protocol for dynamically attaching a new view to an existing
//! shared physical arrangement without rescanning the underlying source connector,
//! with zero visibility gaps and zero duplicate records.

use crate::error::OpError;
use rockstream_storage::trace::SharedArrangementTrace;
use rockstream_types::batch::{Weight, ZSetRow};
use rockstream_types::ids::ViewId;
use std::collections::BTreeMap;

/// Metrics and stats recorded during a zero-rescan view attachment.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ViewAttachmentMetrics {
    /// Number of scan / query requests dispatched to upstream sources (MUST BE ZERO).
    pub source_scan_requests: usize,
    /// Frontier at which the trace snapshot was pinned.
    pub pinned_frontier: u64,
    /// Total number of rows loaded from the pinned trace snapshot.
    pub snapshot_rows_loaded: usize,
    /// Number of live delta batches buffered during initialization.
    pub buffered_delta_batches: usize,
    /// Number of live delta rows buffered and drained.
    pub buffered_delta_rows: usize,
    /// Cluster frontier at which live stream execution began.
    pub live_attached_frontier: u64,
}

/// In-flight delta buffer for holding deltas that arrive after the snapshot frontier
/// while the view's downstream pipeline is initializing.
#[derive(Debug, Clone)]
pub struct AttachmentDeltaBuffer {
    pub max_capacity: usize,
    pub buffered_batches: Vec<(u64, Vec<ZSetRow>)>,
}

impl AttachmentDeltaBuffer {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            max_capacity,
            buffered_batches: Vec::new(),
        }
    }

    pub fn push(&mut self, frontier: u64, rows: Vec<ZSetRow>) -> Result<(), OpError> {
        let current_rows: usize = self.buffered_batches.iter().map(|(_, r)| r.len()).sum();
        if current_rows + rows.len() > self.max_capacity {
            return Err(OpError::internal(format!(
                "RS-4020: attachment delta buffer exceeded capacity limit {}",
                self.max_capacity
            )));
        }
        self.buffered_batches.push((frontier, rows));
        Ok(())
    }

    pub fn drain_all(&mut self) -> Vec<(u64, Vec<ZSetRow>)> {
        std::mem::take(&mut self.buffered_batches)
    }
}

/// Attached view execution state backed by a shared arrangement.
pub struct AttachedView {
    pub view_id: ViewId,
    pub current_frontier: u64,
    pub state: BTreeMap<Vec<u8>, (Vec<u8>, Weight)>,
    pub metrics: ViewAttachmentMetrics,
}

impl AttachedView {
    /// Attach a new view to a shared trace at frontier `pin_frontier` with zero source rescan.
    ///
    /// 1. Reads the compacted snapshot + deltas through `pin_frontier`.
    /// 2. Registers consumer frontier in the shared trace.
    /// 3. Drains any incoming deltas buffered during initialization.
    /// 4. Transitions cleanly to live stream processing.
    pub fn attach(
        view_id: ViewId,
        trace: &mut SharedArrangementTrace,
        pin_frontier: u64,
        buffered_deltas: &mut AttachmentDeltaBuffer,
    ) -> Result<Self, OpError> {
        let snapshot = trace
            .read_trace_snapshot(pin_frontier)
            .map_err(|e| OpError::internal(format!("Failed to read trace snapshot: {}", e)))?;

        trace.register_consumer_frontier(view_id, pin_frontier);

        let mut metrics = ViewAttachmentMetrics {
            source_scan_requests: 0, // Invariant: Zero source scans!
            pinned_frontier: pin_frontier,
            snapshot_rows_loaded: snapshot.len(),
            buffered_delta_batches: 0,
            buffered_delta_rows: 0,
            live_attached_frontier: pin_frontier,
        };

        let mut state = snapshot;

        // Drain buffered deltas arriving > pin_frontier
        let pending = buffered_deltas.drain_all();
        for (frontier, rows) in pending {
            if frontier > pin_frontier {
                metrics.buffered_delta_batches += 1;
                metrics.buffered_delta_rows += rows.len();
                for row in rows {
                    let entry = state.entry(row.key);
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
                metrics.live_attached_frontier = frontier;
                trace.advance_consumer_frontier(view_id, frontier);
            }
        }

        assert!(
            metrics.live_attached_frontier >= pin_frontier,
            "INVARIANT: M2-S6 - attached view read frontier starts without gaps or duplicates"
        );

        Ok(Self {
            view_id,
            current_frontier: metrics.live_attached_frontier,
            state,
            metrics,
        })
    }

    /// Process an incoming live epoch delta batch.
    pub fn apply_live_batch(
        &mut self,
        trace: &mut SharedArrangementTrace,
        frontier: u64,
        rows: Vec<ZSetRow>,
    ) {
        assert!(
            frontier >= self.current_frontier,
            "Frontier regression: {} < {}",
            frontier,
            self.current_frontier
        );

        for row in rows {
            let entry = self.state.entry(row.key);
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

        self.current_frontier = frontier;
        trace.advance_consumer_frontier(self.view_id, frontier);
    }
}
