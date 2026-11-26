//! Resumable CPU/byte-budgeted chunked operator execution (Issue #402 R4).
//!
//! High-fanout stateful operators (such as equi-joins matching thousands of
//! rows per key) must execute in bounded, resumable chunks so a single
//! expensive delta cannot monopolize worker CPU or starve sibling
//! freshness-critical views.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::error::OpError;
use crate::zset::ArrowZSet;

/// Resource limits for a single resumable operator execution quantum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkBudget {
    /// Maximum output rows produced before yielding back to the scheduler.
    pub max_output_rows: usize,
    /// Maximum estimated output bytes produced before yielding.
    pub max_output_bytes: usize,
    /// Maximum arrangement probes performed before yielding.
    pub max_probes: usize,
    /// Maximum wall-clock CPU time spent in one chunk before yielding.
    pub max_cpu_time: Duration,
}

impl Default for ChunkBudget {
    fn default() -> Self {
        Self {
            max_output_rows: 4_096,
            max_output_bytes: 512 * 1024,
            max_probes: 8_192,
            max_cpu_time: Duration::from_millis(5),
        }
    }
}

impl ChunkBudget {
    /// Construct a custom chunk budget ensuring non-zero progress bounds.
    pub fn new(
        max_output_rows: usize,
        max_output_bytes: usize,
        max_probes: usize,
        max_cpu_time: Duration,
    ) -> Self {
        Self {
            max_output_rows: max_output_rows.max(1),
            max_output_bytes: max_output_bytes.max(64),
            max_probes: max_probes.max(1),
            max_cpu_time: if max_cpu_time.is_zero() {
                Duration::from_micros(100)
            } else {
                max_cpu_time
            },
        }
    }
}

/// Work counters observed during a single chunk execution step.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChunkStats {
    pub output_rows: usize,
    pub output_bytes: usize,
    pub probes: usize,
    pub cpu_time: Duration,
}

/// Outcome of advancing a resumable chunked operator by one quantum.
#[derive(Debug)]
pub enum ChunkStepResult<T = ArrowZSet> {
    /// Budget exhausted before completion; cursor is preserved for subsequent `step()` calls.
    Yielded {
        partial_output: T,
        stats: ChunkStats,
    },
    /// Epoch finished and state committed atomically.
    Completed { final_output: T, stats: ChunkStats },
    /// Cancellation token fired; uncommitted state was rolled back cleanly.
    Cancelled { stats: ChunkStats },
}

/// Trait implemented by resumable chunked operator executions scheduled by `FairChunkScheduler`.
pub trait ResumableChunkedOp: Send {
    /// Advance work by at most one `ChunkBudget`, checking `cancel` cooperatively.
    fn step_chunk(
        &mut self,
        budget: &ChunkBudget,
        cancel: &CancellationToken,
    ) -> Result<ChunkStepResult<ArrowZSet>, OpError>;
}
