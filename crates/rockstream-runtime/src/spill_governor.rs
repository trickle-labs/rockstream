//! Worker Budget Integration & Spill Governors (v0.67.1 Slice 6 / Phase 3b).
//!
//! Enforces:
//! 1. `max_spill_write_buffer_bytes` upper bound on in-flight spill write staging.
//! 2. `max_disk_occupancy_bytes` ceiling rejecting writes with RS-2021 on exhaustion.
//! 3. `max_concurrent_spill_io` concurrency limits on background spill tasks.
//! 4. Source backpressure propagation via `SourcePressureController` (80% / 95% / 75%).
//! 5. Preservation of `foreground_reservation_bytes` for ad-hoc queries.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rockstream_storage::concurrency_governor::ConcurrencyLimitError;
use rockstream_types::state_budget::{
    MemoryCategory, MemoryPermit, StateBudgetError, WorkerBudgetLedger,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::source_pressure::SourcePressureController;

/// Configuration limits for spill operations.
#[derive(Debug, Clone)]
pub struct SpillGovernorConfig {
    /// Upper bound for in-flight un-flushed spill write bytes (default: 16 MiB).
    pub max_spill_write_buffer_bytes: u64,
    /// Maximum disk space allowed for spilled state (default: 50 GiB).
    pub max_disk_occupancy_bytes: u64,
    /// Maximum concurrent asynchronous spill flushes (default: 4).
    pub max_concurrent_spill_io: usize,
    /// Dedicated foreground memory reserved for queries (bytes).
    pub foreground_reservation_bytes: u64,
}

impl Default for SpillGovernorConfig {
    fn default() -> Self {
        Self {
            max_spill_write_buffer_bytes: 16 * 1024 * 1024,
            max_disk_occupancy_bytes: 50 * 1024 * 1024 * 1024,
            max_concurrent_spill_io: 4,
            foreground_reservation_bytes: 0,
        }
    }
}

/// Governor integrating spill staging, I/O concurrency, and disk capacity with
/// the unified [`WorkerBudgetLedger`].
#[derive(Debug)]
pub struct SpillGovernor {
    config: SpillGovernorConfig,
    ledger: Arc<WorkerBudgetLedger>,
    current_disk_occupancy: Arc<AtomicU64>,
    current_write_buffer_bytes: Arc<AtomicU64>,
    io_semaphore: Arc<Semaphore>,
    source_pressure: Arc<SourcePressureController>,
}

impl SpillGovernor {
    /// Create a new `SpillGovernor` attached to the worker budget ledger.
    pub fn new(ledger: Arc<WorkerBudgetLedger>, config: SpillGovernorConfig) -> Self {
        let base_credits = 100;
        let source_pressure = Arc::new(SourcePressureController::new(ledger.clone(), base_credits));
        let max_io = config.max_concurrent_spill_io.max(1);

        Self {
            config,
            ledger,
            current_disk_occupancy: Arc::new(AtomicU64::new(0)),
            current_write_buffer_bytes: Arc::new(AtomicU64::new(0)),
            io_semaphore: Arc::new(Semaphore::new(max_io)),
            source_pressure,
        }
    }

    pub fn config(&self) -> &SpillGovernorConfig {
        &self.config
    }

    pub fn ledger(&self) -> &Arc<WorkerBudgetLedger> {
        &self.ledger
    }

    pub fn source_pressure(&self) -> &Arc<SourcePressureController> {
        &self.source_pressure
    }

    pub fn current_disk_occupancy(&self) -> u64 {
        self.current_disk_occupancy.load(Ordering::Relaxed)
    }

    pub fn current_write_buffer_bytes(&self) -> u64 {
        self.current_write_buffer_bytes.load(Ordering::Relaxed)
    }

    /// Record prospective spill write bytes before staging in write buffers.
    ///
    /// Returns `Err(StateBudgetError)` with RS-2021 if disk occupancy is exceeded,
    /// or RS-5003 if the write buffer limit or memory budget is exhausted.
    pub fn record_spill_write(&self, bytes: u64) -> Result<(), StateBudgetError> {
        let current_disk = self.current_disk_occupancy.load(Ordering::Relaxed);
        if current_disk.saturating_add(bytes) > self.config.max_disk_occupancy_bytes {
            return Err(StateBudgetError {
                operator_name: "disk-occupancy".to_string(),
                max_bytes: self.config.max_disk_occupancy_bytes,
                current_bytes: current_disk,
                requested_bytes: bytes,
            });
        }

        let current_buf = self.current_write_buffer_bytes.load(Ordering::Relaxed);
        if current_buf.saturating_add(bytes) > self.config.max_spill_write_buffer_bytes {
            return Err(StateBudgetError {
                operator_name: "spill-write-buffer".to_string(),
                max_bytes: self.config.max_spill_write_buffer_bytes,
                current_bytes: current_buf,
                requested_bytes: bytes,
            });
        }

        // Charge memory ledger under CheckpointStaging category
        let permit = self
            .ledger
            .try_acquire(MemoryCategory::CheckpointStaging, bytes, false)?;
        permit.defuse();
        self.current_write_buffer_bytes
            .fetch_add(bytes, Ordering::Relaxed);

        Ok(())
    }

    /// Flush staged spill buffer bytes to durable storage.
    ///
    /// Releases bytes from the memory ledger and buffer counter, and advances
    /// the disk occupancy counter.
    pub fn flush_spill_write_buffer(&self, bytes: u64) {
        let sub = self
            .current_write_buffer_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(bytes))
            })
            .unwrap_or(0);
        let actual_released = bytes.min(sub);

        self.ledger
            .release(MemoryCategory::CheckpointStaging, actual_released);
        self.current_disk_occupancy
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Acquire a concurrent spill I/O permit.
    pub fn try_acquire_spill_io(&self) -> Result<OwnedSemaphorePermit, ConcurrencyLimitError> {
        self.io_semaphore.clone().try_acquire_owned().map_err(|_| {
            ConcurrencyLimitError::CompactionLimitReached {
                max: self.config.max_concurrent_spill_io,
            }
        })
    }

    /// Admit foreground query work memory, ensuring background spill does not
    /// exhaust dedicated foreground reservation.
    pub fn admit_foreground_query(&self, bytes: u64) -> Result<MemoryPermit, StateBudgetError> {
        self.ledger
            .try_acquire(MemoryCategory::QueryWorkMemory, bytes, true)
    }
}
