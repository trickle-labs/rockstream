//! Concurrency Governors for Background Compaction, Backfill, and Migration (v0.62.1 Slice 5).

use rockstream_types::config::WorkerSection;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Error)]
pub enum ConcurrencyLimitError {
    #[error("RS-9001: background compaction concurrency limit reached (max {max})")]
    CompactionLimitReached { max: usize },
    #[error("RS-9001: background backfill concurrency limit reached (max {max})")]
    BackfillLimitReached { max: usize },
    #[error("RS-9001: background migration concurrency limit reached (max {max})")]
    MigrationLimitReached { max: usize },
}

/// Governor that bounds background worker concurrency independently across
/// compaction, backfill, and migration pipelines.
#[derive(Debug, Clone)]
pub struct ConcurrencyGovernor {
    compaction_semaphore: Arc<Semaphore>,
    backfill_semaphore: Arc<Semaphore>,
    migration_semaphore: Arc<Semaphore>,
    max_compaction: usize,
    max_backfill: usize,
    max_migration: usize,
}

impl ConcurrencyGovernor {
    /// Create a concurrency governor with explicit maximums.
    pub fn new(max_compaction: usize, max_backfill: usize, max_migration: usize) -> Self {
        Self {
            compaction_semaphore: Arc::new(Semaphore::new(max_compaction.max(1))),
            backfill_semaphore: Arc::new(Semaphore::new(max_backfill.max(1))),
            migration_semaphore: Arc::new(Semaphore::new(max_migration.max(1))),
            max_compaction: max_compaction.max(1),
            max_backfill: max_backfill.max(1),
            max_migration: max_migration.max(1),
        }
    }

    /// Construct a concurrency governor directly from `WorkerSection` configuration.
    pub fn from_worker_config(cfg: &WorkerSection) -> Self {
        Self::new(
            cfg.max_compaction_concurrency,
            cfg.max_backfill_concurrency,
            cfg.max_migration_concurrency,
        )
    }

    pub fn max_compaction(&self) -> usize {
        self.max_compaction
    }

    pub fn max_backfill(&self) -> usize {
        self.max_backfill
    }

    pub fn max_migration(&self) -> usize {
        self.max_migration
    }

    pub fn available_compaction(&self) -> usize {
        self.compaction_semaphore.available_permits()
    }

    pub fn available_backfill(&self) -> usize {
        self.backfill_semaphore.available_permits()
    }

    pub fn available_migration(&self) -> usize {
        self.migration_semaphore.available_permits()
    }

    /// Acquire a background compaction execution permit.
    pub fn try_acquire_compaction(&self) -> Result<OwnedSemaphorePermit, ConcurrencyLimitError> {
        self.compaction_semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| ConcurrencyLimitError::CompactionLimitReached {
                max: self.max_compaction,
            })
    }

    /// Acquire a background backfill execution permit.
    pub fn try_acquire_backfill(&self) -> Result<OwnedSemaphorePermit, ConcurrencyLimitError> {
        self.backfill_semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| ConcurrencyLimitError::BackfillLimitReached {
                max: self.max_backfill,
            })
    }

    /// Acquire a background migration execution permit.
    pub fn try_acquire_migration(&self) -> Result<OwnedSemaphorePermit, ConcurrencyLimitError> {
        self.migration_semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| ConcurrencyLimitError::MigrationLimitReached {
                max: self.max_migration,
            })
    }
}
