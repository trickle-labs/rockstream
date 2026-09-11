//! Worker-side prospective quota enforcement and batch shedding (v0.51.10).

use rockstream_types::ids::WorkloadId;
use rockstream_types::state_budget::{
    DistributedQuotaLedger, MemoryCategory, MemoryPermit, QuotaGuard, StateBudgetError,
    WorkerBudgetLedger,
};
use rockstream_types::view_lifecycle::ViewState;
use std::sync::Arc;

/// Worker-side prospective quota manager.
/// Enforces workload memory limits and parallelism bounds BEFORE batch arrangement memory allocation.
#[derive(Debug, Clone)]
pub struct WorkerQuotaManager {
    ledger: Arc<DistributedQuotaLedger>,
    budget_ledger: Arc<WorkerBudgetLedger>,
}

impl Default for WorkerQuotaManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerQuotaManager {
    /// Create a new `WorkerQuotaManager`.
    pub fn new() -> Self {
        Self {
            ledger: Arc::new(DistributedQuotaLedger::new()),
            budget_ledger: Arc::new(WorkerBudgetLedger::new(2_147_483_648, 429_496_729)),
        }
    }

    /// Create with an existing `DistributedQuotaLedger`.
    pub fn with_ledger(ledger: Arc<DistributedQuotaLedger>) -> Self {
        Self {
            ledger,
            budget_ledger: Arc::new(WorkerBudgetLedger::new(2_147_483_648, 429_496_729)),
        }
    }

    /// Create with an existing `WorkerBudgetLedger`.
    pub fn with_budget_ledger(budget_ledger: Arc<WorkerBudgetLedger>) -> Self {
        Self {
            ledger: Arc::new(DistributedQuotaLedger::new()),
            budget_ledger,
        }
    }

    /// Access the underlying distributed quota ledger.
    pub fn ledger(&self) -> &Arc<DistributedQuotaLedger> {
        &self.ledger
    }

    /// Access the underlying worker budget ledger.
    pub fn budget_ledger(&self) -> &Arc<WorkerBudgetLedger> {
        &self.budget_ledger
    }

    /// Try acquire a byte permit for the given category before memory allocation.
    pub fn try_acquire_permit(
        &self,
        category: MemoryCategory,
        bytes: u64,
    ) -> Result<MemoryPermit, StateBudgetError> {
        self.budget_ledger.try_acquire(category, bytes, false)
    }

    /// Try acquire a byte permit specifying whether the allocation is for foreground work.
    pub fn try_acquire_permit_with_priority(
        &self,
        category: MemoryCategory,
        bytes: u64,
        is_foreground: bool,
    ) -> Result<MemoryPermit, StateBudgetError> {
        self.budget_ledger
            .try_acquire(category, bytes, is_foreground)
    }

    /// Try acquire a byte permit for foreground work (reads, epoch commits, metadata flushes),
    /// which can draw from the reserved foreground memory capacity.
    pub fn try_acquire_foreground_permit(
        &self,
        category: MemoryCategory,
        bytes: u64,
    ) -> Result<MemoryPermit, StateBudgetError> {
        self.budget_ledger.try_acquire(category, bytes, true)
    }

    /// Try acquire a byte permit for background work (compaction, backfill, migration),
    /// which can only allocate from the unreserved worker budget.
    pub fn try_acquire_background_permit(
        &self,
        category: MemoryCategory,
        bytes: u64,
    ) -> Result<MemoryPermit, StateBudgetError> {
        self.budget_ledger.try_acquire(category, bytes, false)
    }

    /// Acquire a byte permit, waiting up to `timeout` if memory is currently exhausted.
    /// Rejects with `RS-9001` if waiter limit is exceeded, or `RS-5003` if timeout expires.
    pub async fn acquire_permit_with_timeout(
        &self,
        category: MemoryCategory,
        bytes: u64,
        timeout: std::time::Duration,
    ) -> Result<MemoryPermit, StateBudgetError> {
        // Fast path: try acquire immediately
        if let Ok(permit) = self.try_acquire_permit(category, bytes) {
            return Ok(permit);
        }

        // Enforce waiter queue bound
        self.budget_ledger.increment_waiters()?;

        let deadline = tokio::time::Instant::now() + timeout;
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(5));

        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    self.budget_ledger.decrement_waiters();
                    return Err(StateBudgetError {
                        operator_name: format!("waiter-timeout-{}", category.name()),
                        max_bytes: self.budget_ledger.total_budget_bytes(),
                        current_bytes: self.budget_ledger.total_allocated_bytes(),
                        requested_bytes: bytes,
                    });
                }
                _ = interval.tick() => {
                    if let Ok(permit) = self.try_acquire_permit(category, bytes) {
                        self.budget_ledger.decrement_waiters();
                        return Ok(permit);
                    }
                }
            }
        }
    }

    /// Prospectively consult quota ledger and acquire capacity for a batch arrangement allocation.
    /// Returns `Ok(QuotaGuard)` if approved, or `Err(StateBudgetError)` if rejected.
    pub fn try_allocate_batch(
        &self,
        workload_id: WorkloadId,
        requested_bytes: u64,
        parallelism: u32,
    ) -> Result<QuotaGuard, StateBudgetError> {
        let reservation = self
            .ledger
            .try_acquire_batch(workload_id, requested_bytes, parallelism);
        let within_bounds = self.ledger.get_entry(workload_id).is_none_or(|entry| {
            let memory_limit = entry
                .memory_limit_bytes
                .load(std::sync::atomic::Ordering::Relaxed);
            let parallelism_limit = entry
                .max_parallelism
                .load(std::sync::atomic::Ordering::Relaxed);
            let memory_ok = memory_limit == 0
                || entry
                    .current_memory_bytes
                    .load(std::sync::atomic::Ordering::Relaxed)
                    <= memory_limit;
            let parallelism_ok = parallelism_limit == 0
                || entry
                    .current_parallelism
                    .load(std::sync::atomic::Ordering::Relaxed)
                    <= parallelism_limit;
            memory_ok && parallelism_ok
        });
        assert!(
            reservation.is_err() || within_bounds,
            "EDGE-QUOTA: accepted reservation must remain within memory and parallelism bounds"
        );
        reservation
    }

    /// Handle prospective batch rejection: returns `ViewState::OverBudgetRejected`
    /// and logs audit event for quota limit breach.
    pub fn handle_prospective_rejection(
        &self,
        workload_id: WorkloadId,
        err: &StateBudgetError,
    ) -> ViewState {
        tracing::warn!(
            "RS-5003 / RS-9001: Prospective worker quota rejection for workload-{}: current={}B, requested={}B, limit={}B",
            workload_id.0,
            err.current_bytes,
            err.requested_bytes,
            err.max_bytes
        );
        ViewState::OverBudgetRejected
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prospective_batch_allocation_and_rejection() {
        let mgr = WorkerQuotaManager::new();
        mgr.ledger()
            .register_workload(WorkloadId(1), 1024, 4)
            .unwrap();

        // 512 bytes succeeds
        let guard = mgr.try_allocate_batch(WorkloadId(1), 512, 1);
        assert!(guard.is_ok());

        // 600 bytes fails prospectively
        let err = mgr.try_allocate_batch(WorkloadId(1), 600, 1).unwrap_err();
        let state = mgr.handle_prospective_rejection(WorkloadId(1), &err);
        assert_eq!(state, ViewState::OverBudgetRejected);
    }
}
