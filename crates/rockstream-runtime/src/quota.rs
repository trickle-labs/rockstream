//! Worker-side prospective quota enforcement and batch shedding (v0.51.10).

use std::sync::Arc;
use rockstream_types::ids::WorkloadId;
use rockstream_types::state_budget::{DistributedQuotaLedger, QuotaGuard, StateBudgetError};
use rockstream_types::view_lifecycle::ViewState;

/// Worker-side prospective quota manager.
/// Enforces workload memory limits and parallelism bounds BEFORE batch arrangement memory allocation.
#[derive(Debug, Clone)]
pub struct WorkerQuotaManager {
    ledger: Arc<DistributedQuotaLedger>,
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
        }
    }

    /// Create with an existing `DistributedQuotaLedger`.
    pub fn with_ledger(ledger: Arc<DistributedQuotaLedger>) -> Self {
        Self { ledger }
    }

    /// Access the underlying distributed quota ledger.
    pub fn ledger(&self) -> &Arc<DistributedQuotaLedger> {
        &self.ledger
    }

    /// Prospectively consult quota ledger and acquire capacity for a batch arrangement allocation.
    /// Returns `Ok(QuotaGuard)` if approved, or `Err(StateBudgetError)` if rejected.
    pub fn try_allocate_batch(
        &self,
        workload_id: WorkloadId,
        requested_bytes: u64,
        parallelism: u32,
    ) -> Result<QuotaGuard, StateBudgetError> {
        self.ledger.try_acquire_batch(workload_id, requested_bytes, parallelism)
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
        mgr.ledger().register_workload(WorkloadId(1), 1024, 4).unwrap();

        // 512 bytes succeeds
        let guard = mgr.try_allocate_batch(WorkloadId(1), 512, 1);
        assert!(guard.is_ok());

        // 600 bytes fails prospectively
        let err = mgr.try_allocate_batch(WorkloadId(1), 600, 1).unwrap_err();
        let state = mgr.handle_prospective_rejection(WorkloadId(1), &err);
        assert_eq!(state, ViewState::OverBudgetRejected);
    }
}
