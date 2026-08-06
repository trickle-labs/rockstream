//! State-budget enforcement for IVM operators (v0.27, DESIGN.md §5.4).
//!
//! Every operator that accumulates per-key arrangement state must declare a
//! named upper bound, expose a metric for current fill level, and either apply
//! back-pressure or return an error when the bound is reached.
//!
//! # Usage
//!
//! ```rust
//! use rockstream_types::state_budget::{StateBudget, StateBudgetError};
//!
//! let budget = StateBudget::new("my_op", 64 * 1024 * 1024); // 64 MiB cap
//!
//! // Before inserting state:
//! budget.try_acquire(512)?;  // returns Err if over budget
//! // … insert 512 bytes into arrangement …
//!
//! // When state is freed:
//! budget.release(512);
//! # Ok::<(), StateBudgetError>(())
//! ```

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ─── Error type ──────────────────────────────────────────────────────────────

/// Error returned when a state-budget acquisition would exceed the limit.
///
/// Corresponds to error code RS-5003 in `rockstream-types::error_code`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateBudgetError {
    /// Operator name that owns this budget.
    pub operator_name: String,
    /// The limit (bytes).
    pub max_bytes: u64,
    /// Current usage (bytes) before this attempted acquisition.
    pub current_bytes: u64,
    /// The number of bytes requested.
    pub requested_bytes: u64,
}

impl fmt::Display for StateBudgetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.operator_name.starts_with("quota-overflow-") {
            return write!(
                f,
                "RS-5004: quota counter overflow for '{}': current={}, requested={}; next_steps: release quota or create a new workload before retrying",
                self.operator_name, self.current_bytes, self.requested_bytes
            );
        }
        write!(
            f,
            "RS-5003: state budget exceeded for '{}': current={} bytes, \
             requested={} bytes, limit={} bytes",
            self.operator_name, self.current_bytes, self.requested_bytes, self.max_bytes
        )
    }
}

fn quota_overflow_error(name: &str, current: u64, requested: u64) -> StateBudgetError {
    StateBudgetError {
        operator_name: format!("quota-overflow-{name}"),
        max_bytes: u64::MAX,
        current_bytes: current,
        requested_bytes: requested,
    }
}

impl std::error::Error for StateBudgetError {}

use crate::ids::WorkloadId;
use std::sync::atomic::AtomicBool;

// ─── WorkloadBudget ──────────────────────────────────────────────────────────

/// Memory budget for a workload (shared across multiple operators/views).
#[derive(Debug)]
pub struct WorkloadBudget {
    workload_id: WorkloadId,
    max_bytes: u64,
    current_bytes: AtomicU64,
    notice_emitted_80: AtomicBool,
    warning_emitted_95: AtomicBool,
}

impl WorkloadBudget {
    /// Create a new workload-level budget.
    pub fn new(workload_id: WorkloadId, max_bytes: u64) -> Self {
        Self {
            workload_id,
            max_bytes,
            current_bytes: AtomicU64::new(0),
            notice_emitted_80: AtomicBool::new(false),
            warning_emitted_95: AtomicBool::new(false),
        }
    }

    /// Retrieve the workload ID.
    pub fn workload_id(&self) -> WorkloadId {
        self.workload_id
    }

    /// Retrieve the maximum bytes.
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Retrieve current bytes.
    pub fn current_bytes(&self) -> u64 {
        self.current_bytes.load(Ordering::Relaxed)
    }

    /// Charge/acquire bytes against the workload budget.
    pub fn try_acquire(&self, bytes: u64) -> Result<(), StateBudgetError> {
        loop {
            let current = self.current_bytes.load(Ordering::Relaxed);
            let proposed = current.saturating_add(bytes);
            if self.max_bytes > 0 && proposed > self.max_bytes {
                return Err(StateBudgetError {
                    operator_name: format!("workload-{}", self.workload_id.0),
                    max_bytes: self.max_bytes,
                    current_bytes: current,
                    requested_bytes: bytes,
                });
            }
            match self.current_bytes.compare_exchange_weak(
                current,
                proposed,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if self.max_bytes > 0 {
                        let pct = (proposed as f64 / self.max_bytes as f64) * 100.0;
                        if pct >= 95.0 && !self.warning_emitted_95.swap(true, Ordering::Relaxed) {
                            tracing::warn!(
                                "RS-5019: Workload {} is at {:.1}% memory utilization (current={}, limit={})",
                                self.workload_id, pct, proposed, self.max_bytes
                            );
                        } else if pct >= 80.0
                            && !self.notice_emitted_80.swap(true, Ordering::Relaxed)
                        {
                            tracing::info!(
                                "RS-5018: Workload {} is at {:.1}% memory utilization (current={}, limit={})",
                                self.workload_id, pct, proposed, self.max_bytes
                            );
                        }
                    }
                    return Ok(());
                }
                Err(_) => continue,
            }
        }
    }

    /// Release bytes from the workload budget.
    pub fn release(&self, bytes: u64) {
        loop {
            let current = self.current_bytes.load(Ordering::Relaxed);
            let proposed = current.saturating_sub(bytes);
            match self.current_bytes.compare_exchange_weak(
                current,
                proposed,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if self.max_bytes > 0 {
                        let pct = (proposed as f64 / self.max_bytes as f64) * 100.0;
                        if pct < 95.0 {
                            self.warning_emitted_95.store(false, Ordering::Relaxed);
                        }
                        if pct < 80.0 {
                            self.notice_emitted_80.store(false, Ordering::Relaxed);
                        }
                    }
                    return;
                }
                Err(_) => continue,
            }
        }
    }
}

// ─── StateBudget ─────────────────────────────────────────────────────────────

/// An operator-scoped state budget: tracks how many bytes of arrangement state
/// are in use and rejects acquisitions that would exceed the declared limit.
///
/// The budget is thread-safe and can be shared via `Arc<StateBudget>`.
#[derive(Debug)]
pub struct StateBudget {
    operator_name: String,
    max_bytes: u64,
    current_bytes: AtomicU64,
    workload_budget: Option<Arc<WorkloadBudget>>,
    notice_emitted_80: AtomicBool,
    warning_emitted_95: AtomicBool,
}

impl StateBudget {
    /// Create a new budget with the given maximum.
    ///
    /// - `operator_name`: human-readable label for error messages / metrics.
    /// - `max_bytes`: hard upper bound in bytes. Set to `u64::MAX` for
    ///   "effectively unbounded" (useful during development before a real
    ///   budget is determined).
    pub fn new(operator_name: impl Into<String>, max_bytes: u64) -> Self {
        Self {
            operator_name: operator_name.into(),
            max_bytes,
            current_bytes: AtomicU64::new(0),
            workload_budget: None,
            notice_emitted_80: AtomicBool::new(false),
            warning_emitted_95: AtomicBool::new(false),
        }
    }

    /// Create a new budget associated with a workload budget.
    pub fn new_with_workload(
        operator_name: impl Into<String>,
        max_bytes: u64,
        workload_budget: Option<Arc<WorkloadBudget>>,
    ) -> Self {
        Self {
            operator_name: operator_name.into(),
            max_bytes,
            current_bytes: AtomicU64::new(0),
            workload_budget,
            notice_emitted_80: AtomicBool::new(false),
            warning_emitted_95: AtomicBool::new(false),
        }
    }

    /// Wrap in an `Arc` for shared ownership.
    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }

    /// The name of the operator that owns this budget.
    pub fn operator_name(&self) -> &str {
        &self.operator_name
    }

    /// The maximum allowed bytes.
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Current bytes in use (approximate under concurrency).
    pub fn current_bytes(&self) -> u64 {
        self.current_bytes.load(Ordering::Relaxed)
    }

    /// Retrieve the workload budget.
    pub fn workload_budget(&self) -> Option<&Arc<WorkloadBudget>> {
        self.workload_budget.as_ref()
    }

    /// Fill fraction in the range `[0.0, ∞)`.
    ///
    /// Values > 1.0 indicate the budget has been exceeded (possible if the
    /// budget was `u64::MAX` and usage is released with no prior check, or
    /// if code paths bypass `try_acquire`).
    pub fn utilization(&self) -> f64 {
        if self.max_bytes == 0 {
            return f64::INFINITY;
        }
        self.current_bytes.load(Ordering::Relaxed) as f64 / self.max_bytes as f64
    }

    /// Attempt to acquire `bytes` of state space.
    ///
    /// Returns `Ok(())` if the acquisition keeps usage within `max_bytes`.
    /// Returns `Err(StateBudgetError)` if it would exceed the limit — in
    /// which case the usage counter is **not** incremented.
    pub fn try_acquire(&self, bytes: u64) -> Result<(), StateBudgetError> {
        if let Some(ref wl) = self.workload_budget {
            wl.try_acquire(bytes)?;
        }

        // Use a compare-exchange loop so that two concurrent acquisitions do
        // not both "succeed" and overshoot the budget together.
        loop {
            let current = self.current_bytes.load(Ordering::Relaxed);
            let proposed = current.saturating_add(bytes);
            if proposed > self.max_bytes {
                if let Some(ref wl) = self.workload_budget {
                    wl.release(bytes);
                }
                return Err(StateBudgetError {
                    operator_name: self.operator_name.clone(),
                    max_bytes: self.max_bytes,
                    current_bytes: current,
                    requested_bytes: bytes,
                });
            }
            // Try to commit the new value.
            match self.current_bytes.compare_exchange_weak(
                current,
                proposed,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if self.max_bytes > 0 {
                        let pct = (proposed as f64 / self.max_bytes as f64) * 100.0;
                        if pct >= 95.0 && !self.warning_emitted_95.swap(true, Ordering::Relaxed) {
                            tracing::warn!(
                                "RS-5019: State budget for '{}' is at {:.1}% utilization (current={}, limit={})",
                                self.operator_name, pct, proposed, self.max_bytes
                            );
                        } else if pct >= 80.0
                            && !self.notice_emitted_80.swap(true, Ordering::Relaxed)
                        {
                            tracing::info!(
                                "RS-5018: State budget for '{}' is at {:.1}% utilization (current={}, limit={})",
                                self.operator_name, pct, proposed, self.max_bytes
                            );
                        }
                    }
                    return Ok(());
                }
                Err(_) => continue, // lost race, retry
            }
        }
    }

    /// Unconditionally acquire `bytes` without enforcing the budget limit.
    ///
    /// Use this for pre-existing state loaded on restart (where enforcement
    /// has already been applied previously). Also used in tests to set up
    /// a pre-filled budget.
    pub fn force_acquire(&self, bytes: u64) {
        if let Some(ref wl) = self.workload_budget {
            wl.current_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        self.current_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Release `bytes` of state (e.g., on tombstone GC or operator shutdown).
    ///
    /// Saturates at zero rather than wrapping.
    pub fn release(&self, bytes: u64) {
        if let Some(ref wl) = self.workload_budget {
            wl.release(bytes);
        }
        // Saturating subtract via compare-exchange loop.
        loop {
            let current = self.current_bytes.load(Ordering::Relaxed);
            let proposed = current.saturating_sub(bytes);
            match self.current_bytes.compare_exchange_weak(
                current,
                proposed,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if self.max_bytes > 0 {
                        let pct = (proposed as f64 / self.max_bytes as f64) * 100.0;
                        if pct < 95.0 {
                            self.warning_emitted_95.store(false, Ordering::Relaxed);
                        }
                        if pct < 80.0 {
                            self.notice_emitted_80.store(false, Ordering::Relaxed);
                        }
                    }
                    return;
                }
                Err(_) => continue,
            }
        }
    }

    /// Reset the usage counter to zero. For use in tests only.
    #[doc(hidden)]
    pub fn reset(&self) {
        if let Some(ref wl) = self.workload_budget {
            wl.current_bytes.store(0, Ordering::Relaxed);
            wl.notice_emitted_80.store(false, Ordering::Relaxed);
            wl.warning_emitted_95.store(false, Ordering::Relaxed);
        }
        self.current_bytes.store(0, Ordering::Relaxed);
        self.notice_emitted_80.store(false, Ordering::Relaxed);
        self.warning_emitted_95.store(false, Ordering::Relaxed);
    }
}

/// Alias for `StateBudget` as used by operators.
pub type StateBudgetMeter = StateBudget;

// ─── DistributedQuotaLedger ───────────────────────────────────────────────

/// Upper bound on the number of tracked workloads in the distributed quota ledger.
pub const DEFAULT_MAX_WORKLOADS: usize = 10_000;

/// Entry in the distributed quota ledger for a single workload.
#[derive(Debug)]
pub struct WorkloadQuotaLedgerEntry {
    pub workload_id: WorkloadId,
    pub memory_limit_bytes: AtomicU64,
    pub max_parallelism: AtomicU64,
    pub current_memory_bytes: AtomicU64,
    pub current_parallelism: AtomicU64,
}

impl WorkloadQuotaLedgerEntry {
    pub fn new(workload_id: WorkloadId, memory_limit_bytes: u64, max_parallelism: u32) -> Self {
        Self {
            workload_id,
            memory_limit_bytes: AtomicU64::new(memory_limit_bytes),
            max_parallelism: AtomicU64::new(max_parallelism as u64),
            current_memory_bytes: AtomicU64::new(0),
            current_parallelism: AtomicU64::new(0),
        }
    }
}

/// Thread-safe distributed quota ledger for prospective worker-side quota consultation
/// before arrangement state allocation and batch processing.
#[derive(Debug)]
pub struct DistributedQuotaLedger {
    entries: dashmap::DashMap<WorkloadId, Arc<WorkloadQuotaLedgerEntry>>,
    max_workloads: usize,
    total_reservations: AtomicU64,
    total_rejections: AtomicU64,
}

impl Default for DistributedQuotaLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl DistributedQuotaLedger {
    /// Create a new `DistributedQuotaLedger` with default workload bound (10,000).
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_MAX_WORKLOADS)
    }

    /// Create a `DistributedQuotaLedger` with a custom maximum workloads capacity bound.
    pub fn with_capacity(max_workloads: usize) -> Self {
        Self {
            entries: dashmap::DashMap::new(),
            max_workloads,
            total_reservations: AtomicU64::new(0),
            total_rejections: AtomicU64::new(0),
        }
    }

    /// Register or update a workload's quota parameters.
    pub fn register_workload(
        &self,
        workload_id: WorkloadId,
        memory_limit_bytes: u64,
        max_parallelism: u32,
    ) -> Result<(), StateBudgetError> {
        if !self.entries.contains_key(&workload_id) && self.entries.len() >= self.max_workloads {
            return Err(StateBudgetError {
                operator_name: format!("ledger-capacity-{}", workload_id.0),
                max_bytes: self.max_workloads as u64,
                current_bytes: self.entries.len() as u64,
                requested_bytes: 1,
            });
        }
        self.entries
            .entry(workload_id)
            .and_modify(|entry| {
                entry
                    .memory_limit_bytes
                    .store(memory_limit_bytes, Ordering::Relaxed);
                entry
                    .max_parallelism
                    .store(max_parallelism as u64, Ordering::Relaxed);
            })
            .or_insert_with(|| {
                Arc::new(WorkloadQuotaLedgerEntry::new(
                    workload_id,
                    memory_limit_bytes,
                    max_parallelism,
                ))
            });
        Ok(())
    }

    /// Unregister a workload from the ledger.
    pub fn unregister_workload(&self, workload_id: WorkloadId) {
        self.entries.remove(&workload_id);
    }

    /// Retrieve entry for a workload if registered.
    pub fn get_entry(&self, workload_id: WorkloadId) -> Option<Arc<WorkloadQuotaLedgerEntry>> {
        self.entries.get(&workload_id).map(|r| r.value().clone())
    }

    /// Prospective check and acquisition for a batch arrangement allocation.
    /// Checks workload `memory_limit` and `max_parallelism` BEFORE batch allocation.
    pub fn try_acquire_batch(
        self: &Arc<Self>,
        workload_id: WorkloadId,
        requested_bytes: u64,
        parallelism: u32,
    ) -> Result<QuotaGuard, StateBudgetError> {
        let entry = self.entries.get(&workload_id).map(|r| r.value().clone());
        if let Some(entry) = entry {
            let memory_limit = entry.memory_limit_bytes.load(Ordering::Relaxed);
            if memory_limit > 0 {
                loop {
                    let current = entry.current_memory_bytes.load(Ordering::Relaxed);
                    let Some(proposed) = current.checked_add(requested_bytes) else {
                        self.total_rejections.fetch_add(1, Ordering::Relaxed);
                        return Err(quota_overflow_error("memory", current, requested_bytes));
                    };
                    if proposed > memory_limit {
                        self.total_rejections.fetch_add(1, Ordering::Relaxed);
                        return Err(StateBudgetError {
                            operator_name: format!("workload-{}", workload_id.0),
                            max_bytes: memory_limit,
                            current_bytes: current,
                            requested_bytes,
                        });
                    }
                    if entry
                        .current_memory_bytes
                        .compare_exchange_weak(
                            current,
                            proposed,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
            } else {
                loop {
                    let current = entry.current_memory_bytes.load(Ordering::Relaxed);
                    let Some(proposed) = current.checked_add(requested_bytes) else {
                        self.total_rejections.fetch_add(1, Ordering::Relaxed);
                        return Err(quota_overflow_error("memory", current, requested_bytes));
                    };
                    if entry
                        .current_memory_bytes
                        .compare_exchange_weak(
                            current,
                            proposed,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
            }

            let max_p = entry.max_parallelism.load(Ordering::Relaxed);
            if max_p > 0 {
                loop {
                    let current_p = entry.current_parallelism.load(Ordering::Relaxed);
                    let Some(proposed_p) = current_p.checked_add(parallelism as u64) else {
                        entry
                            .current_memory_bytes
                            .fetch_sub(requested_bytes, Ordering::Relaxed);
                        self.total_rejections.fetch_add(1, Ordering::Relaxed);
                        return Err(quota_overflow_error(
                            "parallelism",
                            current_p,
                            parallelism as u64,
                        ));
                    };
                    if proposed_p > u64::from(u32::MAX) {
                        entry
                            .current_memory_bytes
                            .fetch_sub(requested_bytes, Ordering::Relaxed);
                        self.total_rejections.fetch_add(1, Ordering::Relaxed);
                        return Err(quota_overflow_error(
                            "parallelism",
                            current_p,
                            parallelism as u64,
                        ));
                    }
                    if proposed_p > max_p {
                        entry
                            .current_memory_bytes
                            .fetch_sub(requested_bytes, Ordering::Relaxed);
                        self.total_rejections.fetch_add(1, Ordering::Relaxed);
                        return Err(StateBudgetError {
                            operator_name: format!("workload-parallelism-{}", workload_id.0),
                            max_bytes: max_p,
                            current_bytes: current_p,
                            requested_bytes: parallelism as u64,
                        });
                    }
                    if entry
                        .current_parallelism
                        .compare_exchange_weak(
                            current_p,
                            proposed_p,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
            } else {
                loop {
                    let current = entry.current_parallelism.load(Ordering::Relaxed);
                    let Some(proposed) = current.checked_add(parallelism as u64) else {
                        entry
                            .current_memory_bytes
                            .fetch_sub(requested_bytes, Ordering::Relaxed);
                        self.total_rejections.fetch_add(1, Ordering::Relaxed);
                        return Err(quota_overflow_error(
                            "parallelism",
                            current,
                            parallelism as u64,
                        ));
                    };
                    if entry
                        .current_parallelism
                        .compare_exchange_weak(
                            current,
                            proposed,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
            }
        }
        self.total_reservations.fetch_add(1, Ordering::Relaxed);
        Ok(QuotaGuard {
            ledger: Some(self.clone()),
            workload_id,
            bytes: requested_bytes,
            parallelism,
            released: false,
        })
    }

    /// Release batch resources from the ledger.
    pub fn release_batch(&self, workload_id: WorkloadId, bytes: u64, parallelism: u32) {
        if let Some(entry) = self.entries.get(&workload_id) {
            loop {
                let current = entry.current_memory_bytes.load(Ordering::Relaxed);
                let proposed = current.saturating_sub(bytes);
                if entry
                    .current_memory_bytes
                    .compare_exchange_weak(current, proposed, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
            loop {
                let current_p = entry.current_parallelism.load(Ordering::Relaxed);
                let proposed_p = current_p.saturating_sub(parallelism as u64);
                if entry
                    .current_parallelism
                    .compare_exchange_weak(
                        current_p,
                        proposed_p,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    break;
                }
            }
        }
    }

    /// Get current fill level / utilization fraction for a workload.
    pub fn utilization(&self, workload_id: WorkloadId) -> Option<f64> {
        let entry = self.entries.get(&workload_id)?;
        let limit = entry.memory_limit_bytes.load(Ordering::Relaxed);
        if limit == 0 {
            return Some(0.0);
        }
        let current = entry.current_memory_bytes.load(Ordering::Relaxed);
        Some(current as f64 / limit as f64)
    }

    /// Total successful prospective reservations.
    pub fn total_reservations(&self) -> u64 {
        self.total_reservations.load(Ordering::Relaxed)
    }

    /// Total prospective rejections.
    pub fn total_rejections(&self) -> u64 {
        self.total_rejections.load(Ordering::Relaxed)
    }
}

/// RAII Guard for prospective quota batch reservation. Automatically releases resources when dropped.
#[derive(Debug)]
pub struct QuotaGuard {
    ledger: Option<Arc<DistributedQuotaLedger>>,
    workload_id: WorkloadId,
    bytes: u64,
    parallelism: u32,
    released: bool,
}

impl QuotaGuard {
    /// Explicitly release quota reservation without dropping.
    pub fn release(&mut self) {
        if !self.released {
            if let Some(ref ledger) = self.ledger {
                ledger.release_batch(self.workload_id, self.bytes, self.parallelism);
            }
            self.released = true;
        }
    }
}

impl Drop for QuotaGuard {
    fn drop(&mut self) {
        self.release();
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn acquire_within_budget_succeeds() {
        let b = StateBudget::new("op", 1024);
        assert!(b.try_acquire(512).is_ok());
        assert_eq!(b.current_bytes(), 512);
        assert!(b.try_acquire(512).is_ok());
        assert_eq!(b.current_bytes(), 1024);
    }

    #[test]
    fn acquire_over_budget_fails() {
        let b = StateBudget::new("op", 1024);
        b.force_acquire(900);
        let err = b.try_acquire(200).unwrap_err();
        assert_eq!(err.max_bytes, 1024);
        assert_eq!(err.current_bytes, 900);
        assert_eq!(err.requested_bytes, 200);
        assert!(err.to_string().contains("RS-5003"));
        // Counter must not have been incremented.
        assert_eq!(b.current_bytes(), 900);
    }

    #[test]
    fn release_reduces_counter() {
        let b = StateBudget::new("op", 1024);
        b.force_acquire(800);
        b.release(300);
        assert_eq!(b.current_bytes(), 500);
    }

    #[test]
    fn release_saturates_at_zero() {
        let b = StateBudget::new("op", 1024);
        b.release(500); // nothing to release
        assert_eq!(b.current_bytes(), 0);
    }

    #[test]
    fn utilization_fraction() {
        let b = StateBudget::new("op", 1000);
        b.force_acquire(250);
        let u = b.utilization();
        assert!((u - 0.25).abs() < 1e-9, "utilization={u}");
    }

    #[test]
    fn utilization_zero_max_is_infinity() {
        let b = StateBudget::new("op", 0);
        assert_eq!(b.utilization(), f64::INFINITY);
    }

    #[test]
    fn exact_budget_boundary_accepted() {
        let b = StateBudget::new("op", 512);
        assert!(b.try_acquire(512).is_ok());
        assert_eq!(b.current_bytes(), 512);
        // One byte over should fail.
        let err = b.try_acquire(1).unwrap_err();
        assert_eq!(err.current_bytes, 512);
    }

    #[test]
    fn arc_shared_between_threads() {
        use std::thread;
        let b = Arc::new(StateBudget::new("op", 10_000));
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let b2 = Arc::clone(&b);
                thread::spawn(move || {
                    // Each thread tries to acquire 500 bytes.
                    let _ = b2.try_acquire(500);
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // At most 10 × 500 = 5000 bytes, within the 10k limit.
        assert!(b.current_bytes() <= 5000);
    }

    #[test]
    fn operator_name_in_error() {
        let b = StateBudget::new("aggregate_sum", 100);
        b.force_acquire(90);
        let err = b.try_acquire(20).unwrap_err();
        assert_eq!(err.operator_name, "aggregate_sum");
    }

    #[test]
    fn workload_budget_enforcement() {
        let wl = Arc::new(WorkloadBudget::new(WorkloadId(42), 1000));
        let b1 = StateBudget::new_with_workload("op1", 800, Some(wl.clone()));
        let b2 = StateBudget::new_with_workload("op2", 800, Some(wl.clone()));

        // Acquire within workload and operator limits
        assert!(b1.try_acquire(400).is_ok());
        assert_eq!(wl.current_bytes(), 400);

        assert!(b2.try_acquire(400).is_ok());
        assert_eq!(wl.current_bytes(), 800);

        // This would exceed workload limit (800 + 300 = 1100 > 1000), even though within operator limit (400 + 300 = 700 < 800)
        let err = b1.try_acquire(300).unwrap_err();
        assert_eq!(err.operator_name, "workload-42");
        assert_eq!(wl.current_bytes(), 800); // counter should not change

        // Release works correctly
        b1.release(400);
        assert_eq!(wl.current_bytes(), 400);
    }

    #[test]
    fn distributed_quota_ledger_prospective_rejection() {
        let ledger = Arc::new(DistributedQuotaLedger::new());
        ledger.register_workload(WorkloadId(101), 1000, 2).unwrap();

        // Batch 1: 600 bytes, parallelism 1 -> succeeds
        let mut guard1 = ledger.try_acquire_batch(WorkloadId(101), 600, 1).unwrap();
        assert_eq!(ledger.total_reservations(), 1);

        // Batch 2: 500 bytes -> exceeds memory limit 1000 (600 + 500 = 1100 > 1000)
        let err = ledger
            .try_acquire_batch(WorkloadId(101), 500, 1)
            .unwrap_err();
        assert_eq!(err.operator_name, "workload-101");
        assert_eq!(ledger.total_rejections(), 1);

        // Release guard1 (600 bytes)
        guard1.release();

        // Batch 3: 500 bytes -> succeeds now
        let _guard2 = ledger.try_acquire_batch(WorkloadId(101), 500, 1).unwrap();
        assert_eq!(ledger.total_reservations(), 2);
    }

    #[test]
    fn distributed_quota_ledger_parallelism_cap() {
        let ledger = Arc::new(DistributedQuotaLedger::new());
        ledger
            .register_workload(WorkloadId(202), 10_000, 2)
            .unwrap();

        let _g1 = ledger.try_acquire_batch(WorkloadId(202), 100, 2).unwrap();
        let err = ledger
            .try_acquire_batch(WorkloadId(202), 100, 1)
            .unwrap_err();
        assert_eq!(err.operator_name, "workload-parallelism-202");
    }

    #[test]
    fn distributed_quota_ledger_bounded_capacity() {
        let ledger = Arc::new(DistributedQuotaLedger::with_capacity(2));
        assert!(ledger.register_workload(WorkloadId(1), 100, 1).is_ok());
        assert!(ledger.register_workload(WorkloadId(2), 100, 1).is_ok());
        assert!(ledger.register_workload(WorkloadId(3), 100, 1).is_err());
    }

    #[test]
    fn unlimited_quota_counter_overflow_is_rs5004_and_does_not_wrap() {
        let ledger = Arc::new(DistributedQuotaLedger::new());
        let workload_id = WorkloadId(303);
        ledger.register_workload(workload_id, 0, 0).unwrap();
        let entry = ledger.get_entry(workload_id).unwrap();
        entry
            .current_memory_bytes
            .store(u64::MAX - 1, Ordering::Relaxed);

        let err = ledger.try_acquire_batch(workload_id, 2, 1).unwrap_err();
        assert_eq!(
            err.to_string(),
            "RS-5004: quota counter overflow for 'quota-overflow-memory': current=18446744073709551614, requested=2; next_steps: release quota or create a new workload before retrying"
        );
        assert_eq!(
            entry.current_memory_bytes.load(Ordering::Relaxed),
            u64::MAX - 1
        );
    }

    proptest! {
        #[test]
        fn quota_boundary_proptest(
            byte_delta in 0u8..=1,
            parallelism_delta in 0u8..=1,
        ) {
            let ledger = Arc::new(DistributedQuotaLedger::new());
            let workload_id = WorkloadId(304);
            ledger.register_workload(workload_id, u64::MAX, u32::MAX).unwrap();
            let entry = ledger.get_entry(workload_id).unwrap();

            let bytes = u64::MAX - u64::from(byte_delta);
            entry.current_memory_bytes.store(bytes, Ordering::Relaxed);
            match ledger.try_acquire_batch(workload_id, 1, 0) {
                Ok(mut guard) => {
                    prop_assert_eq!(byte_delta, 1);
                    prop_assert_eq!(entry.current_memory_bytes.load(Ordering::Relaxed), u64::MAX);
                    guard.release();
                }
                Err(error) => {
                    prop_assert_eq!(byte_delta, 0);
                    prop_assert_eq!(error.to_string(), format!(
                        "RS-5004: quota counter overflow for 'quota-overflow-memory': current={bytes}, requested=1; next_steps: release quota or create a new workload before retrying"
                    ));
                }
            }
            prop_assert_eq!(entry.current_memory_bytes.load(Ordering::Relaxed), bytes);

            entry.current_memory_bytes.store(0, Ordering::Relaxed);
            let parallelism = u64::from(u32::MAX) - u64::from(parallelism_delta);
            entry.current_parallelism.store(parallelism, Ordering::Relaxed);
            match ledger.try_acquire_batch(workload_id, 0, 1) {
                Ok(mut guard) => {
                    prop_assert_eq!(parallelism_delta, 1);
                    prop_assert_eq!(entry.current_parallelism.load(Ordering::Relaxed), u64::from(u32::MAX));
                    guard.release();
                }
                Err(error) => {
                    prop_assert_eq!(parallelism_delta, 0);
                    prop_assert_eq!(error.to_string(), format!(
                        "RS-5004: quota counter overflow for 'quota-overflow-parallelism': current={parallelism}, requested=1; next_steps: release quota or create a new workload before retrying"
                    ));
                }
            }
            prop_assert_eq!(entry.current_parallelism.load(Ordering::Relaxed), parallelism);
            prop_assert_eq!(entry.current_memory_bytes.load(Ordering::Relaxed), 0);
        }
    }
}
