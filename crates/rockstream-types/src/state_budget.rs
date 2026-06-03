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
        write!(
            f,
            "RS-5003: state budget exceeded for '{}': current={} bytes, \
             requested={} bytes, limit={} bytes",
            self.operator_name, self.current_bytes, self.requested_bytes, self.max_bytes
        )
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
                        if pct >= 95.0 {
                            if !self.warning_emitted_95.swap(true, Ordering::Relaxed) {
                                tracing::warn!(
                                    "RS-5019: Workload {} is at {:.1}% memory utilization (current={}, limit={})",
                                    self.workload_id, pct, proposed, self.max_bytes
                                );
                            }
                        } else if pct >= 80.0 {
                            if !self.notice_emitted_80.swap(true, Ordering::Relaxed) {
                                tracing::info!(
                                    "RS-5018: Workload {} is at {:.1}% memory utilization (current={}, limit={})",
                                    self.workload_id, pct, proposed, self.max_bytes
                                );
                            }
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
                        if pct >= 95.0 {
                            if !self.warning_emitted_95.swap(true, Ordering::Relaxed) {
                                tracing::warn!(
                                    "RS-5019: State budget for '{}' is at {:.1}% utilization (current={}, limit={})",
                                    self.operator_name, pct, proposed, self.max_bytes
                                );
                            }
                        } else if pct >= 80.0 {
                            if !self.notice_emitted_80.swap(true, Ordering::Relaxed) {
                                tracing::info!(
                                    "RS-5018: State budget for '{}' is at {:.1}% utilization (current={}, limit={})",
                                    self.operator_name, pct, proposed, self.max_bytes
                                );
                            }
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

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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
}
