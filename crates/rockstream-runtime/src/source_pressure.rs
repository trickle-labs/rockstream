//! Source Pressure Propagation and Credit Control (v0.62.1 Slice 7).

use rockstream_types::state_budget::{StateBudgetError, WorkerBudgetLedger};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SourcePressureState {
    Normal = 0,
    Throttled = 1,
    Paused = 2,
}

impl std::fmt::Display for SourcePressureState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Normal => write!(f, "normal"),
            Self::Throttled => write!(f, "throttled"),
            Self::Paused => write!(f, "paused"),
        }
    }
}

/// Controller that monitors worker memory pressure and dynamically throttles
/// or pauses CDC source poll credits and ingress streams.
#[derive(Debug)]
pub struct SourcePressureController {
    ledger: Arc<WorkerBudgetLedger>,
    base_credits: u32,
    last_state: AtomicU8,
    configured_limits: Option<(u64, u64)>,
}

impl SourcePressureController {
    /// Create a new source pressure controller.
    pub fn new(ledger: Arc<WorkerBudgetLedger>, base_credits: u32) -> Self {
        Self {
            ledger,
            base_credits: base_credits.max(1),
            last_state: AtomicU8::new(SourcePressureState::Normal as u8),
            configured_limits: None,
        }
    }

    /// Attach workload/worker soft and hard pressure thresholds in bytes.
    /// Invalid or nonsensical bounds fail before a controller is published.
    pub fn with_limits(
        mut self,
        soft_limit_bytes: u64,
        hard_limit_bytes: u64,
    ) -> Result<Self, StateBudgetError> {
        if soft_limit_bytes == 0
            || soft_limit_bytes >= hard_limit_bytes
            || hard_limit_bytes > self.ledger.total_budget_bytes()
        {
            return Err(StateBudgetError {
                operator_name: "source-pressure-bounds".to_string(),
                max_bytes: self.ledger.total_budget_bytes(),
                current_bytes: soft_limit_bytes,
                requested_bytes: hard_limit_bytes,
            });
        }
        self.configured_limits = Some((soft_limit_bytes, hard_limit_bytes));
        Ok(self)
    }

    /// Effective pressure ceilings, including a recovery threshold below soft.
    /// The default thresholds preserve the earlier 80% / 95% worker behavior.
    pub fn pressure_limits(&self) -> (u64, u64) {
        self.configured_limits.unwrap_or_else(|| {
            let total = u128::from(self.ledger.total_budget_bytes());
            ((total * 80 / 100) as u64, (total * 95 / 100) as u64)
        })
    }

    fn gross_reserved_bytes(&self) -> u128 {
        let allocated = u128::from(self.ledger.total_allocated_bytes());
        allocated * u128::from(100 + self.ledger.allocator_overhead_pct()) / 100
    }

    pub fn base_credits(&self) -> u32 {
        self.base_credits
    }

    /// Compute current memory pressure ratio including allocator overhead.
    pub fn utilization_ratio(&self) -> f64 {
        let total = self.ledger.total_budget_bytes();
        if total == 0 {
            return f64::INFINITY;
        }
        self.gross_reserved_bytes() as f64 / total as f64
    }

    /// Return the current pressure state evaluated against the worker budget ledger.
    ///
    /// - `Normal`: utilization < 80% (or < 75% if recovering from throttled)
    /// - `Throttled`: utilization >= 80% (retained until clearing below 75%)
    /// - `Paused`: utilization >= 95%
    pub fn pressure_state(&self) -> SourcePressureState {
        let gross = self.gross_reserved_bytes();
        let (soft, hard) = self.pressure_limits();
        let prev = self.last_state.load(Ordering::Relaxed);
        // Zero capacity is not a source-ingestion entitlement.
        let recovery_threshold = u128::from(soft) * 75 / 80;
        let new_state = if self.ledger.total_budget_bytes() == 0 || gross >= u128::from(hard) {
            SourcePressureState::Paused
        } else if gross >= u128::from(soft)
            || (gross >= recovery_threshold && prev != SourcePressureState::Normal as u8)
        {
            SourcePressureState::Throttled
        } else {
            SourcePressureState::Normal
        };

        self.last_state.store(new_state as u8, Ordering::Relaxed);
        new_state
    }

    /// Current available CDC poll credits / source ingestion window.
    ///
    /// - `Normal`: 100% of base credits
    /// - `Throttled`: 50% of base credits (minimum 1)
    /// - `Paused`: 0 credits
    pub fn available_credits(&self) -> u32 {
        match self.pressure_state() {
            SourcePressureState::Normal => self.base_credits,
            SourcePressureState::Throttled => (self.base_credits / 2).max(1),
            SourcePressureState::Paused => 0,
        }
    }

    /// Check whether source ingestion is permitted.
    /// Returns `Ok(())` if permitted, or `Err(StateBudgetError)` with RS-5003 if paused.
    pub fn can_ingest(&self) -> Result<(), StateBudgetError> {
        match self.pressure_state() {
            SourcePressureState::Normal | SourcePressureState::Throttled => Ok(()),
            SourcePressureState::Paused => Err(StateBudgetError {
                operator_name: "source_pressure_paused".to_string(),
                max_bytes: self.ledger.total_budget_bytes(),
                current_bytes: self.ledger.total_allocated_bytes(),
                requested_bytes: 0,
            }),
        }
    }
}
