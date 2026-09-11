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
}

impl SourcePressureController {
    /// Create a new source pressure controller.
    pub fn new(ledger: Arc<WorkerBudgetLedger>, base_credits: u32) -> Self {
        Self {
            ledger,
            base_credits: base_credits.max(1),
            last_state: AtomicU8::new(SourcePressureState::Normal as u8),
        }
    }

    pub fn base_credits(&self) -> u32 {
        self.base_credits
    }

    /// Compute current memory pressure ratio including allocator overhead.
    pub fn utilization_ratio(&self) -> f64 {
        let total = self.ledger.total_budget_bytes();
        if total == 0 {
            return 0.0;
        }
        let allocated = self.ledger.total_allocated_bytes();
        let overhead = (allocated * self.ledger.allocator_overhead_pct()) / 100;
        let gross = allocated.saturating_add(overhead);
        gross as f64 / total as f64
    }

    /// Return the current pressure state evaluated against the worker budget ledger.
    ///
    /// - `Normal`: utilization < 80% (or < 75% if recovering from throttled)
    /// - `Throttled`: utilization >= 80% (retained until clearing below 75%)
    /// - `Paused`: utilization >= 95%
    pub fn pressure_state(&self) -> SourcePressureState {
        let ratio = self.utilization_ratio();
        let prev = self.last_state.load(Ordering::Relaxed);

        let new_state = if ratio >= 0.95 {
            SourcePressureState::Paused
        } else if ratio >= 0.80 {
            SourcePressureState::Throttled
        } else if ratio >= 0.75 && prev != SourcePressureState::Normal as u8 {
            // Hysteresis: remain throttled until recovering below 75%
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
