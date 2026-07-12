//! View lifecycle state types for RockStream.
//!
//! A materialized view transitions through the following states:
//!
//! ```text
//! BackfillingFromEpoch(n)  →  Running  →  OverBudgetRelaxed  →  Running
//!                                        ↘ Paused ↗
//! ```
//!
//! The `ViewState` enum represents these states. `ViewStatus` combines the
//! state with SLO metadata for `SHOW VIEW STATUS FOR NAMESPACE`. `BackfillStatus`
//! provides progress detail for `SHOW BACKFILL STATUS FOR MATERIALIZED VIEW`.

use crate::ids::NamespaceId;
use crate::workload::WorkloadDef;
use serde::{Deserialize, Serialize};

/// Lifecycle state of a materialized view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ViewState {
    /// The view is actively processing deltas and keeping up with its sources.
    #[default]
    Running,
    /// The view has been paused; no deltas are processed until resumed.
    Paused,
    /// The view exceeded its workload memory limit and is running in a relaxed mode.
    OverBudgetRelaxed,
    /// The view is currently backfilling from the given starting epoch.
    BackfillingFromEpoch(u64),
}

impl ViewState {
    /// Returns true if the view is running (not paused, not backfilling).
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }

    /// Returns true if the view is paused.
    pub fn is_paused(&self) -> bool {
        matches!(self, Self::Paused)
    }

    /// Returns true if the view is running in over-budget relaxed mode.
    pub fn is_over_budget_relaxed(&self) -> bool {
        matches!(self, Self::OverBudgetRelaxed)
    }

    /// Returns true if the view is backfilling.
    pub fn is_backfilling(&self) -> bool {
        matches!(self, Self::BackfillingFromEpoch(_))
    }
}

impl std::fmt::Display for ViewState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => write!(f, "RUNNING"),
            Self::Paused => write!(f, "PAUSED"),
            Self::OverBudgetRelaxed => write!(f, "OVER_BUDGET_RELAXED"),
            Self::BackfillingFromEpoch(epoch) => write!(f, "BACKFILLING(from epoch {epoch})"),
        }
    }
}

/// Summary row returned by `SHOW VIEW STATUS FOR NAMESPACE`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewStatus {
    /// Namespace this view belongs to.
    pub namespace_id: NamespaceId,
    /// View name.
    pub view_name: String,
    /// Current lifecycle state.
    pub state: ViewState,
    /// Assigned workload name, if any.
    pub workload_name: Option<String>,
    /// Freshness SLO target in milliseconds, inherited from the workload.
    pub freshness_slo_ms: Option<u64>,
    /// Memory limit in bytes, inherited from the workload.
    pub memory_limit_bytes: Option<u64>,
    /// Names of views and sources this view directly depends on.
    pub depends_on: Vec<String>,
}

impl ViewStatus {
    /// Build a `ViewStatus` from a view name and its assigned workload (if any).
    pub fn new(
        namespace_id: NamespaceId,
        view_name: impl Into<String>,
        state: ViewState,
        workload: Option<&WorkloadDef>,
        depends_on: Vec<String>,
    ) -> Self {
        Self {
            namespace_id,
            view_name: view_name.into(),
            state,
            workload_name: workload.map(|w| w.name.clone()),
            freshness_slo_ms: workload.and_then(|w| w.freshness_slo).map(|s| s.target_ms),
            memory_limit_bytes: workload.and_then(|w| w.memory_limit).map(|m| m.bytes),
            depends_on,
        }
    }
}

/// Progress detail returned by `SHOW BACKFILL STATUS FOR MATERIALIZED VIEW`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillStatus {
    /// View name.
    pub view_name: String,
    /// Current lifecycle state.
    pub state: ViewState,
    /// Epoch from which the backfill started.
    pub backfill_started_epoch: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_state_predicates() {
        assert!(ViewState::Running.is_running());
        assert!(!ViewState::Running.is_paused());
        assert!(!ViewState::Running.is_backfilling());
        assert!(!ViewState::Running.is_over_budget_relaxed());

        assert!(ViewState::Paused.is_paused());
        assert!(!ViewState::Paused.is_running());

        assert!(ViewState::OverBudgetRelaxed.is_over_budget_relaxed());
        assert!(!ViewState::OverBudgetRelaxed.is_paused());

        assert!(ViewState::BackfillingFromEpoch(5).is_backfilling());
        assert!(!ViewState::BackfillingFromEpoch(5).is_paused());
    }

    #[test]
    fn view_state_display() {
        assert_eq!(ViewState::Running.to_string(), "RUNNING");
        assert_eq!(ViewState::Paused.to_string(), "PAUSED");
        assert_eq!(
            ViewState::OverBudgetRelaxed.to_string(),
            "OVER_BUDGET_RELAXED"
        );
        assert_eq!(
            ViewState::BackfillingFromEpoch(42).to_string(),
            "BACKFILLING(from epoch 42)"
        );
    }

    #[test]
    fn view_state_serializes_round_trip() {
        for state in [
            ViewState::Running,
            ViewState::Paused,
            ViewState::OverBudgetRelaxed,
            ViewState::BackfillingFromEpoch(7),
        ] {
            let json = serde_json::to_string(&state).unwrap();
            let back: ViewState = serde_json::from_str(&json).unwrap();
            assert_eq!(state, back);
        }
    }

    #[test]
    fn view_status_new_without_workload() {
        let ns = NamespaceId(1);
        let status = ViewStatus::new(ns, "orders_mv", ViewState::Running, None, vec![]);
        assert_eq!(status.view_name, "orders_mv");
        assert!(status.workload_name.is_none());
        assert!(status.freshness_slo_ms.is_none());
        assert!(status.memory_limit_bytes.is_none());
    }

    #[test]
    fn view_status_new_with_workload() {
        use crate::workload::{FreshnessSlo, MemoryLimit, WorkloadDef};
        let ns = NamespaceId(2);
        let wl = WorkloadDef::new("fast")
            .with_freshness_slo(FreshnessSlo::new(500))
            .with_memory_limit(MemoryLimit::new(1024));
        let status = ViewStatus::new(
            ns,
            "live_mv",
            ViewState::Running,
            Some(&wl),
            vec!["orders".into()],
        );
        assert_eq!(status.workload_name.as_deref(), Some("fast"));
        assert_eq!(status.freshness_slo_ms, Some(500));
        assert_eq!(status.memory_limit_bytes, Some(1024));
        assert_eq!(status.depends_on, vec!["orders".to_string()]);
    }

    #[test]
    fn backfill_status_serializes_round_trip() {
        let bs = BackfillStatus {
            view_name: "mv1".into(),
            state: ViewState::BackfillingFromEpoch(10),
            backfill_started_epoch: Some(10),
        };
        let json = serde_json::to_string(&bs).unwrap();
        let back: BackfillStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(bs, back);
    }
}
