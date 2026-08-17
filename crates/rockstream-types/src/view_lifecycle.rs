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

use crate::{
    error_code::{
        ErrorCode, RS_3701, RS_3702, RS_3703, RS_3704, RS_3705, RS_3706, RS_3707, RS_3708,
    },
    ids::NamespaceId,
    metrics::{
        read_storage_pressure_signals, StageLagBreakdown, StoragePressureSignals,
        STORAGE_PRESSURE_FLUSH_LATENCY_MS_THRESHOLD, STORAGE_PRESSURE_L0_BACKLOG_THRESHOLD,
        STORAGE_PRESSURE_OBJECT_STORE_FAILURE_RATE_THRESHOLD,
        STORAGE_PRESSURE_OBJECT_STORE_LATENCY_MS_THRESHOLD,
        STORAGE_PRESSURE_PENDING_COMPACTION_BYTES_THRESHOLD,
        STORAGE_PRESSURE_WRITE_AMPLIFICATION_THRESHOLD,
    },
    workload::WorkloadDef,
};
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
    /// The view exceeded its workload memory limit and rejected incoming batches prospectively.
    OverBudgetRejected,
    /// The view is currently backfilling from the given starting epoch.
    BackfillingFromEpoch(u64),
}

impl ViewState {
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }

    pub fn is_paused(&self) -> bool {
        matches!(self, Self::Paused)
    }

    pub fn is_over_budget_relaxed(&self) -> bool {
        matches!(self, Self::OverBudgetRelaxed)
    }

    pub fn is_over_budget_rejected(&self) -> bool {
        matches!(self, Self::OverBudgetRejected)
    }

    pub fn is_backfilling(&self) -> bool {
        matches!(self, Self::BackfillingFromEpoch(_))
    }

    pub fn from_status_text(state: &str) -> Option<Self> {
        let trimmed = state.trim();
        if trimmed.eq_ignore_ascii_case("RUNNING") {
            return Some(Self::Running);
        }
        if trimmed.eq_ignore_ascii_case("PAUSED") {
            return Some(Self::Paused);
        }
        if trimmed.eq_ignore_ascii_case("OVER_BUDGET_RELAXED") {
            return Some(Self::OverBudgetRelaxed);
        }
        if trimmed.eq_ignore_ascii_case("OVER_BUDGET_REJECTED") {
            return Some(Self::OverBudgetRejected);
        }
        if let Some(epoch) = trimmed
            .strip_prefix("BACKFILLING(from epoch ")
            .and_then(|rest| rest.strip_suffix(')'))
            .and_then(|value| value.parse::<u64>().ok())
        {
            return Some(Self::BackfillingFromEpoch(epoch));
        }
        None
    }
}

impl std::fmt::Display for ViewState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => write!(f, "RUNNING"),
            Self::Paused => write!(f, "PAUSED"),
            Self::OverBudgetRelaxed => write!(f, "OVER_BUDGET_RELAXED"),
            Self::OverBudgetRejected => write!(f, "OVER_BUDGET_REJECTED"),
            Self::BackfillingFromEpoch(epoch) => write!(f, "BACKFILLING(from epoch {epoch})"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DegradationReason {
    WaitingOnSource,
    QuotaAdmissionRejected,
    Spilling,
    OverBudgetRelaxed,
    CheckpointAlignmentStalled,
    SinkBlocked,
    TopologyTransitionInProgress,
    Recovering,
}

impl DegradationReason {
    pub const ALL: [Self; 8] = [
        Self::WaitingOnSource,
        Self::QuotaAdmissionRejected,
        Self::Spilling,
        Self::OverBudgetRelaxed,
        Self::CheckpointAlignmentStalled,
        Self::SinkBlocked,
        Self::TopologyTransitionInProgress,
        Self::Recovering,
    ];

    pub fn all() -> &'static [Self; 8] {
        &Self::ALL
    }

    pub const fn reason_code(self) -> ErrorCode {
        match self {
            Self::WaitingOnSource => RS_3701,
            Self::QuotaAdmissionRejected => RS_3702,
            Self::Spilling => RS_3703,
            Self::OverBudgetRelaxed => RS_3704,
            Self::CheckpointAlignmentStalled => RS_3705,
            Self::SinkBlocked => RS_3706,
            Self::TopologyTransitionInProgress => RS_3707,
            Self::Recovering => RS_3708,
        }
    }

    pub fn error_code(&self) -> ErrorCode {
        self.reason_code()
    }
}

impl std::fmt::Display for DegradationReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::WaitingOnSource => "waiting_on_source",
            Self::QuotaAdmissionRejected => "quota_admission_rejected",
            Self::Spilling => "spilling",
            Self::OverBudgetRelaxed => "over_budget_relaxed",
            Self::CheckpointAlignmentStalled => "checkpoint_alignment_stalled",
            Self::SinkBlocked => "sink_blocked",
            Self::TopologyTransitionInProgress => "topology_transition_in_progress",
            Self::Recovering => "recovering",
        };
        write!(f, "{value}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DominantContributor {
    Healthy,
    SourceLag,
    DecodeLag,
    ComputeLag,
    AlignmentLag,
    SinkLag,
    SpillLag,
    StoragePressure,
    StoragePressureL0Backlog,
    StoragePressurePendingCompaction,
    StoragePressureFlushLatency,
    StoragePressureWriteAmplification,
    StoragePressureObjectStoreLatency,
    StoragePressureObjectStoreFailures,
}

impl std::fmt::Display for DominantContributor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Healthy => "healthy",
            Self::SourceLag => "source_lag",
            Self::DecodeLag => "decode_lag",
            Self::ComputeLag => "compute_lag",
            Self::AlignmentLag => "alignment_lag",
            Self::SinkLag => "sink_lag",
            Self::SpillLag => "spill_lag",
            Self::StoragePressure => "storage_pressure",
            Self::StoragePressureL0Backlog => "storage_pressure_l0_backlog",
            Self::StoragePressurePendingCompaction => "storage_pressure_pending_compaction_bytes",
            Self::StoragePressureFlushLatency => "storage_pressure_flush_latency",
            Self::StoragePressureWriteAmplification => "storage_pressure_write_amplification",
            Self::StoragePressureObjectStoreLatency => "storage_pressure_object_store_latency",
            Self::StoragePressureObjectStoreFailures => "storage_pressure_object_store_failures",
        };
        write!(f, "{value}")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DegradationStatus {
    pub degradation_reason: DegradationReason,
    pub reason_code: String,
    pub dominant_contributor: DominantContributor,
    #[serde(default)]
    pub progress_phase: Option<String>,
    #[serde(default)]
    pub bytes_remaining: Option<u64>,
    #[serde(default)]
    pub rows_remaining: Option<u64>,
    #[serde(default)]
    pub estimated_remaining_ms: Option<u64>,
}

impl DegradationStatus {
    pub fn new(reason: DegradationReason, dominant_contributor: DominantContributor) -> Self {
        Self {
            degradation_reason: reason,
            reason_code: reason.reason_code().to_string(),
            dominant_contributor,
            progress_phase: None,
            bytes_remaining: None,
            rows_remaining: None,
            estimated_remaining_ms: None,
        }
    }

    pub fn with_progress(
        mut self,
        progress_phase: Option<String>,
        bytes_remaining: Option<u64>,
        rows_remaining: Option<u64>,
        estimated_remaining_ms: Option<u64>,
    ) -> Self {
        self.progress_phase = progress_phase;
        self.bytes_remaining = bytes_remaining;
        self.rows_remaining = rows_remaining;
        self.estimated_remaining_ms = estimated_remaining_ms;
        self
    }
}

pub fn dominant_storage_contributor_for_signals(
    signals: &StoragePressureSignals,
) -> Option<DominantContributor> {
    let l0_ratio = signals.l0_backlog as f64 / STORAGE_PRESSURE_L0_BACKLOG_THRESHOLD as f64;
    let pending_ratio = signals.pending_compaction_bytes as f64
        / STORAGE_PRESSURE_PENDING_COMPACTION_BYTES_THRESHOLD as f64;
    let flush_ratio =
        signals.flush_latency_ms as f64 / STORAGE_PRESSURE_FLUSH_LATENCY_MS_THRESHOLD as f64;
    let write_amp_ratio =
        signals.write_amplification / STORAGE_PRESSURE_WRITE_AMPLIFICATION_THRESHOLD;
    let obj_lat_ratio = signals.object_store_latency_ms as f64
        / STORAGE_PRESSURE_OBJECT_STORE_LATENCY_MS_THRESHOLD as f64;
    let obj_fail_ratio =
        signals.object_store_failure_rate / STORAGE_PRESSURE_OBJECT_STORE_FAILURE_RATE_THRESHOLD;

    let candidates = [
        (DominantContributor::StoragePressureL0Backlog, l0_ratio),
        (
            DominantContributor::StoragePressurePendingCompaction,
            pending_ratio,
        ),
        (
            DominantContributor::StoragePressureFlushLatency,
            flush_ratio,
        ),
        (
            DominantContributor::StoragePressureWriteAmplification,
            write_amp_ratio,
        ),
        (
            DominantContributor::StoragePressureObjectStoreLatency,
            obj_lat_ratio,
        ),
        (
            DominantContributor::StoragePressureObjectStoreFailures,
            obj_fail_ratio,
        ),
    ];

    let mut best = candidates[0];
    for candidate in &candidates[1..] {
        if candidate.1 > best.1 {
            best = *candidate;
        }
    }

    if best.1 > 1.0 {
        Some(best.0)
    } else {
        None
    }
}

pub fn dominant_contributor(stage_lag: Option<StageLagBreakdown>) -> DominantContributor {
    dominant_contributor_with_signals(stage_lag, None)
}

pub fn dominant_contributor_with_signals(
    stage_lag: Option<StageLagBreakdown>,
    signals: Option<StoragePressureSignals>,
) -> DominantContributor {
    let signals = signals.unwrap_or_else(read_storage_pressure_signals);
    let storage_dominant = dominant_storage_contributor_for_signals(&signals);
    let storage_variant = storage_dominant.unwrap_or(DominantContributor::StoragePressure);

    let Some(lag) = stage_lag else {
        if signals.is_pressured() {
            if let Some(sd) = storage_dominant {
                return sd;
            }
        }
        return DominantContributor::Healthy;
    };
    if lag.source_lag_ms == 0
        && lag.decode_lag_ms == 0
        && lag.compute_lag_ms == 0
        && lag.alignment_lag_ms == 0
        && lag.sink_lag_ms == 0
        && lag.spill_lag_ms == 0
        && lag.storage_pressure_ms == 0
    {
        if signals.is_pressured() {
            if let Some(sd) = storage_dominant {
                return sd;
            }
        }
        return DominantContributor::Healthy;
    }
    let ordered = [
        (DominantContributor::SourceLag, lag.source_lag_ms),
        (DominantContributor::DecodeLag, lag.decode_lag_ms),
        (DominantContributor::ComputeLag, lag.compute_lag_ms),
        (DominantContributor::AlignmentLag, lag.alignment_lag_ms),
        (DominantContributor::SinkLag, lag.sink_lag_ms),
        (DominantContributor::SpillLag, lag.spill_lag_ms),
        (storage_variant, lag.storage_pressure_ms),
    ];
    let mut best = ordered[0];
    for candidate in ordered.into_iter().skip(1) {
        if candidate.1 > best.1 {
            best = candidate;
        }
    }
    best.0
}

pub fn derive_degradation_status(
    view_state: &ViewState,
    stage_lag: Option<StageLagBreakdown>,
) -> DegradationStatus {
    derive_degradation_status_with_signals(view_state, stage_lag, None)
}

pub fn derive_degradation_status_with_signals(
    view_state: &ViewState,
    stage_lag: Option<StageLagBreakdown>,
    signals: Option<StoragePressureSignals>,
) -> DegradationStatus {
    let dominant = dominant_contributor_with_signals(stage_lag, signals);
    if view_state.is_backfilling() {
        return DegradationStatus::new(DegradationReason::Recovering, dominant);
    }
    if view_state.is_paused() {
        let (phase, bytes_remaining, rows_remaining, estimated_remaining_ms) =
            if let Some(lag) = stage_lag {
                if lag.storage_pressure_ms > 0 {
                    (
                        Some("shard_migration".to_string()),
                        Some(lag.storage_pressure_ms),
                        Some(lag.compute_lag_ms),
                        Some(lag.total_lag_ms),
                    )
                } else {
                    (
                        Some("worker_drain".to_string()),
                        Some(lag.sink_lag_ms),
                        Some(lag.decode_lag_ms),
                        Some(lag.total_lag_ms),
                    )
                }
            } else {
                (Some("worker_drain".to_string()), None, None, None)
            };
        return DegradationStatus::new(DegradationReason::TopologyTransitionInProgress, dominant)
            .with_progress(
                phase,
                bytes_remaining,
                rows_remaining,
                estimated_remaining_ms,
            );
    }
    if view_state.is_over_budget_rejected() {
        return DegradationStatus::new(DegradationReason::QuotaAdmissionRejected, dominant);
    }
    if view_state.is_over_budget_relaxed() {
        return DegradationStatus::new(DegradationReason::OverBudgetRelaxed, dominant);
    }
    if let Some(lag) = stage_lag {
        if lag.spill_lag_ms > 0 {
            return DegradationStatus::new(DegradationReason::Spilling, dominant);
        }
        if lag.sink_lag_ms > 0 {
            return DegradationStatus::new(DegradationReason::SinkBlocked, dominant);
        }
        if lag.alignment_lag_ms > 0 {
            return DegradationStatus::new(DegradationReason::CheckpointAlignmentStalled, dominant);
        }
    }
    DegradationStatus::new(DegradationReason::WaitingOnSource, dominant)
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
    /// Decomposed stage lag breakdown, if available.
    #[serde(default)]
    pub stage_lag: Option<StageLagBreakdown>,
    /// Typed degradation reason, contributor, and progress fields.
    #[serde(default)]
    pub degradation_status: Option<DegradationStatus>,
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
            stage_lag: None,
            degradation_status: None,
        }
    }

    /// Attach stage lag breakdown to the view status.
    pub fn with_stage_lag(mut self, stage_lag: StageLagBreakdown) -> Self {
        self.stage_lag = Some(stage_lag);
        self
    }

    pub fn with_degradation_status(mut self, status: DegradationStatus) -> Self {
        self.degradation_status = Some(status);
        self
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
            ViewState::OverBudgetRejected,
            ViewState::BackfillingFromEpoch(7),
        ] {
            let json = serde_json::to_string(&state).unwrap();
            let back: ViewState = serde_json::from_str(&json).unwrap();
            assert_eq!(state, back);
        }
    }

    fn lag(
        source_lag_ms: u64,
        decode_lag_ms: u64,
        compute_lag_ms: u64,
        alignment_lag_ms: u64,
        sink_lag_ms: u64,
        spill_lag_ms: u64,
        storage_pressure_ms: u64,
    ) -> StageLagBreakdown {
        StageLagBreakdown {
            source_lag_ms,
            decode_lag_ms,
            compute_lag_ms,
            alignment_lag_ms,
            sink_lag_ms,
            spill_lag_ms,
            storage_pressure_ms,
            total_lag_ms: source_lag_ms
                + decode_lag_ms
                + compute_lag_ms
                + alignment_lag_ms
                + sink_lag_ms
                + spill_lag_ms
                + storage_pressure_ms,
        }
    }

    #[test]
    fn test_degradation_reason_contract_is_closed() {
        let values: Vec<(String, String)> = DegradationReason::all()
            .iter()
            .map(|reason| (reason.to_string(), reason.reason_code().to_string()))
            .collect();
        assert_eq!(
            values,
            vec![
                ("waiting_on_source".to_string(), "RS-3701".to_string()),
                (
                    "quota_admission_rejected".to_string(),
                    "RS-3702".to_string()
                ),
                ("spilling".to_string(), "RS-3703".to_string()),
                ("over_budget_relaxed".to_string(), "RS-3704".to_string()),
                (
                    "checkpoint_alignment_stalled".to_string(),
                    "RS-3705".to_string()
                ),
                ("sink_blocked".to_string(), "RS-3706".to_string()),
                (
                    "topology_transition_in_progress".to_string(),
                    "RS-3707".to_string()
                ),
                ("recovering".to_string(), "RS-3708".to_string()),
            ]
        );
    }

    #[test]
    fn test_dominant_contributor_single_max_matrix() {
        let matrix = [
            (lag(12, 2, 3, 4, 5, 6, 7), DominantContributor::SourceLag),
            (lag(1, 12, 3, 4, 5, 6, 7), DominantContributor::DecodeLag),
            (lag(1, 2, 12, 4, 5, 6, 7), DominantContributor::ComputeLag),
            (lag(1, 2, 3, 12, 5, 6, 7), DominantContributor::AlignmentLag),
            (lag(1, 2, 3, 4, 12, 6, 7), DominantContributor::SinkLag),
            (lag(1, 2, 3, 4, 5, 12, 7), DominantContributor::SpillLag),
            (
                lag(1, 2, 3, 4, 5, 6, 12),
                DominantContributor::StoragePressure,
            ),
        ];
        for (input, expected) in matrix {
            assert_eq!(dominant_contributor(Some(input)), expected);
        }
    }

    #[test]
    fn test_dominant_contributor_multi_cause_max() {
        let status =
            derive_degradation_status(&ViewState::Running, Some(lag(15, 2, 2, 2, 2, 24, 1)));
        assert_eq!(status.degradation_reason, DegradationReason::Spilling);
        assert_eq!(status.dominant_contributor, DominantContributor::SpillLag);
    }

    #[test]
    fn test_dominant_contributor_tie_break_is_deterministic() {
        let tied = lag(9, 9, 9, 9, 9, 9, 9);
        assert_eq!(
            dominant_contributor(Some(tied)),
            DominantContributor::SourceLag
        );
        assert_eq!(
            derive_degradation_status(&ViewState::Running, Some(tied)).degradation_reason,
            DegradationReason::Spilling
        );
    }

    #[test]
    fn test_healthy_status_has_closed_vocabulary() {
        let status = derive_degradation_status(&ViewState::Running, Some(lag(0, 0, 0, 0, 0, 0, 0)));
        assert_eq!(
            status.degradation_reason,
            DegradationReason::WaitingOnSource
        );
        assert_eq!(status.reason_code, "RS-3701");
        assert_eq!(status.dominant_contributor, DominantContributor::Healthy);
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

    #[test]
    fn test_degradation_reason_runbook_conformance() {
        let all_reasons = DegradationReason::all();
        assert_eq!(all_reasons.len(), 8);

        let mut seen_codes = std::collections::BTreeSet::new();
        let mut seen_reasons = std::collections::BTreeSet::new();

        let doc_content = std::fs::read_to_string("../../docs/sre-operations.md")
            .or_else(|_| std::fs::read_to_string("docs/sre-operations.md"))
            .expect("must be able to read docs/sre-operations.md");

        for &reason in all_reasons {
            let reason_str = reason.to_string();
            let code_str = reason.reason_code().to_string();

            assert!(
                doc_content.contains(&reason_str),
                "docs/sre-operations.md must document reason {reason_str}"
            );
            assert!(
                doc_content.contains(&code_str),
                "docs/sre-operations.md must document code {code_str}"
            );

            assert!(seen_reasons.insert(reason_str));
            assert!(seen_codes.insert(code_str));
        }

        assert_eq!(seen_codes.len(), 8);
        for expected_code in 3701..=3708 {
            let code_str = format!("RS-{expected_code}");
            assert!(
                seen_codes.contains(&code_str),
                "missing allocated code {code_str}"
            );
        }
    }

    #[test]
    fn test_storage_pressure_dominant_contributors() {
        let cases = [
            (
                StoragePressureSignals {
                    l0_backlog: 15,
                    ..Default::default()
                },
                DominantContributor::StoragePressureL0Backlog,
                "storage_pressure_l0_backlog",
            ),
            (
                StoragePressureSignals {
                    pending_compaction_bytes: 128 * 1024 * 1024,
                    ..Default::default()
                },
                DominantContributor::StoragePressurePendingCompaction,
                "storage_pressure_pending_compaction_bytes",
            ),
            (
                StoragePressureSignals {
                    flush_latency_ms: 800,
                    ..Default::default()
                },
                DominantContributor::StoragePressureFlushLatency,
                "storage_pressure_flush_latency",
            ),
            (
                StoragePressureSignals {
                    write_amplification: 25.0,
                    ..Default::default()
                },
                DominantContributor::StoragePressureWriteAmplification,
                "storage_pressure_write_amplification",
            ),
            (
                StoragePressureSignals {
                    object_store_latency_ms: 2000,
                    ..Default::default()
                },
                DominantContributor::StoragePressureObjectStoreLatency,
                "storage_pressure_object_store_latency",
            ),
            (
                StoragePressureSignals {
                    object_store_failure_rate: 0.10,
                    ..Default::default()
                },
                DominantContributor::StoragePressureObjectStoreFailures,
                "storage_pressure_object_store_failures",
            ),
        ];

        for (signals, expected_dominant, expected_str) in cases {
            assert_eq!(
                dominant_storage_contributor_for_signals(&signals),
                Some(expected_dominant)
            );
            assert_eq!(expected_dominant.to_string(), expected_str);
            assert_eq!(
                dominant_contributor_with_signals(None, Some(signals)),
                expected_dominant
            );
        }
    }
}
