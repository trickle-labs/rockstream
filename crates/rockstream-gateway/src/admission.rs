//! Priority-driven admission control (§14.16, v0.45.1 Group E).
//!
//! `AdmissionController` decides, given a requesting workload's demand for
//! additional cluster state capacity, whether to admit the request outright,
//! pause a lower-priority workload's views to make room (reusing the
//! existing `ViewState::Paused` mechanism — no new pause state is
//! introduced), or reject the request with `RS-9001` when no lower-priority
//! capacity is available to reclaim.
//!
//! This reuses the existing per-view state-bytes metric
//! (`rockstream_types::metrics::read_pipeline_state_bytes`) as the measure
//! of cluster-wide demand rather than inventing a new accounting mechanism:
//! "current utilization" is the sum of state bytes across every view that is
//! not already `Paused`. Every admit/pause/reject decision is audited via
//! `FileAuditLog` (P5/P6).

use rockstream_control::audit::FileAuditLog;
use rockstream_types::audit::AuditEvent;
use rockstream_types::metrics::{
    read_pipeline_state_bytes, read_storage_pressure_signals, StoragePressureSignals,
};
use rockstream_types::view_lifecycle::{dominant_storage_contributor_for_signals, ViewState};

use crate::catalog_stubs::CatalogStubs;

/// Individual shedding steps executed in strict documented priority order under storage pressure:
/// Step 1: Throttle / reject backfills.
/// Step 2: Reduce source ingestion.
/// Step 3: Refuse parallelism increases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StoragePressureSheddingStep {
    /// Step 1: Throttle / reject backfills first.
    Step1ThrottleBackfills,
    /// Step 2: Reduce source ingestion second.
    Step2ReduceSourceIngestion,
    /// Step 3: Refuse parallelism scale-up third.
    Step3RefuseParallelismExpansion,
}

impl std::fmt::Display for StoragePressureSheddingStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Step1ThrottleBackfills => write!(f, "step_1_throttle_backfills"),
            Self::Step2ReduceSourceIngestion => write!(f, "step_2_reduce_source_ingestion"),
            Self::Step3RefuseParallelismExpansion => {
                write!(f, "step_3_refuse_parallelism_expansion")
            }
        }
    }
}

/// Storage-pressure shedding engine enforcing the documented 3-step hierarchy:
/// Step 1: Throttle backfills (v0.52.1) first.
/// Step 2: Reduce source ingestion second.
/// Step 3: Refuse parallelism increases that would worsen compaction debt third.
#[derive(Debug, Clone, Default)]
pub struct StoragePressureSheddingEngine;

impl StoragePressureSheddingEngine {
    /// Evaluate shedding steps in strict order (1 -> 2 -> 3) for the given storage pressure signals.
    pub fn evaluate_shedding(
        signals: &StoragePressureSignals,
        audit_log: Option<&FileAuditLog>,
    ) -> Vec<StoragePressureSheddingStep> {
        if !signals.is_pressured() {
            return Vec::new();
        }

        let dominant = dominant_storage_contributor_for_signals(signals)
            .map(|d| d.to_string())
            .unwrap_or_else(|| "storage_pressure".to_string());

        let mut steps = Vec::new();

        // Step 1: Throttle backfills
        steps.push(StoragePressureSheddingStep::Step1ThrottleBackfills);
        if let Some(log) = audit_log {
            let _ = log.append(&AuditEvent::now(
                "system",
                "storage_pressure.shedding_step_1_backfill_throttled",
                &dominant,
            ));
        }

        // Step 2: Reduce source ingestion
        steps.push(StoragePressureSheddingStep::Step2ReduceSourceIngestion);
        if let Some(log) = audit_log {
            let _ = log.append(&AuditEvent::now(
                "system",
                "storage_pressure.shedding_step_2_source_throttled",
                &dominant,
            ));
        }

        // Step 3: Refuse parallelism expansion
        steps.push(StoragePressureSheddingStep::Step3RefuseParallelismExpansion);
        if let Some(log) = audit_log {
            let _ = log.append(&AuditEvent::now(
                "system",
                "storage_pressure.shedding_step_3_parallelism_refused",
                &dominant,
            ));
        }

        steps
    }

    pub fn should_throttle_backfills(signals: &StoragePressureSignals) -> bool {
        signals.is_pressured()
    }

    pub fn should_reduce_source_ingestion(signals: &StoragePressureSignals) -> bool {
        signals.is_pressured()
    }

    pub fn should_refuse_parallelism_expansion(signals: &StoragePressureSignals) -> bool {
        signals.is_pressured()
    }
}

/// Independent bounded reservation lane for snapshot/delta backfills. Unlike
/// ordinary admission it never pauses a running view to make room.
#[derive(Debug, Default)]
pub struct BackfillAdmissionController {
    reserved_bytes: std::sync::Mutex<u64>,
}

/// Result of a backfill reservation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackfillAdmissionDecision {
    Admit,
    Reject { code: &'static str, reason: String },
}

impl BackfillAdmissionController {
    /// Reserve bounded backfill memory without changing normal view state.
    pub fn reserve(&self, requested_bytes: u64, capacity_bytes: u64) -> BackfillAdmissionDecision {
        self.reserve_with_signals(requested_bytes, capacity_bytes, None, None)
    }

    /// Reserve bounded backfill memory under optional storage pressure signals and audit log.
    pub fn reserve_with_signals(
        &self,
        requested_bytes: u64,
        capacity_bytes: u64,
        signals: Option<&StoragePressureSignals>,
        audit_log: Option<&FileAuditLog>,
    ) -> BackfillAdmissionDecision {
        let global_signals = read_storage_pressure_signals();
        let sigs = signals.unwrap_or(&global_signals);
        if sigs.is_pressured() {
            let dominant = dominant_storage_contributor_for_signals(sigs)
                .map(|d| d.to_string())
                .unwrap_or_else(|| "storage_pressure".to_string());
            if let Some(log) = audit_log {
                let _ = log.append(&AuditEvent::now(
                    "system",
                    "storage_pressure.shedding_step_1_backfill_throttled",
                    &dominant,
                ));
            }
            return BackfillAdmissionDecision::Reject {
                code: "RS-4021",
                reason: format!(
                    "backfill.admission_rejected: storage pressure active (dominant_cause={dominant}); next_steps: wait for compaction/storage debt to clear before retrying backfill"
                ),
            };
        }

        let mut reserved = self.reserved_bytes.lock().unwrap();
        if requested_bytes > capacity_bytes
            || reserved.saturating_add(requested_bytes) > capacity_bytes
        {
            return BackfillAdmissionDecision::Reject {
                code: "RS-4021",
                reason: format!(
                    "backfill.admission_rejected: requested {requested_bytes} bytes exceeds the available backfill reservation under capacity {capacity_bytes}; next_steps: wait for a backfill to finish or reduce BACKFILL_LIVE_DELTA_MAX_BYTES"
                ),
            };
        }
        *reserved += requested_bytes;
        BackfillAdmissionDecision::Admit
    }

    /// Account bounded live deltas against an admitted reservation.
    pub fn admit_live_delta(&self, bytes: u64, max_bytes: u64) -> BackfillAdmissionDecision {
        if bytes > max_bytes {
            return BackfillAdmissionDecision::Reject {
                code: "RS-4020",
                reason: format!(
                    "backfill.live_delta_buffer_full: live delta buffer is {bytes} bytes, above BACKFILL_LIVE_DELTA_MAX_BYTES={max_bytes}; next_steps: wait for snapshot catch-up before retrying"
                ),
            };
        }
        BackfillAdmissionDecision::Admit
    }

    pub fn release(&self, bytes: u64) {
        let mut reserved = self.reserved_bytes.lock().unwrap();
        *reserved = reserved.saturating_sub(bytes);
    }

    pub fn reserved_bytes(&self) -> u64 {
        *self.reserved_bytes.lock().unwrap()
    }
}

/// Outcome of an admission-control decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionDecision {
    /// The request was admitted without pausing any other workload.
    Admit,
    /// The request was admitted after pausing the listed views, which
    /// belonged to one or more lower-priority workloads.
    AdmitAfterPausing { paused_views: Vec<String> },
    /// The request was rejected: no lower-priority workload had capacity to
    /// give up, so the requesting workload's demand could not be satisfied
    /// under the cluster's `global_capacity_bytes`. Corresponds to `RS-9001`.
    Reject,
}

pub struct AdmissionController;

impl AdmissionController {
    /// Evaluate whether `requesting_workload` can be granted `requested_bytes`
    /// of additional cluster state capacity, given a hard `global_capacity_bytes`
    /// ceiling shared by every workload.
    ///
    /// Algorithm:
    /// 1. Compute current cluster-wide utilization as the sum of
    ///    `read_pipeline_state_bytes` across every view that is not already
    ///    `Paused` (a paused view contributes nothing to demand).
    /// 2. If `utilization + requested_bytes <= global_capacity_bytes`, admit
    ///    outright.
    /// 3. Otherwise, find workloads whose `WorkloadPriority` is strictly
    ///    lower-priority than the requester's (a numerically larger
    ///    priority value — `WorkloadPriority` orders `HIGH < DEFAULT < LOW`),
    ///    ordered lowest-priority first, and pause their `Running` views one
    ///    at a time — reducing utilization as each is paused — until the
    ///    requester's demand fits, or there are no more lower-priority views
    ///    left to pause.
    /// 4. If demand still does not fit after exhausting lower-priority
    ///    views, reject the request (`RS-9001`) and audit the rejection.
    pub fn evaluate_and_admit(
        catalog: &CatalogStubs,
        requesting_workload: &str,
        requested_bytes: u64,
        global_capacity_bytes: u64,
        audit_log: Option<&FileAuditLog>,
    ) -> AdmissionDecision {
        let Some(requester) = catalog.get_workload(requesting_workload) else {
            // Unknown workload: no priority to arbitrate against, admit.
            return AdmissionDecision::Admit;
        };

        let mut utilization = cluster_utilization(catalog);
        if utilization.saturating_add(requested_bytes) <= global_capacity_bytes {
            return AdmissionDecision::Admit;
        }

        // Lower-priority workloads (strictly greater WorkloadPriority value
        // than the requester), lowest-priority first.
        let mut contenders: Vec<_> = catalog
            .list_workloads()
            .into_iter()
            .filter(|workload| {
                workload.name != requesting_workload && workload.priority > requester.priority
            })
            .collect();
        contenders.sort_by_key(|c| std::cmp::Reverse(c.priority));

        let mut paused_views = Vec::new();
        'outer: for contender in &contenders {
            for view_name in catalog.views_for_workload(&contender.name) {
                if utilization.saturating_add(requested_bytes) <= global_capacity_bytes {
                    break 'outer;
                }
                if catalog.view_state(&view_name) != ViewState::Running {
                    continue;
                }
                let freed = read_pipeline_state_bytes(&view_name).unwrap_or(0);
                catalog.set_view_state(&view_name, ViewState::Paused);
                utilization = utilization.saturating_sub(freed);
                if let Some(log) = audit_log {
                    let _ = log.append(&AuditEvent::now(
                        "system",
                        "admission_control.paused",
                        &view_name,
                    ));
                }
                paused_views.push(view_name);
            }
        }

        if utilization.saturating_add(requested_bytes) <= global_capacity_bytes {
            if paused_views.is_empty() {
                AdmissionDecision::Admit
            } else {
                AdmissionDecision::AdmitAfterPausing { paused_views }
            }
        } else {
            if let Some(log) = audit_log {
                let _ = log.append(&AuditEvent::now(
                    "system",
                    "admission_control.rejected",
                    requesting_workload,
                ));
            }
            AdmissionDecision::Reject
        }
    }

    /// Evaluate admission under storage pressure signals. If signals are pressured,
    /// triggers shedding steps and evaluates priority-driven admission.
    pub fn evaluate_and_admit_with_storage_pressure(
        catalog: &CatalogStubs,
        requesting_workload: &str,
        requested_bytes: u64,
        global_capacity_bytes: u64,
        signals: Option<&StoragePressureSignals>,
        audit_log: Option<&FileAuditLog>,
    ) -> AdmissionDecision {
        let global_signals = read_storage_pressure_signals();
        let sigs = signals.unwrap_or(&global_signals);
        if sigs.is_pressured() {
            let _ = StoragePressureSheddingEngine::evaluate_shedding(sigs, audit_log);
        }
        Self::evaluate_and_admit(
            catalog,
            requesting_workload,
            requested_bytes,
            global_capacity_bytes,
            audit_log,
        )
    }
}

/// Sum of `read_pipeline_state_bytes` across every view in the catalog that
/// is not currently `Paused`. Paused views contribute zero demand.
fn cluster_utilization(catalog: &CatalogStubs) -> u64 {
    catalog
        .list_views()
        .into_iter()
        .filter(|view| catalog.view_state(&view.name) != ViewState::Paused)
        .map(|view| read_pipeline_state_bytes(&view.name).unwrap_or(0))
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_stubs::CatalogView;
    use rockstream_types::metrics::{reset_all, set_pipeline_state_bytes, METRICS_TEST_LOCK};
    use rockstream_types::workload::{WorkloadDef, WorkloadPriority};
    use tempfile::NamedTempFile;

    fn add_workload_view(catalog: &CatalogStubs, workload: &str, view: &str, bytes: u64) {
        catalog.add_view_in_namespace(CatalogView {
            name: view.to_string(),
            sql: "SELECT 1".to_string(),
            columns: vec![],
            namespace: "public".to_string(),
            op_id: None,
        });
        catalog.assign_view_workload(view, workload);
        set_pipeline_state_bytes(view, bytes);
    }

    #[test]
    fn admits_outright_when_capacity_available() {
        let _guard = METRICS_TEST_LOCK.lock().unwrap();
        reset_all();
        let catalog = CatalogStubs::new();
        catalog.add_workload(WorkloadDef::new("fast").with_priority(WorkloadPriority::HIGH));
        let decision = AdmissionController::evaluate_and_admit(&catalog, "fast", 100, 1_000, None);
        assert_eq!(decision, AdmissionDecision::Admit);
    }

    #[test]
    fn pauses_lower_priority_workload_to_make_room() {
        let _guard = METRICS_TEST_LOCK.lock().unwrap();
        reset_all();
        let tmp = NamedTempFile::new().unwrap();
        let log = FileAuditLog::open(tmp.path()).unwrap();

        let catalog = CatalogStubs::new();
        catalog.add_workload(WorkloadDef::new("high").with_priority(WorkloadPriority::HIGH));
        catalog.add_workload(WorkloadDef::new("low").with_priority(WorkloadPriority::LOW));
        add_workload_view(&catalog, "low", "low_view", 900);

        let decision =
            AdmissionController::evaluate_and_admit(&catalog, "high", 500, 1_000, Some(&log));
        assert_eq!(
            decision,
            AdmissionDecision::AdmitAfterPausing {
                paused_views: vec!["low_view".to_string()]
            }
        );
        assert_eq!(catalog.view_state("low_view"), ViewState::Paused);

        let actions: Vec<String> = log
            .read_all()
            .unwrap()
            .into_iter()
            .map(|event| event.action)
            .collect();
        assert!(actions.contains(&"admission_control.paused".to_string()));
    }

    #[test]
    fn rejects_when_no_lower_priority_capacity_available() {
        let _guard = METRICS_TEST_LOCK.lock().unwrap();
        reset_all();
        let tmp = NamedTempFile::new().unwrap();
        let log = FileAuditLog::open(tmp.path()).unwrap();

        let catalog = CatalogStubs::new();
        catalog.add_workload(WorkloadDef::new("high").with_priority(WorkloadPriority::HIGH));
        add_workload_view(&catalog, "high", "high_view", 900);

        let decision =
            AdmissionController::evaluate_and_admit(&catalog, "high", 500, 1_000, Some(&log));
        assert_eq!(decision, AdmissionDecision::Reject);

        let actions: Vec<String> = log
            .read_all()
            .unwrap()
            .into_iter()
            .map(|event| event.action)
            .collect();
        assert!(actions.contains(&"admission_control.rejected".to_string()));
    }

    #[test]
    fn does_not_pause_equal_or_higher_priority_workloads() {
        let _guard = METRICS_TEST_LOCK.lock().unwrap();
        reset_all();
        let catalog = CatalogStubs::new();
        catalog.add_workload(WorkloadDef::new("high").with_priority(WorkloadPriority::HIGH));
        catalog.add_workload(WorkloadDef::new("also_high").with_priority(WorkloadPriority::HIGH));
        add_workload_view(&catalog, "also_high", "peer_view", 900);

        let decision = AdmissionController::evaluate_and_admit(&catalog, "high", 500, 1_000, None);
        assert_eq!(decision, AdmissionDecision::Reject);
        assert_eq!(catalog.view_state("peer_view"), ViewState::Running);
    }

    #[test]
    fn backfill_budget_rejects_without_degrading_running_view() {
        let _guard = METRICS_TEST_LOCK.lock().unwrap();
        reset_all();
        let catalog = CatalogStubs::new();
        catalog.add_workload(WorkloadDef::new("fast").with_priority(WorkloadPriority::HIGH));
        add_workload_view(&catalog, "fast", "running_view", 900);
        let admission = BackfillAdmissionController::default();

        assert_eq!(
            admission.reserve(900, 1_000),
            BackfillAdmissionDecision::Admit
        );
        assert_eq!(
            admission.reserve(200, 1_000),
            BackfillAdmissionDecision::Reject {
                code: "RS-4021",
                reason: "backfill.admission_rejected: requested 200 bytes exceeds the available backfill reservation under capacity 1000; next_steps: wait for a backfill to finish or reduce BACKFILL_LIVE_DELTA_MAX_BYTES".to_string(),
            }
        );
        assert_eq!(catalog.view_state("running_view"), ViewState::Running);
        assert_eq!(admission.reserved_bytes(), 900);
    }

    #[test]
    fn full_live_delta_buffer_returns_rs4020() {
        let admission = BackfillAdmissionController::default();
        assert_eq!(
            admission.admit_live_delta(101, 100),
            BackfillAdmissionDecision::Reject {
                code: "RS-4020",
                reason: "backfill.live_delta_buffer_full: live delta buffer is 101 bytes, above BACKFILL_LIVE_DELTA_MAX_BYTES=100; next_steps: wait for snapshot catch-up before retrying".to_string(),
            }
        );
    }

    #[test]
    fn storage_pressure_triggers_shedding_hierarchy_in_order() {
        let _guard = METRICS_TEST_LOCK.lock().unwrap();
        reset_all();
        let tmp = NamedTempFile::new().unwrap();
        let log = FileAuditLog::open(tmp.path()).unwrap();

        let signals = StoragePressureSignals {
            l0_backlog: 10,
            ..Default::default()
        };

        let steps = StoragePressureSheddingEngine::evaluate_shedding(&signals, Some(&log));
        assert_eq!(
            steps,
            vec![
                StoragePressureSheddingStep::Step1ThrottleBackfills,
                StoragePressureSheddingStep::Step2ReduceSourceIngestion,
                StoragePressureSheddingStep::Step3RefuseParallelismExpansion,
            ]
        );

        let actions: Vec<String> = log
            .read_all()
            .unwrap()
            .into_iter()
            .map(|event| event.action)
            .collect();
        assert_eq!(
            actions,
            vec![
                "storage_pressure.shedding_step_1_backfill_throttled".to_string(),
                "storage_pressure.shedding_step_2_source_throttled".to_string(),
                "storage_pressure.shedding_step_3_parallelism_refused".to_string(),
            ]
        );
    }

    #[test]
    fn storage_pressure_rejects_backfill_with_dominant_cause() {
        let _guard = METRICS_TEST_LOCK.lock().unwrap();
        reset_all();
        let tmp = NamedTempFile::new().unwrap();
        let log = FileAuditLog::open(tmp.path()).unwrap();

        let signals = StoragePressureSignals {
            pending_compaction_bytes: 100 * 1024 * 1024,
            ..Default::default()
        };

        let admission = BackfillAdmissionController::default();
        let decision = admission.reserve_with_signals(100, 1000, Some(&signals), Some(&log));

        match decision {
            BackfillAdmissionDecision::Reject { code, reason } => {
                assert_eq!(code, "RS-4021");
                assert!(reason.contains("storage_pressure_pending_compaction_bytes"));
            }
            other => panic!("expected reject, got {other:?}"),
        }
    }
}
