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
use rockstream_types::metrics::read_pipeline_state_bytes;
use rockstream_types::view_lifecycle::ViewState;

use crate::catalog_stubs::CatalogStubs;

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
    use rockstream_types::metrics::{reset_all, set_pipeline_state_bytes};
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
        reset_all();
        let catalog = CatalogStubs::new();
        catalog.add_workload(WorkloadDef::new("fast").with_priority(WorkloadPriority::HIGH));
        let decision = AdmissionController::evaluate_and_admit(&catalog, "fast", 100, 1_000, None);
        assert_eq!(decision, AdmissionDecision::Admit);
    }

    #[test]
    fn pauses_lower_priority_workload_to_make_room() {
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
        reset_all();
        let catalog = CatalogStubs::new();
        catalog.add_workload(WorkloadDef::new("high").with_priority(WorkloadPriority::HIGH));
        catalog.add_workload(WorkloadDef::new("also_high").with_priority(WorkloadPriority::HIGH));
        add_workload_view(&catalog, "also_high", "peer_view", 900);

        let decision = AdmissionController::evaluate_and_admit(&catalog, "high", 500, 1_000, None);
        assert_eq!(decision, AdmissionDecision::Reject);
        assert_eq!(catalog.view_state("peer_view"), ViewState::Running);
    }
}
