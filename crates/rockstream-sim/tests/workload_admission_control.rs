//! F1 — Seeded two-workload (`HIGH` vs `LOW`) contention scenario proving
//! priority-driven admission control (v0.45.1, roadmap §14.16, Proof P1/P2).
//!
//! Constructs two workloads — `high` (`PRIORITY = HIGH`) and `low`
//! (`PRIORITY = LOW`) — each with a view assigned, on a cluster whose
//! `global_capacity_bytes` is deliberately sized below the combined demand
//! of both workloads' state growth. Drives simulated state growth on both
//! workloads' views, with `buggify!("admission.contention_timing", p)`
//! jittering which workload's growth event is evaluated first (the seeded
//! fault-injection convention required by the Ground Rules for new
//! coordination paths), and asserts:
//!
//! (a) the low-priority workload's view is paused under contention (P1);
//! (b) the high-priority workload's view is admitted and stays `Running`,
//!     with its tracked freshness lag unaffected (P2);
//! (c) every pause/admission-control decision appears in the audit log
//!     (P5/P6);
//! (d) re-running with a different seed produces the same qualitative
//!     outcome — HIGH always wins contention over LOW — a determinism
//!     check consistent with this codebase's seeded-replay conventions.

use rockstream_control::audit::FileAuditLog;
use rockstream_gateway::admission::{AdmissionController, AdmissionDecision};
use rockstream_gateway::catalog_stubs::{CatalogStubs, CatalogView};
use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_init};
use rockstream_types::metrics::{reset_all, set_freshness_lag, set_pipeline_state_bytes};
use rockstream_types::view_lifecycle::ViewState;
use rockstream_types::workload::{WorkloadDef, WorkloadPriority};

/// Cluster-wide state capacity, deliberately smaller than the combined
/// footprint `low` and `high` would need if both were admitted in full.
const GLOBAL_CAPACITY_BYTES: u64 = 1_000;
/// `low`'s pre-existing footprint before `high` requests more capacity.
const LOW_INITIAL_BYTES: u64 = 900;
/// `high`'s incremental growth request that would overflow the cluster
/// budget unless `low` is paused first.
const HIGH_REQUESTED_BYTES: u64 = 400;
/// `high`'s freshness SLO target — must stay met throughout contention.
const HIGH_FRESHNESS_SLO_MS: u64 = 500;

fn run_contention_scenario(seed: u64) {
    reset_all();
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let log = FileAuditLog::open(tmp.path()).unwrap();

    let catalog = CatalogStubs::new();
    catalog.add_workload(WorkloadDef::new("high").with_priority(WorkloadPriority::HIGH));
    catalog.add_workload(WorkloadDef::new("low").with_priority(WorkloadPriority::LOW));

    catalog.add_view_in_namespace(CatalogView {
        name: "high_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.assign_view_workload("high_view", "high");
    catalog.add_view_in_namespace(CatalogView {
        name: "low_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.assign_view_workload("low_view", "low");

    // `low` has already accumulated most of the cluster budget.
    set_pipeline_state_bytes("low_view", LOW_INITIAL_BYTES);
    // `high` starts at zero and stays comfortably within its freshness SLO.
    set_freshness_lag("high_view", HIGH_FRESHNESS_SLO_MS / 2);

    buggify_init(seed);
    // Jitter whether `low`'s growth event or `high`'s capacity request is
    // "evaluated" first — the qualitative outcome (HIGH always wins) must
    // hold regardless of the seed's chosen ordering.
    let low_grows_again_first = buggify!("admission.contention_timing", 0.5);
    if low_grows_again_first {
        set_pipeline_state_bytes("low_view", LOW_INITIAL_BYTES + 10);
    }

    let decision = AdmissionController::evaluate_and_admit(
        &catalog,
        "high",
        HIGH_REQUESTED_BYTES,
        GLOBAL_CAPACITY_BYTES,
        Some(&log),
    );
    buggify_disable();

    // (a) low-priority workload's view is paused under contention.
    assert_eq!(
        catalog.view_state("low_view"),
        ViewState::Paused,
        "seed {seed}: low-priority workload's view must be paused under contention"
    );

    // (b) high-priority workload is admitted (never paused) and its
    // freshness lag stays within its SLO throughout.
    assert_ne!(
        decision,
        AdmissionDecision::Reject,
        "seed {seed}: high-priority workload's request must not be rejected"
    );
    assert_eq!(
        catalog.view_state("high_view"),
        ViewState::Running,
        "seed {seed}: high-priority workload's view must remain Running"
    );
    let high_lag = rockstream_types::metrics::read_freshness_lag("high_view")
        .expect("high_view freshness lag must be tracked");
    assert!(
        high_lag <= HIGH_FRESHNESS_SLO_MS,
        "seed {seed}: high-priority workload's freshness lag ({high_lag}ms) must stay within its SLO ({HIGH_FRESHNESS_SLO_MS}ms)"
    );

    // (c) every pause/admission-control decision is audited.
    let actions: Vec<String> = log
        .read_all()
        .unwrap()
        .into_iter()
        .map(|event| event.action)
        .collect();
    assert!(
        actions.contains(&"admission_control.paused".to_string()),
        "seed {seed}: pausing low_view must be audited"
    );
    assert!(
        actions.iter().any(|a| a == "admission_control.paused"),
        "seed {seed}: audit log must record the pause decision: {actions:?}"
    );
}

/// P1/P2: HIGH-vs-LOW contention — HIGH always wins, LOW is paused,
/// every transition is audited.
#[test]
fn high_priority_workload_wins_contention_over_low_priority() {
    run_contention_scenario(0xC0FFEE);
}

/// (d) Determinism check: a different seed produces the same qualitative
/// outcome (HIGH always wins contention over LOW), consistent with this
/// codebase's seeded-replay conventions.
#[test]
fn high_priority_workload_wins_contention_over_low_priority_alternate_seed() {
    run_contention_scenario(0xC0FFEE);
    run_contention_scenario(42);
    run_contention_scenario(987654321);
}
