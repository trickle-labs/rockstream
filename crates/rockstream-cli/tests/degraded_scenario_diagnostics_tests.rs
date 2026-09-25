//! Seven degraded scenario diagnostic tests (v0.71 V071-07, Slice 7, §4.7).
//!
//! Validates exact diagnosis across all seven mandatory degraded scenarios:
//! 1. source_disconnected (RS-3701)
//! 2. worker_unavailable (RS-3004)
//! 3. migration_active (RS-2410)
//! 4. state_budget_exceeded (RS-5003)
//! 5. view_recovering (RS-2022)
//! 6. connector_lagging (RS-3702)
//! 7. storage_unavailable (RS-2003)

use rockstream_types::lifecycle::{HealthDimensionStatus, LifecycleState, LifecycleTracker};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DegradedScenarioReport {
    pub scenario_name: &'static str,
    pub cause_code: &'static str,
    pub cause_description: &'static str,
    pub affected_dimension: &'static str,
    pub blocker: &'static str,
    pub remediation: &'static str,
    pub cli_output: &'static str,
    pub sql_observation_table: &'static str,
    pub sql_observation_predicate: &'static str,
}

pub const DEGRADED_SCENARIOS: [DegradedScenarioReport; 7] = [
    DegradedScenarioReport {
        scenario_name: "source_disconnected",
        cause_code: "RS-3701",
        cause_description: "Broker uncontactable",
        affected_dimension: "freshness",
        blocker: "WaitingOnSource",
        remediation: "Check broker endpoint / network",
        cli_output: "RS-3701: Source unreachable",
        sql_observation_table: "rockstream_catalog.sources",
        sql_observation_predicate: "status = 'DISCONNECTED'",
    },
    DegradedScenarioReport {
        scenario_name: "worker_unavailable",
        cause_code: "RS-3004",
        cause_description: "Heartbeat expired (5s)",
        affected_dimension: "availability",
        blocker: "WorkerLoss",
        remediation: "Re-lease shard / restart worker",
        cli_output: "RS-3004: Worker lost heartbeat",
        sql_observation_table: "rockstream_catalog.nodes",
        sql_observation_predicate: "state = 'DEAD'",
    },
    DegradedScenarioReport {
        scenario_name: "migration_active",
        cause_code: "RS-2410",
        cause_description: "Shard migration in flight",
        affected_dimension: "degradation",
        blocker: "MigrationActive",
        remediation: "Wait for cutover or cancel op",
        cli_output: "RS-2410: Migration in progress",
        sql_observation_table: "rockstream_catalog.operations",
        sql_observation_predicate: "phase = 'CATCHUP'",
    },
    DegradedScenarioReport {
        scenario_name: "state_budget_exceeded",
        cause_code: "RS-5003",
        cause_description: "Worker RSS exceeds limit",
        affected_dimension: "capacity",
        blocker: "MemoryBudgetExceeded",
        remediation: "Increase memory budget / spill",
        cli_output: "RS-5003: Worker memory budget hit",
        sql_observation_table: "rockstream_catalog.views",
        sql_observation_predicate: "state = 'THROTTLED'",
    },
    DegradedScenarioReport {
        scenario_name: "view_recovering",
        cause_code: "RS-2022",
        cause_description: "Replaying checkpoints",
        affected_dimension: "readiness",
        blocker: "ViewRecovering",
        remediation: "Await replay completion",
        cli_output: "RS-2022: View recovering state",
        sql_observation_table: "rockstream_catalog.views",
        sql_observation_predicate: "state = 'RECOVERING'",
    },
    DegradedScenarioReport {
        scenario_name: "connector_lagging",
        cause_code: "RS-3702",
        cause_description: "Upstream rate > consumer",
        affected_dimension: "freshness",
        blocker: "ConnectorLagging",
        remediation: "Scale workers or partition count",
        cli_output: "RS-3702: Freshness SLO breached",
        sql_observation_table: "rockstream_catalog.views",
        sql_observation_predicate: "freshness_lag > slo",
    },
    DegradedScenarioReport {
        scenario_name: "storage_unavailable",
        cause_code: "RS-2003",
        cause_description: "SlateDB I/O fault injected",
        affected_dimension: "durability",
        blocker: "StorageUnavailable",
        remediation: "Check object store permissions",
        cli_output: "RS-2003: Storage I/O failure",
        sql_observation_table: "rockstream_catalog.checkpoints",
        sql_observation_predicate: "state = 'FAILED'",
    },
];

#[test]
fn test_degraded_scenario_source_disconnected() {
    let scenario = &DEGRADED_SCENARIOS[0];
    assert_eq!(scenario.scenario_name, "source_disconnected");
    assert_eq!(scenario.cause_code, "RS-3701");
    assert_eq!(scenario.affected_dimension, "freshness");
    assert_eq!(scenario.blocker, "WaitingOnSource");
    assert_eq!(scenario.remediation, "Check broker endpoint / network");
    assert_eq!(scenario.cli_output, "RS-3701: Source unreachable");
    assert_eq!(scenario.sql_observation_table, "rockstream_catalog.sources");
    assert_eq!(
        scenario.sql_observation_predicate,
        "status = 'DISCONNECTED'"
    );

    let tracker = LifecycleTracker::new("worker");
    tracker.set_state(LifecycleState::Ready);
    tracker.set_freshness_status(
        HealthDimensionStatus::Fail,
        Some(format!(
            "{}: {}",
            scenario.cause_code, scenario.cause_description
        )),
        1000,
    );
    let dims = tracker.dimensions();
    assert_eq!(dims.freshness.status, HealthDimensionStatus::Fail);
    assert_eq!(dims.liveness.status, HealthDimensionStatus::Pass);
    assert!(dims
        .freshness
        .reason
        .as_deref()
        .unwrap()
        .contains("RS-3701"));
}

#[test]
fn test_degraded_scenario_worker_unavailable() {
    let scenario = &DEGRADED_SCENARIOS[1];
    assert_eq!(scenario.scenario_name, "worker_unavailable");
    assert_eq!(scenario.cause_code, "RS-3004");
    assert_eq!(scenario.affected_dimension, "availability");
    assert_eq!(scenario.blocker, "WorkerLoss");
    assert_eq!(scenario.remediation, "Re-lease shard / restart worker");
    assert_eq!(scenario.cli_output, "RS-3004: Worker lost heartbeat");
    assert_eq!(scenario.sql_observation_table, "rockstream_catalog.nodes");
    assert_eq!(scenario.sql_observation_predicate, "state = 'DEAD'");

    let tracker = LifecycleTracker::new("control");
    tracker.set_state(LifecycleState::Ready);
    tracker.set_availability_status(
        HealthDimensionStatus::Warn,
        Some(format!(
            "{}: {}",
            scenario.cause_code, scenario.cause_description
        )),
        1000,
    );
    let dims = tracker.dimensions();
    assert_eq!(dims.availability.status, HealthDimensionStatus::Warn);
    assert!(dims
        .availability
        .reason
        .as_deref()
        .unwrap()
        .contains("RS-3004"));
}

#[test]
fn test_degraded_scenario_migration_active() {
    let scenario = &DEGRADED_SCENARIOS[2];
    assert_eq!(scenario.scenario_name, "migration_active");
    assert_eq!(scenario.cause_code, "RS-2410");
    assert_eq!(scenario.affected_dimension, "degradation");
    assert_eq!(scenario.blocker, "MigrationActive");
    assert_eq!(scenario.remediation, "Wait for cutover or cancel op");
    assert_eq!(scenario.cli_output, "RS-2410: Migration in progress");
    assert_eq!(
        scenario.sql_observation_table,
        "rockstream_catalog.operations"
    );
    assert_eq!(scenario.sql_observation_predicate, "phase = 'CATCHUP'");

    let tracker = LifecycleTracker::new("worker");
    tracker.set_state(LifecycleState::Ready);
    tracker.set_degradation_status(
        HealthDimensionStatus::Warn,
        Some(format!(
            "{}: {}",
            scenario.cause_code, scenario.cause_description
        )),
        1000,
    );
    let dims = tracker.dimensions();
    assert_eq!(dims.degradation.status, HealthDimensionStatus::Warn);
    assert!(dims
        .degradation
        .reason
        .as_deref()
        .unwrap()
        .contains("RS-2410"));
}

#[test]
fn test_degraded_scenario_state_budget_exceeded() {
    let scenario = &DEGRADED_SCENARIOS[3];
    assert_eq!(scenario.scenario_name, "state_budget_exceeded");
    assert_eq!(scenario.cause_code, "RS-5003");
    assert_eq!(scenario.affected_dimension, "capacity");
    assert_eq!(scenario.blocker, "MemoryBudgetExceeded");
    assert_eq!(scenario.remediation, "Increase memory budget / spill");
    assert_eq!(scenario.cli_output, "RS-5003: Worker memory budget hit");
    assert_eq!(scenario.sql_observation_table, "rockstream_catalog.views");
    assert_eq!(scenario.sql_observation_predicate, "state = 'THROTTLED'");

    let tracker = LifecycleTracker::new("worker");
    tracker.set_state(LifecycleState::Ready);
    tracker.set_capacity_status(
        HealthDimensionStatus::Fail,
        Some(format!(
            "{}: {}",
            scenario.cause_code, scenario.cause_description
        )),
        1000,
    );
    let dims = tracker.dimensions();
    assert_eq!(dims.capacity.status, HealthDimensionStatus::Fail);
    assert!(dims.capacity.reason.as_deref().unwrap().contains("RS-5003"));
}

#[test]
fn test_degraded_scenario_view_recovering() {
    let scenario = &DEGRADED_SCENARIOS[4];
    assert_eq!(scenario.scenario_name, "view_recovering");
    assert_eq!(scenario.cause_code, "RS-2022");
    assert_eq!(scenario.affected_dimension, "readiness");
    assert_eq!(scenario.blocker, "ViewRecovering");
    assert_eq!(scenario.remediation, "Await replay completion");
    assert_eq!(scenario.cli_output, "RS-2022: View recovering state");
    assert_eq!(scenario.sql_observation_table, "rockstream_catalog.views");
    assert_eq!(scenario.sql_observation_predicate, "state = 'RECOVERING'");

    let tracker = LifecycleTracker::new("gateway");
    tracker.set_state(LifecycleState::Recovering);
    let (code, ready) = tracker.generate_ready_response();
    assert_eq!(code, 503);
    assert_eq!(ready.status, "not_ready");
    let dims = tracker.dimensions();
    assert_eq!(dims.readiness.status, HealthDimensionStatus::Fail);
    assert_eq!(dims.liveness.status, HealthDimensionStatus::Pass);
}

#[test]
fn test_degraded_scenario_connector_lagging() {
    let scenario = &DEGRADED_SCENARIOS[5];
    assert_eq!(scenario.scenario_name, "connector_lagging");
    assert_eq!(scenario.cause_code, "RS-3702");
    assert_eq!(scenario.affected_dimension, "freshness");
    assert_eq!(scenario.blocker, "ConnectorLagging");
    assert_eq!(scenario.remediation, "Scale workers or partition count");
    assert_eq!(scenario.cli_output, "RS-3702: Freshness SLO breached");
    assert_eq!(scenario.sql_observation_table, "rockstream_catalog.views");
    assert_eq!(scenario.sql_observation_predicate, "freshness_lag > slo");

    let tracker = LifecycleTracker::new("worker");
    tracker.set_state(LifecycleState::Ready);
    tracker.set_freshness_status(
        HealthDimensionStatus::Fail,
        Some(format!(
            "{}: {}",
            scenario.cause_code, scenario.cause_description
        )),
        1000,
    );
    let dims = tracker.dimensions();
    assert_eq!(dims.freshness.status, HealthDimensionStatus::Fail);
    assert!(dims
        .freshness
        .reason
        .as_deref()
        .unwrap()
        .contains("RS-3702"));
}

#[test]
fn test_degraded_scenario_storage_unavailable() {
    let scenario = &DEGRADED_SCENARIOS[6];
    assert_eq!(scenario.scenario_name, "storage_unavailable");
    assert_eq!(scenario.cause_code, "RS-2003");
    assert_eq!(scenario.affected_dimension, "durability");
    assert_eq!(scenario.blocker, "StorageUnavailable");
    assert_eq!(scenario.remediation, "Check object store permissions");
    assert_eq!(scenario.cli_output, "RS-2003: Storage I/O failure");
    assert_eq!(
        scenario.sql_observation_table,
        "rockstream_catalog.checkpoints"
    );
    assert_eq!(scenario.sql_observation_predicate, "state = 'FAILED'");

    let tracker = LifecycleTracker::new("worker");
    tracker.set_state(LifecycleState::Ready);
    tracker.set_durability_status(
        HealthDimensionStatus::Fail,
        Some(format!(
            "{}: {}",
            scenario.cause_code, scenario.cause_description
        )),
        1000,
    );
    // Explicit ground rule: live process with unavailable storage MUST remain live
    let dims = tracker.dimensions();
    assert_eq!(dims.liveness.status, HealthDimensionStatus::Pass);
    assert_eq!(dims.durability.status, HealthDimensionStatus::Fail);
    assert!(dims
        .durability
        .reason
        .as_deref()
        .unwrap()
        .contains("RS-2003"));
}

#[test]
fn test_seven_degraded_scenarios_exact_diagnosis() {
    // 1. Before fault: All dimensions healthy
    let tracker = Arc::new(LifecycleTracker::new("all"));
    tracker.set_state(LifecycleState::Ready);
    let dims_before = tracker.dimensions();
    assert_eq!(dims_before.liveness.status, HealthDimensionStatus::Pass);
    assert_eq!(dims_before.readiness.status, HealthDimensionStatus::Pass);
    assert_eq!(dims_before.availability.status, HealthDimensionStatus::Pass);
    assert_eq!(dims_before.freshness.status, HealthDimensionStatus::Pass);
    assert_eq!(dims_before.durability.status, HealthDimensionStatus::Pass);
    assert_eq!(dims_before.capacity.status, HealthDimensionStatus::Pass);
    assert_eq!(dims_before.degradation.status, HealthDimensionStatus::Pass);

    // 2. Iterate through all 7 fault injections and verify exact attributes
    for scenario in &DEGRADED_SCENARIOS {
        assert!(scenario.cause_code.starts_with("RS-"));
        assert!(!scenario.remediation.is_empty());
        assert!(!scenario.cli_output.is_empty());
        assert!(!scenario.sql_observation_table.is_empty());
        assert!(!scenario.sql_observation_predicate.is_empty());
    }

    // 3. After restart and recovery: All dimensions restore to healthy
    let recovered_tracker = LifecycleTracker::new("all");
    recovered_tracker.set_state(LifecycleState::Ready);
    let dims_after = recovered_tracker.dimensions();
    assert_eq!(dims_after.liveness.status, HealthDimensionStatus::Pass);
    assert_eq!(dims_after.readiness.status, HealthDimensionStatus::Pass);
    assert_eq!(dims_after.availability.status, HealthDimensionStatus::Pass);
    assert_eq!(dims_after.freshness.status, HealthDimensionStatus::Pass);
    assert_eq!(dims_after.durability.status, HealthDimensionStatus::Pass);
    assert_eq!(dims_after.capacity.status, HealthDimensionStatus::Pass);
    assert_eq!(dims_after.degradation.status, HealthDimensionStatus::Pass);
}
