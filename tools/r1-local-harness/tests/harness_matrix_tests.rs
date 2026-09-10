use r1_local_harness::matrix::{
    default_matrix_cells, verify_matrix, ExperimentArea, MatrixCellDefinition, MatrixCellStatus,
    MatrixExecutionResult, MatrixReport,
};
use std::collections::HashSet;

#[test]
fn test_matrix_accounting_covers_all_six_areas() {
    let cells = default_matrix_cells();
    let areas: HashSet<ExperimentArea> = cells.iter().map(|c| c.area).collect();

    assert_eq!(
        areas.len(),
        6,
        "All 6 experiment areas must be present in the matrix"
    );
    assert!(areas.contains(&ExperimentArea::OneKeyUpdates));
    assert!(areas.contains(&ExperimentArea::CompatibleViews));
    assert!(areas.contains(&ExperimentArea::WorkerScaling));
    assert!(areas.contains(&ExperimentArea::ConcurrentLoad));
    assert!(areas.contains(&ExperimentArea::BeyondRamAndCompaction));
    assert!(areas.contains(&ExperimentArea::OverloadAndLoss));
}

#[test]
fn test_runnable_standalone_cells_execute_and_measure() {
    let cells = default_matrix_cells();
    let runnable: Vec<&MatrixCellDefinition> = cells
        .iter()
        .filter(|c| matches!(c.status, MatrixCellStatus::RunnableStandalone))
        .collect();

    assert!(
        runnable.len() >= 8,
        "Must have at least 8 runnable standalone cells across the 6 areas, found {}",
        runnable.len()
    );

    for cell in runnable {
        assert!(
            !cell.cell_id.is_empty(),
            "Runnable cell must have a non-empty ID"
        );
        assert!(
            !cell.description.is_empty(),
            "Runnable cell must have a description"
        );
    }
}

#[test]
fn test_blocked_cells_carry_concrete_blocker_and_owner() {
    let cells = default_matrix_cells();
    let blocked: Vec<&MatrixCellDefinition> = cells
        .iter()
        .filter(|c| matches!(c.status, MatrixCellStatus::Blocked { .. }))
        .collect();

    assert!(
        blocked.len() >= 8,
        "Must have at least 8 blocked cells across areas, found {}",
        blocked.len()
    );

    for cell in blocked {
        if let MatrixCellStatus::Blocked {
            owning_milestone,
            blocker_description,
        } = &cell.status
        {
            assert!(
                !owning_milestone.is_empty(),
                "Blocked cell {} must have an owning milestone",
                cell.cell_id
            );
            assert!(
                owning_milestone.starts_with("v0."),
                "Owning milestone for {} must start with v0., got {}",
                cell.cell_id,
                owning_milestone
            );
            assert!(
                !blocker_description.is_empty(),
                "Blocked cell {} must have a concrete blocker description",
                cell.cell_id
            );
        } else {
            unreachable!();
        }
    }
}

#[test]
fn test_worker_pid_activity_verified() {
    let results = vec![MatrixExecutionResult {
        cell_id: "worker-scaling-1-worker-standalone".to_string(),
        area: ExperimentArea::WorkerScaling,
        status: "MEASURED".to_string(),
        owning_milestone: None,
        blocker_description: None,
        throughput_rows_per_sec: Some(5000.0),
        read_p99_ms: Some(2.1),
        commit_p99_ms: Some(8.5),
        freshness_p99_ms: Some(18.0),
        state_writes_per_change: Some(1.0),
        lookup_cost_ms: Some(0.4),
        worker_pids: vec![12345],
        worker_active: true,
        error: None,
    }];

    let _report = MatrixReport {
        schema_version: 1,
        cells: results.clone(),
        all_areas_covered: false,
        runnable_measured_count: 1,
        blocked_count: 0,
    };

    // An inactive worker must fail verification
    let mut inactive_results = results;
    inactive_results[0].worker_active = false;
    let inactive_report = MatrixReport {
        schema_version: 1,
        cells: inactive_results,
        all_areas_covered: false,
        runnable_measured_count: 1,
        blocked_count: 0,
    };

    assert!(
        verify_matrix(&inactive_report).is_err(),
        "Report with inactive worker must fail verification"
    );
}

#[test]
fn test_one_key_1k_standalone() {
    let cells = default_matrix_cells();
    let cell = cells
        .iter()
        .find(|c| c.cell_id == "one-key-1k-standalone")
        .expect("one-key-1k-standalone cell must exist");
    assert_eq!(cell.area, ExperimentArea::OneKeyUpdates);
    assert!(matches!(cell.status, MatrixCellStatus::RunnableStandalone));
    assert!(cell.lookup_cost_separated);
}

#[test]
fn test_one_key_100k_standalone() {
    let cells = default_matrix_cells();
    let cell = cells
        .iter()
        .find(|c| c.cell_id == "one-key-100k-standalone")
        .expect("one-key-100k-standalone cell must exist");
    assert_eq!(cell.area, ExperimentArea::OneKeyUpdates);
    assert!(matches!(cell.status, MatrixCellStatus::RunnableStandalone));
    assert!(cell.lookup_cost_separated);
}

#[test]
fn test_blocked_cell_accounting_one_key_10m() {
    let cells = default_matrix_cells();
    let cell = cells
        .iter()
        .find(|c| c.cell_id == "one-key-10m-beyond-ram")
        .expect("one-key-10m-beyond-ram cell must exist");
    assert_eq!(cell.area, ExperimentArea::OneKeyUpdates);
    match &cell.status {
        MatrixCellStatus::Blocked {
            owning_milestone,
            blocker_description,
        } => {
            assert_eq!(owning_milestone, "v0.67.1");
            assert!(blocker_description.contains("spillable arrangements"));
        }
        _ => panic!("Expected blocked status"),
    }
}

#[test]
fn test_views_standalone_1_view() {
    let cells = default_matrix_cells();
    let cell = cells
        .iter()
        .find(|c| c.cell_id == "views-1-view-standalone")
        .expect("views-1-view-standalone cell must exist");
    assert_eq!(cell.area, ExperimentArea::CompatibleViews);
    assert!(matches!(cell.status, MatrixCellStatus::RunnableStandalone));
}

#[test]
fn test_views_standalone_20_views() {
    let cells = default_matrix_cells();
    let cell = cells
        .iter()
        .find(|c| c.cell_id == "views-20-views-standalone")
        .expect("views-20-views-standalone cell must exist");
    assert_eq!(cell.area, ExperimentArea::CompatibleViews);
    assert!(matches!(cell.status, MatrixCellStatus::RunnableStandalone));
}

#[test]
fn test_blocked_cell_accounting_shared_views() {
    let cells = default_matrix_cells();
    let cell = cells
        .iter()
        .find(|c| c.cell_id == "views-shared-execution-benefit")
        .expect("views-shared-execution-benefit cell must exist");
    assert_eq!(cell.area, ExperimentArea::CompatibleViews);
    match &cell.status {
        MatrixCellStatus::Blocked {
            owning_milestone,
            blocker_description,
        } => {
            assert_eq!(owning_milestone, "v0.67");
            assert!(blocker_description.contains("shared execution"));
        }
        _ => panic!("Expected blocked status"),
    }
}

#[test]
fn test_worker_scaling_1_worker_standalone() {
    let cells = default_matrix_cells();
    let cell = cells
        .iter()
        .find(|c| c.cell_id == "worker-scaling-1-worker-standalone")
        .expect("worker-scaling-1-worker-standalone cell must exist");
    assert_eq!(cell.area, ExperimentArea::WorkerScaling);
    assert!(matches!(cell.status, MatrixCellStatus::RunnableStandalone));
}

#[test]
fn test_blocked_cell_accounting_2_workers() {
    let cells = default_matrix_cells();
    let cell = cells
        .iter()
        .find(|c| c.cell_id == "worker-scaling-2-workers")
        .expect("worker-scaling-2-workers cell must exist");
    assert_eq!(cell.area, ExperimentArea::WorkerScaling);
    match &cell.status {
        MatrixCellStatus::Blocked {
            owning_milestone,
            blocker_description,
        } => {
            assert_eq!(owning_milestone, "v0.67");
            assert!(blocker_description.contains("distributed data plane"));
        }
        _ => panic!("Expected blocked status"),
    }
}

#[test]
fn test_blocked_cell_accounting_4_workers() {
    let cells = default_matrix_cells();
    let cell = cells
        .iter()
        .find(|c| c.cell_id == "worker-scaling-4-workers")
        .expect("worker-scaling-4-workers cell must exist");
    assert_eq!(cell.area, ExperimentArea::WorkerScaling);
    match &cell.status {
        MatrixCellStatus::Blocked {
            owning_milestone, ..
        } => {
            assert_eq!(owning_milestone, "v0.67");
        }
        _ => panic!("Expected blocked status"),
    }
}

#[test]
fn test_blocked_cell_accounting_8_workers() {
    let cells = default_matrix_cells();
    let cell = cells
        .iter()
        .find(|c| c.cell_id == "worker-scaling-8-workers")
        .expect("worker-scaling-8-workers cell must exist");
    assert_eq!(cell.area, ExperimentArea::WorkerScaling);
    match &cell.status {
        MatrixCellStatus::Blocked {
            owning_milestone, ..
        } => {
            assert_eq!(owning_milestone, "v0.67");
        }
        _ => panic!("Expected blocked status"),
    }
}

#[test]
fn test_concurrent_ingestion_and_queries_standalone() {
    let cells = default_matrix_cells();
    let cell = cells
        .iter()
        .find(|c| c.cell_id == "concurrent-ingestion-and-queries-standalone")
        .expect("concurrent-ingestion-and-queries-standalone cell must exist");
    assert_eq!(cell.area, ExperimentArea::ConcurrentLoad);
    assert!(matches!(cell.status, MatrixCellStatus::RunnableStandalone));
}

#[test]
fn test_compaction_and_restart_standalone() {
    let cells = default_matrix_cells();
    let cell = cells
        .iter()
        .find(|c| c.cell_id == "compaction-and-restart-standalone")
        .expect("compaction-and-restart-standalone cell must exist");
    assert_eq!(cell.area, ExperimentArea::BeyondRamAndCompaction);
    assert!(matches!(cell.status, MatrixCellStatus::RunnableStandalone));
}

#[test]
fn test_blocked_cell_accounting_beyond_ram() {
    let cells = default_matrix_cells();
    let cell = cells
        .iter()
        .find(|c| c.cell_id == "state-beyond-ram-spillable")
        .expect("state-beyond-ram-spillable cell must exist");
    assert_eq!(cell.area, ExperimentArea::BeyondRamAndCompaction);
    match &cell.status {
        MatrixCellStatus::Blocked {
            owning_milestone,
            blocker_description,
        } => {
            assert_eq!(owning_milestone, "v0.67.1");
            assert!(blocker_description.contains("spillable arrangements"));
        }
        _ => panic!("Expected blocked status"),
    }
}

#[test]
fn test_standalone_overload_and_backpressure() {
    let cells = default_matrix_cells();
    let cell = cells
        .iter()
        .find(|c| c.cell_id == "standalone-overload-and-backpressure")
        .expect("standalone-overload-and-backpressure cell must exist");
    assert_eq!(cell.area, ExperimentArea::OverloadAndLoss);
    assert!(matches!(cell.status, MatrixCellStatus::RunnableStandalone));
}

#[test]
fn test_blocked_cell_accounting_worker_loss() {
    let cells = default_matrix_cells();
    let cell = cells
        .iter()
        .find(|c| c.cell_id == "worker-loss-failover")
        .expect("worker-loss-failover cell must exist");
    assert_eq!(cell.area, ExperimentArea::OverloadAndLoss);
    match &cell.status {
        MatrixCellStatus::Blocked {
            owning_milestone,
            blocker_description,
        } => {
            assert_eq!(owning_milestone, "v0.67");
            assert!(blocker_description.contains("distributed worker supervision"));
        }
        _ => panic!("Expected blocked status"),
    }
}

#[test]
fn test_blocked_cell_accounting_migration() {
    let cells = default_matrix_cells();
    let cell = cells
        .iter()
        .find(|c| c.cell_id == "shard-migration")
        .expect("shard-migration cell must exist");
    assert_eq!(cell.area, ExperimentArea::OverloadAndLoss);
    match &cell.status {
        MatrixCellStatus::Blocked {
            owning_milestone,
            blocker_description,
        } => {
            assert_eq!(owning_milestone, "v0.68");
            assert!(blocker_description.contains("migration"));
        }
        _ => panic!("Expected blocked status"),
    }
}
