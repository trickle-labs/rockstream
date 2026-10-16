//! Deliberately fabricated validation fixtures, never benchmark evidence.
use r1_local_harness::matrix::{
    default_matrix_cells, verify_matrix, verify_v072_matrix, ExperimentArea, MatrixCellStatus,
    MatrixExecutionResult, MatrixReport,
};

fn fixture(complete: bool) -> MatrixReport {
    let cells: Vec<_> = default_matrix_cells()
        .into_iter()
        .map(|definition| {
            let mut cell = MatrixExecutionResult {
                cell_id: definition.cell_id,
                area: definition.area,
                status: "MEASURED".to_owned(),
                owning_milestone: None,
                blocker_description: None,
                throughput_rows_per_sec: Some(5000.0),
                read_p99_ms: Some(2.0),
                commit_p99_ms: Some(8.0),
                freshness_p99_ms: Some(20.0),
                state_writes_per_change: Some(1.0),
                lookup_cost_ms: Some(0.5),
                worker_pids: vec![1234],
                worker_active: true,
                error: None,
            };
            let workers = match cell.cell_id.as_str() {
                "worker-scaling-2-workers" => 2,
                "worker-scaling-4-workers" => 4,
                "worker-scaling-8-workers" => 8,
                _ => 1,
            };
            cell.worker_pids = (1234..1234 + workers).collect();
            if !complete {
                if let MatrixCellStatus::Blocked {
                    owning_milestone,
                    blocker_description,
                } = definition.status
                {
                    cell.status = "BLOCKED".to_owned();
                    cell.owning_milestone = Some(owning_milestone);
                    cell.blocker_description = Some(blocker_description);
                    cell.throughput_rows_per_sec = None;
                    cell.read_p99_ms = None;
                    cell.commit_p99_ms = None;
                    cell.freshness_p99_ms = None;
                    cell.state_writes_per_change = None;
                    cell.lookup_cost_ms = None;
                    cell.worker_pids.clear();
                    cell.worker_active = false;
                }
            }
            cell
        })
        .collect();
    MatrixReport {
        schema_version: 1,
        runnable_measured_count: cells
            .iter()
            .filter(|cell| cell.status == "MEASURED")
            .count(),
        blocked_count: cells.iter().filter(|cell| cell.status == "BLOCKED").count(),
        cells,
        all_areas_covered: true,
    }
}

fn error(report: &MatrixReport) -> String {
    verify_matrix(report).unwrap_err().to_string()
}

#[test]
fn v072_historical_blockers_are_not_capacity_exemptions() {
    let historical = fixture(false);
    verify_matrix(&historical).unwrap();
    assert_eq!(
        verify_v072_matrix(&historical).unwrap_err().to_string(),
        "v0.72 applicable cell one-key-10m-beyond-ram is not measured"
    );
    verify_v072_matrix(&fixture(true)).unwrap();
}

#[test]
fn v072_matrix_inventory_is_exact() {
    let observed: Vec<_> = default_matrix_cells()
        .into_iter()
        .map(|cell| (cell.cell_id, cell.area))
        .collect();
    let expected = vec![
        ("one-key-1k-standalone", ExperimentArea::OneKeyUpdates),
        ("one-key-100k-standalone", ExperimentArea::OneKeyUpdates),
        ("one-key-10m-beyond-ram", ExperimentArea::OneKeyUpdates),
        ("views-1-view-standalone", ExperimentArea::CompatibleViews),
        ("views-20-views-standalone", ExperimentArea::CompatibleViews),
        (
            "views-shared-execution-benefit",
            ExperimentArea::CompatibleViews,
        ),
        (
            "worker-scaling-1-worker-standalone",
            ExperimentArea::WorkerScaling,
        ),
        ("worker-scaling-2-workers", ExperimentArea::WorkerScaling),
        ("worker-scaling-4-workers", ExperimentArea::WorkerScaling),
        ("worker-scaling-8-workers", ExperimentArea::WorkerScaling),
        (
            "concurrent-ingestion-and-queries-standalone",
            ExperimentArea::ConcurrentLoad,
        ),
        (
            "compaction-and-restart-standalone",
            ExperimentArea::BeyondRamAndCompaction,
        ),
        (
            "state-beyond-ram-spillable",
            ExperimentArea::BeyondRamAndCompaction,
        ),
        (
            "standalone-overload-and-backpressure",
            ExperimentArea::OverloadAndLoss,
        ),
        ("worker-loss-failover", ExperimentArea::OverloadAndLoss),
        ("shard-migration", ExperimentArea::OverloadAndLoss),
    ]
    .into_iter()
    .map(|(id, area)| (id.to_owned(), area))
    .collect::<Vec<_>>();
    assert_eq!(observed, expected);
}

#[test]
fn v072_every_cell_is_required_exactly_once() {
    let complete = fixture(true);
    for index in 0..complete.cells.len() {
        let mut missing = complete.clone();
        let removed = missing.cells.remove(index);
        assert_eq!(
            error(&missing),
            format!("missing required matrix cell: {}", removed.cell_id)
        );
        let mut duplicate = complete.clone();
        duplicate.cells.push(complete.cells[index].clone());
        assert_eq!(
            error(&duplicate),
            format!("duplicate matrix cell: {}", complete.cells[index].cell_id)
        );
    }
    let mut unknown = complete;
    unknown.cells[0].cell_id = "invented-cell".to_owned();
    assert_eq!(error(&unknown), "unknown matrix cell: invented-cell");
}

#[test]
fn v072_unknown_statuses_and_false_area_are_rejected() {
    for status in ["", "PENDING", "FAILED", "NOT_APPLICABLE", "measured"] {
        let mut report = fixture(true);
        report.cells[0].status = status.to_owned();
        assert_eq!(
            error(&report),
            format!("unsupported matrix cell status {status:?} for one-key-1k-standalone")
        );
    }
    let mut report = fixture(true);
    report.cells[0].area = ExperimentArea::WorkerScaling;
    assert_eq!(
        error(&report),
        "wrong experiment area for cell one-key-1k-standalone"
    );
}

#[test]
fn v072_missing_metrics_cannot_skip_validation() {
    let mutations: [(&str, fn(&mut MatrixExecutionResult)); 6] = [
        ("throughput_rows_per_sec", |cell| {
            cell.throughput_rows_per_sec = None
        }),
        ("read_p99_ms", |cell| cell.read_p99_ms = None),
        ("commit_p99_ms", |cell| cell.commit_p99_ms = None),
        ("freshness_p99_ms", |cell| cell.freshness_p99_ms = None),
        ("state_writes_per_change", |cell| {
            cell.state_writes_per_change = None
        }),
        ("lookup_cost_ms", |cell| cell.lookup_cost_ms = None),
    ];
    for (name, mutate) in mutations {
        let mut report = fixture(true);
        mutate(&mut report.cells[0]);
        assert_eq!(
            error(&report),
            format!("missing {name} in cell one-key-1k-standalone")
        );
    }
}

#[test]
fn v072_nonfinite_negative_and_zero_denominators_are_rejected() {
    for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0, 0.0] {
        let mut report = fixture(true);
        report.cells[0].throughput_rows_per_sec = Some(invalid);
        assert_eq!(
            error(&report),
            "invalid throughput_rows_per_sec in cell one-key-1k-standalone"
        );
        report = fixture(true);
        report.cells[0].state_writes_per_change = Some(invalid);
        assert_eq!(
            error(&report),
            "invalid state_writes_per_change in cell one-key-1k-standalone"
        );
    }
    for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0] {
        let mut report = fixture(true);
        report.cells[0].read_p99_ms = Some(invalid);
        assert_eq!(
            error(&report),
            "invalid read_p99_ms in cell one-key-1k-standalone"
        );
    }
}

#[test]
fn v072_worker_identities_must_be_distinct_and_match_scaling_case() {
    for pids in [vec![0], vec![1, 1]] {
        let mut report = fixture(true);
        report.cells[0].worker_pids = pids;
        assert_eq!(
            error(&report),
            "zero or duplicate worker PID in cell one-key-1k-standalone"
        );
    }
    let mut report = fixture(true);
    report.cells[9].worker_pids = vec![1234];
    assert_eq!(
        error(&report),
        "cell worker-scaling-8-workers requires 8 distinct worker PIDs"
    );
}

#[test]
fn v072_summary_flags_and_counts_are_recomputed() {
    let mut report = fixture(true);
    report.all_areas_covered = false;
    assert_eq!(
        error(&report),
        "matrix report all_areas_covered does not match its cells"
    );
    report = fixture(true);
    report.runnable_measured_count -= 1;
    assert_eq!(
        error(&report),
        "matrix report counts do not match its cells"
    );
    report = fixture(true);
    report.blocked_count = 1;
    assert_eq!(
        error(&report),
        "matrix report counts do not match its cells"
    );
}

#[test]
fn v072_measured_errors_and_blocked_measurements_are_rejected() {
    let mut report = fixture(true);
    report.cells[0].error = Some("flush failed".to_owned());
    assert_eq!(
        error(&report),
        "measured cell one-key-1k-standalone carries an error or blocker"
    );
    report = fixture(false);
    report.cells[2].read_p99_ms = Some(1.0);
    assert_eq!(
        error(&report),
        "blocked cell one-key-10m-beyond-ram must not claim measured evidence"
    );
}

#[test]
fn v072_ten_million_groups_participates_in_write_comparison() {
    let mut report = fixture(true);
    report.cells[2].state_writes_per_change = Some(2.0);
    assert_eq!(
        error(&report),
        "one-key write amplification ratio exceeds frozen threshold 1.10"
    );
    report.cells[2].state_writes_per_change = Some(1.10);
    verify_v072_matrix(&report).unwrap();
}
