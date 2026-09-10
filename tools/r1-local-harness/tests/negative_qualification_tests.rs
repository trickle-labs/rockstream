use r1_local_harness::evidence::{FreshnessHistogram, ProcessUsage, RawSample};
use r1_local_harness::matrix::{
    default_matrix_cells, verify_matrix, MatrixExecutionResult, MatrixReport,
};
use r1_local_harness::metrics::WorkerActivity;
use r1_local_harness::negative::{qualify_matrix, qualify_sample};
use std::collections::BTreeMap;

fn make_valid_sample() -> RawSample {
    RawSample {
        schema_version: 1,
        run_id: "run-valid-1".to_string(),
        pair_id: "pair-1".to_string(),
        order: "a_then_b".to_string(),
        candidate_id: "current".to_string(),
        binary_sha256: "a".repeat(64),
        profile_sha256: "b".repeat(64),
        corpus_sha256: "c".repeat(64),
        thresholds_sha256: "d".repeat(64),
        workload: "uniform-worker-scaling".to_string(),
        strategy: "auto".to_string(),
        worker_count: 1,
        seed: 42,
        change_stream_sha256: "e".repeat(64),
        monotonic_duration_ns: 1_000_000_000,
        accepted_changes: 100,
        visible_changes: 100,
        freshness_histogram: FreshnessHistogram {
            upper_bounds_ms: vec![1, 5, 10, 50, 100],
            counts: vec![10, 50, 30, 10, 0],
        },
        processes: vec![ProcessUsage {
            role: "worker".to_string(),
            pid: 1234,
            user_cpu_ns: 100_000,
            system_cpu_ns: 20_000,
            rss_bytes: 50_000_000,
        }],
        logical_bytes: 1000,
        lfs_bytes: 2000,
        exchange_bytes: 500,
        max_queue_depth: 10,
        operator_counters: BTreeMap::from([("rows".to_string(), 100)]),
        workers: vec![WorkerActivity {
            worker_id: 1,
            pid: 1234,
            shards_owned: 1,
            input_rows: 100,
            output_rows: 100,
            state_writes: 50,
            exchange_bytes: 500,
        }],
        canonical_input_sha256: "f".repeat(64),
        rockstream_output_sha256: "0".repeat(64),
        sqlite_oracle_output_sha256: "0".repeat(64),
        outputs_equal: true,
        state_writes_per_change: Some(0.5),
        intermediate_rows_per_change: Some(1.0),
        network_bytes_per_change: Some(5.0),
        object_store_requests_per_change: Some(0.1),
        read_p99_ms: Some(2.0),
        commit_p99_ms: Some(8.0),
        freshness_p99_ms: Some(20.0),
        generator_delay_p99_ms: Some(1.5),
        generator_queue_drops: Some(0),
        timeouts_and_errors: Some(0),
        queue_age_ms: Some(0.5),
        control_node_load: Some(0.15),
        physical_flushes_per_epoch: Some(1.0),
    }
}

fn make_valid_matrix_report() -> MatrixReport {
    let defined = default_matrix_cells();
    let cells = defined
        .into_iter()
        .map(|d| match d.status {
            r1_local_harness::matrix::MatrixCellStatus::RunnableStandalone => {
                MatrixExecutionResult {
                    cell_id: d.cell_id,
                    area: d.area,
                    status: "MEASURED".to_string(),
                    owning_milestone: None,
                    blocker_description: None,
                    throughput_rows_per_sec: Some(5000.0),
                    read_p99_ms: Some(2.0),
                    commit_p99_ms: Some(8.0),
                    freshness_p99_ms: Some(20.0),
                    state_writes_per_change: Some(1.0),
                    lookup_cost_ms: if d.lookup_cost_separated {
                        Some(0.5)
                    } else {
                        None
                    },
                    worker_pids: vec![1234],
                    worker_active: true,
                    error: None,
                }
            }
            r1_local_harness::matrix::MatrixCellStatus::Blocked {
                owning_milestone,
                blocker_description,
            } => MatrixExecutionResult {
                cell_id: d.cell_id,
                area: d.area,
                status: "BLOCKED".to_string(),
                owning_milestone: Some(owning_milestone),
                blocker_description: Some(blocker_description),
                throughput_rows_per_sec: None,
                read_p99_ms: None,
                commit_p99_ms: None,
                freshness_p99_ms: None,
                state_writes_per_change: None,
                lookup_cost_ms: None,
                worker_pids: Vec::new(),
                worker_active: false,
                error: None,
            },
        })
        .collect();

    MatrixReport {
        schema_version: 1,
        cells,
        all_areas_covered: true,
        runnable_measured_count: 8,
        blocked_count: 8,
    }
}

#[test]
fn test_missing_required_cell_fails() {
    let mut report = make_valid_matrix_report();
    // Omit required cell: one-key-100k-standalone
    report
        .cells
        .retain(|c| c.cell_id != "one-key-100k-standalone");

    let err = qualify_matrix(&report).unwrap_err();
    assert!(
        err.to_string().contains("missing required matrix cell"),
        "Error must mention missing required matrix cell, got: {err}"
    );
}

#[test]
fn test_inactive_worker_fails_qualification() {
    let mut report = make_valid_matrix_report();
    // Inactive worker in a measured cell
    if let Some(cell) = report.cells.iter_mut().find(|c| c.status == "MEASURED") {
        cell.worker_active = false;
    }

    let err = qualify_matrix(&report).unwrap_err();
    assert!(
        err.to_string().contains("inactive worker process"),
        "Error must mention inactive worker process, got: {err}"
    );
}

#[test]
fn test_synthetic_sample_rejected_in_qualification() {
    let mut sample = make_valid_sample();
    sample.candidate_id = "sample_reference_run".to_string();

    let err = qualify_sample(&sample).unwrap_err();
    assert!(
        err.to_string().contains("synthetic sample disallowed"),
        "Error must reject synthetic sample_reference_run, got: {err}"
    );
}

#[test]
fn test_oracle_mismatch_fails_qualification() {
    let mut sample = make_valid_sample();
    sample.outputs_equal = false;

    let err = qualify_sample(&sample).unwrap_err();
    assert!(
        err.to_string().contains("output differs from SQLite")
            || err.to_string().contains("oracle mismatch"),
        "Error must mention oracle mismatch or differing output, got: {err}"
    );
}

#[test]
fn test_corrupt_binary_digest_fails() {
    let mut sample = make_valid_sample();
    sample.binary_sha256 = "corrupted-hash".to_string();

    let err = qualify_sample(&sample).unwrap_err();
    assert!(
        err.to_string().contains("invalid binary digest"),
        "Error must mention invalid binary digest, got: {err}"
    );
}

#[test]
fn test_unowned_blocked_cell_fails() {
    let mut report = make_valid_matrix_report();
    if let Some(cell) = report.cells.iter_mut().find(|c| c.status == "BLOCKED") {
        cell.owning_milestone = None;
    }

    let err = verify_matrix(&report).unwrap_err();
    assert!(
        err.to_string().contains("missing owning milestone"),
        "Error must mention missing owning milestone, got: {err}"
    );
}
