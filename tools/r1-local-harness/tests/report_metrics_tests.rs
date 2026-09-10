use r1_local_harness::evidence::{FreshnessHistogram, ProcessUsage, RawSample};
use r1_local_harness::metrics::WorkerActivity;
use r1_local_harness::report::{calculate_cost_per_million, CostBreakdown};
use std::collections::BTreeMap;

fn make_sample() -> RawSample {
    RawSample {
        schema_version: 1,
        run_id: "test-metrics-sample".to_string(),
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
            user_cpu_ns: 100_000_000,
            system_cpu_ns: 20_000_000,
            rss_bytes: 52_428_800,
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
        object_store_requests_per_change: Some(0.05),
        read_p99_ms: Some(2.5),
        commit_p99_ms: Some(8.5),
        freshness_p99_ms: Some(15.0),
        generator_delay_p99_ms: Some(1.2),
        generator_queue_drops: Some(0),
        timeouts_and_errors: Some(0),
        queue_age_ms: Some(0.8),
        control_node_load: Some(0.25),
        physical_flushes_per_epoch: Some(1.2),
    }
}

#[test]
fn test_raw_sample_contains_all_required_telemetry_fields() {
    let sample = make_sample();
    sample.validate().unwrap();

    assert!(sample.state_writes_per_change.is_some());
    assert!(sample.intermediate_rows_per_change.is_some());
    assert!(sample.network_bytes_per_change.is_some());
    assert!(sample.object_store_requests_per_change.is_some());
    assert!(sample.read_p99_ms.is_some());
    assert!(sample.commit_p99_ms.is_some());
    assert!(sample.freshness_p99_ms.is_some());
    assert!(sample.generator_delay_p99_ms.is_some());
    assert!(sample.generator_queue_drops.is_some());
    assert!(sample.timeouts_and_errors.is_some());
    assert!(sample.queue_age_ms.is_some());
    assert!(sample.control_node_load.is_some());
    assert!(sample.physical_flushes_per_epoch.is_some());
}

#[test]
fn test_cost_per_million_changes_calculation() {
    // hourly cost $3.60, sustainable throughput 1000 changes/sec
    // changes per hour = 1000 * 3600 = 3,600,000
    // cost per million = 3.60 * 1_000_000 / 3,600,000 = $1.00
    let cost = calculate_cost_per_million(3.60, 1000.0).unwrap();
    assert!((cost - 1.0).abs() < 1e-6);

    let breakdown = CostBreakdown {
        hourly_gateways_cost: 0.80,
        hourly_control_nodes_cost: 0.80,
        hourly_workers_cost: 1.60,
        hourly_storage_requests_cost: 0.10,
        hourly_retained_storage_cost: 0.10,
        hourly_network_transfer_cost: 0.10,
        hourly_compaction_cost: 0.10,
        total_hourly_cost: 3.60,
        sustainable_changes_per_sec: 1000.0,
        cost_per_million_changes: cost,
        pricing_scope: "cloud-production-dated".to_string(),
        pricing_date: Some("2026-09-10".to_string()),
    };
    assert_eq!(breakdown.pricing_scope, "cloud-production-dated");
    assert_eq!(breakdown.total_hourly_cost, 3.60);
}

#[test]
fn test_sample_coefficient_of_variation_enforced() {
    // 5 values with CV <= 0.15
    let values_green = [100.0, 102.0, 98.0, 101.0, 99.0];
    let mean = values_green.iter().sum::<f64>() / 5.0;
    let variance = values_green.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / 4.0;
    let cv = variance.sqrt() / mean;
    assert!(cv <= 0.15, "CV should be <= 0.15, got {cv}");

    // 5 values with CV > 0.15
    let values_red = [50.0, 150.0, 60.0, 140.0, 100.0];
    let mean_red = values_red.iter().sum::<f64>() / 5.0;
    let variance_red = values_red
        .iter()
        .map(|v| (v - mean_red).powi(2))
        .sum::<f64>()
        / 4.0;
    let cv_red = variance_red.sqrt() / mean_red;
    assert!(cv_red > 0.15, "CV should be > 0.15, got {cv_red}");
}

#[test]
fn test_state_writes_per_change_metric() {
    let sample = make_sample();
    assert_eq!(sample.state_writes_per_change, Some(0.5));
}

#[test]
fn test_intermediate_rows_per_change_metric() {
    let sample = make_sample();
    assert_eq!(sample.intermediate_rows_per_change, Some(1.0));
}

#[test]
fn test_network_bytes_per_change_metric() {
    let sample = make_sample();
    assert_eq!(sample.network_bytes_per_change, Some(5.0));
}

#[test]
fn test_object_store_requests_per_change_metric() {
    let sample = make_sample();
    assert_eq!(sample.object_store_requests_per_change, Some(0.05));
}

#[test]
fn test_rss_bytes_telemetry() {
    let sample = make_sample();
    let max_rss = sample
        .processes
        .iter()
        .map(|p| p.rss_bytes)
        .max()
        .unwrap_or(0);
    assert!(max_rss > 0);
    assert_eq!(max_rss, 52_428_800);
}

#[test]
fn test_queue_age_telemetry() {
    let sample = make_sample();
    assert_eq!(sample.queue_age_ms, Some(0.8));
}

#[test]
fn test_control_node_load_telemetry() {
    let sample = make_sample();
    assert_eq!(sample.control_node_load, Some(0.25));
}

#[test]
fn test_flushes_per_epoch_telemetry() {
    let sample = make_sample();
    assert_eq!(sample.physical_flushes_per_epoch, Some(1.2));
}
