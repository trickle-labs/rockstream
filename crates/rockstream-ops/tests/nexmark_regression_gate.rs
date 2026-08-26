use std::fs;

use rockstream_ops::nexmark_regression::{
    compare_against_baseline, parse_summary_line, NexmarkBenchmarkSummary,
};

fn baseline() -> NexmarkBenchmarkSummary {
    let baseline_path = format!("{}/benches/baseline/v0.36.json", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_str(&fs::read_to_string(baseline_path).unwrap()).unwrap()
}

#[test]
fn comparator_rejects_regression_over_ten_percent() {
    let baseline = baseline();
    let regressed = parse_summary_line(
        "[nexmark_summary] {\"max_delta_amplification\":15.6,\"propagation_latency_p50_ms\":17.0,\"propagation_latency_p99_ms\":50.0}",
    )
    .unwrap();
    let check = compare_against_baseline(&baseline, &regressed);
    assert!(!check.passed);
    assert!(!check.failures.is_empty());
}

#[test]
fn comparator_accepts_results_within_ten_percent() {
    let baseline = baseline();
    let okay = parse_summary_line(
        "[nexmark_summary] {\"max_delta_amplification\":13.0,\"propagation_latency_p50_ms\":5000.0,\"propagation_latency_p99_ms\":5400.0}",
    )
    .unwrap();
    let check = compare_against_baseline(&baseline, &okay);
    assert!(check.passed, "{:?}", check.failures);
}
