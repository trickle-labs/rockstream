//! Fabricated samples exercise validation only; they are not timing evidence.
use r1_local_harness::evidence::RawSample;
use r1_local_harness::negative::qualify_sample;
use serde_json::json;

type FloatSetter = fn(&mut RawSample, Option<f64>);

const METRICS: [(&str, FloatSetter); 11] = [
    ("state_writes_per_change", |s, v| s.state_writes_per_change = v),
    ("intermediate_rows_per_change", |s, v| s.intermediate_rows_per_change = v),
    ("network_bytes_per_change", |s, v| s.network_bytes_per_change = v),
    ("object_store_requests_per_change", |s, v| s.object_store_requests_per_change = v),
    ("read_p99_ms", |s, v| s.read_p99_ms = v),
    ("commit_p99_ms", |s, v| s.commit_p99_ms = v),
    ("freshness_p99_ms", |s, v| s.freshness_p99_ms = v),
    ("generator_delay_p99_ms", |s, v| s.generator_delay_p99_ms = v),
    ("queue_age_ms", |s, v| s.queue_age_ms = v),
    ("control_node_load", |s, v| s.control_node_load = v),
    ("physical_flushes_per_epoch", |s, v| s.physical_flushes_per_epoch = v),
];

fn fixture() -> RawSample {
    serde_json::from_value(json!({
        "schema_version": 1,
        "run_id": "validator-fixture",
        "pair_id": "pair-1",
        "order": "a_then_b",
        "candidate_id": "current",
        "binary_sha256": "a".repeat(64),
        "profile_sha256": "b".repeat(64),
        "corpus_sha256": "c".repeat(64),
        "thresholds_sha256": "d".repeat(64),
        "workload": "uniform-worker-scaling",
        "strategy": "auto",
        "worker_count": 1,
        "seed": 42,
        "change_stream_sha256": "e".repeat(64),
        "monotonic_duration_ns": 1_000_000_000u64,
        "accepted_changes": 100,
        "visible_changes": 100,
        "freshness_histogram": {"upper_bounds_ms": [1, 10, 100], "counts": [0, 0, 100]},
        "processes": [{
            "role": "worker", "pid": 1234, "user_cpu_ns": 100_000,
            "system_cpu_ns": 20_000, "rss_bytes": 50_000_000
        }],
        "logical_bytes": 1000,
        "lfs_bytes": 2000,
        "exchange_bytes": 500,
        "max_queue_depth": 1,
        "operator_counters": {"rows": 100},
        "workers": [{
            "worker_id": 1, "pid": 1234, "shards_owned": 1,
            "input_rows": 100, "output_rows": 100, "state_writes": 50,
            "exchange_bytes": 500
        }],
        "canonical_input_sha256": "f".repeat(64),
        "rockstream_output_sha256": "0".repeat(64),
        "sqlite_oracle_output_sha256": "0".repeat(64),
        "outputs_equal": true,
        "state_writes_per_change": 0.5,
        "intermediate_rows_per_change": 1.0,
        "network_bytes_per_change": 5.0,
        "object_store_requests_per_change": 0.1,
        "read_p99_ms": 2.0,
        "commit_p99_ms": 8.0,
        "freshness_p99_ms": 20.0,
        "generator_delay_p99_ms": 1.0,
        "generator_queue_drops": 0,
        "timeouts_and_errors": 0,
        "queue_age_ms": 0.5,
        "control_node_load": 0.1,
        "physical_flushes_per_epoch": 1.0
    }))
    .unwrap()
}

#[test]
fn v072_complete_fixture_is_structurally_valid() {
    qualify_sample(&fixture()).unwrap();
}

#[test]
fn v072_each_qualification_metric_is_required() {
    for (name, set) in METRICS {
        let mut sample = fixture();
        set(&mut sample, None);
        assert_eq!(
            qualify_sample(&sample).unwrap_err().to_string(),
            format!("raw sample validator-fixture missing {name}")
        );
    }
}

#[test]
fn v072_each_qualification_metric_must_be_finite_and_nonnegative() {
    for (name, set) in METRICS {
        for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0] {
            let mut sample = fixture();
            set(&mut sample, Some(invalid));
            assert_eq!(
                qualify_sample(&sample).unwrap_err().to_string(),
                format!("raw sample validator-fixture invalid {name}")
            );
        }
    }
}

#[test]
fn v072_drops_and_errors_cannot_be_absent_or_nonzero() {
    for name in ["generator_queue_drops", "timeouts_and_errors"] {
        for value in [None, Some(1), Some(u64::MAX)] {
            let mut sample = fixture();
            match name {
                "generator_queue_drops" => sample.generator_queue_drops = value,
                "timeouts_and_errors" => sample.timeouts_and_errors = value,
                _ => unreachable!(),
            }
            let expected = match value {
                None => format!("raw sample validator-fixture missing {name}"),
                Some(value) => {
                    format!("raw sample validator-fixture has nonzero {name}: {value}")
                }
            };
            assert_eq!(qualify_sample(&sample).unwrap_err().to_string(), expected);
        }
    }
}
