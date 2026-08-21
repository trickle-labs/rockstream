use r1_local_harness::artifact::{append_jsonl, atomic_json};
use r1_local_harness::evidence::{
    FreshnessHistogram, ProcessUsage, RawSample, StructuralEvidence, StructuralResult,
};
use r1_local_harness::metrics::WorkerActivity;
use r1_local_harness::report::{evaluate, verify, Decision, SummaryCell};
use std::collections::BTreeMap;

fn sample(index: usize) -> RawSample {
    RawSample {
        schema_version: 1,
        run_id: format!("run-{index}"),
        pair_id: format!("pair-{index}"),
        order: if index.is_multiple_of(2) {
            "a_then_b"
        } else {
            "b_then_a"
        }
        .to_string(),
        candidate_id: "current".to_string(),
        binary_sha256: "a".repeat(64),
        profile_sha256: "b".repeat(64),
        corpus_sha256: "c".repeat(64),
        thresholds_sha256: "d".repeat(64),
        workload: "uniform-worker-scaling".to_string(),
        strategy: "auto".to_string(),
        worker_count: 1,
        seed: 7,
        change_stream_sha256: "e".repeat(64),
        monotonic_duration_ns: 1_000_000_000,
        accepted_changes: 10,
        visible_changes: 10,
        freshness_histogram: FreshnessHistogram {
            upper_bounds_ms: vec![1, 2],
            counts: vec![9, 1],
        },
        processes: vec![ProcessUsage {
            role: "worker".to_string(),
            pid: 42,
            user_cpu_ns: 1,
            system_cpu_ns: 2,
            rss_bytes: 3,
        }],
        logical_bytes: 4,
        lfs_bytes: 5,
        exchange_bytes: 6,
        max_queue_depth: 0,
        operator_counters: BTreeMap::from([("rows".to_string(), 10)]),
        workers: vec![WorkerActivity {
            worker_id: 9,
            pid: 42,
            shards_owned: 1,
            input_rows: 10,
            output_rows: 10,
            state_writes: 10,
            exchange_bytes: 6,
        }],
        canonical_input_sha256: "f".repeat(64),
        rockstream_output_sha256: "0".repeat(64),
        sqlite_oracle_output_sha256: "0".repeat(64),
        outputs_equal: true,
    }
}

#[test]
fn summary_regenerates_byte_for_byte() {
    let directory = tempfile::tempdir().unwrap();
    let raw = directory.path().join("raw-samples.jsonl");
    for index in 1..=5 {
        append_jsonl(&raw, &sample(index)).unwrap();
    }
    atomic_json(
        &directory.path().join("structural-results.json"),
        &StructuralEvidence {
            schema_version: 1,
            results: vec![StructuralResult {
                name: "proof".to_string(),
                passed: true,
                counters: BTreeMap::from([("exact".to_string(), 1)]),
                log_sha256: "3".repeat(64),
            }],
        },
    )
    .unwrap();
    let expected = Decision {
        schema_version: 1,
        verdict: "INCOMPLETE".to_string(),
        raw_sample_count: 5,
        structural_result_count: 1,
        cells: vec![SummaryCell {
            workload: "uniform-worker-scaling".to_string(),
            candidate_id: "current".to_string(),
            strategy: "auto".to_string(),
            worker_count: 1,
            raw_throughput_rows_per_second: vec![10.0; 5],
            mean_throughput_rows_per_second: 10.0,
            coefficient_of_variation: 0.0,
            max_coefficient_of_variation: 0.15,
            comparator: "<=".to_string(),
            verdict: "GREEN".to_string(),
        }],
    };
    assert_eq!(evaluate(directory.path()).unwrap(), expected);
    atomic_json(&directory.path().join("decision.json"), &expected).unwrap();
    assert_eq!(verify(directory.path()).unwrap(), ());
}
