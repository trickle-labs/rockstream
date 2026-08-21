use r1_local_harness::evidence::{FreshnessHistogram, ProcessUsage, RawSample};
use r1_local_harness::metrics::WorkerActivity;
use std::collections::BTreeMap;

fn sample() -> RawSample {
    RawSample {
        schema_version: 1,
        run_id: "run-1".to_string(),
        pair_id: "pair-1".to_string(),
        order: "a_then_b".to_string(),
        candidate_id: "current".to_string(),
        binary_sha256: "a".repeat(64),
        profile_sha256: "b".repeat(64),
        corpus_sha256: "c".repeat(64),
        thresholds_sha256: "d".repeat(64),
        workload: "ordinary-aggregate".to_string(),
        strategy: "auto".to_string(),
        worker_count: 1,
        seed: 7,
        change_stream_sha256: "e".repeat(64),
        monotonic_duration_ns: 1,
        accepted_changes: 1,
        visible_changes: 1,
        freshness_histogram: FreshnessHistogram {
            upper_bounds_ms: vec![1],
            counts: vec![1],
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
        operator_counters: BTreeMap::from([("rows".to_string(), 1)]),
        workers: vec![WorkerActivity {
            worker_id: 9,
            pid: 42,
            shards_owned: 1,
            input_rows: 1,
            output_rows: 1,
            state_writes: 1,
            exchange_bytes: 6,
        }],
        canonical_input_sha256: "f".repeat(64),
        rockstream_output_sha256: "0".repeat(64),
        sqlite_oracle_output_sha256: "0".repeat(64),
        outputs_equal: true,
    }
}

#[test]
fn rejects_exact_evidence_mutations() {
    let mut changed = sample();
    changed.outputs_equal = false;
    assert_eq!(
        changed.validate().unwrap_err().to_string(),
        "raw sample run-1 output differs from SQLite"
    );

    let mut missing_worker_work = sample();
    missing_worker_work.workers[0].output_rows = 0;
    assert_eq!(
        missing_worker_work.validate().unwrap_err().to_string(),
        "raw sample run-1 has an idle worker 9"
    );

    let mut missing_digest = sample();
    missing_digest.canonical_input_sha256.clear();
    assert_eq!(
        missing_digest.validate().unwrap_err().to_string(),
        "raw sample run-1 has invalid canonical input digest"
    );
}
