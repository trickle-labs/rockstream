//! Auto-tuner chaos soak tests (v0.30).
//!
//! ## Proof obligations
//!
//! - **S5**: Oscillation-detection oracle
//! - **S6**: SimRuntime + buggify chaos soak (1 000 seeds)
//! - **S7**: LFS durability — auto-tuner audit survives crash-replay
//! - **S8**: MinIO TC integration test — stability under real S3

use bytes::Bytes;
use rockstream_sim::buggify::{buggify_disable, buggify_init};
use rockstream_sim::{AutoTuner, OscillationDetector, SimObjectStoreHandle, SpikeScenario};
use rockstream_types::audit::AuditEvent;
use rockstream_types::config::AutotunerConfig;

// ─── S5: Oscillation-detection oracle ────────────────────────────────────────

#[test]
fn proof_oscillation_detector_catches_synthetic_oscillation() {
    // [1.0, 3.0, 1.0, 3.0]: up, down, up → 2 reversals in a 4-sample window → true
    let samples = [1.0f64, 3.0, 1.0, 3.0];
    assert!(
        OscillationDetector::detect(&samples),
        "detector must return true for oscillating series {samples:?}"
    );
}

#[test]
fn proof_oscillation_detector_passes_monotone_series() {
    // [1.0, 2.0, 3.0, 4.0]: strictly increasing → 0 reversals → false
    let samples = [1.0f64, 2.0, 3.0, 4.0];
    assert!(
        !OscillationDetector::detect(&samples),
        "detector must return false for monotone series {samples:?}"
    );
}

#[test]
fn proof_parallelism_trace_no_oscillation() {
    let scenario = SpikeScenario::ten_x_spike(5, 10);
    let result = scenario.run();

    let trace: Vec<f64> = result.parallelism_trace.iter().map(|&p| p as f64).collect();
    assert!(
        !OscillationDetector::detect(&trace),
        "parallelism trace must not oscillate after 10× spike; trace: {trace:?}"
    );
}

// ─── S6: SimRuntime + buggify chaos soak (1 000 seeds) ───────────────────────

#[test]
fn proof_auto_tuner_buggify_chaos_1000_seeds_all_settle() {
    for seed in 0u64..1_000 {
        buggify_init(seed);
        let scenario = SpikeScenario::ten_x_spike(5, 10);
        let result = scenario.run();
        buggify_disable();

        let settled = result.epochs_to_settle.unwrap_or(999);
        assert!(
            settled <= 4,
            "seed {seed}: auto-tuner did not settle within 4 epochs under fault injection (got {settled})"
        );
    }
}

// ─── S7: LFS durability — auto-tuner audit survives crash-replay ──────────────

#[test]
fn proof_auto_tuner_audit_survives_crash_replay() {
    let store = SimObjectStoreHandle::new();

    // Run AutoTuner through a 10× spike; events accumulate in audit_sink.
    let pre_crash_count = {
        let mut tuner = AutoTuner::new_with_state(AutotunerConfig::default(), 2_000, 2_000);
        let mut throttle = 1_024u64;
        for epoch in 0..15usize {
            let in_spike = epoch >= 5;
            let spike_mult = if in_spike { 10.0 } else { 1.0 };
            let wr = 0.1 * spike_mult;
            let slo = if wr >= 1.0 { 0.50 } else { 0.99 };
            let p95 = ((500.0 * spike_mult) / tuner.current_parallelism as f64) as u64;
            let lag_ms = (500.0 * spike_mult) as u64;
            if in_spike {
                tuner.adjust_epoch_sizing(wr, slo);
            }
            tuner.adjust_parallelism(p95);
            throttle = tuner.adjust_source_throttle(lag_ms, 500, throttle);
        }

        // Persist audit events to SimObjectStore.
        let events = &tuner.audit_sink;
        assert!(
            !events.is_empty(),
            "audit sink must have events after spike"
        );
        let serialized = serialize_events(events);
        store.put("audit/auto_tuner.jsonl", serialized).unwrap();
        events.len()
    };
    // Tuner and log handle dropped here (simulated crash).

    // Re-open from SimObjectStore.
    let raw = store.get("audit/auto_tuner.jsonl").unwrap();
    let recovered = deserialize_events(&raw);
    assert_eq!(
        recovered.len(),
        pre_crash_count,
        "all {pre_crash_count} audit events must be present after crash-replay; got {}",
        recovered.len()
    );
}

fn serialize_events(events: &[AuditEvent]) -> Bytes {
    let lines: Vec<String> = events
        .iter()
        .map(|e| serde_json::to_string(e).expect("audit event must serialize"))
        .collect();
    Bytes::from(lines.join("\n"))
}

fn deserialize_events(raw: &Bytes) -> Vec<AuditEvent> {
    let text = std::str::from_utf8(raw).expect("audit log must be UTF-8");
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("audit line must deserialize"))
        .collect()
}

// ─── S8: MinIO TC integration test ───────────────────────────────────────────

const MINIO_BUCKET: &str = "rockstream-autotuner-test";

#[tokio::test]
async fn proof_auto_tuner_stability_minio_tc() {
    let (_container, port) = match rockstream_test_support::minio::start_minio(MINIO_BUCKET).await {
        Some(m) => m,
        None => {
            eprintln!("SKIP proof_auto_tuner_stability_minio_tc: Docker not available");
            return;
        }
    };

    use object_store::path::Path;
    use object_store::PutPayload;

    let store = std::sync::Arc::new(rockstream_test_support::minio::minio_object_store(
        port,
        MINIO_BUCKET,
    ));

    // Run scenario and collect audit events.
    let scenario = SpikeScenario::ten_x_spike(5, 10);
    let result = scenario.run();

    let settled = result
        .epochs_to_settle
        .expect("auto-tuner must settle within the spike window");
    assert!(
        settled <= 3,
        "all loops must settle within 3 epochs of the 10× spike; settled at {settled}"
    );

    // Write audit events to MinIO.
    let mut tuner = AutoTuner::new_with_state(AutotunerConfig::default(), 2_000, 2_000);
    let mut throttle = 1_024u64;
    for epoch in 0..15usize {
        let in_spike = epoch >= 5;
        let spike_mult = if in_spike { 10.0 } else { 1.0 };
        let wr = 0.1 * spike_mult;
        let slo = if wr >= 1.0 { 0.50 } else { 0.99 };
        let p95 = ((500.0 * spike_mult) / tuner.current_parallelism as f64) as u64;
        let lag_ms = (500.0 * spike_mult) as u64;
        if in_spike {
            tuner.adjust_epoch_sizing(wr, slo);
        }
        tuner.adjust_parallelism(p95);
        throttle = tuner.adjust_source_throttle(lag_ms, 500, throttle);
    }

    let events = &tuner.audit_sink;
    assert!(!events.is_empty(), "audit sink must have events");
    let pre_write_count = events.len();
    let payload_bytes = serialize_events(events);
    let path = Path::from("audit/auto_tuner_minio.jsonl");
    store
        .put(&path, PutPayload::from_bytes(payload_bytes))
        .await
        .expect("MinIO put must succeed");

    // Read back and verify durability.
    let get_result = store.get(&path).await.expect("MinIO get must succeed");
    let raw = get_result
        .bytes()
        .await
        .expect("MinIO body must be readable");
    let recovered = deserialize_events(&raw);
    assert_eq!(
        recovered.len(),
        pre_write_count,
        "audit log must be durable: expected {pre_write_count} events, got {}",
        recovered.len()
    );
}
