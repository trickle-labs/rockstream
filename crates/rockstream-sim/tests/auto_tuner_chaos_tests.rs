//! Auto-tuner chaos soak tests (v0.30).
//!
//! ## Proof obligations
//!
//! - **S5**: Oscillation-detection oracle
//! - **S6**: SimRuntime + buggify chaos soak (1 000 seeds)
//! - **S7**: LFS durability — auto-tuner audit survives crash-replay
//! - **S8**: MinIO TC integration test — stability under real S3

use std::borrow::Cow;
use std::collections::HashMap;

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

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-autotuner-test";

fn docker_available() -> bool {
    rockstream_test_support::docker_available()
}

#[derive(Debug, Clone)]
struct MinIO2024 {
    env_vars: HashMap<String, String>,
}

impl Default for MinIO2024 {
    fn default() -> Self {
        let mut env_vars = HashMap::new();
        env_vars.insert("MINIO_CONSOLE_ADDRESS".to_owned(), ":9001".to_owned());
        Self { env_vars }
    }
}

impl testcontainers::Image for MinIO2024 {
    fn name(&self) -> &str {
        "minio/minio"
    }

    fn tag(&self) -> &str {
        "RELEASE.2024-11-07T00-52-20Z"
    }

    fn ready_conditions(&self) -> Vec<testcontainers::core::WaitFor> {
        vec![testcontainers::core::WaitFor::message_on_stderr("API:")]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        &self.env_vars
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        vec!["server", "/data"]
    }
}

async fn start_minio() -> (testcontainers::ContainerAsync<MinIO2024>, u16) {
    use testcontainers::runners::AsyncRunner;
    let container = MinIO2024::default()
        .start()
        .await
        .expect("failed to start MinIO container");
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_BUCKET).await;
    (container, port)
}

fn minio_object_store(port: u16) -> std::sync::Arc<dyn object_store::ObjectStore> {
    use object_store::aws::AmazonS3Builder;
    std::sync::Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://127.0.0.1:{port}"))
            .with_bucket_name(MINIO_BUCKET)
            .with_access_key_id(MINIO_USER)
            .with_secret_access_key(MINIO_PASS)
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .build()
            .expect("failed to build MinIO object store"),
    )
}

async fn create_minio_bucket(port: u16, bucket: &str) {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    type HmacSha256 = Hmac<Sha256>;

    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let (y, mo, d, hh, mm, ss) = epoch_to_ymd_hms(secs);
    let date = format!("{y:04}{mo:02}{d:02}");
    let datetime = format!("{y:04}{mo:02}{d:02}T{hh:02}{mm:02}{ss:02}Z");
    let host = format!("127.0.0.1:{port}");
    let region = "us-east-1";
    let empty_hash = format!("{:x}", Sha256::digest(b""));
    let canonical = format!(
        "PUT\n/{bucket}\n\nhost:{host}\nx-amz-content-sha256:{empty_hash}\nx-amz-date:{datetime}\n\nhost;x-amz-content-sha256;x-amz-date\n{empty_hash}"
    );
    let canonical_hash = format!("{:x}", Sha256::digest(canonical.as_bytes()));
    let scope = format!("{date}/{region}/s3/aws4_request");
    let sts = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{canonical_hash}");
    let k1 = {
        let mut mac = HmacSha256::new_from_slice(format!("AWS4{MINIO_PASS}").as_bytes()).unwrap();
        mac.update(date.as_bytes());
        mac.finalize().into_bytes().to_vec()
    };
    let k2 = {
        let mut mac = HmacSha256::new_from_slice(&k1).unwrap();
        mac.update(region.as_bytes());
        mac.finalize().into_bytes().to_vec()
    };
    let k3 = {
        let mut mac = HmacSha256::new_from_slice(&k2).unwrap();
        mac.update(b"s3");
        mac.finalize().into_bytes().to_vec()
    };
    let signing_key = {
        let mut mac = HmacSha256::new_from_slice(&k3).unwrap();
        mac.update(b"aws4_request");
        mac.finalize().into_bytes().to_vec()
    };
    let sig = {
        let mut mac = HmacSha256::new_from_slice(&signing_key).unwrap();
        mac.update(sts.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    };
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={MINIO_USER}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}"
    );
    let resp = reqwest::Client::new()
        .put(format!("http://{host}/{bucket}"))
        .header("Host", &host)
        .header("X-Amz-Content-Sha256", &empty_hash)
        .header("X-Amz-Date", &datetime)
        .header("Authorization", &auth)
        .header("Content-Length", "0")
        .send()
        .await
        .expect("CreateBucket PUT failed");
    let status = resp.status();
    assert!(
        status.is_success() || status.as_u16() == 409,
        "CreateBucket failed: {status}"
    );
}

fn epoch_to_ymd_hms(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let sod = secs % 86400;
    let mut days = (secs / 86400) as u32;
    let h = (sod / 3600) as u32;
    let m = ((sod % 3600) / 60) as u32;
    let s = (sod % 60) as u32;
    let mut year = 1970u32;
    loop {
        let leap =
            year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
        let dy = if leap { 366 } else { 365 };
        if days < dy {
            break;
        }
        days -= dy;
        year += 1;
    }
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let dpm: [u32; 12] = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 0u32;
    for &d in &dpm {
        if days < d {
            break;
        }
        days -= d;
        month += 1;
    }
    let day = days + 1;
    month += 1;
    (year, month, day, h, m, s)
}

#[tokio::test]
async fn proof_auto_tuner_stability_minio_tc() {
    if !docker_available() {
        eprintln!("SKIP proof_auto_tuner_stability_minio_tc: Docker not available");
        return;
    }

    use object_store::path::Path;
    use object_store::PutPayload;

    let (_container, port) = start_minio().await;
    let store = minio_object_store(port);

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
