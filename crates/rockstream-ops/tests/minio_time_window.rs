//! MinIO (S3) backend integration tests for `TumbleWindowOp` (v0.12 — IVM-8).
//!
//! Tests skip gracefully if Docker is unavailable.
//!
//! 1. `minio_tumble_window_late_data_and_ttl` — late rows dropped; partial state not
//!    evicted on MinIO backend until both TTL and frontier gate are satisfied.

use std::sync::Arc;

use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use rockstream_ops::time_window::{
    load_tumble_window_state, persist_tumble_window_state, CompactionFilter, TumbleWindowOp,
};
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::{keys::ShardKeyEncoder, ShardDb};
use rockstream_types::ids::OperatorId;
use sha2::{Digest, Sha256};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-test-tw";

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .args(["info"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn sha256_hex(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
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
    let dpm: [u32; 12] =
        [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut month = 0u32;
    for &d in &dpm {
        if days < d {
            break;
        }
        days -= d;
        month += 1;
    }
    (year, month + 1, days + 1, h, m, s)
}

async fn create_minio_bucket(port: u16, bucket: &str) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let (y, mo, d, hh, mm, ss) = epoch_to_ymd_hms(secs);
    let date = format!("{y:04}{mo:02}{d:02}");
    let datetime = format!("{y:04}{mo:02}{d:02}T{hh:02}{mm:02}{ss:02}Z");
    let host = format!("127.0.0.1:{port}");
    let region = "us-east-1";
    let empty_hash = sha256_hex(b"");
    let canonical = format!(
        "PUT\n/{bucket}\n\nhost:{host}\nx-amz-content-sha256:{empty_hash}\nx-amz-date:{datetime}\n\nhost;x-amz-content-sha256;x-amz-date\n{empty_hash}"
    );
    let canonical_hash = sha256_hex(canonical.as_bytes());
    let scope = format!("{date}/{region}/s3/aws4_request");
    let sts = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{canonical_hash}");
    let k1 = hmac_sha256(format!("AWS4{MINIO_PASS}").as_bytes(), date.as_bytes());
    let k2 = hmac_sha256(&k1, region.as_bytes());
    let k3 = hmac_sha256(&k2, b"s3");
    let signing_key = hmac_sha256(&k3, b"aws4_request");
    let sig = hex::encode(hmac_sha256(&signing_key, sts.as_bytes()));
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
        .expect("CreateBucket PUT request failed");
    let status = resp.status();
    assert!(
        status.is_success() || status.as_u16() == 409,
        "CreateBucket failed: {status}"
    );
}

async fn start_minio() -> (testcontainers::ContainerAsync<MinIO>, u16) {
    let container = MinIO::default()
        .start()
        .await
        .expect("failed to start MinIO container; is Docker running?");
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_BUCKET).await;
    (container, port)
}

fn minio_object_store(port: u16) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://127.0.0.1:{port}"))
            .with_bucket_name(MINIO_BUCKET)
            .with_access_key_id(MINIO_USER)
            .with_secret_access_key(MINIO_PASS)
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()
            .expect("failed to build S3 object store for MinIO"),
    )
}

async fn open_shard_minio(port: u16, path: &str) -> Arc<ShardDb> {
    let store = minio_object_store(port);
    Arc::new(ShardDb::builder(path, store).build().await.expect("failed to open ShardDb on MinIO"))
}

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("t", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn make_input(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let t: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let v: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let w: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let data = RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(Int64Array::from(t)) as ArrayRef,
            Arc::new(Int64Array::from(v)) as ArrayRef,
        ],
    )
    .unwrap();
    ArrowZSet::new(data, w)
}

// ─── Test 1: Late data + TTL on MinIO backend ─────────────────────────────

#[tokio::test]
async fn minio_tumble_window_late_data_and_ttl() {
    if !docker_available() {
        eprintln!("Docker not available — skipping minio_tumble_window_late_data_and_ttl");
        return;
    }

    let (_container, port) = start_minio().await;
    let db = open_shard_minio(port, "tw-test").await;
    let op_id = OperatorId(30);
    let window_size_ms = 1000i64;

    use rockstream_plan::LateDataPolicy;
    let op = TumbleWindowOp::new(input_schema(), 0, window_size_ms, LateDataPolicy::Drop);

    // Epoch 1: rows in window [0, 1000).
    let out1 = op.process_epoch(make_input(&[(100, 10, 1), (500, 20, 1)]), 1).unwrap();
    assert!(!out1.is_empty(), "epoch 1 must produce output");

    // Epoch 2: advance watermark past window end (t=5000 > window_end=1000).
    let _out2 = op.process_epoch(make_input(&[(5000, 99, 1)]), 2).unwrap();
    assert!(op.watermark_ms() >= 5000, "watermark must be >= 5000 after epoch 2");

    // Persist to MinIO.
    persist_tumble_window_state(&db, &op, op_id).await.unwrap();

    // Epoch 3: late row for window [0, 1000) — t=50 < watermark=5000 → dropped.
    let out3 = op.process_epoch(make_input(&[(50, 77, 1)]), 3).unwrap();
    assert!(out3.is_empty(), "late row must be dropped, got {} rows", out3.num_rows());

    // Reload from MinIO and verify state.
    let op2 = load_tumble_window_state(
        &db,
        input_schema(),
        0,
        window_size_ms,
        LateDataPolicy::Drop,
        op_id,
    )
    .await
    .unwrap();

    assert_eq!(op2.fill_level(), op.fill_level(), "fill level matches after reload");

    // Verify compaction filter refuses early deletion of window [0, 1000) state
    // when frontier has NOT advanced past window_end.
    let sample_key = ShardKeyEncoder::tumble_window_key(op_id.0, 0i64, b"gk");
    let filter = CompactionFilter {
        watermark_ms: 5000,
        window_size_ms,
        allowed_lateness_ms: 0,
        frontier_ms: 500, // NOT past window_end=1000
    };
    assert!(
        !filter.may_delete(&sample_key),
        "must not evict window state when frontier < window_end"
    );
}
