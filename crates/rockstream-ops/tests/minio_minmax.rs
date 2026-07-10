//! MinIO (S3) backend integration tests for v0.6 MinMax operator.
//!
//! Tests:
//! 1. `minio_minmax_writes_and_persists` — MinMaxOp state survives ShardDb
//!    close/reopen on the MinIO (S3-compatible) backend.
//! 2. `minio_minmax_crash_replay_bit_identical` — simulated crash before epoch
//!    commit; on restart the shard replays from its persisted frontier to
//!    **bit-identical** output on the S3 backend.
//!
//! Docker must be running. Tests skip gracefully if Docker is unavailable.

use std::sync::Arc;

use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use rockstream_ops::aggregate::{load_frontier, persist_frontier};
use rockstream_ops::minmax::{persist_minmax_state, MinMaxKind, MinMaxOp};
use rockstream_ops::op::Operator;
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use sha2::{Digest, Sha256};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-test-minmax";

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

async fn open_minio_shard_db(port: u16, path: &str) -> Arc<ShardDb> {
    let store = minio_object_store(port);
    Arc::new(
        ShardDb::builder(path, store)
            .build()
            .await
            .expect("failed to open ShardDb on MinIO"),
    )
}

fn make_kv_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let k_vals: Vec<i64> = rows.iter().map(|(k, _, _)| *k).collect();
    let v_vals: Vec<i64> = rows.iter().map(|(_, v, _)| *v).collect();
    let weights: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(k_vals)),
            Arc::new(Int64Array::from(v_vals)),
        ],
    )
    .unwrap();
    ArrowZSet::new(data, weights)
}

fn extract_output(batch: &ArrowZSet) -> Vec<(i64, i64, i64)> {
    use arrow::array::Int64Array;
    let k_col = batch
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let e_col = batch
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut rows: Vec<(i64, i64, i64)> = (0..batch.num_rows())
        .map(|i| (k_col.value(i), e_col.value(i), batch.weights[i]))
        .collect();
    rows.sort();
    rows
}

// ─── Test 1: State persists across close/reopen on MinIO ─────────────────────

/// Proof: MinMaxOp state (multiset + extremum cache + frontier) survives
/// close/reopen on the S3-compatible MinIO backend.
#[tokio::test]
async fn minio_minmax_writes_and_persists() {
    if !docker_available() {
        eprintln!("SKIP minio_minmax_writes_and_persists: Docker not available");
        return;
    }

    let (_container, port) = start_minio().await;
    let db = open_minio_shard_db(port, "shard-mm-persist").await;
    let op = MinMaxOp::new_min(OperatorId(10));

    // Epoch 1: insert groups.
    let delta1 = make_kv_batch(&[(1, 10, 1), (1, 5, 1), (2, 20, 1)]);
    let _ = op.process_delta(delta1).unwrap();
    persist_minmax_state(&db, &op).await.unwrap();
    persist_frontier(&db, 1).await.unwrap();

    assert_eq!(op.cached_extremum(1), Some(5));
    assert_eq!(op.cached_extremum(2), Some(20));

    drop(op);
    drop(db);

    // ── Reopen on same MinIO path ────────────────────────────────────────────
    let db2 = open_minio_shard_db(port, "shard-mm-persist").await;
    let op2 = MinMaxOp::load_from_storage(&db2, OperatorId(10), MinMaxKind::Min)
        .await
        .unwrap();
    let frontier = load_frontier(&db2).await.unwrap();

    assert_eq!(frontier, Some(1));
    assert_eq!(op2.live_groups(), 2);
    assert_eq!(op2.cached_extremum(1), Some(5));
    assert_eq!(op2.cached_extremum(2), Some(20));

    // Epoch 2: retract min of k=1.
    let delta2 = make_kv_batch(&[(1, 5, -1)]);
    let out = op2.process_delta(delta2).unwrap();
    let rows = extract_output(&out);
    assert!(rows.contains(&(1, 5, -1)), "missing retraction: {rows:?}");
    assert!(rows.contains(&(1, 10, 1)), "missing insertion: {rows:?}");
    assert_eq!(op2.cached_extremum(1), Some(10));

    persist_minmax_state(&db2, &op2).await.unwrap();
    persist_frontier(&db2, 2).await.unwrap();
}

// ─── Test 2: Crash-replay on MinIO ───────────────────────────────────────────

/// Proof: simulated crash on the S3 backend; on restart the shard replays
/// epoch 2 from persisted frontier (epoch 1) to bit-identical output.
#[tokio::test]
async fn minio_minmax_crash_replay_bit_identical() {
    if !docker_available() {
        eprintln!("SKIP minio_minmax_crash_replay_bit_identical: Docker not available");
        return;
    }

    let (_container, port) = start_minio().await;

    let delta1 = make_kv_batch(&[(1, 10, 1), (1, 5, 1), (2, 20, 1)]);
    let delta2 = make_kv_batch(&[(1, 5, -1), (2, 15, 1)]);

    // ── Reference run ────────────────────────────────────────────────────────
    let db_ref = open_minio_shard_db(port, "shard-mm-ref").await;
    let op_ref = MinMaxOp::new_min(OperatorId(11));
    let _ = op_ref.process_delta(delta1.clone()).unwrap();
    persist_minmax_state(&db_ref, &op_ref).await.unwrap();
    persist_frontier(&db_ref, 1).await.unwrap();
    let reference_output = extract_output(&op_ref.process_delta(delta2.clone()).unwrap());
    drop(op_ref);
    drop(db_ref);

    // ── Crash run ────────────────────────────────────────────────────────────
    let db_crash = open_minio_shard_db(port, "shard-mm-crash").await;
    let op_crash = MinMaxOp::new_min(OperatorId(11));
    let _ = op_crash.process_delta(delta1).unwrap();
    persist_minmax_state(&db_crash, &op_crash).await.unwrap();
    persist_frontier(&db_crash, 1).await.unwrap();
    // Simulate crash: process epoch 2 in memory, never persist.
    let _ = op_crash.process_delta(delta2.clone()).unwrap();
    drop(op_crash);
    drop(db_crash);

    // ── Recovery ─────────────────────────────────────────────────────────────
    let db_recovery = open_minio_shard_db(port, "shard-mm-crash").await;
    let frontier = load_frontier(&db_recovery).await.unwrap();
    assert_eq!(
        frontier,
        Some(1),
        "frontier must point to last committed epoch"
    );

    let op_recovery = MinMaxOp::load_from_storage(&db_recovery, OperatorId(11), MinMaxKind::Min)
        .await
        .unwrap();
    let replay_output = extract_output(&op_recovery.process_delta(delta2).unwrap());

    assert_eq!(
        replay_output,
        reference_output,
        "crash-replay (MinIO) not bit-identical:\n  replay:    {replay_output:?}\n  reference: {reference_output:?}"
    );
}
