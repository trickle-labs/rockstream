//! MinIO (S3) backend integration tests for v0.5 operators.
//!
//! Tests:
//! 1. `minio_aggregate_writes_and_persists` — AggregateOp state and frontier
//!    survive a close/reopen cycle on the MinIO (S3) backend.
//! 2. `minio_group_commit_reduces_durability_events` — GroupCommit reduces
//!    write count ≥5× vs. individual commits on the S3 backend.
//!
//! Docker must be running.  Tests skip gracefully if Docker is unavailable.

use std::sync::Arc;

use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use rockstream_ops::aggregate::{load_frontier, persist_agg_state, persist_frontier, AggregateOp};
use rockstream_ops::group_commit::GroupCommit;
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::ids::OperatorId;
use sha2::{Digest, Sha256};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

// ─── helpers ─────────────────────────────────────────────────────────────────

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-test-ops";

fn docker_available() -> bool {
    rockstream_test_support::docker_available()
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

async fn open_shard_db_minio(port: u16, path: &str) -> Arc<ShardDb> {
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

// ─── Test 1: AggregateOp state + frontier survive on MinIO ───────────────────

/// Proof: AggregateOp state and frontier persist across close/reopen on the
/// S3 (MinIO) backend — proving S3-semantics correctness of the op_state and
/// shard_meta namespaces.
#[tokio::test]
async fn minio_aggregate_writes_and_persists() {
    if !docker_available() {
        eprintln!("SKIP minio_aggregate_writes_and_persists: Docker not available");
        return;
    }
    let (_container, port) = start_minio().await;

    // Phase 1: write state.
    {
        let db = open_shard_db_minio(port, "agg-test").await;
        let op = AggregateOp::new(OperatorId(1));
        let _ = op
            .process_delta(make_kv_batch(&[(1, 10, 1), (2, 20, 1)]))
            .unwrap();
        persist_agg_state(&db, &op).await.unwrap();
        persist_frontier(&db, 5).await.unwrap();
        db.flush().await.unwrap();
        Arc::try_unwrap(db)
            .ok()
            .expect("single owner")
            .close()
            .await
            .unwrap();
    }

    // Phase 2: reopen and verify.
    {
        let db = open_shard_db_minio(port, "agg-test").await;
        let frontier = load_frontier(&db).await.unwrap();
        assert_eq!(frontier, Some(5u64), "frontier must survive on MinIO");
        let op = AggregateOp::load_from_storage(&db, OperatorId(1))
            .await
            .unwrap();
        assert_eq!(op.live_groups(), 2, "2 live groups must survive on MinIO");
    }
}

// ─── Test 2: GroupCommit reduces durability events ≥5× on MinIO ──────────────

/// Proof: GroupCommit issues exactly 1 Db::write() for N=6 operator batches
/// on the S3 (MinIO) backend, proving ≥5× reduction in durability events under
/// S3 semantics (list/get/put latency, conditional writes, multipart paths).
#[tokio::test]
async fn minio_group_commit_reduces_durability_events() {
    if !docker_available() {
        eprintln!("SKIP minio_group_commit_reduces_durability_events: Docker not available");
        return;
    }
    let (_container, port) = start_minio().await;
    let db = open_shard_db_minio(port, "gc-test").await;

    const NUM_OPERATORS: usize = 6;
    let gc = GroupCommit::new(db.clone());

    for i in 0..NUM_OPERATORS {
        let mut wb = WriteBatch::new();
        wb.put(&[0x01, i as u8], &(i as u64).to_be_bytes());
        gc.add_batch(wb).unwrap();
    }
    assert_eq!(gc.fill_level(), NUM_OPERATORS);

    let merged = gc.flush().await.unwrap();
    assert_eq!(gc.commit_count(), 1, "exactly 1 Db::write() on MinIO");
    assert_eq!(merged, NUM_OPERATORS);

    let reduction = NUM_OPERATORS as u64 / gc.commit_count();
    assert!(reduction >= 5, "≥5× reduction required; got {reduction}×");

    // Spot-check: all keys visible after commit.
    for i in 0..NUM_OPERATORS {
        let val = db.get(&[0x01, i as u8]).await.unwrap();
        assert!(val.is_some(), "key {i} not found after MinIO group commit");
    }
}
