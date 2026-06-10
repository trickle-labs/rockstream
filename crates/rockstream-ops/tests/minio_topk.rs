//! MinIO (S3) backend integration tests for `TopKOp` (v0.12 — IVM-9).
//!
//! Tests skip gracefully if Docker is unavailable.
//!
//! 1. `minio_topk_random_changes` — random insert/update/delete sequence on MinIO backend;
//!    result after each epoch matches batch oracle.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use rockstream_ops::topk::{load_topk_state, persist_topk_state, TopKOp};
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use sha2::{Digest, Sha256};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-test-topk";

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

fn schema_kv() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
    ]))
}

fn make_input(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let v: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let id: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let w: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let data = RecordBatch::try_new(
        schema_kv(),
        vec![
            Arc::new(Int64Array::from(v)) as ArrayRef,
            Arc::new(Int64Array::from(id)) as ArrayRef,
        ],
    )
    .unwrap();
    ArrowZSet::new(data, w)
}

fn accumulate_vals(state: &mut HashMap<i64, i64>, zset: &ArrowZSet) {
    if zset.is_empty() { return; }
    let col = zset.data.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    for i in 0..zset.num_rows() {
        *state.entry(col.value(i)).or_insert(0) += zset.weights[i];
    }
}

fn live_vals(state: &HashMap<i64, i64>) -> Vec<i64> {
    let mut vals: Vec<i64> = state.iter().filter(|(_, &w)| w > 0).map(|(&v, _)| v).collect();
    vals.sort_by(|a, b| b.cmp(a));
    vals
}

fn batch_topk(input_state: &HashMap<i64, i64>, k: usize) -> Vec<i64> {
    let mut present: Vec<i64> = input_state.iter().filter(|(_, &w)| w > 0).map(|(&v, _)| v).collect();
    present.sort_by(|a, b| b.cmp(a));
    present.into_iter().take(k).collect()
}

// ─── Test 1: Random insert/update/delete on MinIO backend ─────────────────

#[tokio::test]
async fn minio_topk_random_changes() {
    if !docker_available() {
        eprintln!("Docker not available — skipping minio_topk_random_changes");
        return;
    }

    let (_container, port) = start_minio().await;
    let db = open_shard_minio(port, "topk-test").await;
    let op_id = OperatorId(40);
    let k = 3usize;

    let op = TopKOp::new(schema_kv(), k, 0, vec![]);
    let mut input_state: HashMap<i64, i64> = Default::default();
    let mut incr_output: HashMap<i64, i64> = Default::default();

    // Epoch sequences: insert/update/delete pattern.
    let epochs: Vec<Vec<(i64, i64, i64)>> = vec![
        vec![(10, 1, 1), (8, 2, 1), (6, 3, 1), (4, 4, 1), (2, 5, 1)],
        vec![(9, 6, 1)],         // v=9 outranks v=6
        vec![(10, 1, -1)],       // delete rank-1
        vec![(5, 7, 1)],         // insert v=5 (below current k-th=9? no, top-3 is [9,8,6])
        vec![(8, 2, -1)],        // delete rank-1 from {9,8,6}
        vec![(7, 8, 1)],         // insert v=7
    ];

    for (epoch_idx, rows) in epochs.iter().enumerate() {
        let batch = make_input(rows);

        // Update input_state.
        for &(v, _, w) in rows.iter() {
            *input_state.entry(v).or_insert(0) += w;
        }

        let out = op.process_epoch(batch, epoch_idx as u64 + 1).unwrap();
        accumulate_vals(&mut incr_output, &out);

        let incr_live = live_vals(&incr_output);
        let batch_live = batch_topk(&input_state, k);

        assert_eq!(
            incr_live, batch_live,
            "incremental top-K != batch top-K at epoch {}",
            epoch_idx + 1
        );
    }

    // Persist to MinIO and reload, verify state consistent.
    persist_topk_state(&db, &op, op_id).await.unwrap();

    let op2 = load_topk_state(&db, schema_kv(), k, 0, vec![], op_id)
        .await
        .unwrap();
    assert_eq!(op2.fill_level(), op.fill_level(), "fill level matches after MinIO reload");

    // One more epoch after reload.
    let mut incr_output2 = incr_output.clone();
    let out_after = op2.process_epoch(make_input(&[(11, 9, 1)]), 7).unwrap();
    accumulate_vals(&mut incr_output2, &out_after);
    *input_state.entry(11).or_insert(0) += 1;
    let incr_live2 = live_vals(&incr_output2);
    let batch_live2 = batch_topk(&input_state, k);
    assert_eq!(incr_live2, batch_live2, "top-K correct after MinIO reload + one epoch");
}
