//! Multi-source E2E join parity and shuffle GC soak simulation test (v0.19, Slice 6).

use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::{core::WaitFor, Image};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use rockstream_ops::join::JoinOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_runtime::exchange::persistence::{
    gc_exchange_storage, persist_inbox, persist_outbox,
};
use rockstream_storage::shard_db::ShardDb;
use rockstream_types::frontier::{FreshnessToken, Lattice, SourceProgress};
use rockstream_types::ids::{OperatorId, SourceId};

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-test";

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

#[allow(clippy::manual_is_multiple_of)]
fn epoch_to_ymd_hms(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let sod = secs % 86400;
    let mut days = (secs / 86400) as u32;
    let h = (sod / 3600) as u32;
    let m = ((sod % 3600) / 60) as u32;
    let s = (sod % 60) as u32;
    let mut year = 1970u32;
    loop {
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let dy = if leap { 366 } else { 365 };
        if days < dy {
            break;
        }
        days -= dy;
        year += 1;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
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

#[derive(Debug, Clone)]
pub struct MinIO2024 {
    env_vars: HashMap<String, String>,
}

impl Default for MinIO2024 {
    fn default() -> Self {
        let mut env_vars = HashMap::new();
        env_vars.insert("MINIO_CONSOLE_ADDRESS".to_owned(), ":9001".to_owned());
        Self { env_vars }
    }
}

impl Image for MinIO2024 {
    fn name(&self) -> &str {
        "minio/minio"
    }

    fn tag(&self) -> &str {
        "RELEASE.2024-11-07T00-52-20Z"
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stderr("API:")]
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
    let container = MinIO2024::default()
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
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .build()
            .expect("failed to build S3 object store for MinIO"),
    )
}

fn make_kv_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
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

fn empty_kv() -> ArrowZSet {
    ArrowZSet::empty(Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ])))
}

fn extract_output(batch: &ArrowZSet) -> Vec<(i64, i64, i64, i64)> {
    if batch.is_empty() {
        return vec![];
    }
    let lk = batch
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let lv = batch
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let rv = batch
        .data
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut rows: Vec<(i64, i64, i64, i64)> = (0..batch.num_rows())
        .map(|i| (lk.value(i), lv.value(i), rv.value(i), batch.weights[i]))
        .collect();
    rows.sort();
    rows
}

#[test]
fn test_multi_source_join_progress() {
    // 1. Sets up JoinOp
    let join_op = JoinOp::new(OperatorId(1), vec![0], vec![0]);

    // 2. Set up asymmetric source progress:
    // Left side (Source 1) goes fast: epochs 1 to 5.
    // Right side (Source 2) goes slow: stays at epoch 1.

    // In Epoch 1:
    // Left: key=10, val=100. Right: key=10, val=200.
    let left_progress_1 = SourceProgress::new(1, Some(100));
    let right_progress_1 = SourceProgress::new(1, Some(100));
    let left_token_1 = FreshnessToken::new(
        BTreeMap::from([
            (SourceId(1), left_progress_1),
            (SourceId(2), right_progress_1),
        ]),
        0,
    );
    let right_token_1 = FreshnessToken::new(
        BTreeMap::from([
            (SourceId(1), left_progress_1),
            (SourceId(2), right_progress_1),
        ]),
        0,
    );
    let meet_token_1 = left_token_1.meet(&right_token_1);
    assert_eq!(meet_token_1.watermark_ms(), Some(100));

    let left_1 = make_kv_batch(&[(10, 100, 1)]);
    let right_1 = make_kv_batch(&[(10, 200, 1)]);
    let out_1 = join_op.process_epoch(left_1, right_1).unwrap();
    let rows_1 = extract_output(&out_1);
    assert!(rows_1.contains(&(10, 100, 200, 1)));

    // In Epochs 2 to 4:
    // Left Source 1 advances: epoch = i, watermark = 100 * i.
    // Right Source 2 lags: epoch = 1, watermark = 100.
    for i in 2..=4 {
        let left_progress_i = SourceProgress::new(i as u64, Some(100 * i));
        let right_progress_i = SourceProgress::new(1, Some(100));
        let left_token_i = FreshnessToken::new(
            BTreeMap::from([
                (SourceId(1), left_progress_i),
                (SourceId(2), right_progress_i),
            ]),
            0,
        );
        let right_token_i = FreshnessToken::new(
            BTreeMap::from([
                (SourceId(1), left_progress_i),
                (SourceId(2), right_progress_i),
            ]),
            0,
        );
        let meet_token_i = left_token_i.meet(&right_token_i);

        // Assert no premature emissions: the output joint watermark must remain at 100.
        assert_eq!(
            meet_token_i.watermark_ms(),
            Some(100),
            "Watermark advanced prematurely at epoch {}",
            i
        );

        // Feed left delta, right empty
        let left_delta = make_kv_batch(&[(10, 100 + i * 10, 1)]);
        let right_delta = empty_kv();
        let _out_i = join_op.process_epoch(left_delta, right_delta).unwrap();
        // Staged buffers must not accumulate unboundedly (they are drained by commit_epoch).
        assert_eq!(join_op.left_entry_count(), i as usize);
        assert_eq!(join_op.right_entry_count(), 1);
    }

    // In Epoch 5:
    // Slow side (Right Source 2) catches up: epoch = 5, watermark = 500.
    let left_progress_5 = SourceProgress::new(5, Some(500));
    let right_progress_5 = SourceProgress::new(5, Some(500));
    let left_token_5 = FreshnessToken::new(
        BTreeMap::from([
            (SourceId(1), left_progress_5),
            (SourceId(2), right_progress_5),
        ]),
        0,
    );
    let right_token_5 = FreshnessToken::new(
        BTreeMap::from([
            (SourceId(1), left_progress_5),
            (SourceId(2), right_progress_5),
        ]),
        0,
    );
    let meet_token_5 = left_token_5.meet(&right_token_5);

    // Assert the joint watermark has converged correctly to 500.
    assert_eq!(meet_token_5.watermark_ms(), Some(500));

    // Feed left empty, right catching up with new data: (10, 300)
    let left_delta = empty_kv();
    let right_delta = make_kv_batch(&[(10, 300, 1)]);
    let out_5 = join_op.process_epoch(left_delta, right_delta).unwrap();
    let rows_5 = extract_output(&out_5);

    // Verify correct convergence (all historical left rows joined with the new right row).
    assert!(rows_5.contains(&(10, 100, 300, 1)));
    assert!(rows_5.contains(&(10, 120, 300, 1)));
    assert!(rows_5.contains(&(10, 130, 300, 1)));
    assert!(rows_5.contains(&(10, 140, 300, 1)));
}

#[tokio::test]
async fn test_shuffle_storage_gc_bounded() {
    if !docker_available() {
        eprintln!("SKIP test_shuffle_storage_gc_bounded: Docker not available");
        return;
    }

    let (_container, port) = start_minio().await;
    let store = minio_object_store(port);
    let db = ShardDb::builder("test_gc_db", store).build().await.unwrap();

    let inbox_prefix = [0x04];
    let outbox_prefix = [0x05];

    // Write inbox and outbox shuffle entries for 100 consecutive epochs
    for epoch in 1..=100 {
        persist_inbox(&db, 100, 1, epoch, 1, b"inbox_data")
            .await
            .unwrap();
        persist_outbox(&db, 200, 2, epoch, 1, b"outbox_data")
            .await
            .unwrap();

        // Perform GC periodically, keeping only the most recent 5 epochs
        if epoch >= 5 {
            gc_exchange_storage(&db, epoch - 5).await.unwrap();
        }
    }

    // Verify key count in ShardDb is bounded (only epochs 96..100 remain)
    let inbox_keys = db.scan_prefix(&inbox_prefix).await.unwrap();
    let outbox_keys = db.scan_prefix(&outbox_prefix).await.unwrap();

    assert!(
        inbox_keys.len() <= 5,
        "Inbox keys not bounded: {}",
        inbox_keys.len()
    );
    assert!(
        outbox_keys.len() <= 5,
        "Outbox keys not bounded: {}",
        outbox_keys.len()
    );

    // Verify that the remaining keys indeed correspond to the latest epochs
    for (key, _) in inbox_keys {
        if let Some((_, _, suffix)) = rockstream_storage::keys::ShardKeyEncoder::decode(&key) {
            let epoch = u64::from_be_bytes(suffix[4..12].try_into().unwrap());
            assert!(
                epoch >= 96,
                "Stale epoch {} not cleaned up from inbox!",
                epoch
            );
        }
    }

    for (key, _) in outbox_keys {
        if let Some((_, _, suffix)) = rockstream_storage::keys::ShardKeyEncoder::decode(&key) {
            let epoch = u64::from_be_bytes(suffix[4..12].try_into().unwrap());
            assert!(
                epoch >= 96,
                "Stale epoch {} not cleaned up from outbox!",
                epoch
            );
        }
    }

    db.close().await.unwrap();
}
