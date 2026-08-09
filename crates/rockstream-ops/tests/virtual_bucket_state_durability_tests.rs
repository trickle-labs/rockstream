use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_ops::{persist_bucketed_agg_state, BucketedAggregateOp, Operator};
use rockstream_storage::{ShardDb, ShardKeyEncoder, ShardPrefix};
use rockstream_types::ids::OperatorId;

fn docker_available() -> bool {
    rockstream_test_support::docker_available()
}

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-virtual-bucket-state-test";

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(data))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
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
        .unwrap();
    assert!(resp.status().is_success() || resp.status().as_u16() == 409);
}

fn minio_object_store(port: u16) -> Arc<dyn ObjectStore> {
    use object_store::aws::AmazonS3Builder;
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
            .unwrap(),
    )
}

fn make_batch(rows: &[(i64, i64, i64)]) -> rockstream_ops::ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(k, _, _)| *k).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, v, _)| *v).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    rockstream_ops::ArrowZSet::new(data, rows.iter().map(|(_, _, w)| *w).collect())
}

fn extract_rows(batch: &rockstream_ops::ArrowZSet) -> Vec<(i64, i64, i64, f64, i64)> {
    let k = batch
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sum = batch
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let count = batch
        .data
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let avg = batch
        .data
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let mut rows = Vec::new();
    for index in 0..batch.num_rows() {
        rows.push((
            k.value(index),
            sum.value(index),
            count.value(index),
            avg.value(index),
            batch.weights[index],
        ));
    }
    rows
}

async fn run_persistence_roundtrip(store: Arc<dyn ObjectStore>) {
    let db = ShardDb::builder("virtual-bucket/durability".to_string(), store)
        .build()
        .await
        .unwrap();
    let op_id = OperatorId(41);
    let hot_key = 1;
    let original = BucketedAggregateOp::new(op_id, hot_key, 4);
    original
        .process_delta(make_batch(&[(1, 10, 1), (1, 20, 1), (1, 30, 1), (2, 5, 1)]))
        .unwrap();
    persist_bucketed_agg_state(&db, &original).await.unwrap();

    let prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, op_id.0);
    let (entries, truncated) = db
        .scan_prefix_bounded(&prefix, 64 * 1024 * 1024)
        .await
        .unwrap();
    assert!(!truncated);
    let partial_rows = entries
        .iter()
        .filter(|(key, _)| key.len() == prefix.len() + 10)
        .count();
    assert!(
        partial_rows > 0,
        "expected persisted (logical_key,bucket) rows"
    );

    let reloaded = BucketedAggregateOp::load_from_storage(&db, op_id, hot_key, 4)
        .await
        .unwrap();
    assert_eq!(reloaded.live_partials(), partial_rows);

    let retract = make_batch(&[(1, 10, -1)]);
    let original_rows = extract_rows(&original.process_delta(retract.clone()).unwrap());
    let reloaded_rows = extract_rows(&reloaded.process_delta(retract).unwrap());
    assert_eq!(reloaded_rows, original_rows);
}

#[tokio::test]
async fn partial_state_survives_restart_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    run_persistence_roundtrip(store).await;
}

#[tokio::test]
async fn partial_state_survives_restart_minio_tc() {
    if !docker_available() {
        eprintln!("SKIP partial_state_survives_restart_minio_tc: Docker not available");
        return;
    }
    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_BUCKET).await;
    run_persistence_roundtrip(minio_object_store(port)).await;
}

#[tokio::test]
async fn partial_state_is_scan_and_delete_never_range_delete() {
    let source =
        std::fs::read_to_string(format!("{}/src/aggregate.rs", env!("CARGO_MANIFEST_DIR")))
            .unwrap();
    assert!(source.contains("scan_prefix_bounded"));
    assert!(source.contains("wb.delete"));
    assert!(!source.contains("range_delete"));
}

#[tokio::test]
async fn partial_state_is_bounded_with_fill_level_metric() {
    let db = ShardDb::builder(
        "virtual-bucket/bounded".to_string(),
        Arc::new(object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>,
    )
    .build()
    .await
    .unwrap();
    let op = BucketedAggregateOp::new(OperatorId(42), 1, 4);
    op.process_delta(make_batch(&[(1, 10, 1), (1, 20, 1), (1, 30, 1)]))
        .unwrap();
    persist_bucketed_agg_state(&db, &op).await.unwrap();
    assert!(op.live_partials() > 0);
    assert!(op.live_partials() <= 3);
}
