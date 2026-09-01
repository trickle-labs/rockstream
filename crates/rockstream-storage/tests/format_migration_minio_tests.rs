use std::sync::Arc;

use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use rockstream_storage::format_migration::{
    migrate_shard_format, migrate_shard_format_with_options, MigrationOptions,
};
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::ShardDb;
use sha2::{Digest, Sha256};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

const USER: &str = "minioadmin";
const PASSWORD: &str = "minioadmin";

fn docker_available() -> bool {
    rockstream_test_support::docker_available()
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn hmac_sha256(key: &[u8], bytes: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(bytes);
    mac.finalize().into_bytes().to_vec()
}

fn date_parts(mut days: u64) -> (u32, u32, u32) {
    let mut year = 1970;
    loop {
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let year_days = if leap { 366 } else { 365 };
        if days < year_days {
            break;
        }
        days -= year_days;
        year += 1;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = [
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
    for (month, length) in (1..).zip(month_days) {
        if days < length {
            return (year, month, days as u32 + 1);
        }
        days -= length;
    }
    unreachable!()
}

async fn create_bucket(port: u16, bucket: &str) {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let (year, month, day) = date_parts(seconds / 86_400);
    let hour = (seconds / 3_600) % 24;
    let minute = (seconds / 60) % 60;
    let second = seconds % 60;
    let date = format!("{year:04}{month:02}{day:02}");
    let timestamp = format!("{date}T{hour:02}{minute:02}{second:02}Z");
    let host = format!("127.0.0.1:{port}");
    let payload_hash = sha256_hex(b"");
    let canonical = format!(
        "PUT\n/{bucket}\n\nhost:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{timestamp}\n\nhost;x-amz-content-sha256;x-amz-date\n{payload_hash}"
    );
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        sha256_hex(canonical.as_bytes())
    );
    let first = hmac_sha256(format!("AWS4{PASSWORD}").as_bytes(), date.as_bytes());
    let second = hmac_sha256(&first, b"us-east-1");
    let third = hmac_sha256(&second, b"s3");
    let signing_key = hmac_sha256(&third, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={USER}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}"
    );
    let response = reqwest::Client::new()
        .put(format!("http://{host}/{bucket}"))
        .header("Host", &host)
        .header("X-Amz-Content-Sha256", &payload_hash)
        .header("X-Amz-Date", &timestamp)
        .header("Authorization", authorization)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success() || response.status().as_u16() == 409);
}

async fn minio_store(
    bucket: &str,
) -> (testcontainers::ContainerAsync<MinIO>, Arc<dyn ObjectStore>) {
    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_bucket(port, bucket).await;
    let store = AmazonS3Builder::new()
        .with_endpoint(format!("http://127.0.0.1:{port}"))
        .with_bucket_name(bucket)
        .with_access_key_id(USER)
        .with_secret_access_key(PASSWORD)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .build()
        .unwrap();
    (container, Arc::new(store))
}

async fn populate(path: &str, store: Arc<dyn ObjectStore>) -> Vec<(bytes::Bytes, bytes::Bytes)> {
    let db = ShardDb::builder(path, store).build().await.unwrap();
    for suffix in [b"a".as_slice(), b"b", b"c"] {
        let key = ShardKeyEncoder::encode(ShardPrefix::OpState, 7, suffix);
        db.put(&key, suffix).await.unwrap();
    }
    db.flush().await.unwrap();
    let rows = db
        .scan_prefix(&ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, 7))
        .await
        .unwrap();
    db.close().await.unwrap();
    rows
}

#[tokio::test]
async fn migrates_populated_v1_shards_to_v2_bit_identically_tc() {
    if !docker_available() {
        eprintln!("SKIP migrates_populated_v1_shards_to_v2_bit_identically_tc: Docker is not available locally");
        return;
    }
    let bucket = format!("rockstream-format-{}", std::process::id());
    let (_container, store) = minio_store(&bucket).await;
    let before = populate("shards/1/db", store.clone()).await;
    migrate_shard_format("shards/1/db", store.clone(), 1u8, 2u8)
        .await
        .unwrap();
    let db = ShardDb::builder("shards/1/db", store)
        .build()
        .await
        .unwrap();
    assert_eq!(db.format_version(), 2);
    assert_eq!(
        db.scan_prefix(&ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, 7))
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn interrupted_migration_reruns_exactly_tc() {
    if !docker_available() {
        eprintln!("SKIP interrupted_migration_reruns_exactly_tc: Docker is not available locally");
        return;
    }
    let bucket = format!("rockstream-format-rerun-{}", std::process::id());
    let (_container, store) = minio_store(&bucket).await;
    let before = populate("shards/2/db", store.clone()).await;
    migrate_shard_format_with_options(
        "shards/2/db",
        store.clone(),
        1u8,
        2u8,
        MigrationOptions {
            fail_after_objects: Some(1),
        },
    )
    .await
    .unwrap_err();
    migrate_shard_format("shards/2/db", store.clone(), 1u8, 2u8)
        .await
        .unwrap();
    let db = ShardDb::builder("shards/2/db", store)
        .build()
        .await
        .unwrap();
    assert_eq!(db.format_version(), 2);
    assert_eq!(
        db.scan_prefix(&ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, 7))
            .await
            .unwrap(),
        before
    );
}
