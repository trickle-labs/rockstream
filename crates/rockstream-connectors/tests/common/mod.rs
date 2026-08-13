// Shared test helper module included via `mod common;` by several separate
// test binaries (LFS, MinIO, TC variants). Each binary only uses a subset of
// these helpers, so per-binary dead-code lints are false positives here.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::ClientConfig;
use sha2::{Digest, Sha256};
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use testcontainers_modules::kafka::apache::{self, KAFKA_PORT};
use tokio_postgres::{Client, NoTls};

pub const MINIO_USER: &str = "minioadmin";
pub const MINIO_PASS: &str = "minioadmin";
pub const RNG_SEED: u64 = 0x4400_0044;

pub struct ConnectorFixture {
    pub _postgres: ContainerAsync<GenericImage>,
    pub _kafka: ContainerAsync<apache::Kafka>,
    pub postgres: Client,
    pub postgres_host: String,
    pub postgres_port: u16,
    pub kafka_bootstrap: String,
}

pub async fn connector_fixture(label: &str) -> ConnectorFixture {
    assert!(
        docker_available(),
        "Docker is required for connector guarantees"
    );
    let postgres = GenericImage::new("postgres", "11-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_cmd(["postgres", "-c", "wal_level=logical"])
        .start()
        .await
        .unwrap();
    let postgres_host = postgres.get_host().await.unwrap().to_string();
    let postgres_port = postgres.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!(
        "host={postgres_host} port={postgres_port} user=postgres password=postgres dbname=postgres"
    );
    let (postgres_client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    postgres_client
        .batch_execute(&format!(
            "CREATE TABLE orders (id BIGINT PRIMARY KEY); ALTER TABLE orders REPLICA IDENTITY FULL; CREATE PUBLICATION orders_pub FOR TABLE orders; CREATE TABLE health_{label} (id BIGINT);"
        ))
        .await
        .unwrap();
    postgres_client.query_one("SELECT 1", &[]).await.unwrap();

    let kafka = apache::Kafka::default()
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .start()
        .await
        .unwrap();
    let kafka_bootstrap = format!(
        "127.0.0.1:{}",
        kafka.get_host_port_ipv4(KAFKA_PORT).await.unwrap()
    );
    let health: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_bootstrap)
        .create()
        .unwrap();
    health
        .fetch_metadata(None, Duration::from_secs(10))
        .unwrap();

    ConnectorFixture {
        _postgres: postgres,
        _kafka: kafka,
        postgres: postgres_client,
        postgres_host,
        postgres_port,
        kafka_bootstrap,
    }
}

pub fn docker_available() -> bool {
    rockstream_test_support::docker_available()
}

pub fn make_cumulative_batch(last_id: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids: ArrayRef = Arc::new(Int64Array::from((1..=last_id).collect::<Vec<_>>()));
    let names: ArrayRef = Arc::new(StringArray::from(
        (1..=last_id)
            .map(|id| format!("row-{id}"))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema, vec![ids, names]).unwrap()
}

pub fn render_batches(batches: &[RecordBatch]) -> String {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row_idx in 0..batch.num_rows() {
            rows.push(format!("{}|{}", ids.value(row_idx), names.value(row_idx)));
        }
    }
    rows.join("\n")
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
    for &days_in_month in &dpm {
        if days < days_in_month {
            break;
        }
        days -= days_in_month;
        month += 1;
    }
    let day = days + 1;
    (year, month + 1, day, h, m, s)
}

pub async fn create_minio_bucket(port: u16, bucket: &str) {
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
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{canonical_hash}");
    let k1 = hmac_sha256(format!("AWS4{MINIO_PASS}").as_bytes(), date.as_bytes());
    let k2 = hmac_sha256(&k1, region.as_bytes());
    let k3 = hmac_sha256(&k2, b"s3");
    let signing_key = hmac_sha256(&k3, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={MINIO_USER}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}"
    );
    let url = format!("http://{host}/{bucket}");
    let response = reqwest::Client::new()
        .put(url)
        .header("host", host)
        .header("x-amz-date", datetime)
        .header("x-amz-content-sha256", empty_hash)
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success() || response.status().as_u16() == 409,
        "create bucket failed: {}",
        response.status()
    );
}

pub fn build_minio_store(port: u16, bucket: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://127.0.0.1:{port}"))
            .with_bucket_name(bucket)
            .with_access_key_id(MINIO_USER)
            .with_secret_access_key(MINIO_PASS)
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()
            .unwrap(),
    )
}
