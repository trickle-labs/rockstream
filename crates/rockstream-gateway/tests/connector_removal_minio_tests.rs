use std::sync::Arc;

use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::{CatalogSinkEntry, CatalogSourceEntry, CatalogStubs, V0522ConnectorCatalog},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use tokio_postgres::NoTls;

struct NoopViewReader;
#[async_trait::async_trait]
impl ViewReader for NoopViewReader {
    async fn read_view(
        &self,
        _: &str,
        _: Option<usize>,
        _: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }
    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

const REMEDIATION: &str = "[RS-4017] connector.removed: Use an external loader through pgwire or Kafka for S3 input, an external HTTP-to-Kafka (or HTTP-to-PostgreSQL) adapter for webhooks, or RockStream to Kafka to a downstream writer for sink output.";

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(data))
}
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    type HmacSha256 = Hmac<sha2::Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

async fn create_bucket(port: u16, bucket: &str) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let datetime = chrono::DateTime::from_timestamp(now as i64, 0)
        .unwrap()
        .format("%Y%m%dT%H%M%SZ")
        .to_string();
    let date = &datetime[..8];
    let host = format!("127.0.0.1:{port}");
    let empty_hash = sha256_hex(b"");
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let canonical = format!("PUT\n/{bucket}\n\nhost:{host}\nx-amz-content-sha256:{empty_hash}\nx-amz-date:{datetime}\n\nhost;x-amz-content-sha256;x-amz-date\n{empty_hash}");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{}",
        sha256_hex(canonical.as_bytes())
    );
    let k_date = hmac_sha256(b"AWS4minioadmin", date.as_bytes());
    let k_region = hmac_sha256(&k_date, b"us-east-1");
    let k_service = hmac_sha256(&k_region, b"s3");
    let signing_key = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
    let response = reqwest::Client::new().put(format!("http://{host}/{bucket}")).header("Host", &host).header("X-Amz-Content-Sha256", &empty_hash).header("X-Amz-Date", &datetime).header("Authorization", format!("AWS4-HMAC-SHA256 Credential=minioadmin/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}")).send().await.unwrap();
    assert!(response.status().is_success() || response.status().as_u16() == 409);
}

fn store(port: u16, bucket: &str) -> Arc<dyn ObjectStore> {
    use object_store::aws::AmazonS3Builder;
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://127.0.0.1:{port}"))
            .with_bucket_name(bucket)
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()
            .unwrap(),
    )
}

#[tokio::test]
async fn v0522_removed_connector_catalog_loads_as_removed_exactly() {
    if !rockstream_test_support::docker_available() {
        return;
    }
    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let bucket = "connector-removal-v0523";
    create_bucket(port, bucket).await;
    let shard_db = Arc::new(
        ShardDb::builder("connector-removal", store(port, bucket))
            .build()
            .await
            .unwrap(),
    );
    CatalogStubs::seed_v0522_connector_catalog(
        &shard_db,
        &V0522ConnectorCatalog {
            sinks: vec![
                CatalogSinkEntry {
                    name: "iceberg_sink".into(),
                    view: "orders".into(),
                    format: "ICEBERG".into(),
                    path: "s3://bucket/orders".into(),
                    snapshot_interval_epochs: None,
                    snapshot_interval_ms: None,
                    parquet_row_group_bytes: None,
                    format_version: None,
                    partition_by: vec![],
                    catalog: "glue".into(),
                    last_snapshot_epoch: None,
                    state: "OK".into(),
                },
                CatalogSinkEntry {
                    name: "object_store_sink".into(),
                    view: "orders".into(),
                    format: "PARQUET".into(),
                    path: "s3://bucket/archive".into(),
                    snapshot_interval_epochs: None,
                    snapshot_interval_ms: None,
                    parquet_row_group_bytes: None,
                    format_version: None,
                    partition_by: vec![],
                    catalog: "filesystem".into(),
                    last_snapshot_epoch: None,
                    state: "OK".into(),
                },
            ],
            sources: vec![
                CatalogSourceEntry {
                    name: "s3_source".into(),
                    table_name: None,
                    source_type: "s3".into(),
                    options: Default::default(),
                    format: "json".into(),
                    status: "OK".into(),
                    live_offset: "0".into(),
                    live_lag: 0,
                },
                CatalogSourceEntry {
                    name: "webhook_source".into(),
                    table_name: None,
                    source_type: "http_webhook".into(),
                    options: Default::default(),
                    format: "json".into(),
                    status: "OK".into(),
                    live_offset: "0".into(),
                    live_lag: 0,
                },
            ],
        },
    )
    .await
    .unwrap();
    shard_db.flush().await.unwrap();
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        shard_db,
    );
    let (addr, _server) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let sources = client.simple_query("SHOW SOURCES").await.unwrap();
    let source_rows: Vec<_> = sources
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some((row.get(0), row.get(1), row.get(2), row.get(3), row.get(6)))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        source_rows,
        vec![
            (
                Some("s3_source"),
                Some("s3"),
                Some("json"),
                Some("REMOVED"),
                Some(REMEDIATION)
            ),
            (
                Some("webhook_source"),
                Some("http_webhook"),
                Some("json"),
                Some("REMOVED"),
                Some(REMEDIATION)
            )
        ]
    );
    let sinks = client.simple_query("SHOW SINKS").await.unwrap();
    let sink_rows: Vec<_> = sinks
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get(0),
                row.get(1),
                row.get(2),
                row.get(3),
                row.get(4),
                row.get(5),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        sink_rows,
        vec![
            (
                Some("iceberg_sink"),
                Some("ICEBERG"),
                Some("s3://bucket/orders"),
                Some("glue"),
                Some("REMOVED"),
                Some(REMEDIATION)
            ),
            (
                Some("object_store_sink"),
                Some("PARQUET"),
                Some("s3://bucket/archive"),
                Some("filesystem"),
                Some("REMOVED"),
                Some(REMEDIATION)
            )
        ]
    );
}
