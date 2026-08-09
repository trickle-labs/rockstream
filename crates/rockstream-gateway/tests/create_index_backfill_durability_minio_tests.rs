//! v0.51.2 Slice 5 durability (MinIO/TC): the `CREATE INDEX` synchronous
//! backfill writes `0x03‖op_id‖col_val` index arrangement rows into
//! `shard_db` and marks the catalog entry `Ready` — both must survive a
//! process restart against the same SlateDB-on-S3 (MinIO) backend.

use std::sync::Arc;

use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::{CatalogIndexState, CatalogStubs},
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
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }
    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

async fn connect_port(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

async fn start_gateway(
    shard_path: &str,
    store: Arc<dyn ObjectStore>,
    catalog: Arc<CatalogStubs>,
) -> (u16, tokio::task::JoinHandle<()>, Arc<ShardDb>) {
    let shard_db = Arc::new(ShardDb::builder(shard_path, store).build().await.unwrap());
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let (addr, handle) = server.serve_background().await.unwrap();
    (addr.port(), handle, shard_db)
}

fn data_rows(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Vec<&tokio_postgres::SimpleQueryRow> {
    msgs.iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect()
}

fn docker_available() -> bool {
    rockstream_test_support::docker_available()
}

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";

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

fn minio_object_store(port: u16, bucket: &str) -> Arc<dyn ObjectStore> {
    use object_store::aws::AmazonS3Builder;
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://127.0.0.1:{port}"))
            .with_bucket_name(bucket)
            .with_access_key_id(MINIO_USER)
            .with_secret_access_key(MINIO_PASS)
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .build()
            .unwrap(),
    )
}

async fn run_backfilled_index_reconnect_case(store: Arc<dyn ObjectStore>, shard_path: &str) {
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE accounts (id BIGINT, balance BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("SET rockstream.idempotency_key = 'backfill-durability-minio-fixture'")
        .await
        .unwrap();
    client.simple_query("BEGIN").await.unwrap();
    for i in 0..5 {
        client
            .simple_query(&format!(
                "INSERT INTO accounts (id, balance) VALUES ({i}, {})",
                i * 100
            ))
            .await
            .unwrap();
    }
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();

    client
        .simple_query("CREATE INDEX idx_accounts_balance ON accounts (balance)")
        .await
        .expect("CREATE INDEX backfill should succeed");
    assert_eq!(
        catalog.get_index("idx_accounts_balance").unwrap().state,
        CatalogIndexState::Ready
    );
    handle.abort();

    let (port2, _handle2, shard_db2) = start_gateway(shard_path, store, catalog.clone()).await;
    let client2 = connect_port(port2).await;
    shard_db2.flush().await.unwrap();

    let msgs = client2
        .simple_query("SELECT id, balance FROM accounts WHERE balance = 300")
        .await
        .expect("point lookup after reconnect");
    let rows = data_rows(&msgs);
    assert_eq!(
        rows.len(),
        1,
        "backfilled index bytes must still serve the point lookup after reconnect"
    );
    assert_eq!(rows[0].get("balance"), Some("300"));
}

async fn run_index_ready_state_restart_case(store: Arc<dyn ObjectStore>, shard_path: &str) {
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE readings (id BIGINT, value BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("SET rockstream.idempotency_key = 'ready-state-durability-minio-fixture'")
        .await
        .unwrap();
    client.simple_query("BEGIN").await.unwrap();
    for i in 0..5 {
        client
            .simple_query(&format!(
                "INSERT INTO readings (id, value) VALUES ({i}, {})",
                i * 10
            ))
            .await
            .unwrap();
    }
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();

    client
        .simple_query("CREATE INDEX idx_readings_value ON readings (value)")
        .await
        .expect("CREATE INDEX backfill should succeed");
    let op_id_before = catalog
        .get_index("idx_readings_value")
        .and_then(|e| e.op_id)
        .expect("Ready index must carry an op_id before restart");
    handle.abort();

    let (port2, _handle2, shard_db2) = start_gateway(shard_path, store, catalog.clone()).await;
    let client2 = connect_port(port2).await;
    shard_db2.flush().await.unwrap();

    let entry = catalog
        .get_index("idx_readings_value")
        .expect("index catalog entry must survive restart");
    assert_eq!(
        entry.state,
        CatalogIndexState::Ready,
        "index must still be Ready after restart, not fall back to Building/full-scan"
    );
    assert_eq!(
        entry.op_id,
        Some(op_id_before),
        "the minted op_id must be unchanged across restart"
    );

    let msgs = client2
        .simple_query("SELECT id, value FROM readings WHERE value = 20")
        .await
        .expect("point lookup after restart");
    let rows = data_rows(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("value"), Some("20"));
}

#[tokio::test]
async fn backfilled_index_persists_across_reconnect_minio() {
    if !docker_available() {
        eprintln!("SKIP backfilled_index_persists_across_reconnect_minio: Docker not available");
        return;
    }
    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let bucket = "rockstream-create-index-backfill-reconnect-test";
    create_minio_bucket(port, bucket).await;
    run_backfilled_index_reconnect_case(
        minio_object_store(port, bucket),
        "create-index-backfill-durability-minio-reconnect",
    )
    .await;
}

#[tokio::test]
async fn index_ready_state_persists_across_restart_minio() {
    if !docker_available() {
        eprintln!("SKIP index_ready_state_persists_across_restart_minio: Docker not available");
        return;
    }
    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let bucket = "rockstream-create-index-backfill-restart-test";
    create_minio_bucket(port, bucket).await;
    run_index_ready_state_restart_case(
        minio_object_store(port, bucket),
        "create-index-backfill-durability-minio-restart",
    )
    .await;
}
