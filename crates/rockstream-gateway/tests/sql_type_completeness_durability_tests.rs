//! Type Completeness: All Types Arrangement Durability Tests across LFS & MinIO (v0.59.20 Slice 7 / Phase 3b).

use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use tempfile::TempDir;
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

async fn start_gateway(
    path: &str,
    store: Arc<dyn ObjectStore>,
    catalog: Arc<CatalogStubs>,
) -> (u16, tokio::task::JoinHandle<()>, Arc<ShardDb>) {
    let db = Arc::new(ShardDb::builder(path, store).build().await.unwrap());
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        db.clone(),
    );
    let (addr, handle) = server.serve_background().await.unwrap();
    (addr.port(), handle, db)
}

async fn connect_client(port: u16) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test]
async fn test_all_types_arrangement_durability_lfs() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());

    // Phase 1: Initialize table with text, temporal, uuid, decimal keys and insert records
    let (port1, handle1, db1) =
        start_gateway("type-durability-lfs", store.clone(), catalog.clone()).await;
    let client1 = connect_client(port1).await;

    client1
        .simple_query(
            "CREATE TABLE t_all_types (\
                id BIGINT, \
                name TEXT, \
                d DATE, \
                ts TIMESTAMP, \
                uid UUID, \
                amount DECIMAL(18, 4)\
            )",
        )
        .await
        .unwrap();

    client1
        .simple_query(
            "INSERT INTO t_all_types VALUES (\
                1, \
                'rockstream_v1', \
                '2026-09-01', \
                '2026-09-01 12:00:00', \
                'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', \
                9999.5000\
            )",
        )
        .await
        .unwrap();

    db1.flush().await.unwrap();
    handle1.abort();

    // Phase 2: Restart gateway on same LFS store and verify persistent state
    let (port2, handle2, _db2) = start_gateway("type-durability-lfs", store, catalog).await;
    let client2 = connect_client(port2).await;

    let res = client2
        .simple_query("SELECT id, name, d, uid, amount FROM t_all_types WHERE id = 1")
        .await
        .unwrap();

    let mut row_found = false;
    for msg in res {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0).unwrap(), "1");
            assert_eq!(row.get(1).unwrap(), "rockstream_v1");
            assert_eq!(row.get(2).unwrap(), "2026-09-01");
            assert_eq!(row.get(3).unwrap(), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
            assert_eq!(row.get(4).unwrap(), "9999.5000");
            row_found = true;
        }
    }
    assert!(row_found, "Row must persist across ShardDb restart on LFS");

    handle2.abort();
}

fn s3_store(port: u16, bucket: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://127.0.0.1:{port}"))
            .with_bucket_name(bucket)
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .unwrap(),
    )
}

async fn create_minio_bucket(port: u16, bucket: &str) {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    type HmacSha256 = Hmac<Sha256>;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let days = now / 86_400;
    let mut year = 1970;
    let mut remaining = days;
    loop {
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let length = if leap { 366 } else { 365 };
        if remaining < length {
            break;
        }
        remaining -= length;
        year += 1;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_lengths = [
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
    let mut month = 1;
    for length in month_lengths {
        if remaining < length {
            break;
        }
        remaining -= length;
        month += 1;
    }
    let day = remaining + 1;
    let seconds = now % 86_400;
    let datetime = format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        seconds / 3600,
        (seconds % 3600) / 60,
        seconds % 60
    );
    let date = &datetime[..8];
    let host = format!("127.0.0.1:{port}");
    let empty_hash = format!("{:x}", Sha256::digest(b""));
    let canonical = format!(
        "PUT\n/{bucket}\n\nhost:{host}\nx-amz-content-sha256:{empty_hash}\n\
         x-amz-date:{datetime}\n\nhost;x-amz-content-sha256;x-amz-date\n{empty_hash}"
    );
    let canonical_hash = format!("{:x}", Sha256::digest(canonical.as_bytes()));
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{canonical_hash}");
    let sign = |key: &[u8], data: &[u8]| {
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    };
    let k1 = sign(b"AWS4minioadmin", date.as_bytes());
    let k2 = sign(&k1, b"us-east-1");
    let k3 = sign(&k2, b"s3");
    let signing_key = sign(&k3, b"aws4_request");
    let signature = hex::encode(sign(&signing_key, string_to_sign.as_bytes()));
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential=minioadmin/{scope}, \
         SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}"
    );
    let response = reqwest::Client::new()
        .put(format!("http://{host}/{bucket}"))
        .header("Host", &host)
        .header("X-Amz-Content-Sha256", &empty_hash)
        .header("X-Amz-Date", &datetime)
        .header("Authorization", auth)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success() || response.status().as_u16() == 409);
}

#[tokio::test]
async fn test_all_types_arrangement_durability_minio() {
    if !rockstream_test_support::docker_available() {
        eprintln!(
            "SKIP test_all_types_arrangement_durability_minio: Docker is not available locally"
        );
        return;
    }
    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let bucket = "rockstream-type-completeness-durability";
    create_minio_bucket(port, bucket).await;
    let store = s3_store(port, bucket);
    let catalog = Arc::new(CatalogStubs::new());

    let (port1, handle1, db1) =
        start_gateway("type-durability-minio", store.clone(), catalog.clone()).await;
    let client1 = connect_client(port1).await;

    client1
        .simple_query(
            "CREATE TABLE t_minio (\
                id BIGINT, \
                tag TEXT, \
                event_date DATE\
            )",
        )
        .await
        .unwrap();

    client1
        .simple_query("INSERT INTO t_minio VALUES (42, 'minio_persisted', '2026-09-01')")
        .await
        .unwrap();

    db1.flush().await.unwrap();
    handle1.abort();

    let (port2, handle2, _db2) = start_gateway("type-durability-minio", store, catalog).await;
    let client2 = connect_client(port2).await;

    let res = client2
        .simple_query("SELECT id, tag, event_date FROM t_minio WHERE id = 42")
        .await
        .unwrap();

    let mut found = false;
    for msg in res {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0).unwrap(), "42");
            assert_eq!(row.get(1).unwrap(), "minio_persisted");
            assert_eq!(row.get(2).unwrap(), "2026-09-01");
            found = true;
        }
    }
    assert!(found, "Row must persist across ShardDb restart on MinIO");

    handle2.abort();
}
