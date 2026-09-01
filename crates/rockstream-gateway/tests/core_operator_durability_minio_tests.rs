use std::collections::HashMap;
use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
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

async fn gateway(
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

async fn client(port: u16) -> tokio_postgres::Client {
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

async fn create_bucket(port: u16, bucket: &str) {
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
async fn compiled_join_state_persists_across_restart_minio() {
    if !rockstream_test_support::docker_available() {
        eprintln!("SKIP compiled_join_state_persists_across_restart_minio: Docker is not available locally");
        return;
    }
    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let bucket = "rockstream-core-join-durability";
    create_bucket(port, bucket).await;
    let store = s3_store(port, bucket);
    let catalog = Arc::new(CatalogStubs::new());
    let (port1, handle1, db1) = gateway("core-join-minio", store.clone(), catalog.clone()).await;
    let client1 = client(port1).await;
    client1
        .batch_execute(
            "CREATE TABLE a (id BIGINT, k BIGINT); \
             CREATE TABLE b (id BIGINT, k BIGINT, val BIGINT); \
             CREATE VIEW joined AS SELECT a.id, a.k, b.id, b.val FROM a JOIN b ON a.k = b.k; \
             INSERT INTO a VALUES (1, 100);",
        )
        .await
        .unwrap();
    db1.flush().await.unwrap();
    handle1.abort();

    let (port2, _handle2, db2) = gateway("core-join-minio", store, catalog).await;
    let client2 = client(port2).await;
    client2
        .simple_query("INSERT INTO b VALUES (2, 100, 999)")
        .await
        .unwrap();
    db2.flush().await.unwrap();
    let rows = client2
        .simple_query("SELECT * FROM joined")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).unwrap_or("").to_owned())
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![vec!["1", "100", "2", "999"]]);
}

#[tokio::test]
async fn compiled_tumble_window_state_persists_across_restart_minio() {
    if !rockstream_test_support::docker_available() {
        eprintln!("SKIP compiled_tumble_window_state_persists_across_restart_minio: Docker is not available locally");
        return;
    }
    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let bucket = "rockstream-core-window-durability";
    create_bucket(port, bucket).await;
    let store = s3_store(port, bucket);
    let catalog = Arc::new(CatalogStubs::new());
    let (port1, handle1, db1) = gateway("core-window-minio", store.clone(), catalog.clone()).await;
    let client1 = client(port1).await;
    client1
        .batch_execute(
            "CREATE TABLE events (id BIGINT, price BIGINT, date_time BIGINT); \
             CREATE MATERIALIZED VIEW windows AS \
             SELECT CAST(date_bin(INTERVAL '10 seconds', CAST(date_time AS TIMESTAMP)) AS BIGINT), \
             SUM(price) FROM events GROUP BY date_bin(INTERVAL '10 seconds', CAST(date_time AS TIMESTAMP)); \
             INSERT INTO events VALUES (1, 100, 1), (2, 50, 15);",
        )
        .await
        .unwrap();
    db1.flush().await.unwrap();
    handle1.abort();

    let (port2, _handle2, db2) = gateway("core-window-minio", store, catalog).await;
    let client2 = client(port2).await;
    db2.flush().await.unwrap();
    let mut results = HashMap::new();
    for message in client2.simple_query("SELECT * FROM windows").await.unwrap() {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            results.insert(
                row.get(0).unwrap().parse::<i64>().unwrap(),
                row.get(1).unwrap().parse::<i64>().unwrap(),
            );
        }
    }
    assert_eq!(results, HashMap::from([(0, 100), (10_000_000_000, 50)]));
}
