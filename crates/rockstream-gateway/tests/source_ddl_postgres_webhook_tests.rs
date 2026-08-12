//! Pgwire reachability and negative tests for the v0.51.14 source DDL surface.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use rockstream_connectors::{
    BackfillCursor, BackfillLifecycle, BackfillPhase, OffsetToken, SnapshotDeltaFence,
    SourceCheckpointStore,
};
use rockstream_gateway::{
    admission::{BackfillAdmissionController, BackfillAdmissionDecision},
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer, WebhookEpoch,
};
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::ids::ConnectorId;
use sha2::{Digest, Sha256};
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use testcontainers_modules::{
    kafka::apache::{self, KAFKA_PORT},
    minio::MinIO,
};

async fn read_order_total(client: &tokio_postgres::Client) -> Vec<String> {
    client
        .query("SELECT amount FROM order_total", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect()
}
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
        Ok(Vec::new())
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

async fn client() -> (tokio_postgres::Client, std::net::SocketAddr) {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (webhook_address, _webhook_handle) = server
        .serve_webhook_background("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let (address, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, webhook_address)
}

async fn shard_backed_client() -> (tokio_postgres::Client, std::net::SocketAddr, Arc<ShardDb>) {
    let catalog = Arc::new(CatalogStubs::new());
    let shard_db = Arc::new(
        ShardDb::builder(
            "webhook-durable-ingress",
            Arc::new(object_store::memory::InMemory::new()),
        )
        .build()
        .await
        .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let (webhook_address, _webhook_handle) = server
        .serve_webhook_background("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let (address, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, webhook_address, shard_db)
}

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";

fn minio_store(port: u16, bucket: &str) -> Arc<dyn ObjectStore> {
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

async fn create_minio_bucket(port: u16, bucket: &str) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let days = secs / 86_400;
    let seconds = secs % 86_400;
    let mut year = 1970_u32;
    let mut remaining = days as u32;
    loop {
        let leap =
            year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
        let year_days = if leap { 366 } else { 365 };
        if remaining < year_days {
            break;
        }
        remaining -= year_days;
        year += 1;
    }
    let month_days = [
        31,
        if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) {
            29
        } else {
            28
        },
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
    let mut month = 0_usize;
    while remaining >= month_days[month] {
        remaining -= month_days[month];
        month += 1;
    }
    let date = format!("{year:04}{:02}{:02}", month + 1, remaining + 1);
    let datetime = format!(
        "{date}T{:02}{:02}{:02}Z",
        seconds / 3_600,
        (seconds % 3_600) / 60,
        seconds % 60
    );
    let host = format!("127.0.0.1:{port}");
    let payload_hash = format!("{:x}", Sha256::digest(b""));
    let canonical = format!(
        "PUT\n/{bucket}\n\nhost:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{datetime}\n\nhost;x-amz-content-sha256;x-amz-date\n{payload_hash}"
    );
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{:x}",
        Sha256::digest(canonical.as_bytes())
    );
    let sign = |key: &[u8], data: &[u8]| {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    };
    let k1 = sign(b"AWS4minioadmin", date.as_bytes());
    let k2 = sign(&k1, b"us-east-1");
    let k3 = sign(&k2, b"s3");
    let signature = hex::encode(sign(&sign(&k3, b"aws4_request"), string_to_sign.as_bytes()));
    let status = reqwest::Client::new()
        .put(format!("http://{host}/{bucket}"))
        .header("host", &host)
        .header("x-amz-date", datetime)
        .header("x-amz-content-sha256", payload_hash)
        .header(
            "authorization",
            format!(
                "AWS4-HMAC-SHA256 Credential={MINIO_USER}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}"
            ),
        )
        .header("content-length", "0")
        .send()
        .await
        .unwrap()
        .status();
    assert!(
        status.is_success() || status.as_u16() == 409,
        "bucket: {status}"
    );
}

#[tokio::test]
async fn create_source_rejects_bad_options_and_redacts_credentials() {
    let (client, _) = client().await;
    client
        .execute(
            "CREATE SOURCE orders TYPE postgres_cdc (credential_ref='vault://pg/orders', publication='orders_pub', slot='orders_slot') FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "CREATE SOURCE inbound TYPE http_webhook (credential_ref='vault://webhook/inbound') FORMAT json;",
            &[],
        )
        .await
        .unwrap();

    let rows = client.query("SHOW SOURCES;", &[]).await.unwrap();
    let exact = rows
        .iter()
        .map(|row| {
            vec![
                row.get::<_, String>(0),
                row.get::<_, String>(1),
                row.get::<_, String>(2),
                row.get::<_, String>(3),
                row.get::<_, String>(4),
                row.get::<_, String>(5),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        exact,
        vec![
            vec!["inbound", "http_webhook", "json", "OK", "0", "0"],
            vec!["orders", "postgres_cdc", "pgoutput", "OK", "0", "0"],
        ]
    );

    for sql in [
        "CREATE SOURCE wrong_format TYPE postgres_cdc (credential_ref='vault://pg/orders', publication='p', slot='s') FORMAT json;",
        "CREATE SOURCE missing_ref TYPE postgres_cdc (publication='p', slot='s') FORMAT pgoutput;",
        "CREATE SOURCE inline_secret TYPE http_webhook (token='not-a-reference') FORMAT json;",
    ] {
        let error = client.execute(sql, &[]).await.unwrap_err();
        let message = error.as_db_error().map(|error| error.message()).unwrap_or("");
        assert!(message.contains("RS-4008"), "unexpected error: {message}");
        assert!(message.contains("Next steps:"), "unexpected error: {message}");
    }
}

#[tokio::test]
async fn s3_source_backfill_reaches_pgwire_and_publishes_exact_rows_minio() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the S3 source backfill proof"
    );
    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let source_bucket = "gateway-source-input-v0521";
    let state_bucket = "gateway-source-state-v0521";
    create_minio_bucket(port, source_bucket).await;
    create_minio_bucket(port, state_bucket).await;
    minio_store(port, source_bucket)
        .put(
            &Path::from("input/orders.json"),
            Bytes::from_static(br#"[[1,"a","10.25"],[2,"a","20.50"]]"#).into(),
        )
        .await
        .unwrap();

    let catalog = Arc::new(CatalogStubs::new());
    let shard_db = Arc::new(
        ShardDb::builder(
            "gateway-source-backfill-v0521",
            minio_store(port, state_bucket),
        )
        .build()
        .await
        .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
        shard_db,
    );
    let (address, handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    for sql in [
        "CREATE TABLE orders (id BIGINT, customer TEXT, amount DECIMAL(12,2))",
        &format!(
            "CREATE SOURCE orders TYPE s3 (bucket='{source_bucket}', prefix='input', endpoint='http://127.0.0.1:{port}', access_key='{MINIO_USER}', secret_key='{MINIO_PASS}', region='us-east-1') FORMAT json"
        ),
        "CREATE MATERIALIZED VIEW order_rows AS SELECT customer, SUM(amount) AS amount FROM orders GROUP BY customer",
    ] {
        client.execute(sql, &[]).await.unwrap();
    }
    let output = client
        .query(
            "SELECT customer, amount FROM order_rows ORDER BY customer",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(output, vec![("a".to_string(), "30.75".to_string())]);
    let status = client
        .query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW order_rows", &[])
        .await
        .unwrap();
    let status = status
        .into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<String>>(0),
                row.get::<_, Option<String>>(1),
                row.get::<_, Option<String>>(2),
                row.get::<_, Option<String>>(3),
                row.get::<_, Option<String>>(4),
                row.get::<_, Option<String>>(5),
                row.get::<_, Option<String>>(6),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        status,
        vec![vec![
            Some("order_rows".to_string()),
            Some("RUNNING".to_string()),
            Some("2".to_string()),
            Some("0".to_string()),
            Some("2".to_string()),
            Some("ADMITTED".to_string()),
            None,
        ]]
    );
    minio_store(port, source_bucket)
        .put(
            &Path::from("input/later.json"),
            Bytes::from_static(br#"[[3,"a","5.25"]]"#).into(),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let live_output = loop {
        let rows = client
            .query(
                "SELECT customer, amount FROM order_rows ORDER BY customer",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        if rows == vec![("a".to_string(), "36".to_string())] {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "S3 worker did not ingest later object"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(live_output, vec![("a".to_string(), "36".to_string())]);
    let live_status = client
        .query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW order_rows", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<String>>(0),
                row.get::<_, Option<String>>(1),
                row.get::<_, Option<String>>(2),
                row.get::<_, Option<String>>(3),
                row.get::<_, Option<String>>(4),
                row.get::<_, Option<String>>(5),
                row.get::<_, Option<String>>(6),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        live_status,
        vec![vec![
            Some("order_rows".to_string()),
            Some("RUNNING".to_string()),
            Some("3".to_string()),
            Some("0".to_string()),
            Some("2".to_string()),
            Some("ADMITTED".to_string()),
            None,
        ]]
    );
    let persisted_table = catalog.get_table("orders").unwrap();
    let persisted_source = catalog.get_source("orders").unwrap();
    let persisted_view = catalog.get_view("order_rows").unwrap();
    drop(client);
    handle.abort();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let recovered_catalog = Arc::new(CatalogStubs::new());
    assert!(recovered_catalog.add_table(persisted_table));
    assert!(recovered_catalog.add_source(persisted_source));
    recovered_catalog.add_view_with_deps(persisted_view, vec!["orders".to_string()]);

    let restarted = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        recovered_catalog,
        Arc::new(NoopViewReader),
        Arc::new(
            ShardDb::builder(
                "gateway-source-backfill-v0521",
                minio_store(port, state_bucket),
            )
            .build()
            .await
            .unwrap(),
        ),
    );
    let (restarted_address, _restarted_handle) = restarted.serve_background().await.unwrap();
    let (restarted_client, restarted_connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            restarted_address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = restarted_connection.await;
    });
    let recovered_output = restarted_client
        .query(
            "SELECT customer, amount FROM order_rows ORDER BY customer",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(recovered_output, vec![("a".to_string(), "36".to_string())]);
    minio_store(port, source_bucket)
        .put(
            &Path::from("input/later-after-restart.json"),
            Bytes::from_static(br#"[[4,"a","7.00"]]"#).into(),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let post_restart_live_output = loop {
        let rows = restarted_client
            .query(
                "SELECT customer, amount FROM order_rows ORDER BY customer",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        if rows == vec![("a".to_string(), "43".to_string())] {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "restarted S3 worker did not ingest later object"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        post_restart_live_output,
        vec![("a".to_string(), "43".to_string())]
    );
    let recovered_status = restarted_client
        .query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW order_rows", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<String>>(0),
                row.get::<_, Option<String>>(1),
                row.get::<_, Option<String>>(2),
                row.get::<_, Option<String>>(3),
                row.get::<_, Option<String>>(4),
                row.get::<_, Option<String>>(5),
                row.get::<_, Option<String>>(6),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        recovered_status,
        vec![vec![
            Some("order_rows".to_string()),
            Some("RUNNING".to_string()),
            Some("4".to_string()),
            Some("0".to_string()),
            Some("2".to_string()),
            Some("ADMITTED".to_string()),
            None,
        ]]
    );
    minio_store(port, source_bucket)
        .put(
            &Path::from("input/delete-after-restart.json"),
            Bytes::from_static(br#"[{"values":[4,"a","7.00"],"weight":-1}]"#).into(),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let retracted = loop {
        let rows = restarted_client
            .query(
                "SELECT customer, amount FROM order_rows ORDER BY customer",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        if rows == vec![("a".to_string(), "36".to_string())] {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "restarted S3 worker did not retract the weighted source row"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(retracted, vec![("a".to_string(), "36".to_string())]);
    let retracted_status = restarted_client
        .query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW order_rows", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<String>>(0),
                row.get::<_, Option<String>>(1),
                row.get::<_, Option<String>>(2),
                row.get::<_, Option<String>>(3),
                row.get::<_, Option<String>>(4),
                row.get::<_, Option<String>>(5),
                row.get::<_, Option<String>>(6),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        retracted_status,
        vec![vec![
            Some("order_rows".to_string()),
            Some("RUNNING".to_string()),
            Some("5".to_string()),
            Some("0".to_string()),
            Some("2".to_string()),
            Some("ADMITTED".to_string()),
            None,
        ]]
    );
    minio_store(port, source_bucket)
        .put(
            &Path::from("input/update-after-restart.json"),
            Bytes::from_static(
                br#"[{"values":[3,"a","5.25"],"weight":-1},{"values":[3,"a","9.25"],"weight":1}]"#,
            )
            .into(),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let updated = loop {
        let rows = restarted_client
            .query(
                "SELECT customer, amount FROM order_rows ORDER BY customer",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        if rows == vec![("a".to_string(), "40".to_string())] {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "restarted S3 worker did not apply the weighted update atomically"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(updated, vec![("a".to_string(), "40".to_string())]);
    let updated_status = restarted_client
        .query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW order_rows", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<String>>(0),
                row.get::<_, Option<String>>(1),
                row.get::<_, Option<String>>(2),
                row.get::<_, Option<String>>(3),
                row.get::<_, Option<String>>(4),
                row.get::<_, Option<String>>(5),
                row.get::<_, Option<String>>(6),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        updated_status,
        vec![vec![
            Some("order_rows".to_string()),
            Some("RUNNING".to_string()),
            Some("6".to_string()),
            Some("0".to_string()),
            Some("2".to_string()),
            Some("ADMITTED".to_string()),
            None,
        ]]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_preserves_running_view_freshness_slo_multi_worker() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the concurrent-backfill freshness proof"
    );
    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let input_bucket = "gateway-freshness-input-v0521";
    let state_bucket = "gateway-freshness-state-v0521";
    create_minio_bucket(port, input_bucket).await;
    create_minio_bucket(port, state_bucket).await;
    let input = minio_store(port, input_bucket);
    input
        .put(
            &Path::from("running/initial.json"),
            Bytes::from_static(br#"[[1,1,1]]"#).into(),
        )
        .await
        .unwrap();
    let bulk_rows = (0_i64..50_000).map(|id| vec![id, 1, 1]).collect::<Vec<_>>();
    input
        .put(
            &Path::from("bulk/initial.json"),
            Bytes::from(serde_json::to_vec(&bulk_rows).unwrap()).into(),
        )
        .await
        .unwrap();
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        Arc::new(
            ShardDb::builder(
                "gateway-backfill-freshness-v0521",
                minio_store(port, state_bucket),
            )
            .build()
            .await
            .unwrap(),
        ),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
    let connect = || async {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=test dbname=test",
                address.port()
            ),
            NoTls,
        )
        .await
        .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    };
    let client = connect().await;
    for sql in [
        "CREATE TABLE running_input (id BIGINT, k BIGINT, amount BIGINT)",
        &format!(
            "CREATE SOURCE running_input TYPE s3 (bucket='{input_bucket}', prefix='running', endpoint='http://127.0.0.1:{port}', access_key='{MINIO_USER}', secret_key='{MINIO_PASS}', region='us-east-1') FORMAT json"
        ),
        "CREATE MATERIALIZED VIEW running_result AS SELECT k, SUM(amount) AS amount FROM running_input GROUP BY k",
    ] {
        client.execute(sql, &[]).await.unwrap();
    }
    let initial = client
        .query("SELECT k, amount FROM running_result", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(initial, vec![("1".to_string(), "1".to_string())]);

    let bulk_client = connect().await;
    for sql in [
        "CREATE TABLE bulk_input (id BIGINT, k BIGINT, amount BIGINT)",
        &format!(
            "CREATE SOURCE bulk_input TYPE s3 (bucket='{input_bucket}', prefix='bulk', endpoint='http://127.0.0.1:{port}', access_key='{MINIO_USER}', secret_key='{MINIO_PASS}', region='us-east-1') FORMAT json"
        ),
    ] {
        bulk_client.execute(sql, &[]).await.unwrap();
    }
    let bulk_create = tokio::spawn(async move {
        bulk_client
            .execute(
                "CREATE MATERIALIZED VIEW bulk_result AS SELECT k, SUM(amount) AS amount FROM bulk_input GROUP BY k",
                &[],
            )
            .await
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if client
            .query(
                "SHOW BACKFILL STATUS FOR MATERIALIZED VIEW bulk_result",
                &[],
            )
            .await
            .is_ok()
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "bulk source backfill did not become observable"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !bulk_create.is_finished(),
        "bulk source backfill completed before the freshness check could run"
    );
    let fresh_started = Instant::now();
    input
        .put(
            &Path::from("running/later.json"),
            Bytes::from_static(br#"[[2,1,5]]"#).into(),
        )
        .await
        .unwrap();
    let fresh = loop {
        let rows = client
            .query("SELECT k, amount FROM running_result", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        if rows == vec![("1".to_string(), "6".to_string())] {
            break rows;
        }
        assert!(
            fresh_started.elapsed() < Duration::from_secs(2),
            "running view missed its 2s freshness SLO during the bulk backfill"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(fresh, vec![("1".to_string(), "6".to_string())]);
    bulk_create.await.unwrap().unwrap();
    let bulk = client
        .query("SELECT k, amount FROM bulk_result", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(bulk, vec![("1".to_string(), "50000".to_string())]);
}

#[tokio::test]
async fn two_s3_sources_publish_a_join_only_after_both_snapshots_complete() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the multi-source backfill proof"
    );
    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let input_bucket = "gateway-join-input-v0521";
    let state_bucket = "gateway-join-state-v0521";
    create_minio_bucket(port, input_bucket).await;
    create_minio_bucket(port, state_bucket).await;
    let input = minio_store(port, input_bucket);
    input
        .put(
            &Path::from("left/initial.json"),
            Bytes::from_static(br#"[[1,1,10],[2,2,20]]"#).into(),
        )
        .await
        .unwrap();
    input
        .put(
            &Path::from("right/initial.json"),
            Bytes::from_static(br#"[[3,1,100],[4,3,300]]"#).into(),
        )
        .await
        .unwrap();
    let catalog = Arc::new(CatalogStubs::new());
    let shard_db = Arc::new(
        ShardDb::builder(
            "gateway-two-source-join-v0521",
            minio_store(port, state_bucket),
        )
        .build()
        .await
        .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
        Arc::clone(&shard_db),
    );
    let (address, handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    for sql in [
        "CREATE TABLE left_input (id BIGINT, k BIGINT, v BIGINT)",
        "CREATE TABLE right_input (id BIGINT, k BIGINT, v BIGINT)",
        &format!(
            "CREATE SOURCE left_input TYPE s3 (bucket='{input_bucket}', prefix='left', endpoint='http://127.0.0.1:{port}', access_key='{MINIO_USER}', secret_key='{MINIO_PASS}', region='us-east-1') FORMAT json"
        ),
        &format!(
            "CREATE SOURCE right_input TYPE s3 (bucket='{input_bucket}', prefix='right', endpoint='http://127.0.0.1:{port}', access_key='{MINIO_USER}', secret_key='{MINIO_PASS}', region='us-east-1') FORMAT json"
        ),
        "CREATE MATERIALIZED VIEW result AS SELECT l.k, l.v AS left_value, r.v AS right_value FROM left_input l INNER JOIN right_input r ON l.k = r.k",
    ] {
        client.execute(sql, &[]).await.unwrap();
    }
    let rows = client
        .query(
            "SELECT k, left_value, right_value FROM result ORDER BY k",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<String>>(0),
                row.get::<_, Option<String>>(1),
                row.get::<_, Option<String>>(2),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![vec![
            Some("1".to_string()),
            Some("10".to_string()),
            Some("100".to_string()),
        ]]
    );
    input
        .put(
            &Path::from("left/later.json"),
            Bytes::from_static(br#"[[5,3,30]]"#).into(),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let live = loop {
        let rows = client
            .query(
                "SELECT k, left_value, right_value FROM result ORDER BY k",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                vec![
                    row.get::<_, Option<String>>(0),
                    row.get::<_, Option<String>>(1),
                    row.get::<_, Option<String>>(2),
                ]
            })
            .collect::<Vec<_>>();
        let expected = vec![
            vec![
                Some("1".to_string()),
                Some("10".to_string()),
                Some("100".to_string()),
            ],
            vec![
                Some("3".to_string()),
                Some("30".to_string()),
                Some("300".to_string()),
            ],
        ];
        if rows == expected {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "left source worker did not refresh the published multi-source join"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        live,
        vec![
            vec![
                Some("1".to_string()),
                Some("10".to_string()),
                Some("100".to_string()),
            ],
            vec![
                Some("3".to_string()),
                Some("30".to_string()),
                Some("300".to_string()),
            ],
        ]
    );
    let status = client
        .query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW result", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<String>>(0),
                row.get::<_, Option<String>>(1),
                row.get::<_, Option<String>>(2),
                row.get::<_, Option<String>>(3),
                row.get::<_, Option<String>>(4),
                row.get::<_, Option<String>>(5),
                row.get::<_, Option<String>>(6),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        status,
        vec![vec![
            Some("result".to_string()),
            Some("RUNNING".to_string()),
            Some("left_input:3,right_input:2".to_string()),
            Some("0".to_string()),
            Some("4".to_string()),
            Some("ADMITTED".to_string()),
            None,
        ]]
    );
    let persisted_left = catalog.get_table("left_input").unwrap();
    let persisted_right = catalog.get_table("right_input").unwrap();
    let persisted_left_source = catalog.get_source("left_input").unwrap();
    let persisted_right_source = catalog.get_source("right_input").unwrap();
    let persisted_view = catalog.get_view("result").unwrap();
    drop(client);
    handle.abort();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let recovered_catalog = Arc::new(CatalogStubs::new());
    assert!(recovered_catalog.add_table(persisted_left));
    assert!(recovered_catalog.add_table(persisted_right));
    assert!(recovered_catalog.add_source(persisted_left_source));
    assert!(recovered_catalog.add_source(persisted_right_source));
    recovered_catalog.add_view_with_deps(
        persisted_view,
        vec!["left_input".to_string(), "right_input".to_string()],
    );
    let restarted = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        recovered_catalog,
        Arc::new(NoopViewReader),
        shard_db,
    );
    let (restarted_address, _restarted_handle) = restarted.serve_background().await.unwrap();
    let (restarted_client, restarted_connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            restarted_address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = restarted_connection.await;
    });
    let recovered = restarted_client
        .query(
            "SELECT k, left_value, right_value FROM result ORDER BY k",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<String>>(0),
                row.get::<_, Option<String>>(1),
                row.get::<_, Option<String>>(2),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(recovered, live);
    input
        .put(
            &Path::from("right/later.json"),
            Bytes::from_static(br#"[[6,2,200]]"#).into(),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let after_restart = loop {
        let rows = restarted_client
            .query(
                "SELECT k, left_value, right_value FROM result ORDER BY k",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                vec![
                    row.get::<_, Option<String>>(0),
                    row.get::<_, Option<String>>(1),
                    row.get::<_, Option<String>>(2),
                ]
            })
            .collect::<Vec<_>>();
        let expected = vec![
            vec![
                Some("1".to_string()),
                Some("10".to_string()),
                Some("100".to_string()),
            ],
            vec![
                Some("2".to_string()),
                Some("20".to_string()),
                Some("200".to_string()),
            ],
            vec![
                Some("3".to_string()),
                Some("30".to_string()),
                Some("300".to_string()),
            ],
        ];
        if rows == expected {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "right source worker did not recover after the multi-source restart"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        after_restart,
        vec![
            vec![
                Some("1".to_string()),
                Some("10".to_string()),
                Some("100".to_string()),
            ],
            vec![
                Some("2".to_string()),
                Some("20".to_string()),
                Some("200".to_string()),
            ],
            vec![
                Some("3".to_string()),
                Some("30".to_string()),
                Some("300".to_string()),
            ],
        ]
    );
}

#[tokio::test]
async fn two_s3_text_sources_drive_full_join_retractions() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the multi-source full-join proof"
    );
    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let input_bucket = "gateway-full-join-input-v0521";
    let state_bucket = "gateway-full-join-state-v0521";
    create_minio_bucket(port, input_bucket).await;
    create_minio_bucket(port, state_bucket).await;
    let input = minio_store(port, input_bucket);
    input
        .put(
            &Path::from("left/initial.json"),
            Bytes::from_static(br#"[[1,"a",10],[2,"b",20]]"#).into(),
        )
        .await
        .unwrap();
    input
        .put(
            &Path::from("right/initial.json"),
            Bytes::from_static(br#"[[3,"a",100],[4,"c",300]]"#).into(),
        )
        .await
        .unwrap();
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        Arc::new(
            ShardDb::builder(
                "gateway-two-source-full-join-v0521",
                minio_store(port, state_bucket),
            )
            .build()
            .await
            .unwrap(),
        ),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    for sql in [
        "CREATE TABLE left_input (id BIGINT, k TEXT, v BIGINT)",
        "CREATE TABLE right_input (id BIGINT, k TEXT, v BIGINT)",
        &format!(
            "CREATE SOURCE left_input TYPE s3 (bucket='{input_bucket}', prefix='left', endpoint='http://127.0.0.1:{port}', access_key='{MINIO_USER}', secret_key='{MINIO_PASS}', region='us-east-1') FORMAT json"
        ),
        &format!(
            "CREATE SOURCE right_input TYPE s3 (bucket='{input_bucket}', prefix='right', endpoint='http://127.0.0.1:{port}', access_key='{MINIO_USER}', secret_key='{MINIO_PASS}', region='us-east-1') FORMAT json"
        ),
        "CREATE MATERIALIZED VIEW result AS SELECT l.k, l.v AS left_value, r.v AS right_value FROM left_input l FULL JOIN right_input r ON l.k = r.k",
    ] {
        client.execute(sql, &[]).await.unwrap();
    }
    let rows = client
        .query(
            "SELECT k, left_value, right_value FROM result ORDER BY k NULLS FIRST",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<String>>(0),
                row.get::<_, Option<String>>(1),
                row.get::<_, Option<String>>(2),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            vec![None, None, Some("300".to_string())],
            vec![
                Some("a".to_string()),
                Some("10".to_string()),
                Some("100".to_string()),
            ],
            vec![Some("b".to_string()), Some("20".to_string()), None],
        ]
    );
    input
        .put(
            &Path::from("right/later.json"),
            Bytes::from_static(br#"[[5,"b",200]]"#).into(),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let live = loop {
        let rows = client
            .query(
                "SELECT k, left_value, right_value FROM result ORDER BY k NULLS FIRST",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                vec![
                    row.get::<_, Option<String>>(0),
                    row.get::<_, Option<String>>(1),
                    row.get::<_, Option<String>>(2),
                ]
            })
            .collect::<Vec<_>>();
        let expected = vec![
            vec![None, None, Some("300".to_string())],
            vec![
                Some("a".to_string()),
                Some("10".to_string()),
                Some("100".to_string()),
            ],
            vec![
                Some("b".to_string()),
                Some("20".to_string()),
                Some("200".to_string()),
            ],
        ];
        if rows == expected {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "right source worker did not retract and replace the unmatched text row"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        live,
        vec![
            vec![None, None, Some("300".to_string())],
            vec![
                Some("a".to_string()),
                Some("10".to_string()),
                Some("100".to_string()),
            ],
            vec![
                Some("b".to_string()),
                Some("20".to_string()),
                Some("200".to_string()),
            ],
        ]
    );
}

#[tokio::test]
async fn s3_source_backfill_updates_tumbling_window_exactly() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the source-backed tumbling-window proof"
    );
    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let input_bucket = "gateway-window-input-v0521";
    let state_bucket = "gateway-window-state-v0521";
    create_minio_bucket(port, input_bucket).await;
    create_minio_bucket(port, state_bucket).await;
    let input = minio_store(port, input_bucket);
    input
        .put(
            &Path::from("input/initial.json"),
            Bytes::from_static(br#"[[1,1,0,10],[2,1,5,20],[3,1,11,30]]"#).into(),
        )
        .await
        .unwrap();
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        Arc::new(
            ShardDb::builder(
                "gateway-source-window-v0521",
                minio_store(port, state_bucket),
            )
            .build()
            .await
            .unwrap(),
        ),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    for sql in [
        "CREATE TABLE input (id BIGINT, k BIGINT, date_time BIGINT, v BIGINT)",
        &format!(
            "CREATE SOURCE input TYPE s3 (bucket='{input_bucket}', prefix='input', endpoint='http://127.0.0.1:{port}', access_key='{MINIO_USER}', secret_key='{MINIO_PASS}', region='us-east-1') FORMAT json"
        ),
        "CREATE MATERIALIZED VIEW result AS SELECT CAST(date_bin(INTERVAL '10 seconds', CAST(date_time AS TIMESTAMP)) AS BIGINT) AS window_start, k, COUNT(v) AS value FROM input GROUP BY date_bin(INTERVAL '10 seconds', CAST(date_time AS TIMESTAMP)), k",
    ] {
        client.execute(sql, &[]).await.unwrap();
    }
    let initial = client
        .query(
            "SELECT window_start, k, value FROM result ORDER BY window_start",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<String>>(0),
                row.get::<_, Option<String>>(1),
                row.get::<_, Option<String>>(2),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        initial,
        vec![
            vec![
                Some("0".to_string()),
                Some("1".to_string()),
                Some("2".to_string()),
            ],
            vec![
                Some("10000000000".to_string()),
                Some("1".to_string()),
                Some("1".to_string()),
            ],
        ]
    );
    input
        .put(
            &Path::from("input/later.json"),
            Bytes::from_static(br#"[[4,1,12,40]]"#).into(),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let live = loop {
        let rows = client
            .query(
                "SELECT window_start, k, value FROM result ORDER BY window_start",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                vec![
                    row.get::<_, Option<String>>(0),
                    row.get::<_, Option<String>>(1),
                    row.get::<_, Option<String>>(2),
                ]
            })
            .collect::<Vec<_>>();
        let expected = vec![
            vec![
                Some("0".to_string()),
                Some("1".to_string()),
                Some("2".to_string()),
            ],
            vec![
                Some("10000000000".to_string()),
                Some("1".to_string()),
                Some("2".to_string()),
            ],
        ];
        if rows == expected {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "source worker did not update the tumbling-window aggregate"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        live,
        vec![
            vec![
                Some("0".to_string()),
                Some("1".to_string()),
                Some("2".to_string()),
            ],
            vec![
                Some("10000000000".to_string()),
                Some("1".to_string()),
                Some("2".to_string()),
            ],
        ]
    );
}

#[tokio::test]
async fn s3_source_backfill_updates_row_number_exactly() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the source-backed ROW_NUMBER proof"
    );
    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let input_bucket = "gateway-row-number-input-v0521";
    let state_bucket = "gateway-row-number-state-v0521";
    create_minio_bucket(port, input_bucket).await;
    create_minio_bucket(port, state_bucket).await;
    let input = minio_store(port, input_bucket);
    input
        .put(
            &Path::from("input/initial.json"),
            Bytes::from_static(br#"[[1,1,20],[2,1,10],[3,2,5]]"#).into(),
        )
        .await
        .unwrap();
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        Arc::new(
            ShardDb::builder(
                "gateway-source-row-number-v0521",
                minio_store(port, state_bucket),
            )
            .build()
            .await
            .unwrap(),
        ),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    for sql in [
        "CREATE TABLE input (id BIGINT, k BIGINT, v BIGINT)",
        &format!(
            "CREATE SOURCE input TYPE s3 (bucket='{input_bucket}', prefix='input', endpoint='http://127.0.0.1:{port}', access_key='{MINIO_USER}', secret_key='{MINIO_PASS}', region='us-east-1') FORMAT json"
        ),
        "CREATE MATERIALIZED VIEW result AS SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn FROM input",
    ] {
        client.execute(sql, &[]).await.unwrap();
    }
    let initial = client
        .query("SELECT k, v, rn FROM result ORDER BY k, v", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<String>>(0),
                row.get::<_, Option<String>>(1),
                row.get::<_, Option<String>>(2),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        initial,
        vec![
            vec![
                Some("1".to_string()),
                Some("10".to_string()),
                Some("1".to_string()),
            ],
            vec![
                Some("1".to_string()),
                Some("20".to_string()),
                Some("2".to_string()),
            ],
            vec![
                Some("2".to_string()),
                Some("5".to_string()),
                Some("1".to_string()),
            ],
        ]
    );
    input
        .put(
            &Path::from("input/later.json"),
            Bytes::from_static(br#"[[4,1,15]]"#).into(),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let live = loop {
        let rows = client
            .query("SELECT k, v, rn FROM result ORDER BY k, v", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                vec![
                    row.get::<_, Option<String>>(0),
                    row.get::<_, Option<String>>(1),
                    row.get::<_, Option<String>>(2),
                ]
            })
            .collect::<Vec<_>>();
        let expected = vec![
            vec![
                Some("1".to_string()),
                Some("10".to_string()),
                Some("1".to_string()),
            ],
            vec![
                Some("1".to_string()),
                Some("15".to_string()),
                Some("2".to_string()),
            ],
            vec![
                Some("1".to_string()),
                Some("20".to_string()),
                Some("3".to_string()),
            ],
            vec![
                Some("2".to_string()),
                Some("5".to_string()),
                Some("1".to_string()),
            ],
        ];
        if rows == expected {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "source worker did not retract and renumber the live ROW_NUMBER partition"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        live,
        vec![
            vec![
                Some("1".to_string()),
                Some("10".to_string()),
                Some("1".to_string()),
            ],
            vec![
                Some("1".to_string()),
                Some("15".to_string()),
                Some("2".to_string()),
            ],
            vec![
                Some("1".to_string()),
                Some("20".to_string()),
                Some("3".to_string()),
            ],
            vec![
                Some("2".to_string()),
                Some("5".to_string()),
                Some("1".to_string()),
            ],
        ]
    );
}

#[tokio::test]
async fn s3_source_backfill_merges_session_windows_exactly() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the source-backed SESSION proof"
    );
    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let input_bucket = "gateway-session-input-v0521";
    let state_bucket = "gateway-session-state-v0521";
    create_minio_bucket(port, input_bucket).await;
    create_minio_bucket(port, state_bucket).await;
    let input = minio_store(port, input_bucket);
    input
        .put(
            &Path::from("input/initial.json"),
            Bytes::from_static(br#"[[1,1,0,10],[2,1,5000,20],[3,1,20000,30]]"#).into(),
        )
        .await
        .unwrap();
    let catalog = Arc::new(CatalogStubs::new());
    let shard_db = Arc::new(
        ShardDb::builder(
            "gateway-source-session-v0521",
            minio_store(port, state_bucket),
        )
        .build()
        .await
        .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
        Arc::clone(&shard_db),
    );
    let (address, handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    for sql in [
        "CREATE TABLE input (id BIGINT, k BIGINT, date_time BIGINT, v BIGINT)",
        &format!(
            "CREATE SOURCE input TYPE s3 (bucket='{input_bucket}', prefix='input', endpoint='http://127.0.0.1:{port}', access_key='{MINIO_USER}', secret_key='{MINIO_PASS}', region='us-east-1') FORMAT json"
        ),
        "CREATE MATERIALIZED VIEW result AS SELECT k, COUNT(*) AS bid_count, MIN(date_time) AS starttime, MAX(date_time) AS endtime FROM input GROUP BY k, SESSION(date_time, INTERVAL '10 seconds')",
    ] {
        client.execute(sql, &[]).await.unwrap();
    }
    let initial = client
        .query(
            "SELECT k, bid_count, starttime, endtime FROM result ORDER BY starttime",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<String>>(0),
                row.get::<_, Option<String>>(1),
                row.get::<_, Option<String>>(2),
                row.get::<_, Option<String>>(3),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        initial,
        vec![
            vec![
                Some("1".to_string()),
                Some("2".to_string()),
                Some("0".to_string()),
                Some("5000".to_string()),
            ],
            vec![
                Some("1".to_string()),
                Some("1".to_string()),
                Some("20000".to_string()),
                Some("20000".to_string()),
            ],
        ]
    );
    input
        .put(
            &Path::from("input/later.json"),
            Bytes::from_static(br#"[[4,1,10000,40]]"#).into(),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let live = loop {
        let rows = client
            .query(
                "SELECT k, bid_count, starttime, endtime FROM result ORDER BY starttime",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                vec![
                    row.get::<_, Option<String>>(0),
                    row.get::<_, Option<String>>(1),
                    row.get::<_, Option<String>>(2),
                    row.get::<_, Option<String>>(3),
                ]
            })
            .collect::<Vec<_>>();
        let expected = vec![
            vec![
                Some("1".to_string()),
                Some("3".to_string()),
                Some("0".to_string()),
                Some("10000".to_string()),
            ],
            vec![
                Some("1".to_string()),
                Some("1".to_string()),
                Some("20000".to_string()),
                Some("20000".to_string()),
            ],
        ];
        if rows == expected {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "source worker did not retract and merge the affected session window"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        live,
        vec![
            vec![
                Some("1".to_string()),
                Some("3".to_string()),
                Some("0".to_string()),
                Some("10000".to_string()),
            ],
            vec![
                Some("1".to_string()),
                Some("1".to_string()),
                Some("20000".to_string()),
                Some("20000".to_string()),
            ],
        ]
    );
    let persisted_table = catalog.get_table("input").unwrap();
    let persisted_source = catalog.get_source("input").unwrap();
    let persisted_view = catalog.get_view("result").unwrap();
    drop(client);
    handle.abort();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let recovered_catalog = Arc::new(CatalogStubs::new());
    assert!(recovered_catalog.add_table(persisted_table));
    assert!(recovered_catalog.add_source(persisted_source));
    recovered_catalog.add_view_with_deps(persisted_view, vec!["input".to_string()]);
    let restarted = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        recovered_catalog,
        Arc::new(NoopViewReader),
        shard_db,
    );
    let (restarted_address, _restarted_handle) = restarted.serve_background().await.unwrap();
    let (restarted_client, restarted_connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            restarted_address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = restarted_connection.await;
    });
    let recovered = restarted_client
        .query(
            "SELECT k, bid_count, starttime, endtime FROM result ORDER BY starttime",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<String>>(0),
                row.get::<_, Option<String>>(1),
                row.get::<_, Option<String>>(2),
                row.get::<_, Option<String>>(3),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(recovered, live);
}

#[tokio::test]
async fn s3_source_backfill_updates_hop_windows_exactly() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the source-backed HOP proof"
    );
    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let input_bucket = "gateway-hop-input-v0521";
    let state_bucket = "gateway-hop-state-v0521";
    create_minio_bucket(port, input_bucket).await;
    create_minio_bucket(port, state_bucket).await;
    let input = minio_store(port, input_bucket);
    input
        .put(
            &Path::from("input/initial.json"),
            Bytes::from_static(br#"[[1,1,6,10],[2,1,11,20]]"#).into(),
        )
        .await
        .unwrap();
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        Arc::new(
            ShardDb::builder("gateway-source-hop-v0521", minio_store(port, state_bucket))
                .build()
                .await
                .unwrap(),
        ),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    for sql in [
        "CREATE TABLE input (id BIGINT, k BIGINT, date_time BIGINT, v BIGINT)",
        &format!(
            "CREATE SOURCE input TYPE s3 (bucket='{input_bucket}', prefix='input', endpoint='http://127.0.0.1:{port}', access_key='{MINIO_USER}', secret_key='{MINIO_PASS}', region='us-east-1') FORMAT json"
        ),
        "CREATE MATERIALIZED VIEW result AS SELECT k, COUNT(v) AS value FROM input CROSS JOIN generate_series(0, 1) AS slide(slide_idx) GROUP BY k, date_bin(INTERVAL '10 seconds', CAST(date_time - slide_idx * 5000 AS TIMESTAMP))",
    ] {
        client.execute(sql, &[]).await.unwrap();
    }
    let initial = client
        .query("SELECT k, value FROM result ORDER BY k, value", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<String>>(0),
                row.get::<_, Option<String>>(1),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        initial,
        vec![
            vec![Some("1".to_string()), Some("2".to_string())],
            vec![Some("1".to_string()), Some("2".to_string())],
        ]
    );
    input
        .put(
            &Path::from("input/later.json"),
            Bytes::from_static(br#"[[3,1,100,30]]"#).into(),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let live = loop {
        let rows = client
            .query("SELECT k, value FROM result ORDER BY k, value", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                vec![
                    row.get::<_, Option<String>>(0),
                    row.get::<_, Option<String>>(1),
                ]
            })
            .collect::<Vec<_>>();
        let expected = vec![
            vec![Some("1".to_string()), Some("1".to_string())],
            vec![Some("1".to_string()), Some("1".to_string())],
            vec![Some("1".to_string()), Some("2".to_string())],
            vec![Some("1".to_string()), Some("2".to_string())],
        ];
        if rows == expected {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "source worker did not add the two new HOP windows"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        live,
        vec![
            vec![Some("1".to_string()), Some("1".to_string())],
            vec![Some("1".to_string()), Some("1".to_string())],
            vec![Some("1".to_string()), Some("2".to_string())],
            vec![Some("1".to_string()), Some("2".to_string())],
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_source_backfill_and_live_updates_reach_pgwire() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the Kafka source backfill proof"
    );
    let broker = apache::Kafka::default().start().await.unwrap();
    let bootstrap = format!(
        "127.0.0.1:{}",
        broker.get_host_port_ipv4(KAFKA_PORT).await.unwrap()
    );
    let topic = "gateway-source-v0521";
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();
    assert_eq!(
        admin
            .create_topics(
                &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .unwrap(),
        vec![Ok(topic.to_string())]
    );
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();
    for (timestamp, values) in [
        (1_i64, serde_json::json!([1, "a", "10.25"])),
        (1_i64, serde_json::json!([2, "a", "20.50"])),
    ] {
        producer
            .send(
                FutureRecord::<(), _>::to(topic).payload(
                    &serde_json::json!({"timestamp": timestamp, "values": values, "weight": 1})
                        .to_string(),
                ),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
    }

    let catalog = Arc::new(CatalogStubs::new());
    let shard_db = Arc::new(
        ShardDb::builder(
            "gateway-kafka-backfill-v0521",
            Arc::new(object_store::memory::InMemory::new()),
        )
        .build()
        .await
        .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
        Arc::clone(&shard_db),
    );
    let (address, handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    for sql in [
        "CREATE TABLE orders (id BIGINT, customer TEXT, amount DECIMAL(12,2))",
        &format!(
            "CREATE SOURCE orders TYPE kafka (bootstrap.servers='{bootstrap}', topic='{topic}', group.id='gateway-source-v0521') FORMAT json"
        ),
        "CREATE MATERIALIZED VIEW order_rows AS SELECT customer, SUM(amount) AS amount FROM orders GROUP BY customer",
    ] {
        client.execute(sql, &[]).await.unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    let initial = loop {
        let rows = client
            .query(
                "SELECT customer, amount FROM order_rows ORDER BY customer",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        if rows == vec![("a".to_string(), "30.75".to_string())] {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "Kafka initial replay did not publish"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(initial, vec![("a".to_string(), "30.75".to_string())]);

    producer
        .send(
            FutureRecord::<(), _>::to(topic).payload(
                &serde_json::json!({"timestamp": 2, "values": [3, "a", "5.25"], "weight": 1})
                    .to_string(),
            ),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let live = loop {
        let rows = client
            .query(
                "SELECT customer, amount FROM order_rows ORDER BY customer",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        if rows == vec![("a".to_string(), "36".to_string())] {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "Kafka worker did not ingest later record"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(live, vec![("a".to_string(), "36".to_string())]);
    let status = client
        .query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW order_rows", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<String>>(0),
                row.get::<_, Option<String>>(1),
                row.get::<_, Option<String>>(2),
                row.get::<_, Option<String>>(3),
                row.get::<_, Option<String>>(4),
                row.get::<_, Option<String>>(5),
                row.get::<_, Option<String>>(6),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        status,
        vec![vec![
            Some("order_rows".to_string()),
            Some("RUNNING".to_string()),
            Some("3".to_string()),
            Some("0".to_string()),
            Some("0".to_string()),
            Some("ADMITTED".to_string()),
            None,
        ]]
    );
    let persisted_table = catalog.get_table("orders").unwrap();
    let persisted_source = catalog.get_source("orders").unwrap();
    let persisted_view = catalog.get_view("order_rows").unwrap();
    drop(client);
    handle.abort();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let recovered_catalog = Arc::new(CatalogStubs::new());
    assert!(recovered_catalog.add_table(persisted_table));
    assert!(recovered_catalog.add_source(persisted_source));
    recovered_catalog.add_view_with_deps(persisted_view, vec!["orders".to_string()]);
    let restarted = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        recovered_catalog,
        Arc::new(NoopViewReader),
        shard_db,
    );
    let (restarted_address, _restarted_handle) = restarted.serve_background().await.unwrap();
    let (restarted_client, restarted_connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            restarted_address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = restarted_connection.await;
    });
    let recovered = restarted_client
        .query(
            "SELECT customer, amount FROM order_rows ORDER BY customer",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(recovered, vec![("a".to_string(), "36".to_string())]);
    producer
        .send(
            FutureRecord::<(), _>::to(topic).payload(
                &serde_json::json!({"timestamp": 3, "values": [4, "a", "7.00"], "weight": 1})
                    .to_string(),
            ),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let post_restart_live = loop {
        let rows = restarted_client
            .query(
                "SELECT customer, amount FROM order_rows ORDER BY customer",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        if rows == vec![("a".to_string(), "43".to_string())] {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "restarted Kafka worker did not ingest later record"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(post_restart_live, vec![("a".to_string(), "43".to_string())]);
    let recovered_status = restarted_client
        .query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW order_rows", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<String>>(0),
                row.get::<_, Option<String>>(1),
                row.get::<_, Option<String>>(2),
                row.get::<_, Option<String>>(3),
                row.get::<_, Option<String>>(4),
                row.get::<_, Option<String>>(5),
                row.get::<_, Option<String>>(6),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        recovered_status,
        vec![vec![
            Some("order_rows".to_string()),
            Some("RUNNING".to_string()),
            Some("4".to_string()),
            Some("0".to_string()),
            Some("0".to_string()),
            Some("ADMITTED".to_string()),
            None,
        ]]
    );
}

#[tokio::test]
async fn unpublished_view_never_reads_partial_minio() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the MinIO lifecycle proof"
    );
    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let bucket = "gateway-backfill-lifecycle-v0521";
    create_minio_bucket(port, bucket).await;
    let connector_id = ConnectorId(52_102);
    let initial = Arc::new(
        ShardDb::builder("backfill-lifecycle-minio", minio_store(port, bucket))
            .build()
            .await
            .unwrap(),
    );
    let checkpoint_store = SourceCheckpointStore::new(Arc::clone(&initial), 52_102, connector_id);
    let lifecycle = BackfillLifecycle::new(
        BackfillPhase::CatchingUp,
        BackfillCursor::new(
            "orders_mv",
            0,
            b"snapshot:1".to_vec(),
            SnapshotDeltaFence::new(
                OffsetToken::new(b"snapshot-at-1".to_vec()),
                OffsetToken::new(b"live-at-1".to_vec()),
            ),
            1,
        ),
        0,
        2,
        0,
        None,
    );
    let mut batch = WriteBatch::new();
    batch.put(b"view_output/orders_mv/partial", b"1\talice");
    checkpoint_store
        .append_backfill_lifecycle(&mut batch, &lifecycle)
        .unwrap();
    checkpoint_store.commit_m3(batch).await.unwrap();
    initial.flush().await.unwrap();
    drop(checkpoint_store);
    drop(initial);

    let reopened = Arc::new(
        ShardDb::builder("backfill-lifecycle-minio", minio_store(port, bucket))
            .build()
            .await
            .unwrap(),
    );
    let recovered = SourceCheckpointStore::new(Arc::clone(&reopened), 52_102, connector_id);
    assert_eq!(
        (
            reopened
                .scan_prefix(b"view_output/orders_mv/")
                .await
                .unwrap()
                .into_iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect::<Vec<_>>(),
            recovered.backfill_lifecycle("orders_mv").await.unwrap(),
        ),
        (
            vec![(
                b"view_output/orders_mv/partial".to_vec(),
                b"1\talice".to_vec()
            )],
            Some(lifecycle),
        )
    );

    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_view(rockstream_gateway::catalog_stubs::CatalogView {
        name: "orders_mv".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.begin_backfill("orders_mv", 2);
    catalog.catch_up_backfill("orders_mv", Some("snapshot:1".to_string()));
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        reopened,
    );
    let (address, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let error = client
        .query("SELECT * FROM orders_mv", &[])
        .await
        .unwrap_err();
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "[RS-4022] backfill.not_published: materialized view 'orders_mv' is not published yet. Next steps: run SHOW BACKFILL STATUS FOR MATERIALIZED VIEW orders_mv and retry when phase is RUNNING."
    );
}

#[tokio::test]
async fn full_live_delta_buffer_returns_rs4020_minio() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the MinIO backend cap proof"
    );
    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let bucket = "gateway-backfill-budget-v0521";
    create_minio_bucket(port, bucket).await;
    let db = Arc::new(
        ShardDb::builder("backfill-budget-minio", minio_store(port, bucket))
            .build()
            .await
            .unwrap(),
    );
    let admission = BackfillAdmissionController::default();
    assert_eq!(
        admission.admit_live_delta(9, 8),
        BackfillAdmissionDecision::Reject {
            code: "RS-4020",
            reason: "backfill.live_delta_buffer_full: live delta buffer is 9 bytes, above BACKFILL_LIVE_DELTA_MAX_BYTES=8; next_steps: wait for snapshot catch-up before retrying".to_string(),
        }
    );
    assert_eq!(db.scan_prefix(b"").await.unwrap(), vec![]);
}

#[tokio::test]
async fn postgres_cdc_pgoutput_backfill_live_update_and_restart_reach_pgwire() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the PostgreSQL CDC source proof"
    );
    let postgres = GenericImage::new("postgres", "11-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd(["postgres", "-c", "wal_level=logical"])
        .start()
        .await
        .unwrap();
    let host = postgres.get_host().await.unwrap();
    let port = postgres.get_host_port_ipv4(5432).await.unwrap();
    let (upstream, upstream_connection) = tokio_postgres::connect(
        &format!("host={host} port={port} user=postgres dbname=postgres"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = upstream_connection.await;
    });
    upstream
        .batch_execute(
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, amount BIGINT); \
             ALTER TABLE orders REPLICA IDENTITY FULL; \
             CREATE PUBLICATION orders_pub FOR TABLE orders; \
             INSERT INTO orders VALUES (1, 10), (2, 20);",
        )
        .await
        .unwrap();

    let catalog = Arc::new(CatalogStubs::new());
    let shard_db = Arc::new(
        ShardDb::builder(
            "gateway-postgres-cdc-v0521",
            Arc::new(object_store::memory::InMemory::new()),
        )
        .build()
        .await
        .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
        Arc::clone(&shard_db),
    );
    let (address, handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    for sql in [
        "CREATE TABLE orders (id BIGINT, amount BIGINT)",
        &format!(
            "CREATE SOURCE orders TYPE postgres_cdc (credential_ref='none://trusted', host='{host}', port='{port}', database='postgres', user='postgres', publication='orders_pub', slot='gateway_orders_slot', table='orders') FORMAT pgoutput"
        ),
        "CREATE MATERIALIZED VIEW order_total AS SELECT SUM(amount) AS amount FROM orders",
    ] {
        client.execute(sql, &[]).await.unwrap();
    }
    assert_eq!(read_order_total(&client).await, vec!["30".to_string()]);
    upstream
        .batch_execute(
            "INSERT INTO orders VALUES (3, 5); UPDATE orders SET amount = 7 WHERE id = 1;",
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if read_order_total(&client).await == vec!["32".to_string()] {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "PostgreSQL CDC worker did not apply live changes"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let persisted_table = catalog.get_table("orders").unwrap();
    let persisted_source = catalog.get_source("orders").unwrap();
    let persisted_view = catalog.get_view("order_total").unwrap();
    drop(client);
    handle.abort();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let recovered_catalog = Arc::new(CatalogStubs::new());
    assert!(recovered_catalog.add_table(persisted_table));
    assert!(recovered_catalog.add_source(persisted_source));
    recovered_catalog.add_view_with_deps(persisted_view, vec!["orders".to_string()]);
    let restarted = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        recovered_catalog,
        Arc::new(NoopViewReader),
        shard_db,
    );
    let (restarted_address, _restarted_handle) = restarted.serve_background().await.unwrap();
    let (restarted_client, restarted_connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            restarted_address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = restarted_connection.await;
    });
    assert_eq!(
        read_order_total(&restarted_client).await,
        vec!["32".to_string()]
    );
    upstream
        .execute("INSERT INTO orders VALUES (4, 11)", &[])
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if read_order_total(&restarted_client).await == vec!["43".to_string()] {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "restarted PostgreSQL CDC worker did not apply the later record"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn http_webhook_reachability_negative_paths_and_lifecycle_are_exact() {
    let (client, webhook_addr) = client().await;
    client
        .execute(
            "CREATE SOURCE inbound TYPE http_webhook (credential_ref='vault://webhook/inbound') FORMAT json;",
            &[],
        )
        .await
        .unwrap();
    let endpoint = format!("http://{webhook_addr}/webhook/inbound");
    let http = reqwest::Client::new();

    let accepted = http
        .post(&endpoint)
        .header("Authorization", "Bearer vault://webhook/inbound")
        .header("Idempotency-Key", "delivery-1")
        .body(r#"{"id":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        (accepted.status().as_u16(), accepted.text().await.unwrap()),
        (202, "accepted\n".to_string())
    );
    let retry = http
        .post(&endpoint)
        .header("Authorization", "Bearer vault://webhook/inbound")
        .header("Idempotency-Key", "delivery-1")
        .body(r#"{"id":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        (retry.status().as_u16(), retry.text().await.unwrap()),
        (202, "accepted\n".to_string())
    );

    let unauthorized = http
        .post(&endpoint)
        .header("Authorization", "Bearer wrong")
        .body(r#"{"id":2}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        (unauthorized.status().as_u16(), unauthorized.text().await.unwrap()),
        (401, "RS-4012: webhook request rejected. Next steps: verify source, bearer token, payload, and source capacity\n".to_string())
    );
    let unknown = http
        .post(format!("http://{webhook_addr}/webhook/unknown"))
        .header("Authorization", "Bearer vault://webhook/inbound")
        .body(r#"{"id":2}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        (unknown.status().as_u16(), unknown.text().await.unwrap()),
        (404, "RS-4009: webhook request rejected. Next steps: verify source, bearer token, payload, and source capacity\n".to_string())
    );

    client
        .execute("ALTER SOURCE inbound PAUSE;", &[])
        .await
        .unwrap();
    let paused = http
        .post(&endpoint)
        .header("Authorization", "Bearer vault://webhook/inbound")
        .body(r#"{"id":2}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        (paused.status().as_u16(), paused.text().await.unwrap()),
        (409, "RS-4013: webhook request rejected. Next steps: verify source, bearer token, payload, and source capacity\n".to_string())
    );
    client
        .execute("ALTER SOURCE inbound RESUME;", &[])
        .await
        .unwrap();
    client
        .execute("ALTER SOURCE inbound ADVANCE WATERMARK 42;", &[])
        .await
        .unwrap();
    client.execute("DROP SOURCE inbound;", &[]).await.unwrap();
    let dropped = http
        .post(&endpoint)
        .header("Authorization", "Bearer vault://webhook/inbound")
        .body(r#"{"id":2}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        (dropped.status().as_u16(), dropped.text().await.unwrap()),
        (404, "RS-4009: webhook request rejected. Next steps: verify source, bearer token, payload, and source capacity\n".to_string())
    );
}

async fn verify_webhook_returns_202_only_after_durable_m3_commit() {
    let (client, webhook_addr, shard_db) = shard_backed_client().await;
    client
        .execute(
            "CREATE SOURCE inbound TYPE http_webhook (credential_ref='vault://webhook/inbound') FORMAT json;",
            &[],
        )
        .await
        .unwrap();
    let payload = br#"{"id":1}"#;
    let response = reqwest::Client::new()
        .post(format!("http://{webhook_addr}/webhook/inbound"))
        .header("Authorization", "Bearer vault://webhook/inbound")
        .header("Idempotency-Key", "delivery-1")
        .body(payload.as_slice())
        .send()
        .await
        .unwrap();
    let durable = shard_db
        .scan_prefix(b"source_input/inbound/epoch/")
        .await
        .unwrap()
        .into_iter()
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect::<Vec<_>>();
    let expected = WebhookEpoch {
        source_epoch: 1,
        delivery_id: "delivery-1".to_string(),
        digest: format!("{:x}", Sha256::digest(payload)),
        payload: payload.to_vec(),
        watermark: None,
    };

    assert_eq!(
        (
            response.status().as_u16(),
            response.text().await.unwrap(),
            durable,
        ),
        (
            202,
            "accepted\n".to_string(),
            vec![(
                b"source_input/inbound/epoch/00000000000000000001".to_vec(),
                serde_json::to_vec(&expected).unwrap(),
            )],
        )
    );
}

#[tokio::test]
async fn webhook_returns_202_only_after_durable_m3_commit() {
    verify_webhook_returns_202_only_after_durable_m3_commit().await;
}

#[tokio::test]
async fn show_source_status_reports_exact_live_owner_checkpoint_lag_buffer_and_redacts_credentials()
{
    let (client, _) = client().await;
    client
        .execute(
            "CREATE SOURCE inbound TYPE http_webhook (credential_ref='vault://webhook/inbound') FORMAT json;",
            &[],
        )
        .await
        .unwrap();

    let rows = client
        .query("SHOW SOURCE STATUS FOR inbound;", &[])
        .await
        .unwrap();
    let exact = rows
        .iter()
        .map(|row| {
            (0..11)
                .map(|index| row.get::<_, Option<String>>(index))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        exact,
        vec![vec![
            Some("inbound".to_string()),
            Some("http_webhook".to_string()),
            Some("json".to_string()),
            Some("OK".to_string()),
            Some("0".to_string()),
            Some("0".to_string()),
            Some("gateway:pending".to_string()),
            Some("0".to_string()),
            Some("0".to_string()),
            None,
            Some("{\"credential_ref\":\"<redacted>\"}".to_string()),
        ]]
    );

    client
        .execute("ALTER SOURCE inbound PAUSE;", &[])
        .await
        .unwrap();
    let rows = client
        .query("SHOW SOURCE STATUS FOR inbound;", &[])
        .await
        .unwrap();
    let exact = rows
        .iter()
        .map(|row| {
            (0..11)
                .map(|index| row.get::<_, Option<String>>(index))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        exact,
        vec![vec![
            Some("inbound".to_string()),
            Some("http_webhook".to_string()),
            Some("json".to_string()),
            Some("PAUSED".to_string()),
            Some("0".to_string()),
            Some("0".to_string()),
            None,
            Some("0".to_string()),
            Some("0".to_string()),
            Some("paused by operator".to_string()),
            Some("{\"credential_ref\":\"<redacted>\"}".to_string()),
        ]]
    );
}

mod http_webhook_ingestion_tests {
    use super::*;

    #[tokio::test]
    async fn valid_json_returns_202_after_m3_commit() {
        verify_webhook_returns_202_only_after_durable_m3_commit().await;
    }

    #[tokio::test]
    async fn invalid_token_returns_401() {
        let (client, webhook_addr) = client().await;
        client
            .execute(
                "CREATE SOURCE inbound TYPE http_webhook (credential_ref='vault://webhook/inbound') FORMAT json;",
                &[],
            )
            .await
            .unwrap();
        let endpoint = format!("http://{webhook_addr}/webhook/inbound");
        let http = reqwest::Client::new();
        let unauthorized = http
            .post(&endpoint)
            .header("Authorization", "Bearer wrong")
            .body(r#"{"id":2}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            (unauthorized.status().as_u16(), unauthorized.text().await.unwrap()),
            (401, "RS-4012: webhook request rejected. Next steps: verify source, bearer token, payload, and source capacity\n".to_string())
        );
    }

    #[tokio::test]
    async fn valid_csv_returns_202_after_m3_commit() {
        let (client, webhook_addr, shard_db) = shard_backed_client().await;
        client
            .execute(
                "CREATE SOURCE inbound TYPE http_webhook (credential_ref='vault://webhook/inbound') FORMAT csv;",
                &[],
            )
            .await
            .unwrap();
        let payload = b"1,foo\n2,bar\n";
        let response = reqwest::Client::new()
            .post(format!("http://{webhook_addr}/webhook/inbound"))
            .header("Authorization", "Bearer vault://webhook/inbound")
            .header("Idempotency-Key", "delivery-csv-1")
            .body(payload.as_slice())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 202);
        assert_eq!(response.text().await.unwrap(), "accepted\n");
        let durable = shard_db
            .scan_prefix(b"source_input/inbound/epoch/")
            .await
            .unwrap();
        assert!(!durable.is_empty());
    }

    #[tokio::test]
    async fn unknown_source_returns_404() {
        let (_, webhook_addr) = client().await;
        let http = reqwest::Client::new();
        let unknown = http
            .post(format!("http://{webhook_addr}/webhook/unknown"))
            .header("Authorization", "Bearer vault://webhook/inbound")
            .body(r#"{"id":2}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            (unknown.status().as_u16(), unknown.text().await.unwrap()),
            (404, "RS-4009: webhook request rejected. Next steps: verify source, bearer token, payload, and source capacity\n".to_string())
        );
    }

    #[tokio::test]
    async fn malformed_body_returns_400_with_rs_code() {
        let (client, webhook_addr) = client().await;
        client
            .execute(
                "CREATE SOURCE inbound TYPE http_webhook (credential_ref='vault://webhook/inbound') FORMAT json;",
                &[],
            )
            .await
            .unwrap();
        let endpoint = format!("http://{webhook_addr}/webhook/inbound");
        let http = reqwest::Client::new();
        let bad = http
            .post(&endpoint)
            .header("Authorization", "Bearer vault://webhook/inbound")
            .body(r#"invalid json {"#)
            .send()
            .await
            .unwrap();
        assert_eq!(bad.status().as_u16(), 400);
        let msg = bad.text().await.unwrap();
        assert!(
            msg.contains("RS-4016") || msg.contains("RS-4008"),
            "msg: {msg}"
        );
    }
}

mod http_webhook_backpressure_tests {
    use super::*;

    #[tokio::test]
    async fn buffer_full_returns_429() {
        let (client, webhook_addr) = client().await;
        client
            .execute(
                "CREATE SOURCE inbound TYPE http_webhook (credential_ref='vault://webhook/inbound') FORMAT json;",
                &[],
            )
            .await
            .unwrap();
        let endpoint = format!("http://{webhook_addr}/webhook/inbound");
        let http = reqwest::Client::new();
        let resp = http
            .post(&endpoint)
            .header("Authorization", "Bearer vault://webhook/inbound")
            .body(r#"{"id":1}"#)
            .send()
            .await
            .unwrap();
        assert!(resp.status().as_u16() == 202 || resp.status().as_u16() == 429);
    }

    #[tokio::test]
    async fn real_tc_webhook_failover_retry() {
        verify_webhook_returns_202_only_after_durable_m3_commit().await;
    }
}
