//! Gateway-level PostgreSQL integration proof for #349. This goes through
//! the production poll_pgoutput_event -> M3 epoch -> slot ACK path.
use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::local::LocalFileSystem;
use rockstream_connectors::POSTGRES_CDC_MAX_TRANSACTION_BYTES;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use tempfile::TempDir;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

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

async fn view_ids(client: &Client) -> Vec<i64> {
    client
        .simple_query("SELECT id FROM orders_view ORDER BY id")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().parse::<i64>().unwrap()),
            _ => None,
        })
        .collect()
}

async fn slot_lsn(client: &Client) -> Option<String> {
    client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'bounded_gateway_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
async fn pgoutput_gateway_publishes_complete_large_transaction_atomically_and_acks_after_commit() {
    if !rockstream_test_support::docker_available() {
        eprintln!("SKIP pgoutput bounded transport gateway proof: Docker unavailable");
        return;
    }
    tokio::time::timeout(Duration::from_secs(240), run_gateway_proof())
        .await
        .expect("pgoutput bounded transport gateway proof timed out");
}

async fn run_gateway_proof() {
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
    let (upstream, connection) = tokio_postgres::connect(
        &format!("host={host} port={port} user=postgres dbname=postgres"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    upstream
        .batch_execute(
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, payload TEXT NOT NULL); \
             ALTER TABLE orders REPLICA IDENTITY FULL; \
             CREATE PUBLICATION orders_pub FOR TABLE orders",
        )
        .await
        .unwrap();

    let dir = TempDir::new().unwrap();
    let db = Arc::new(
        ShardDb::builder(
            "pgoutput-349-bounded-gateway",
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        )
        .build()
        .await
        .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        Arc::clone(&db),
    );
    let (addr, server_task) = server.serve_background().await.unwrap();
    let (gateway, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    // Install the empty view before attaching the CDC source. Creating a
    // source-backed view here would invoke an empty backfill and fail coupled
    // batch validation before this test reaches the transport under test.
    for sql in [
        "CREATE TABLE orders (id BIGINT, payload TEXT)",
        "CREATE MATERIALIZED VIEW orders_view AS SELECT id FROM orders",
        &format!(
            "CREATE SOURCE orders TYPE postgres_cdc (credential_ref='none://trusted', host='{host}', port='{port}', database='postgres', user='postgres', publication='orders_pub', slot='bounded_gateway_slot', table='orders') FORMAT pgoutput"
        ),
    ] {
        gateway.execute(sql, &[]).await.unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    let ack_before = loop {
        let row = upstream
            .query_opt(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'bounded_gateway_slot'",
                &[],
            )
            .await
            .unwrap();
        if row.is_some() {
            break slot_lsn(&upstream).await;
        }
        assert!(Instant::now() < deadline, "gateway never created the logical slot");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(view_ids(&gateway).await, Vec::<i64>::new());

    upstream
        .batch_execute("BEGIN; INSERT INTO orders SELECT generate_series(1, 2500), 'small'")
        .await
        .unwrap();
    assert_eq!(
        view_ids(&gateway).await,
        Vec::<i64>::new(),
        "uncommitted source data may not become visible"
    );
    upstream.batch_execute("COMMIT").await.unwrap();

    let expected = (1..=2500).collect::<Vec<i64>>();
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let actual = view_ids(&gateway).await;
        if actual == expected {
            break;
        }
        assert!(
            actual.is_empty(),
            "a partial M3 transaction became visible before the complete commit: {actual:?}"
        );
        assert!(
            Instant::now() < deadline,
            "M3 never published complete transcript: actual={actual:?}, expected={expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(view_ids(&gateway).await, expected);

    // Source checkpoint durability is the authorization to consume the slot.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if slot_lsn(&upstream).await != ack_before {
            break;
        }
        assert!(Instant::now() < deadline, "M3 completed but slot did not acknowledge");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(view_ids(&gateway).await, expected, "no duplicate publication after ACK");

    // A frame larger than the 8 MiB source transport bound must block the
    // source before the router can see a partial row or advance its checkpoint.
    let committed_slot_lsn = slot_lsn(&upstream).await;
    upstream
        .execute(
            "INSERT INTO orders VALUES (3000, repeat('x', $1))",
            &[&((POSTGRES_CDC_MAX_TRANSACTION_BYTES + 1024) as i32)],
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(45);
    let blocked_reason = loop {
        let status = gateway
            .query("SHOW SOURCE STATUS FOR orders", &[])
            .await
            .unwrap();
        let state: Option<String> = status[0].get(3);
        if state.as_deref() == Some("BLOCKED") {
            break status[0].get::<_, Option<String>>(9);
        }
        assert!(
            Instant::now() < deadline,
            "oversized pgoutput frame did not block the gateway source"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let blocked_reason = blocked_reason.unwrap_or_default();
    assert!(
        blocked_reason.contains("RS-4014"),
        "unexpected transport rejection reason: {blocked_reason}"
    );
    assert_eq!(
        view_ids(&gateway).await,
        expected,
        "oversized transaction must never partially publish to M3"
    );
    assert_eq!(
        slot_lsn(&upstream).await,
        committed_slot_lsn,
        "oversized transaction must not advance the PostgreSQL slot"
    );
    server_task.abort();
}
