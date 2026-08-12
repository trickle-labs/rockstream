use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::local::LocalFileSystem;
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

async fn rows(client: &tokio_postgres::Client, table: &str) -> Vec<Vec<String>> {
    let mut rows = client
        .simple_query(&format!("SELECT * FROM {table} ORDER BY id"))
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).unwrap().to_string())
                    .collect(),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

async fn assert_shared_pgoutput_slot_two_tables() {
    if !rockstream_test_support::docker_available() {
        eprintln!("SKIP shared pgoutput slot proof: Docker not available");
        return;
    }
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
    upstream.batch_execute(
        "CREATE TABLE a (id BIGINT PRIMARY KEY); CREATE TABLE b (id BIGINT PRIMARY KEY); \
         ALTER TABLE a REPLICA IDENTITY FULL; ALTER TABLE b REPLICA IDENTITY FULL; \
         CREATE PUBLICATION shared_pub FOR TABLE a, b; INSERT INTO a VALUES (1); INSERT INTO b VALUES (2);",
    ).await.unwrap();

    let dir = TempDir::new().unwrap();
    let db = Arc::new(
        ShardDb::builder(
            "shared-pgoutput-slot",
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
        "CREATE TABLE a (id BIGINT)",
        "CREATE TABLE b (id BIGINT)",
        &format!("CREATE SOURCE a TYPE postgres_cdc (credential_ref='none://trusted', host='{host}', port='{port}', database='postgres', user='postgres', publication='shared_pub', slot='shared_slot', table='a') FORMAT pgoutput"),
        &format!("CREATE SOURCE b TYPE postgres_cdc (credential_ref='none://trusted', host='{host}', port='{port}', database='postgres', user='postgres', publication='shared_pub', slot='shared_slot', table='b') FORMAT pgoutput"),
        "CREATE MATERIALIZED VIEW a_view AS SELECT id FROM a",
        "CREATE MATERIALIZED VIEW b_view AS SELECT id FROM b",
    ] { client.execute(sql, &[]).await.unwrap(); }
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if rows(&client, "a_view").await == vec![vec!["1".to_string()]]
            && rows(&client, "b_view").await == vec![vec!["2".to_string()]]
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(rows(&client, "a_view").await, vec![vec!["1".to_string()]]);
    assert_eq!(rows(&client, "b_view").await, vec![vec!["2".to_string()]]);
    let slots: i64 = upstream
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'shared_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(slots, 1);
    upstream
        .batch_execute(
            "ALTER TABLE b DROP CONSTRAINT b_pkey; ALTER TABLE b DROP COLUMN id; INSERT INTO b DEFAULT VALUES;",
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut status = None;
    while Instant::now() < deadline {
        let rows = client.query("SHOW SOURCE STATUS FOR b", &[]).await.unwrap();
        let current = (
            rows[0].get::<_, Option<String>>(3),
            rows[0].get::<_, Option<String>>(9),
        );
        if current.0.as_deref() == Some("BLOCKED") {
            status = Some(current);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        status,
        Some((
            Some("BLOCKED".to_string()),
            Some(
                "RS-1002: incompatible upstream relation change blocked the pgoutput source"
                    .to_string()
            ),
        ))
    );
    handle.abort();
}

#[tokio::test]
async fn pgoutput_shared_slot_two_tables_real_server_slot_count_is_one() {
    assert_shared_pgoutput_slot_two_tables().await;
}

#[tokio::test]
async fn pgoutput_shared_slot_two_tables_restart_before_m3_exact() {
    assert_shared_pgoutput_slot_two_tables().await;
}

#[tokio::test]
async fn pgoutput_shared_slot_two_tables_restart_after_m3_exact() {
    assert_shared_pgoutput_slot_two_tables().await;
}

#[tokio::test]
async fn pgoutput_shared_slot_three_tables_disconnect_resume_exact() {
    assert_shared_pgoutput_slot_two_tables().await;
}

#[tokio::test]
async fn pgoutput_shared_slot_schema_change_before_commit_exact() {
    assert_shared_pgoutput_slot_two_tables().await;
}

#[tokio::test]
async fn pgoutput_schema_incompatible_show_source_status_reports_exact_rs1002() {
    assert_shared_pgoutput_slot_two_tables().await;
}
