//! Real PostgreSQL pgoutput TRUNCATE proof (issue #339).
//! The row-only decoder cannot prove this: the gateway must retract imported
//! state and dependent views in the same durable epoch as its slot checkpoint.

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

async fn query_pairs(client: &Client, sql: &str) -> Vec<Vec<i64>> {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).unwrap().parse::<i64>().unwrap())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

async fn ids(client: &Client, table: &str) -> Vec<Vec<i64>> {
    query_pairs(client, &format!("SELECT id FROM {table} ORDER BY id")).await
}

async fn wait_ids(client: &Client, table: &str, expected: &[Vec<i64>]) {
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let found = ids(client, table).await;
        if found == expected {
            return;
        }
        if Instant::now() > deadline {
            let a_status = client
                .query("SHOW SOURCE STATUS FOR a", &[])
                .await
                .map(|rows| {
                    rows.into_iter()
                        .map(|row| {
                            (
                                row.get::<_, Option<String>>(3),
                                row.get::<_, Option<String>>(9),
                            )
                        })
                        .collect::<Vec<_>>()
                });
            panic!(
                "pgoutput '{table}' did not converge: expected={expected:?} actual={found:?} a_status={a_status:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn connect_gateway(port: u16) -> Client {
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
async fn pgoutput_truncate_retracts_nonempty_views_and_survives_gateway_restart() {
    tokio::time::timeout(Duration::from_secs(600), run_pgoutput_truncate_proof())
        .await
        .expect("pgoutput TRUNCATE proof exceeded the ten-minute CI bound");
}

async fn run_pgoutput_truncate_proof() {
    if !rockstream_test_support::docker_available() {
        eprintln!("SKIP pgoutput TRUNCATE end-to-end proof: Docker not available");
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
            "CREATE TABLE a (id BIGINT PRIMARY KEY); CREATE TABLE b (id BIGINT PRIMARY KEY); \
             ALTER TABLE a REPLICA IDENTITY FULL; ALTER TABLE b REPLICA IDENTITY FULL; \
             CREATE PUBLICATION trunc_pub FOR TABLE a, b; \
             INSERT INTO a VALUES (1), (2); INSERT INTO b VALUES (2), (3);",
        )
        .await
        .unwrap();

    let dir = TempDir::new().unwrap();
    let db = Arc::new(
        ShardDb::builder(
            "pgoutput-truncate-339",
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        )
        .build()
        .await
        .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
        Arc::clone(&db),
    );
    let (addr, handle) = server.serve_background().await.unwrap();
    let client = connect_gateway(addr.port()).await;
    for sql in [
        "CREATE TABLE a (id BIGINT)",
        "CREATE TABLE b (id BIGINT)",
        &format!(
            "CREATE SOURCE a TYPE postgres_cdc (credential_ref='none://trusted', host='{host}', port='{port}', database='postgres', user='postgres', publication='trunc_pub', slot='trunc_slot', table='a') FORMAT pgoutput"
        ),
        &format!(
            "CREATE SOURCE b TYPE postgres_cdc (credential_ref='none://trusted', host='{host}', port='{port}', database='postgres', user='postgres', publication='trunc_pub', slot='trunc_slot', table='b') FORMAT pgoutput"
        ),
        "CREATE MATERIALIZED VIEW a_view AS SELECT id FROM a",
        "CREATE MATERIALIZED VIEW b_view AS SELECT id FROM b",
        "CREATE MATERIALIZED VIEW a_doubled AS SELECT id, id * 2 AS doubled FROM a",
    ] {
        client.execute(sql, &[]).await.unwrap();
    }
    wait_ids(&client, "a_view", &[vec![1], vec![2]]).await;
    wait_ids(&client, "b_view", &[vec![2], vec![3]]).await;

    // An INSERT followed by TRUNCATE inside one upstream transaction must
    // cancel both pre-existing rows and that INSERT. Only the later INSERT
    // survives; b is unaffected even though it shares the replication slot.
    upstream
        .batch_execute("BEGIN; INSERT INTO a VALUES (4); TRUNCATE a; INSERT INTO a VALUES (5);")
        .await
        .unwrap();
    assert_eq!(ids(&client, "a_view").await, vec![vec![1], vec![2]]);
    upstream.batch_execute("COMMIT").await.unwrap();
    wait_ids(&client, "a_view", &[vec![5]]).await;
    assert_eq!(ids(&client, "b_view").await, vec![vec![2], vec![3]]);
    assert_eq!(
        query_pairs(&client, "SELECT id, doubled FROM a_doubled ORDER BY id").await,
        vec![vec![5, 10]]
    );
    assert_eq!(ids(&client, "a").await, vec![vec![5]]);

    // Multi-relation T with both flags set must retract all tables and
    // every dependent view; a later commit should still be consumed.
    upstream
        .batch_execute("TRUNCATE a, b RESTART IDENTITY CASCADE")
        .await
        .unwrap();
    wait_ids(&client, "a_view", &[]).await;
    wait_ids(&client, "b_view", &[]).await;
    assert!(ids(&client, "a").await.is_empty());
    assert!(ids(&client, "b").await.is_empty());
    assert!(
        query_pairs(&client, "SELECT id, doubled FROM a_doubled ORDER BY id")
            .await
            .is_empty()
    );
    upstream
        .batch_execute("INSERT INTO a VALUES (7); INSERT INTO b VALUES (9)")
        .await
        .unwrap();
    wait_ids(&client, "a_view", &[vec![7]]).await;
    wait_ids(&client, "b_view", &[vec![9]]).await;

    // Recreate the gateway with the same durable shard and catalog entries;
    // checkpoint replay must not resurrect pre-TRUNCATE rows.
    let saved_tables = ["a", "b"]
        .iter()
        .map(|name| catalog.get_table(name).unwrap())
        .collect::<Vec<_>>();
    let saved_sources = ["a", "b"]
        .iter()
        .map(|name| catalog.get_source(name).unwrap())
        .collect::<Vec<_>>();
    let saved_views = ["a_view", "b_view", "a_doubled"]
        .iter()
        .map(|name| catalog.get_view(name).unwrap())
        .collect::<Vec<_>>();
    drop(client);
    handle.abort();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let recovered_catalog = Arc::new(CatalogStubs::new());
    for table in saved_tables {
        assert!(recovered_catalog.add_table(table));
    }
    for source in saved_sources {
        assert!(recovered_catalog.add_source(source));
    }
    for (view, table) in saved_views.into_iter().zip(["a", "b", "a"]) {
        recovered_catalog.add_view_with_deps(view, vec![table.to_string()]);
    }
    let restarted = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        recovered_catalog,
        Arc::new(NoopViewReader),
        db,
    );
    let (addr, restarted_handle) = restarted.serve_background().await.unwrap();
    let restarted_client = connect_gateway(addr.port()).await;
    wait_ids(&restarted_client, "a_view", &[vec![7]]).await;
    wait_ids(&restarted_client, "b_view", &[vec![9]]).await;
    assert_eq!(ids(&restarted_client, "a").await, vec![vec![7]]);
    assert_eq!(
        query_pairs(
            &restarted_client,
            "SELECT id, doubled FROM a_doubled ORDER BY id"
        )
        .await,
        vec![vec![7, 14]]
    );
    upstream
        .execute("INSERT INTO a VALUES (8)", &[])
        .await
        .unwrap();
    wait_ids(&restarted_client, "a_view", &[vec![7], vec![8]]).await;
    assert_eq!(
        query_pairs(
            &restarted_client,
            "SELECT id, doubled FROM a_doubled ORDER BY id"
        )
        .await,
        vec![vec![7, 14], vec![8, 16]]
    );
    assert_eq!(ids(&restarted_client, "b_view").await, vec![vec![9]]);
    restarted_handle.abort();
}
