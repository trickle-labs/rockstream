//! Unscoped Pgwire Reachability Test Suite (v0.58.3).
//!
//! Asserts that every SQL keyword, DDL statement, DML mutation, SHOW diagnostic,
//! and protocol feature documented in `docs/language-features.md` executes cleanly
//! through the real pgwire server dispatcher and returns valid PostgreSQL protocol responses.

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use rockstream_control::kek::EnvKekProvider;
use rockstream_control::SecretStore;
use rockstream_gateway::{
    catalog_stubs::{CatalogStubs, CatalogView},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use tempfile::TempDir;
use tokio_postgres::{NoTls, SimpleQueryMessage};

struct StubViewReader;

#[async_trait::async_trait]
impl ViewReader for StubViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }

    fn published_frontier(&self) -> Option<u64> {
        Some(1)
    }
}

struct TestGateway {
    catalog: Arc<CatalogStubs>,
    _handle: tokio::task::JoinHandle<()>,
    _temp_dir: TempDir,
}

async fn start_test_gateway() -> (tokio_postgres::Client, TestGateway) {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("unscoped-reachability", store)
            .build()
            .await
            .unwrap(),
    );

    let catalog = Arc::new(CatalogStubs::new());

    let secret_store = Arc::new(SecretStore::new(
        None,
        Arc::new(EnvKekProvider::from_passphrase("test-passphrase")),
    ));

    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(StubViewReader),
        shard_db,
    )
    .with_secret_store(secret_store);

    let (addr, handle) = server.serve_background().await.unwrap();

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        let _ = connection.await;
    });

    (
        client,
        TestGateway {
            catalog,
            _handle: handle,
            _temp_dir: temp_dir,
        },
    )
}

async fn query_messages(client: &tokio_postgres::Client, query: &str) -> Vec<SimpleQueryMessage> {
    client.simple_query(query).await.unwrap()
}

async fn query_rows(client: &tokio_postgres::Client, query: &str) -> Vec<Vec<String>> {
    let msgs = query_messages(client, query).await;
    let mut out = Vec::new();
    for msg in msgs {
        if let SimpleQueryMessage::Row(row) = msg {
            out.push(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or("").to_owned())
                    .collect(),
            );
        }
    }
    out
}

async fn assert_command_ok(client: &tokio_postgres::Client, query: &str) {
    let msgs = query_messages(client, query).await;
    assert!(
        msgs.iter()
            .any(|m| matches!(m, SimpleQueryMessage::CommandComplete(_))),
        "expected CommandComplete response for query: {query}"
    );
}

#[tokio::test]
async fn test_reachability_create_materialized_view() {
    let (client, _gw) = start_test_gateway().await;
    assert_command_ok(&client, "CREATE TABLE t_mv (id BIGINT, val BIGINT);").await;
    assert_command_ok(
        &client,
        "CREATE MATERIALIZED VIEW v_reach AS SELECT id, val FROM t_mv;",
    )
    .await;
}

#[tokio::test]
async fn test_reachability_refresh_and_replace_view() {
    let (client, _gw) = start_test_gateway().await;
    assert_command_ok(&client, "CREATE TABLE t_ref (id BIGINT, val BIGINT);").await;
    assert_command_ok(
        &client,
        "CREATE OR REPLACE MATERIALIZED VIEW v_refresh AS SELECT id, val FROM t_ref;",
    )
    .await;
    assert_command_ok(&client, "REFRESH MATERIALIZED VIEW v_refresh;").await;
}

#[tokio::test]
async fn test_reachability_create_and_select_inline_view() {
    let (client, _gw) = start_test_gateway().await;
    assert_command_ok(&client, "CREATE TABLE t_inline (id BIGINT, val BIGINT);").await;
    assert_command_ok(
        &client,
        "INSERT INTO t_inline VALUES (1, 100), (2, 200), (3, 300);",
    )
    .await;
    assert_command_ok(
        &client,
        "CREATE VIEW v_inline AS SELECT id, val FROM t_inline WHERE id > 1;",
    )
    .await;

    let mut rows = query_rows(&client, "SELECT id, val FROM v_inline;").await;
    rows.sort();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["2", "200"]);
    assert_eq!(rows[1], vec!["3", "300"]);
}

#[tokio::test]
async fn test_reachability_scalar_expressions() {
    let (client, _gw) = start_test_gateway().await;
    assert_command_ok(&client, "CREATE TABLE t_scalar (id BIGINT, val BIGINT);").await;
    assert_command_ok(&client, "INSERT INTO t_scalar VALUES (1, 100);").await;
    let rows = query_rows(
        &client,
        "SELECT id, CASE WHEN val > 50 THEN 1 ELSE 0 END FROM t_scalar WHERE id = 1;",
    )
    .await;
    assert_eq!(rows, vec![vec!["1", "1"]]);
}

#[tokio::test]
async fn test_reachability_aggregates() {
    let (client, _gw) = start_test_gateway().await;
    assert_command_ok(&client, "CREATE TABLE t_agg (grp BIGINT, val BIGINT);").await;
    assert_command_ok(
        &client,
        "INSERT INTO t_agg VALUES (10, 100), (10, 200), (20, 300);",
    )
    .await;
    let mut rows = query_rows(
        &client,
        "SELECT grp, SUM(val), COUNT(*) FROM t_agg GROUP BY grp ORDER BY grp;",
    )
    .await;
    rows.sort();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["10", "300", "2"]);
    assert_eq!(rows[1], vec!["20", "300", "1"]);
}

#[tokio::test]
async fn test_reachability_joins_and_set_ops() {
    let (client, _gw) = start_test_gateway().await;
    assert_command_ok(&client, "CREATE TABLE t_j1 (id BIGINT, v1 BIGINT);").await;
    assert_command_ok(&client, "CREATE TABLE t_j2 (id BIGINT, v2 BIGINT);").await;
    assert_command_ok(&client, "INSERT INTO t_j1 VALUES (1, 10), (2, 20);").await;
    assert_command_ok(&client, "INSERT INTO t_j2 VALUES (1, 100), (3, 300);").await;

    let join_rows = query_rows(
        &client,
        "SELECT a.id, a.v1, b.v2 FROM t_j1 a INNER JOIN t_j2 b ON a.id = b.id;",
    )
    .await;
    assert_eq!(join_rows, vec![vec!["1", "10", "100"]]);

    let mut union_rows =
        query_rows(&client, "SELECT id FROM t_j1 UNION SELECT id FROM t_j2;").await;
    union_rows.sort();
    assert_eq!(union_rows, vec![vec!["1"], vec!["2"], vec!["3"]]);
}

#[tokio::test]
async fn test_reachability_window_functions() {
    let (client, _gw) = start_test_gateway().await;
    assert_command_ok(
        &client,
        "CREATE TABLE t_win (id BIGINT, k BIGINT, v BIGINT);",
    )
    .await;
    assert_command_ok(
        &client,
        "CREATE MATERIALIZED VIEW v_win AS SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) FROM t_win;",
    )
    .await;
    assert_command_ok(
        &client,
        "INSERT INTO t_win VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30);",
    )
    .await;

    let mut rows = query_rows(&client, "SELECT * FROM v_win;").await;
    rows.sort();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "10", "1"]);
    assert_eq!(rows[1], vec!["1", "20", "2"]);
    assert_eq!(rows[2], vec!["2", "30", "1"]);
}

#[tokio::test]
async fn test_reachability_subscribe_stream() {
    let (client, _gw) = start_test_gateway().await;
    assert_command_ok(&client, "CREATE TABLE t_sub (id BIGINT, val BIGINT);").await;
    assert_command_ok(
        &client,
        "CREATE MATERIALIZED VIEW v_sub AS SELECT id, val FROM t_sub;",
    )
    .await;

    // Negative case over pgwire: invalid target returns RS-2005
    let err = client
        .simple_query("SUBSCRIBE nonexistent_view AS OF NOW WITH SNAPSHOT;")
        .await
        .unwrap_err();
    let msg = err
        .as_db_error()
        .map(|e| e.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(
        msg.contains("RS-2005"),
        "expected RS-2005 error on invalid subscribe target, got {msg}"
    );
}

#[tokio::test]
async fn test_reachability_dml_operations() {
    let (client, _gw) = start_test_gateway().await;
    assert_command_ok(
        &client,
        "CREATE TABLE t_dml (id BIGINT, val BIGINT, grp BIGINT);",
    )
    .await;
    assert_command_ok(
        &client,
        "INSERT INTO t_dml (id, val, grp) VALUES (4, 400, 30);",
    )
    .await;
    assert_command_ok(&client, "UPDATE t_dml SET val = 500 WHERE id = 4;").await;
    assert_command_ok(&client, "DELETE FROM t_dml WHERE id = 4;").await;

    let ret_rows = query_rows(
        &client,
        "INSERT INTO t_dml (id, val, grp) VALUES (5, 500, 30) RETURNING id, val, grp;",
    )
    .await;
    assert_eq!(ret_rows, vec![vec!["5", "500", "30"]]);
}

#[tokio::test]
async fn test_reachability_workload_and_resource_ddl() {
    let (client, gw) = start_test_gateway().await;
    assert_command_ok(&client, "CREATE WORKLOAD wl_prod WITH (cpu_shares = 2);").await;

    assert_command_ok(&client, "ALTER WORKLOAD wl_prod SET (cpu_shares = 4);").await;

    let rows_status = query_rows(&client, "SHOW WORKLOAD STATUS FOR wl_prod;").await;
    assert!(!rows_status.is_empty());

    // Register a sample view to populate resource usage
    gw.catalog.add_view(CatalogView {
        name: "v_res".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let rows_res = query_rows(&client, "SHOW RESOURCE USAGE;").await;
    assert!(!rows_res.is_empty());

    assert_command_ok(&client, "DROP WORKLOAD wl_prod;").await;
}

#[tokio::test]
async fn test_reachability_secret_ddl() {
    let (client, _gw) = start_test_gateway().await;
    assert_command_ok(
        &client,
        "CREATE SECRET s_kfk (TYPE = 'sasl_plain', username = 'u', password = 'p');",
    )
    .await;

    assert_command_ok(&client, "ALTER SECRET s_kfk SET (password = 'p2');").await;

    let rows_secrets = query_rows(&client, "SHOW SECRETS;").await;
    assert!(!rows_secrets.is_empty());

    assert_command_ok(&client, "DROP SECRET s_kfk;").await;
}

#[tokio::test]
async fn test_reachability_index_and_explain() {
    let (client, _gw) = start_test_gateway().await;
    assert_command_ok(&client, "CREATE TABLE t_idx (id BIGINT, val BIGINT);").await;
    assert_command_ok(&client, "CREATE INDEX idx_t ON t_idx (id);").await;

    let exp_rows = query_rows(&client, "EXPLAIN SELECT * FROM t_idx WHERE id = 1;").await;
    assert!(!exp_rows.is_empty());

    let inc_rows = query_rows(
        &client,
        "EXPLAIN INCREMENTAL SELECT id, val FROM t_idx WHERE id > 1;",
    )
    .await;
    assert!(!inc_rows.is_empty());

    assert_command_ok(&client, "DROP INDEX idx_t;").await;
}

#[tokio::test]
async fn test_reachability_system_catalog() {
    let (client, _gw) = start_test_gateway().await;
    let _ = query_rows(
        &client,
        "SELECT * FROM rockstream_catalog.view_resource_usage;",
    )
    .await;
    let _ = query_rows(
        &client,
        "SELECT * FROM rockstream_catalog.workload_resource_usage;",
    )
    .await;
}

#[tokio::test]
async fn test_reachability_removed_connectors_fail_closed() {
    let (client, _gw) = start_test_gateway().await;
    let err = client
        .simple_query("CREATE SOURCE src_s3 TYPE s3 WITH (bucket = 'test');")
        .await
        .unwrap_err();

    let msg = err
        .as_db_error()
        .map(|e| e.message().to_string())
        .unwrap_or_else(|| err.to_string());

    assert!(
        msg.contains("RS-4017"),
        "expected RS-4017 connector.removed error, got: {msg}"
    );
}
