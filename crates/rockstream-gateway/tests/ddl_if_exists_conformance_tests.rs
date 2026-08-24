use std::sync::Arc;

use object_store::local::LocalFileSystem;
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

fn check_err(err: &tokio_postgres::Error, patterns: &[&str]) {
    let err_str = err.to_string();
    let db_err = err.as_db_error();
    let msg = db_err.map(|e| e.message()).unwrap_or("");
    let code = db_err.map(|e| e.code().code()).unwrap_or("");
    let matched = patterns
        .iter()
        .any(|p| err_str.contains(p) || msg.contains(p) || code == *p);
    assert!(
        matched,
        "Expected one of patterns {patterns:?}, got err_str='{err_str}', db_msg='{msg}', code='{code}'"
    );
}

#[tokio::test]
async fn test_table_if_exists_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("table-if-exists", store, catalog).await;
    let client = connect_port(port).await;

    // CREATE TABLE IF NOT EXISTS on fresh table
    client
        .simple_query("CREATE TABLE IF NOT EXISTS my_tbl (id BIGINT, name TEXT)")
        .await
        .unwrap();

    // CREATE TABLE IF NOT EXISTS on already existing table -> clean no-op
    client
        .simple_query("CREATE TABLE IF NOT EXISTS my_tbl (id BIGINT, name TEXT)")
        .await
        .unwrap();

    // CREATE TABLE without IF NOT EXISTS on existing table -> RS-4001 / already exists
    let err = client
        .simple_query("CREATE TABLE my_tbl (id BIGINT, name TEXT)")
        .await
        .unwrap_err();
    check_err(&err, &["already exists", "42P07", "RS-4001"]);

    // DROP TABLE IF EXISTS on existing table -> dropped
    client
        .simple_query("DROP TABLE IF EXISTS my_tbl")
        .await
        .unwrap();

    // DROP TABLE IF EXISTS on absent table -> clean no-op
    client
        .simple_query("DROP TABLE IF EXISTS my_tbl")
        .await
        .unwrap();

    // DROP TABLE without IF EXISTS on absent table -> RS-4004 / does not exist
    let err2 = client.simple_query("DROP TABLE my_tbl").await.unwrap_err();
    check_err(&err2, &["does not exist", "42P01", "RS-4004"]);
}

#[tokio::test]
async fn test_view_if_exists_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("view-if-exists", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE base_t (id BIGINT, val TEXT)")
        .await
        .unwrap();

    // CREATE VIEW IF NOT EXISTS on fresh view
    client
        .simple_query("CREATE VIEW IF NOT EXISTS v_test AS SELECT id, val FROM base_t")
        .await
        .unwrap();

    // CREATE VIEW IF NOT EXISTS on existing view -> clean no-op
    client
        .simple_query("CREATE VIEW IF NOT EXISTS v_test AS SELECT id, val FROM base_t")
        .await
        .unwrap();

    // CREATE VIEW without IF NOT EXISTS / OR REPLACE on existing view -> error
    let err = client
        .simple_query("CREATE VIEW v_test AS SELECT id, val FROM base_t")
        .await
        .unwrap_err();
    check_err(&err, &["already exists", "42P07", "RS-4001"]);

    // DROP VIEW IF EXISTS on existing view -> dropped
    client
        .simple_query("DROP VIEW IF EXISTS v_test")
        .await
        .unwrap();

    // DROP VIEW IF EXISTS on absent view -> clean no-op
    client
        .simple_query("DROP VIEW IF EXISTS v_test")
        .await
        .unwrap();

    // DROP VIEW without IF EXISTS on absent view -> error
    let err2 = client.simple_query("DROP VIEW v_test").await.unwrap_err();
    check_err(&err2, &["does not exist", "42P01", "RS-4004"]);
}

#[tokio::test]
async fn test_materialized_view_if_exists_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("matview-if-exists", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE base_m (id BIGINT, val TEXT)")
        .await
        .unwrap();

    // CREATE MATERIALIZED VIEW IF NOT EXISTS on fresh view
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_test AS SELECT id, val FROM base_m",
        )
        .await
        .unwrap();

    // CREATE MATERIALIZED VIEW IF NOT EXISTS on existing view -> clean no-op
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_test AS SELECT id, val FROM base_m",
        )
        .await
        .unwrap();

    // DROP MATERIALIZED VIEW IF EXISTS on existing view -> dropped
    client
        .simple_query("DROP MATERIALIZED VIEW IF EXISTS mv_test")
        .await
        .unwrap();

    // DROP MATERIALIZED VIEW IF EXISTS on absent view -> clean no-op
    client
        .simple_query("DROP MATERIALIZED VIEW IF EXISTS mv_test")
        .await
        .unwrap();

    // DROP MATERIALIZED VIEW without IF EXISTS on absent view -> error
    let err = client
        .simple_query("DROP MATERIALIZED VIEW mv_test")
        .await
        .unwrap_err();
    check_err(&err, &["does not exist", "42P01", "RS-4004"]);
}

#[tokio::test]
async fn test_index_if_exists_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("index-if-exists", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_idx (id BIGINT, val TEXT)")
        .await
        .unwrap();

    // CREATE INDEX IF NOT EXISTS on fresh index
    client
        .simple_query("CREATE INDEX IF NOT EXISTS idx_id ON t_idx (id)")
        .await
        .unwrap();

    // CREATE INDEX IF NOT EXISTS on existing index -> clean no-op
    client
        .simple_query("CREATE INDEX IF NOT EXISTS idx_id ON t_idx (id)")
        .await
        .unwrap();

    // CREATE INDEX without IF NOT EXISTS on existing index -> error
    let err = client
        .simple_query("CREATE INDEX idx_id ON t_idx (id)")
        .await
        .unwrap_err();
    check_err(&err, &["already exists", "42710", "RS-2016", "RS-4001"]);

    // DROP INDEX IF EXISTS on existing index -> dropped
    client
        .simple_query("DROP INDEX IF EXISTS idx_id")
        .await
        .unwrap();

    // DROP INDEX IF EXISTS on absent index -> clean no-op
    client
        .simple_query("DROP INDEX IF EXISTS idx_id")
        .await
        .unwrap();

    // DROP INDEX without IF EXISTS on absent index -> error
    let err2 = client.simple_query("DROP INDEX idx_id").await.unwrap_err();
    check_err(&err2, &["does not exist", "42704", "RS-4004"]);
}

#[tokio::test]
async fn test_workload_if_exists_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("workload-if-exists", store, catalog).await;
    let client = connect_port(port).await;

    // CREATE WORKLOAD IF NOT EXISTS on fresh workload
    client
        .simple_query("CREATE WORKLOAD IF NOT EXISTS wl_fast WITH (MEMORY_LIMIT = 1048576)")
        .await
        .unwrap();

    // CREATE WORKLOAD IF NOT EXISTS on existing workload -> clean no-op
    client
        .simple_query("CREATE WORKLOAD IF NOT EXISTS wl_fast WITH (MEMORY_LIMIT = 1048576)")
        .await
        .unwrap();

    // CREATE WORKLOAD without IF NOT EXISTS on existing workload -> error
    let err = client
        .simple_query("CREATE WORKLOAD wl_fast WITH (MEMORY_LIMIT = 1048576)")
        .await
        .unwrap_err();
    check_err(&err, &["already exists", "42710", "RS-1006", "RS-4001"]);

    // DROP WORKLOAD IF EXISTS on existing workload -> dropped
    client
        .simple_query("DROP WORKLOAD IF EXISTS wl_fast")
        .await
        .unwrap();

    // DROP WORKLOAD IF EXISTS on absent workload -> clean no-op
    client
        .simple_query("DROP WORKLOAD IF EXISTS wl_fast")
        .await
        .unwrap();

    // DROP WORKLOAD without IF EXISTS on absent workload -> error
    let err2 = client
        .simple_query("DROP WORKLOAD wl_fast")
        .await
        .unwrap_err();
    check_err(&err2, &["does not exist", "42704", "RS-1005", "RS-4004"]);
}

#[tokio::test]
async fn test_secret_if_exists_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("secret-if-exists", store, catalog).await;
    let client = connect_port(port).await;

    // CREATE SECRET IF NOT EXISTS
    client
        .simple_query(
            "CREATE SECRET IF NOT EXISTS s_key (TYPE = 'postgres_role', PASSWORD = 'abc')",
        )
        .await
        .unwrap();

    // CREATE SECRET IF NOT EXISTS on existing -> clean no-op
    client
        .simple_query(
            "CREATE SECRET IF NOT EXISTS s_key (TYPE = 'postgres_role', PASSWORD = 'abc')",
        )
        .await
        .unwrap();

    // DROP SECRET IF EXISTS on existing -> dropped
    client
        .simple_query("DROP SECRET IF EXISTS s_key")
        .await
        .unwrap();

    // DROP SECRET IF EXISTS on absent -> clean no-op
    client
        .simple_query("DROP SECRET IF EXISTS s_key")
        .await
        .unwrap();

    // DROP SECRET without IF EXISTS on absent -> error
    let err = client.simple_query("DROP SECRET s_key").await.unwrap_err();
    check_err(&err, &["does not exist", "42704", "RS-2420", "RS-4004"]);
}

#[tokio::test]
async fn test_source_if_exists_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("source-if-exists", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE SECRET s_cdc (TYPE = 'postgres_role', PASSWORD = 'p')")
        .await
        .unwrap();

    // CREATE SOURCE IF NOT EXISTS
    client
        .simple_query("CREATE SOURCE IF NOT EXISTS src_test TYPE postgres_cdc (publication = 'pub1', slot = 'slot1', credential_ref = 's_cdc') FORMAT pgoutput")
        .await
        .unwrap();

    // CREATE SOURCE IF NOT EXISTS on existing -> clean no-op
    client
        .simple_query("CREATE SOURCE IF NOT EXISTS src_test TYPE postgres_cdc (publication = 'pub1', slot = 'slot1', credential_ref = 's_cdc') FORMAT pgoutput")
        .await
        .unwrap();

    // CREATE SOURCE without IF NOT EXISTS on existing -> error
    let err = client
        .simple_query("CREATE SOURCE src_test TYPE postgres_cdc (publication = 'pub1', slot = 'slot1', credential_ref = 's_cdc') FORMAT pgoutput")
        .await
        .unwrap_err();
    check_err(&err, &["already exists", "42710", "RS-4001", "RS-4010"]);

    // DROP SOURCE IF EXISTS on existing -> dropped
    client
        .simple_query("DROP SOURCE IF EXISTS src_test")
        .await
        .unwrap();

    // DROP SOURCE IF EXISTS on absent -> clean no-op
    client
        .simple_query("DROP SOURCE IF EXISTS src_test")
        .await
        .unwrap();

    // DROP SOURCE without IF EXISTS on absent -> error
    let err = client
        .simple_query("DROP SOURCE src_test")
        .await
        .unwrap_err();
    check_err(&err, &["does not exist", "42704", "RS-4009", "RS-4004"]);
}

#[tokio::test]
async fn test_sink_if_exists_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("sink-if-exists", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE base_sink (id BIGINT, val TEXT)")
        .await
        .unwrap();
    client
        .simple_query("CREATE VIEW v_sink AS SELECT id, val FROM base_sink")
        .await
        .unwrap();

    // CREATE SINK IF NOT EXISTS
    client
        .simple_query("CREATE SINK IF NOT EXISTS sink_test FOR VIEW v_sink TO ICEBERG 's3://bucket/path' WITH (catalog = 'filesystem')")
        .await
        .unwrap();

    // CREATE SINK IF NOT EXISTS on existing -> clean no-op
    client
        .simple_query("CREATE SINK IF NOT EXISTS sink_test FOR VIEW v_sink TO ICEBERG 's3://bucket/path' WITH (catalog = 'filesystem')")
        .await
        .unwrap();

    // CREATE SINK without IF NOT EXISTS on existing -> error
    let err = client
        .simple_query("CREATE SINK sink_test FOR VIEW v_sink TO ICEBERG 's3://bucket/path' WITH (catalog = 'filesystem')")
        .await
        .unwrap_err();
    check_err(&err, &["already exists", "42710", "RS-4001", "RS-4007"]);

    // DROP SINK IF EXISTS on existing -> dropped
    client
        .simple_query("DROP SINK IF EXISTS sink_test")
        .await
        .unwrap();

    // DROP SINK IF EXISTS on absent -> clean no-op
    client
        .simple_query("DROP SINK IF EXISTS sink_test")
        .await
        .unwrap();

    // DROP SINK without IF EXISTS on absent -> error
    let err = client
        .simple_query("DROP SINK sink_test")
        .await
        .unwrap_err();
    check_err(&err, &["does not exist", "42704", "RS-4004"]);
}

#[tokio::test]
async fn test_schema_if_exists_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("schema-if-exists", store, catalog).await;
    let client = connect_port(port).await;

    // CREATE SCHEMA IF NOT EXISTS on fresh schema
    client
        .simple_query("CREATE SCHEMA IF NOT EXISTS my_schema")
        .await
        .unwrap();

    // CREATE SCHEMA IF NOT EXISTS on existing schema -> clean no-op
    client
        .simple_query("CREATE SCHEMA IF NOT EXISTS my_schema")
        .await
        .unwrap();

    // CREATE SCHEMA without IF NOT EXISTS on existing schema -> error
    let err = client
        .simple_query("CREATE SCHEMA my_schema")
        .await
        .unwrap_err();
    check_err(&err, &["already exists", "42P06", "RS-4001"]);

    // DROP SCHEMA IF EXISTS on existing schema -> dropped
    client
        .simple_query("DROP SCHEMA IF EXISTS my_schema")
        .await
        .unwrap();

    // DROP SCHEMA IF EXISTS on absent schema -> clean no-op
    client
        .simple_query("DROP SCHEMA IF EXISTS my_schema")
        .await
        .unwrap();

    // DROP SCHEMA without IF EXISTS on absent schema -> error
    let err2 = client
        .simple_query("DROP SCHEMA my_schema")
        .await
        .unwrap_err();
    check_err(&err2, &["does not exist", "3F000", "RS-4004"]);
}
