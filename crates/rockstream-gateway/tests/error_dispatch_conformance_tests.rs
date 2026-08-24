//! Dispatch and wire error propagation conformance tests (v0.59.12 / DOC-01).

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_control::kek::EnvKekProvider;
use rockstream_control::SecretStore;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use rockstream_types::error_code::{
    ErrorDescriptor, RS_1016, RS_2001, RS_2404, RS_2426, RS_4001, RS_4017,
};
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
    shard_path: &str,
    store: Arc<dyn ObjectStore>,
    catalog: Arc<CatalogStubs>,
) -> (u16, tokio::task::JoinHandle<()>, Arc<ShardDb>) {
    let shard_db = Arc::new(ShardDb::builder(shard_path, store).build().await.unwrap());
    let secret_store = Arc::new(SecretStore::new(
        None,
        Arc::new(EnvKekProvider::from_passphrase("test-kek-passphrase")),
    ));
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shard_db.clone(),
    )
    .with_secret_store(secret_store);
    let (addr, handle) = server.serve_background().await.unwrap();
    (addr.port(), handle, shard_db)
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

#[tokio::test]
async fn test_absent_view_error_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("absent-view-test", store, catalog).await;
    let client = connect_port(port).await;

    let res = client
        .simple_query("SELECT * FROM non_existent_view_42")
        .await;
    assert!(res.is_err(), "querying absent view must fail");
    let err = res.unwrap_err();
    let db_err = err.as_db_error().expect("must be a Postgres DB error");
    assert_eq!(db_err.code().code(), "42P01", "must return SQLSTATE 42P01");
    assert!(
        db_err.message().contains("RS-1004")
            || db_err.message().contains("RS-2001")
            || db_err.message().contains("non_existent_view_42"),
        "error message must contain relation or error code: {}",
        db_err.message()
    );

    let desc = ErrorDescriptor::lookup(RS_2001).expect("RS_2001 must exist");
    assert_eq!(desc.sqlstate, "42P01");
}

#[tokio::test]
async fn test_duplicate_table_error_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("dup-table-test", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE existing_t (id INT)")
        .await
        .unwrap();

    let res = client
        .simple_query("CREATE TABLE existing_t (id INT)")
        .await;
    assert!(
        res.is_err(),
        "duplicate table creation without IF NOT EXISTS must fail"
    );
    let err = res.unwrap_err();
    let db_err = err.as_db_error().expect("must be a Postgres DB error");
    assert!(
        db_err.code().code() == "42710" || db_err.code().code() == "42P07",
        "expected duplicate table SQLSTATE 42710 or 42P07, got {}",
        db_err.code().code()
    );
    assert!(
        db_err.message().contains("RS-4001") || db_err.message().contains("already exists"),
        "error message must reference RS-4001 or already exists: {}",
        db_err.message()
    );

    let desc = ErrorDescriptor::lookup(RS_4001).expect("RS_4001 must exist");
    assert_eq!(desc.sqlstate, "42710");
}

#[tokio::test]
async fn test_malformed_returning_error_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("returning-err-test", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_ret (id BIGINT, val TEXT)")
        .await
        .unwrap();

    // Invalid syntax or malformed returning clause
    let res = client
        .simple_query("UPDATE t_ret SET val = 'x' RETURNING")
        .await;
    assert!(res.is_err(), "malformed returning must fail");
    let err = res.unwrap_err();
    let db_err = err.as_db_error().expect("must be DB error");
    assert_eq!(
        db_err.code().code(),
        "42601",
        "syntax error must have SQLSTATE 42601"
    );
}

#[tokio::test]
async fn test_arithmetic_overflow_error_dispatch() {
    let desc = ErrorDescriptor::lookup(RS_1016).expect("RS_1016 must exist");
    assert_eq!(desc.sqlstate, "22003");
    assert_eq!(desc.key, "aggregate.numeric_overflow");
    assert_eq!(desc.severity.to_string(), "ERROR");
}

#[tokio::test]
async fn test_mtls_missing_cert_error_dispatch() {
    let desc = ErrorDescriptor::lookup(RS_2404).expect("RS_2404 must exist");
    assert_eq!(desc.sqlstate, "28000");
    assert_eq!(desc.key, "auth.mtls_no_verified_cert");
}

#[tokio::test]
async fn test_secret_in_use_error_dispatch() {
    let desc = ErrorDescriptor::lookup(RS_2426).expect("RS_2426 must exist");
    assert_eq!(desc.sqlstate, "55000");
    assert_eq!(desc.key, "secret.in_use_by_source_or_sink");
}

#[tokio::test]
async fn test_removed_connector_error_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("removed-conn-test", store, catalog).await;
    let client = connect_port(port).await;

    let res = client
        .simple_query("CREATE SOURCE s TYPE s3 (bucket='b') FORMAT json")
        .await;
    assert!(res.is_err(), "creating removed source must fail");
    let err = res.unwrap_err();
    let db_err = err.as_db_error().expect("must be DB error");
    assert_eq!(db_err.code().code(), "0A000");
    assert!(db_err.message().contains("RS-4017"));

    let desc = ErrorDescriptor::lookup(RS_4017).expect("RS_4017 must exist");
    assert_eq!(desc.sqlstate, "0A000");
}
