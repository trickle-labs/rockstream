//! v0.51.7 Slice 2 — Catalog Pre-Resolution & Honest Relation Error Semantics Tests.

use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs, view_reader::ViewReader, GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;

struct DummyViewReader;
#[async_trait::async_trait]
impl ViewReader for DummyViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: rockstream_gateway::view_reader::ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }
    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

async fn start_test_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(DummyViewReader);
    let server = GatewayServer::with_catalog("127.0.0.1:0".parse().unwrap(), catalog, view_reader);
    server.serve_background().await.unwrap()
}

async fn start_test_server_with_shard_db() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let store = Arc::new(object_store::memory::InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("absent-rel-shard", store)
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(DummyViewReader);
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        view_reader,
        shard_db,
    );
    server.serve_background().await.unwrap()
}

#[tokio::test]
async fn test_absent_relation_returns_42p01() {
    let (addr, _handle) = start_test_server().await;
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=rockstream dbname=test sslmode=disable",
            addr.port()
        ),
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let res = client.query("SELECT * FROM does_not_exist_xyz", &[]).await;
    assert!(
        res.is_err(),
        "Querying non-existent relation must return error"
    );
    let err = res.unwrap_err();
    let db_err = err.as_db_error().expect("Must be a Postgres DB error");
    assert_eq!(
        db_err.code().code(),
        "42P01",
        "Expected SQLSTATE 42P01 (relation does not exist)"
    );
    assert!(
        db_err.message().contains("RS-1004") || db_err.message().contains("does_not_exist_xyz"),
        "Error message must reference RS-1004 or relation name, got: {}",
        db_err.message()
    );
}

#[tokio::test]
async fn test_failed_create_view_select_returns_error() {
    let (addr, _handle) = start_test_server_with_shard_db().await;
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=rockstream dbname=test sslmode=disable",
            addr.port()
        ),
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Create table first
    client
        .batch_execute("CREATE TABLE t_abs (a int);")
        .await
        .unwrap();

    // Attempt invalid view creation (references non-existent column in shard mode)
    let create_res = client
        .batch_execute(
            "CREATE MATERIALIZED VIEW mv_fail_abc AS SELECT non_existent_col FROM t_abs;",
        )
        .await;
    assert!(
        create_res.is_err(),
        "CREATE VIEW with non-existent column must fail"
    );

    // SELECT from failed view MUST fail with 42P01, NOT return empty OK
    let select_res = client.query("SELECT * FROM mv_fail_abc", &[]).await;
    assert!(
        select_res.is_err(),
        "SELECT from failed view must return error"
    );
    let err = select_res.unwrap_err();
    let db_err = err.as_db_error().expect("Must be a Postgres DB error");
    assert_eq!(
        db_err.code().code(),
        "42P01",
        "Expected SQLSTATE 42P01 for failed view select"
    );
}
