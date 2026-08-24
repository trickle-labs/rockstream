//! End-to-end pgwire error protocol envelope tests (v0.59.12 / DOC-01).

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use rockstream_types::error_code::ErrorCatalog;
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
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
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
async fn test_pgwire_error_response_envelope() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("error-envelope-test", store, catalog).await;
    let client = connect_port(port).await;

    // 1. Missing table/view error
    let res = client
        .simple_query("SELECT * FROM non_existent_table")
        .await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    let db_err = err.as_db_error().expect("must be pg db error");
    assert_eq!(db_err.code().code(), "42P01");
    assert_eq!(db_err.severity(), "ERROR");

    // 2. Syntax/parse error on CREATE SINK
    let res2 = client
        .simple_query("CREATE SINK s FOR VIEW missing_view")
        .await;
    assert!(res2.is_err());
    let err2 = res2.unwrap_err();
    let db_err2 = err2.as_db_error().expect("must be pg db error");
    assert_eq!(db_err2.code().code(), "42601");
    assert_eq!(db_err2.severity(), "ERROR");

    // 3. Removed connector
    let res3 = client
        .simple_query("CREATE SOURCE s TYPE s3 (bucket='b') FORMAT json")
        .await;
    assert!(res3.is_err());
    let err3 = res3.unwrap_err();
    let db_err3 = err3.as_db_error().expect("must be pg db error");
    assert_eq!(db_err3.code().code(), "0A000");
    assert_eq!(db_err3.severity(), "ERROR");
    assert!(db_err3.message().contains("RS-4017"));

    // 4. Verify catalog integrity
    let cat = ErrorCatalog::current();
    assert!(!cat.errors().is_empty());
}
