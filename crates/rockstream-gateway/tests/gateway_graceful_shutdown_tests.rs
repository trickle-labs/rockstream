//! Gateway Graceful Shutdown & Drain Tests (v0.59.21 Slice 3 / Phase 3a).

use object_store::local::LocalFileSystem;
use std::sync::Arc;
use tempfile::TempDir;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;

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

#[tokio::test]
async fn test_gateway_drain_client_notice_and_socket_close() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("gateway-drain-test", store)
            .build()
            .await
            .unwrap(),
    );

    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        shard_db,
    );
    let handler = server.handler().clone();
    let (addr, handle) = server.serve_background().await.unwrap();

    // 1. Client connects and executes queries successfully
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=rockstream dbname=rockstream",
            addr.port()
        ),
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        let _ = connection.await;
    });

    let rows = client.simple_query("SELECT 1;").await.unwrap();
    assert!(!rows.is_empty());

    // 2. Mark gateway as draining (admin shutdown initiated)
    handler.mark_draining();
    assert!(handler.is_draining());

    // 3. New query received while draining must be rejected with 57P01 (admin_shutdown / RS-2056)
    let err = client.simple_query("SELECT 2;").await.unwrap_err();
    let code = err.code().map(|c| c.code()).unwrap_or("");
    assert_eq!(
        code, "57P01",
        "expected SQLSTATE 57P01 admin_shutdown, got {err:?}"
    );
    let db_msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(
        db_msg.contains("RS-2056")
            || db_msg.contains("administrator shutdown")
            || err.to_string().contains("RS-2056"),
        "error message must mention admin shutdown / RS-2056: {db_msg}"
    );

    handle.abort();
}
