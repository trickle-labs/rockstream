//! Type Completeness: Fail-Closed Unsupported Operation Rejection Tests (v0.59.20 Slice 6 / Phase 3b).

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use tempfile::TempDir;
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

async fn client() -> (tokio_postgres::Client, tokio::task::JoinHandle<()>, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("type-completeness-rejections", store)
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
    (client, handle, dir)
}

#[tokio::test]
async fn test_unsupported_float_join_rejection() {
    let (client, handle, _dir) = client().await;

    client
        .simple_query("CREATE TABLE t_f1 (id INT, val DOUBLE PRECISION)")
        .await
        .unwrap();

    client
        .simple_query("CREATE TABLE t_f2 (id INT, val DOUBLE PRECISION)")
        .await
        .unwrap();

    // Floating-point equi-join in view compilation must be rejected fail-closed with RS-1019
    let res = client
        .simple_query(
            "CREATE VIEW v_f_join AS SELECT t_f1.id FROM t_f1 JOIN t_f2 ON t_f1.val = t_f2.val",
        )
        .await;

    assert!(res.is_err(), "Float equi-join view must be rejected");
    let err = res.err().unwrap();
    let err_msg = err.as_db_error().map(|d| d.message()).unwrap_or("");
    assert!(
        err_msg.contains("RS-1019")
            || err_msg.contains("join keys must have total ordering")
            || err_msg.contains("floating-point"),
        "Expected error mentioning RS-1019 or float ordering, got: {err_msg}"
    );

    handle.abort();
}

#[tokio::test]
async fn test_unsupported_invalid_casts_rejection() {
    let (client, handle, _dir) = client().await;

    client
        .simple_query("CREATE TABLE t_data (id INT, s TEXT)")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_data VALUES (1, 'not_a_number')")
        .await
        .unwrap();

    // Invalid string to integer cast at runtime / query level
    let res_int = client
        .simple_query("SELECT CAST(s AS INT) FROM t_data WHERE id = 1")
        .await;
    assert!(
        res_int.is_err(),
        "Invalid string to integer cast must be rejected"
    );

    handle.abort();
}

#[tokio::test]
async fn test_unsupported_non_integer_bitwise_rejection() {
    let (client, handle, _dir) = client().await;

    client
        .simple_query("CREATE TABLE t_bitwise (id INT, f DOUBLE PRECISION, s TEXT)")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_bitwise VALUES (1, 1.5, 'abc')")
        .await
        .unwrap();

    // Bitwise operator on float / text must be rejected fail-closed during query evaluation
    let res_str = client
        .simple_query("SELECT s | 2 FROM t_bitwise WHERE id = 1")
        .await;
    assert!(res_str.is_err(), "Bitwise op on text must fail");

    handle.abort();
}
