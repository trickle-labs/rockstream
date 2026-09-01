//! Type Completeness: Checked Arithmetic and Numeric Overflow Tests (v0.59.20 Slice 2 / Phase 3a).

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
        ShardDb::builder("type-completeness-arithmetic", store)
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
async fn test_checked_integer_arithmetic_exact_results() {
    let (client, handle, _dir) = client().await;

    client
        .simple_query("CREATE TABLE t_arith (id INT, a BIGINT, b BIGINT)")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_arith VALUES (1, 40, 2), (2, 100, 58), (3, 6, 7)")
        .await
        .unwrap();

    let res = client
        .simple_query("SELECT a + b, a - b, a * b FROM t_arith WHERE id = 1")
        .await
        .unwrap();

    for msg in res {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0).unwrap(), "42");
            assert_eq!(row.get(1).unwrap(), "38");
            assert_eq!(row.get(2).unwrap(), "80");
        }
    }

    handle.abort();
}

#[tokio::test]
async fn test_float_arithmetic() {
    let (client, handle, _dir) = client().await;

    client
        .simple_query("CREATE TABLE t_floats (id INT, x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_floats VALUES (1, 1.5, 2.5), (2, 10.0, 2.5)")
        .await
        .unwrap();

    let res = client
        .simple_query("SELECT x + y, x - y, x * y FROM t_floats WHERE id = 1")
        .await
        .unwrap();

    for msg in res {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0).unwrap(), "4");
            assert_eq!(row.get(1).unwrap(), "-1");
            assert_eq!(row.get(2).unwrap(), "3.75");
        }
    }

    handle.abort();
}

#[tokio::test]
async fn test_decimal_bounds_and_arithmetic() {
    let (client, handle, _dir) = client().await;

    client
        .simple_query("CREATE TABLE t_dec (id INT, amount DECIMAL(18, 4))")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_dec VALUES (1, 100.5000), (2, 25.2500)")
        .await
        .unwrap();

    let res = client
        .simple_query("SELECT amount FROM t_dec ORDER BY id")
        .await
        .unwrap();

    let mut vals = Vec::new();
    for msg in res {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            vals.push(row.get("amount").unwrap().to_string());
        }
    }
    assert_eq!(vals, vec!["100.5000", "25.2500"]);

    handle.abort();
}
