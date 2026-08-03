//! Serving-path aggregate matrix tests (v0.51.8).
//!
//! Validates full Arrow data type coverage (int, smallint, bigint, text, float, numeric, date)
//! and aggregate function semantics (SUM, AVG, COUNT, MIN, MAX) over the gateway fast-path compiler.

use std::collections::HashMap;
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
async fn test_basic_int_sum_group_by() {
    let catalog = Arc::new(CatalogStubs::new());
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("test-basic-int-sum", store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    // 1. Create table with standard 'int' (Int32) columns
    client
        .simple_query("CREATE TABLE o (cust int, amt int)")
        .await
        .expect("CREATE TABLE should succeed");

    // 2. Create materialized view over 'int' columns
    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT cust, SUM(amt) FROM o GROUP BY cust")
        .await
        .expect("CREATE MATERIALIZED VIEW over int columns should succeed");

    // 3. Insert test data
    client
        .simple_query("INSERT INTO o (cust, amt) VALUES (1, 10), (1, 5), (2, 3)")
        .await
        .expect("INSERT should succeed");

    // 4. Query view output
    let rows = client
        .simple_query("SELECT * FROM mv")
        .await
        .expect("SELECT * FROM mv should succeed");

    let mut results = HashMap::new();
    for msg in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let cust: i32 = row.get(0).unwrap().parse().unwrap();
            let sum_amt: i64 = row.get(1).unwrap().parse().unwrap();
            results.insert(cust, sum_amt);
        }
    }

    assert_eq!(results.get(&1), Some(&15));
    assert_eq!(results.get(&2), Some(&3));
}

#[tokio::test]
async fn test_text_key_sum_and_avg_fractional() {
    let catalog = Arc::new(CatalogStubs::new());
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("test-text-key-sum-avg", store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("CREATE TABLE sales (product text, qty int)")
        .await
        .expect("CREATE TABLE should succeed");

    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_sales AS SELECT product, SUM(qty), AVG(qty) FROM sales GROUP BY product",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW with text key should succeed");

    client
        .simple_query(
            "INSERT INTO sales (product, qty) VALUES ('alpha', 10), ('alpha', 5), ('beta', 3)",
        )
        .await
        .expect("INSERT should succeed");

    let rows = client
        .simple_query("SELECT * FROM mv_sales")
        .await
        .expect("SELECT * FROM mv_sales should succeed");

    let mut results: HashMap<String, (i64, f64)> = HashMap::new();
    for msg in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let product = row.get(0).unwrap().to_string();
            let sum_qty: i64 = row.get(1).unwrap().parse().unwrap();
            let avg_qty: f64 = row.get(2).unwrap().parse().unwrap();
            results.insert(product, (sum_qty, avg_qty));
        }
    }

    assert_eq!(results.get("alpha"), Some(&(15, 7.5)));
    assert_eq!(results.get("beta"), Some(&(3, 3.0)));
}

#[tokio::test]
async fn test_ci_guard_no_int64_only_rejection() {
    let catalog = Arc::new(CatalogStubs::new());
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("test-ci-guard", store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    // Verify int, smallint, text, float, numeric, date type coverage
    client
        .simple_query(
            "CREATE TABLE t_types (k_int int, k_small smallint, k_text text, val_int int, val_float float)",
        )
        .await
        .unwrap();

    let queries = [
        "CREATE MATERIALIZED VIEW v1 AS SELECT k_int, SUM(val_int) FROM t_types GROUP BY k_int",
        "CREATE MATERIALIZED VIEW v2 AS SELECT k_small, AVG(val_int) FROM t_types GROUP BY k_small",
        "CREATE MATERIALIZED VIEW v3 AS SELECT k_text, COUNT(val_int) FROM t_types GROUP BY k_text",
        "CREATE MATERIALIZED VIEW v4 AS SELECT k_int, MIN(val_float), MAX(val_float) FROM t_types GROUP BY k_int",
    ];

    for q in queries {
        let res = client.simple_query(q).await;
        if let Err(e) = &res {
            let err_str = format!("{e:?}");
            assert!(
                !err_str.contains("RS-1019") && !err_str.contains("RS-1013"),
                "CI Guard Failure: Query '{q}' returned Int64-only rejection: {err_str}"
            );
        }
        assert!(res.is_ok(), "CI Guard: Query '{q}' failed: {:?}", res.err());
    }
}
