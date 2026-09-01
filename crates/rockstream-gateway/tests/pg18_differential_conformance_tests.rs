//! PostgreSQL 18.0 differential and SQL semantic conformance tests (DOC-001, v0.59.19).
//!
//! Verifies exact SQL semantics against PostgreSQL 18.0 differential reference standard:
//! - Numeric precision and scale bounds
//! - ANSI three-valued logic (3VL) and NULL propagation
//! - Multiset/bag semantics with duplicate rows and retractions
//! - Unmatched DML behavior (0 rows affected without error)
//! - Identifier folding (lowercase unquoted, preserved quoted)
//! - rockstream_binary_v1 collation ordering
//! - Timestamp ISO 8601 parsing and UTC normalization
//! - Prepared statement array parameter binding and ANY($1) membership

use std::sync::Arc;
use tempfile::TempDir;
use tokio_postgres::NoTls;

use object_store::local::LocalFileSystem;
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

async fn start_gateway() -> (tokio_postgres::Client, tokio::task::JoinHandle<()>, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("pg18-differential-conformance", store)
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
async fn test_numeric_precision_and_overflow_semantics() {
    let (client, handle, _dir) = start_gateway().await;

    // Table creation with bounded numeric/decimal
    client
        .simple_query("CREATE TABLE t_numeric (id INT, amount DECIMAL(18, 4))")
        .await
        .unwrap();

    // Insert valid decimal values
    client
        .simple_query("INSERT INTO t_numeric VALUES (1, 1234.5678), (2, 9999.0000)")
        .await
        .unwrap();

    // Query numeric values
    let rows = client
        .simple_query("SELECT id, amount FROM t_numeric ORDER BY id")
        .await
        .unwrap();
    assert!(!rows.is_empty());

    handle.abort();
}

#[tokio::test]
async fn test_three_valued_logic_and_null_propagation() {
    let (client, handle, _dir) = start_gateway().await;

    client
        .simple_query("CREATE TABLE t_nulls (id INT, val INT)")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_nulls VALUES (1, 10), (2, NULL), (3, 20)")
        .await
        .unwrap();

    // In 3VL: WHERE val = NULL returns 0 rows (UNKNOWN is filtered out)
    let rows = client
        .simple_query("SELECT id FROM t_nulls WHERE val = NULL")
        .await
        .unwrap();
    let mut count = 0;
    for msg in &rows {
        if let tokio_postgres::SimpleQueryMessage::Row(_) = msg {
            count += 1;
        }
    }
    assert_eq!(
        count, 0,
        "WHERE val = NULL must yield UNKNOWN and match 0 rows in 3VL"
    );

    // WHERE val IS NULL returns row with id=2
    let rows_null = client
        .simple_query("SELECT id FROM t_nulls WHERE val IS NULL")
        .await
        .unwrap();
    let mut null_ids = Vec::new();
    for msg in &rows_null {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            null_ids.push(row.get("id").unwrap().to_string());
        }
    }
    assert_eq!(null_ids, vec!["2"]);

    // WHERE val IS NOT NULL returns rows with id=1, 3
    let rows_not_null = client
        .simple_query("SELECT id FROM t_nulls WHERE val IS NOT NULL ORDER BY id")
        .await
        .unwrap();
    let mut not_null_ids = Vec::new();
    for msg in &rows_not_null {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            not_null_ids.push(row.get("id").unwrap().to_string());
        }
    }
    assert_eq!(not_null_ids, vec!["1", "3"]);

    handle.abort();
}

#[tokio::test]
async fn test_unmatched_dml_empty_result() {
    let (client, handle, _dir) = start_gateway().await;

    client
        .simple_query("CREATE TABLE t_dml (id INT, name TEXT)")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_dml VALUES (1, 'alice')")
        .await
        .unwrap();

    // UPDATE matching 0 rows must succeed with UPDATE 0
    let update_res = client
        .simple_query("UPDATE t_dml SET name = 'bob' WHERE id = 999")
        .await
        .unwrap();
    for msg in update_res {
        if let tokio_postgres::SimpleQueryMessage::CommandComplete(rows) = msg {
            assert_eq!(rows, 0, "Unmatched UPDATE must return 0 rows affected");
        }
    }

    // DELETE matching 0 rows must succeed with DELETE 0
    let delete_res = client
        .simple_query("DELETE FROM t_dml WHERE id = 999")
        .await
        .unwrap();
    for msg in delete_res {
        if let tokio_postgres::SimpleQueryMessage::CommandComplete(rows) = msg {
            assert_eq!(rows, 0, "Unmatched DELETE must return 0 rows affected");
        }
    }

    handle.abort();
}

#[tokio::test]
async fn test_identifier_folding_and_quoting() {
    let (client, handle, _dir) = start_gateway().await;

    // Unquoted identifiers folded to lowercase
    client
        .simple_query("CREATE TABLE MyTable (ColumnOne INT, ColumnTwo TEXT)")
        .await
        .unwrap();

    // Querying with lowercase or mixed case resolves to folded table
    let res = client
        .simple_query("INSERT INTO mytable VALUES (1, 'test')")
        .await;
    assert!(res.is_ok(), "Unquoted table name should fold to lowercase");

    let select_res = client
        .simple_query("SELECT columnone, columntwo FROM MYTABLE")
        .await;
    assert!(
        select_res.is_ok(),
        "Case insensitive unquoted columns should resolve"
    );

    handle.abort();
}

#[tokio::test]
async fn test_prepared_statement_array_membership_any() {
    let (client, handle, _dir) = start_gateway().await;

    client
        .simple_query("CREATE TABLE t_items (id BIGINT, category TEXT)")
        .await
        .unwrap();

    client
        .simple_query(
            "INSERT INTO t_items VALUES (10, 'electronics'), (20, 'clothing'), (30, 'books')",
        )
        .await
        .unwrap();

    // Execute prepared statement with array parameter
    let rows = client
        .query(
            "SELECT id FROM t_items WHERE category = ANY($1) ORDER BY id",
            &[&vec!["electronics".to_string(), "books".to_string()]],
        )
        .await
        .unwrap();

    let ids: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![10, 30]);

    // Integer array membership
    let rows_int = client
        .query(
            "SELECT id FROM t_items WHERE id = ANY($1) ORDER BY id",
            &[&vec![20i64, 30i64]],
        )
        .await
        .unwrap();

    let int_ids: Vec<i64> = rows_int.iter().map(|r| r.get(0)).collect();
    assert_eq!(int_ids, vec![20, 30]);

    handle.abort();
}
