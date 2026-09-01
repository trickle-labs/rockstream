//! Type Completeness: ANSI 3VL and Null Preservation Tests (v0.59.20 Slice 3 / Phase 3a).

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
        ShardDb::builder("type-completeness-null-logic", store)
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
async fn test_ansi_3vl_and_truth_tables() {
    let (client, handle, _dir) = client().await;

    client
        .simple_query("CREATE TABLE t_3vl (id INT, a BOOLEAN, b BOOLEAN)")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_3vl VALUES (1, true, true), (2, true, false), (3, false, NULL), (4, NULL, NULL)")
        .await
        .unwrap();

    // In 3VL: WHERE a AND b matches only row 1 (true AND true)
    let res = client
        .simple_query("SELECT id FROM t_3vl WHERE a AND b")
        .await
        .unwrap();

    let mut ids = Vec::new();
    for msg in res {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            ids.push(row.get("id").unwrap().to_string());
        }
    }
    assert_eq!(ids, vec!["1"]);

    handle.abort();
}

#[tokio::test]
async fn test_null_predicates_and_functions() {
    let (client, handle, _dir) = client().await;

    client
        .simple_query("CREATE TABLE t_nulls (id INT, val INT, alt INT)")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_nulls VALUES (1, 10, 20), (2, NULL, 42), (3, NULL, NULL)")
        .await
        .unwrap();

    // IS NULL and IS NOT NULL
    let res_null = client
        .simple_query("SELECT id FROM t_nulls WHERE val IS NULL ORDER BY id")
        .await
        .unwrap();
    let mut null_ids = Vec::new();
    for msg in res_null {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            null_ids.push(row.get("id").unwrap().to_string());
        }
    }
    assert_eq!(null_ids, vec!["2", "3"]);

    let res_not_null = client
        .simple_query("SELECT id FROM t_nulls WHERE val IS NOT NULL")
        .await
        .unwrap();
    let mut not_null_ids = Vec::new();
    for msg in res_not_null {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            not_null_ids.push(row.get("id").unwrap().to_string());
        }
    }
    assert_eq!(not_null_ids, vec!["1"]);

    // COALESCE(val, alt)
    let res_coalesce = client
        .simple_query("SELECT COALESCE(val, alt) FROM t_nulls WHERE id = 2")
        .await
        .unwrap();
    for msg in res_coalesce {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0).unwrap(), "42");
        }
    }

    handle.abort();
}

#[tokio::test]
async fn test_is_distinct_from_semantics() {
    let (client, handle, _dir) = client().await;

    client
        .simple_query("CREATE TABLE t_distinct (id INT, x INT, y INT)")
        .await
        .unwrap();

    client
        .simple_query(
            "INSERT INTO t_distinct VALUES (1, 5, 5), (2, 5, 10), (3, NULL, 5), (4, NULL, NULL)",
        )
        .await
        .unwrap();

    // x IS DISTINCT FROM y should match row 2 (5 != 10) and row 3 (NULL vs 5), but NOT row 1 (5 == 5) and NOT row 4 (NULL vs NULL)
    let res = client
        .simple_query("SELECT id FROM t_distinct WHERE x IS DISTINCT FROM y ORDER BY id")
        .await
        .unwrap();

    let mut distinct_ids = Vec::new();
    for msg in res {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            distinct_ids.push(row.get("id").unwrap().to_string());
        }
    }
    assert_eq!(distinct_ids, vec!["2", "3"]);

    // x IS NOT DISTINCT FROM y should match row 1 (5 == 5) and row 4 (NULL is not distinct from NULL)
    let res_not = client
        .simple_query("SELECT id FROM t_distinct WHERE x IS NOT DISTINCT FROM y ORDER BY id")
        .await
        .unwrap();

    let mut not_distinct_ids = Vec::new();
    for msg in res_not {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            not_distinct_ids.push(row.get("id").unwrap().to_string());
        }
    }
    assert_eq!(not_distinct_ids, vec!["1", "4"]);

    handle.abort();
}
