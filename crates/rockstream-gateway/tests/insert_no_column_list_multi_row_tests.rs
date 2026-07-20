//! v0.51.1 Slice 1 — no-column-list multi-row INSERT proof tests.
//!
//! `INSERT INTO t VALUES (...), (...)` (no explicit column list) must resolve
//! each VALUES tuple against the table's declared column order, not silently
//! drop every value into empty-string columns. All tests in this file run
//! without --features testcontainers.

use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use tokio_postgres::NoTls;

// ── Shared helpers ─────────────────────────────────────────────────────────────

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
    .expect("connect failed");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {e}");
        }
    });
    client
}

fn data_rows_from(
    msgs: &[tokio_postgres::SimpleQueryMessage],
) -> Vec<&tokio_postgres::SimpleQueryRow> {
    msgs.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect()
}

async fn start_gateway_with_shard(
    shard_path: &str,
) -> (u16, tokio::task::JoinHandle<()>, Arc<ShardDb>) {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder(shard_path, store.clone())
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db.clone());
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.port(), handle, shard_db)
}

/// Slice 1 green gate: `INSERT INTO t VALUES (1,'a'),(2,'b')` (no column
/// list) resolves positional values against the table's declared column
/// order — not a NULL/empty-string phantom row.
#[tokio::test]
async fn insert_no_column_list_multi_row_resolves_positional_values() {
    let (port, _handle, shard_db) = start_gateway_with_shard("s1-no-col-list-multi-row").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, name TEXT)")
        .await
        .expect("CREATE TABLE failed");

    client
        .simple_query("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
        .await
        .expect("INSERT failed");

    shard_db.flush().await.unwrap();
    let msgs = client
        .simple_query("SELECT * FROM t ORDER BY id")
        .await
        .expect("SELECT t failed");
    let rows = data_rows_from(&msgs);
    assert_eq!(rows.len(), 2, "expected 2 rows, got {}", rows.len());
    assert_eq!(rows[0].get("id").unwrap_or(""), "1");
    assert_eq!(rows[0].get("name").unwrap_or(""), "alice");
    assert_eq!(rows[1].get("id").unwrap_or(""), "2");
    assert_eq!(rows[1].get("name").unwrap_or(""), "bob");
}

/// Slice 1 green gate: no-column-list INSERT still works for a single row.
#[tokio::test]
async fn insert_no_column_list_single_row_resolves_positional_values() {
    let (port, _handle, shard_db) = start_gateway_with_shard("s1-no-col-list-single-row").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, name TEXT)")
        .await
        .expect("CREATE TABLE failed");

    client
        .simple_query("INSERT INTO t VALUES (3, 'carol')")
        .await
        .expect("INSERT failed");

    shard_db.flush().await.unwrap();
    let msgs = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT t failed");
    let rows = data_rows_from(&msgs);
    assert_eq!(rows.len(), 1, "expected 1 row, got {}", rows.len());
    assert_eq!(rows[0].get("id").unwrap_or(""), "3");
    assert_eq!(rows[0].get("name").unwrap_or(""), "carol");
}

/// Slice 1 regression: no-column-list INSERT into an unresolvable (unknown)
/// table falls through to the existing RS-2056 malformed-row error path
/// rather than silently inserting empty-string columns.
#[tokio::test]
async fn insert_no_column_list_unknown_table_returns_rs2056() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("s1-no-col-list-unknown").await;
    let client = connect_port(port).await;

    // No CREATE TABLE — the table is unknown to the catalog.
    let result = client
        .simple_query("INSERT INTO unknown_table VALUES (1, 'alice')")
        .await;
    let got_rs2056 = match &result {
        Err(e) => {
            e.as_db_error()
                .map(|d| d.message().contains("RS-2056"))
                .unwrap_or(false)
                || e.to_string().contains("RS-2056")
        }
        Ok(msgs) => msgs.iter().any(|m| format!("{m:?}").contains("RS-2056")),
    };
    assert!(
        got_rs2056,
        "expected RS-2056 for no-column-list INSERT into unknown table, got {result:?}"
    );
}

/// Slice 1 regression: an explicit column list continues to work exactly as
/// before (this fix only changes the no-column-list path).
#[tokio::test]
async fn insert_with_explicit_column_list_still_works() {
    let (port, _handle, shard_db) = start_gateway_with_shard("s1-explicit-col-list").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, name TEXT)")
        .await
        .expect("CREATE TABLE failed");

    client
        .simple_query("INSERT INTO t (name, id) VALUES ('dave', 4)")
        .await
        .expect("INSERT failed");

    shard_db.flush().await.unwrap();
    let msgs = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT t failed");
    let rows = data_rows_from(&msgs);
    assert_eq!(rows.len(), 1, "expected 1 row, got {}", rows.len());
    assert_eq!(rows[0].get("id").unwrap_or(""), "4");
    assert_eq!(rows[0].get("name").unwrap_or(""), "dave");
}
