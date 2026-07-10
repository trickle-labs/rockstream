//! v0.41 Slice 9 — transaction savepoint proof tests (LFS + unit-level).
//!
//! All tests in this file run without --features testcontainers.

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

async fn start_gateway_noop() -> (u16, tokio::task::JoinHandle<()>) {
    let catalog = Arc::new(CatalogStubs::new());
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, catalog, Arc::new(NoopViewReader));
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.port(), handle)
}

// ── S9/P1 — Savepoint partial-write (LFS) ─────────────────────────────────────

/// P1 green gate (LFS): BEGIN; INSERT; SAVEPOINT s; INSERT; ROLLBACK TO s; COMMIT
/// → only the pre-savepoint row is visible.
#[tokio::test]
async fn test_savepoint_rollback_partial_write() {
    let (port, _handle, shard_db) = start_gateway_with_shard("sp-p1-lfs").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE items (id BIGINT, val TEXT)")
        .await
        .expect("CREATE TABLE failed");

    client
        .simple_query("SET rockstream.idempotency_key = 'sp-p1'")
        .await
        .expect("SET idempotency_key failed");

    client.simple_query("BEGIN").await.expect("BEGIN failed");

    client
        .simple_query("INSERT INTO items (id, val) VALUES (1, 'before')")
        .await
        .expect("INSERT 1 failed");

    client
        .simple_query("SAVEPOINT s")
        .await
        .expect("SAVEPOINT failed");

    client
        .simple_query("INSERT INTO items (id, val) VALUES (2, 'after')")
        .await
        .expect("INSERT 2 failed");

    client
        .simple_query("ROLLBACK TO SAVEPOINT s")
        .await
        .expect("ROLLBACK TO SAVEPOINT failed");

    client.simple_query("COMMIT").await.expect("COMMIT failed");

    shard_db.flush().await.unwrap();

    let msgs = client
        .simple_query("SELECT * FROM items")
        .await
        .expect("SELECT failed");
    let rows = data_rows_from(&msgs);
    assert_eq!(
        rows.len(),
        1,
        "expected exactly 1 row after ROLLBACK TO SAVEPOINT, got {}",
        rows.len()
    );
    assert_eq!(
        rows[0].get("val").unwrap_or(""),
        "before",
        "expected 'before' row, got: {:?}",
        rows[0].get("val")
    );
}

// ── S9/P2 — Release savepoint does not discard (LFS) ──────────────────────────

/// P2 green gate (LFS): RELEASE SAVEPOINT does not discard already-buffered ops.
#[tokio::test]
async fn test_savepoint_release_does_not_discard() {
    let (port, _handle, shard_db) = start_gateway_with_shard("sp-p2-lfs").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE items (id BIGINT, val TEXT)")
        .await
        .expect("CREATE TABLE failed");

    client
        .simple_query("SET rockstream.idempotency_key = 'sp-p2'")
        .await
        .expect("SET idempotency_key failed");

    client.simple_query("BEGIN").await.expect("BEGIN failed");

    client
        .simple_query("INSERT INTO items (id, val) VALUES (1, 'a')")
        .await
        .expect("INSERT 1 failed");

    client
        .simple_query("SAVEPOINT s")
        .await
        .expect("SAVEPOINT failed");

    client
        .simple_query("INSERT INTO items (id, val) VALUES (2, 'b')")
        .await
        .expect("INSERT 2 failed");

    client
        .simple_query("RELEASE SAVEPOINT s")
        .await
        .expect("RELEASE SAVEPOINT failed");

    client.simple_query("COMMIT").await.expect("COMMIT failed");

    shard_db.flush().await.unwrap();

    let msgs = client
        .simple_query("SELECT * FROM items")
        .await
        .expect("SELECT failed");
    let rows = data_rows_from(&msgs);
    assert_eq!(
        rows.len(),
        2,
        "expected 2 rows after RELEASE SAVEPOINT + COMMIT, got {}",
        rows.len()
    );
}

// ── S9/P3 — Transaction-status byte lifecycle (unit, no shard) ────────────────

/// P3 green gate: tx-status byte ('I'/'T'/'E') lifecycle through BEGIN/COMMIT/error/ROLLBACK.
#[tokio::test]
async fn test_tx_status_lifecycle() {
    let (port, _handle) = start_gateway_noop().await;
    let client = connect_port(port).await;

    // Idle state: SELECT 1 succeeds
    client
        .simple_query("SELECT 1")
        .await
        .expect("initial SELECT 1 failed");

    // BEGIN → InTransaction
    client.simple_query("BEGIN").await.expect("BEGIN failed");

    // SELECT 1 inside transaction → still OK
    client
        .simple_query("SELECT 1")
        .await
        .expect("SELECT 1 inside BEGIN failed");

    // COMMIT → Idle
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    // New BEGIN
    client
        .simple_query("BEGIN")
        .await
        .expect("second BEGIN failed");

    // Force error inside block → Failed (use SERIALIZABLE which is not supported)
    let _ = client
        .simple_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .await;

    // SELECT 1 in failed block → must return SQLSTATE 25P02
    let blocked = client.simple_query("SELECT 1").await;
    let has_25p02 = match &blocked {
        Err(e) => e
            .as_db_error()
            .map(|d| d.code().code() == "25P02")
            .unwrap_or(false),
        Ok(msgs) => msgs.iter().any(|m| format!("{m:?}").contains("25P02")),
    };
    assert!(
        has_25p02,
        "expected 25P02 in failed block, got: {blocked:?}"
    );

    // ROLLBACK → Idle
    client
        .simple_query("ROLLBACK")
        .await
        .expect("ROLLBACK from failed block failed");

    // SELECT 1 again → success
    client
        .simple_query("SELECT 1")
        .await
        .expect("SELECT 1 after ROLLBACK failed");
}

// ── S9/P4 — PREPARE TRANSACTION rejected (unit) ───────────────────────────────

/// P4 green gate: PREPARE TRANSACTION returns SQLSTATE 0A000.
#[tokio::test]
async fn test_prepare_transaction_rejected() {
    let (port, _handle) = start_gateway_noop().await;
    let client = connect_port(port).await;

    let result = client.simple_query("PREPARE TRANSACTION 'xid'").await;
    let has_0a000 = match &result {
        Err(e) => e
            .as_db_error()
            .map(|d| d.code().code() == "0A000")
            .unwrap_or(false),
        Ok(msgs) => msgs.iter().any(|m| format!("{m:?}").contains("0A000")),
    };
    assert!(
        has_0a000,
        "expected SQLSTATE 0A000 for PREPARE TRANSACTION, got: {result:?}"
    );

    // Connection must still be usable
    client
        .simple_query("SELECT 1")
        .await
        .expect("SELECT 1 after PREPARE TRANSACTION rejected should succeed");
}

// ── S9/P5 — SAVEPOINT outside transaction rejected (unit) ─────────────────────

/// P5 green gate: SAVEPOINT outside explicit BEGIN returns an error.
#[tokio::test]
async fn test_savepoint_outside_transaction_rejected() {
    let (port, _handle) = start_gateway_noop().await;
    let client = connect_port(port).await;

    let result = client.simple_query("SAVEPOINT s").await;
    let got_error = match &result {
        Err(e) => e.as_db_error().is_some(),
        Ok(msgs) => msgs.iter().any(|m| format!("{m:?}").contains("ERROR")),
    };
    assert!(
        got_error,
        "expected error for SAVEPOINT outside BEGIN, got: {result:?}"
    );
}
