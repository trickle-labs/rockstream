//! v0.51.6 Slice 1 proof test: prepared-statement/portal cache is bounded
//! per connection via LRU eviction rather than a hard cap-and-error.
//!
//! Roadmap Proof claim: "A connection that opens 10,000 prepared statements
//! without `DISCARD ALL` stays bounded in memory with eviction observable
//! via a metric."

use std::sync::atomic::Ordering;
use std::sync::Arc;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    server::{
        GatewayServer, MAX_PREPARED_STATEMENTS_PER_CONN, PREPARED_STATEMENTS_COUNT,
        PREPARED_STATEMENTS_EVICTED_COUNT,
    },
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError,
};
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

async fn start_gateway() -> (String, tokio::task::JoinHandle<()>) {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(
        addr,
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.to_string(), handle)
}

async fn connect(addr: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            addr.split(':').next_back().unwrap()
        ),
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

/// Opening 10,000 named prepared statements on one connection without
/// `DISCARD ALL` must stay bounded in memory (peak per-connection statement
/// count never exceeds `MAX_PREPARED_STATEMENTS_PER_CONN`), never return
/// `RS-2600`, and increase the eviction counter. Re-executing the
/// most-recently-used statement must still work (it was not evicted); a
/// long-unused one (evicted) must fail with `StatementNotFound`.
#[tokio::test]
async fn opening_10k_statements_without_discard_stays_bounded_with_lru_eviction() {
    let (addr, _handle) = start_gateway().await;
    let client = connect(&addr).await;

    let evicted_before = PREPARED_STATEMENTS_EVICTED_COUNT.load(Ordering::Relaxed);

    const N: usize = 10_000;
    let mut peak_count: u64 = 0;
    let mut first_statement = None;
    let mut last_statement = None;
    // Every statement handle must be retained: tokio-postgres's client-side
    // `Statement` sends its own CLOSE message on `Drop`, which would
    // otherwise trigger our `on_close` cleanup path instead of exercising
    // server-side LRU eviction under test.
    let mut statements = Vec::with_capacity(N);

    for i in 0..N {
        let sql = format!("SELECT 1 -- lru_uniq_{}", i);
        let res = client.prepare(&sql).await;
        assert!(
            res.is_ok(),
            "prepare {} must never fail with RS-2600 under LRU eviction, got: {:?}",
            i,
            res.err()
        );
        let stmt = res.unwrap();
        if i == 0 {
            first_statement = Some(stmt.clone());
        }
        last_statement = Some(stmt.clone());
        statements.push(stmt);

        let current = PREPARED_STATEMENTS_COUNT.load(Ordering::Relaxed);
        peak_count = peak_count.max(current);
    }

    assert!(
        peak_count <= MAX_PREPARED_STATEMENTS_PER_CONN as u64,
        "peak PREPARED_STATEMENTS_COUNT {} exceeded bound {} — LRU eviction did not keep memory bounded",
        peak_count,
        MAX_PREPARED_STATEMENTS_PER_CONN
    );

    let evicted_after = PREPARED_STATEMENTS_EVICTED_COUNT.load(Ordering::Relaxed);
    assert!(
        evicted_after - evicted_before >= (N - MAX_PREPARED_STATEMENTS_PER_CONN) as u64,
        "expected at least {} evictions, observed {}",
        N - MAX_PREPARED_STATEMENTS_PER_CONN,
        evicted_after - evicted_before
    );

    // Re-execute the most-recently-used statement: must still work since it
    // was not evicted.
    let last_statement = last_statement.expect("at least one statement prepared");
    let rows = client.query(&last_statement, &[]).await;
    assert!(
        rows.is_ok(),
        "most-recently-used statement must not have been evicted: {:?}",
        rows.err()
    );

    // The first statement prepared is the least-recently-used of all 10,000
    // and must have been evicted server-side; re-using its (stale) handle
    // must fail as StatementNotFound, not silently succeed.
    let first_statement = first_statement.expect("first statement was prepared");
    let err = client
        .query(&first_statement, &[])
        .await
        .expect_err("long-unused (first) statement must have been evicted");
    let msg = err
        .as_db_error()
        .map(|e| e.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(
        msg.contains("Statement not found") || msg.contains("does not exist"),
        "expected a StatementNotFound-style error for the evicted statement, got: {}",
        msg
    );
}
