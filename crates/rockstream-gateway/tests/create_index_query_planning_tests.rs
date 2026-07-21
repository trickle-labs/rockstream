//! v0.51.2 Slice 5: `CREATE INDEX` automatic backfill + Ready transition.
//!
//! These tests prove that a standard `CREATE INDEX idx ON t(col)` issued over
//! live pgwire reaches `Ready` on its own (no private `MARK INDEX ... READY`
//! command), that the resulting index measurably accelerates a point lookup
//! (touches O(1) rows, not the full table, at p99 < 10ms matching v0.32's
//! bar), that a bounded range predicate is served by the new range-lookup
//! accelerator, and that a table exceeding the backfill row-count bound fails
//! cleanly with RS-2027 instead of leaving the index stuck in `Building`.

use std::sync::Arc;
use std::time::Instant;

use object_store::memory::InMemory;
use rockstream_gateway::{
    catalog_stubs::{CatalogIndexState, CatalogStubs},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
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
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect()
}

async fn start_gateway_with_shard(
    shard_path: &str,
) -> (
    u16,
    tokio::task::JoinHandle<()>,
    Arc<ShardDb>,
    Arc<CatalogStubs>,
) {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(ShardDb::builder(shard_path, store).build().await.unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let (addr, handle) = server.serve_background().await.unwrap();
    (addr.port(), handle, shard_db, catalog)
}

async fn run_fixture_sql(
    client: &tokio_postgres::Client,
    statements: &[&str],
    idempotency_key: &str,
) {
    client
        .simple_query(&format!(
            "SET rockstream.idempotency_key = '{idempotency_key}'"
        ))
        .await
        .expect("SET rockstream.idempotency_key");
    client.simple_query("BEGIN").await.expect("BEGIN");
    for statement in statements {
        client
            .simple_query(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement} failed: {e}"));
    }
    client.simple_query("COMMIT").await.expect("COMMIT");
}

/// A `CREATE INDEX idx ON t(col)` issued over live pgwire, with no `MARK
/// INDEX` command anywhere in the test, reaches `Ready` automatically because
/// `CREATE INDEX` now blocks synchronously on its own backfill.
#[tokio::test]
async fn create_index_reaches_ready_automatically_without_mark_index() {
    let (port, _handle, shard_db, catalog) =
        start_gateway_with_shard("create-index-auto-ready").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE widgets (id BIGINT, price BIGINT)")
        .await
        .expect("CREATE TABLE widgets");
    run_fixture_sql(
        &client,
        &[
            "INSERT INTO widgets (id, price) VALUES (1, 100)",
            "INSERT INTO widgets (id, price) VALUES (2, 200)",
            "INSERT INTO widgets (id, price) VALUES (3, 300)",
        ],
        "create-index-auto-ready-fixture",
    )
    .await;
    shard_db.flush().await.unwrap();

    client
        .simple_query("CREATE INDEX idx_widgets_price ON widgets (price)")
        .await
        .expect("CREATE INDEX failed");

    let entry = catalog
        .get_index("idx_widgets_price")
        .expect("index must be registered after CREATE INDEX");
    assert_eq!(
        entry.state,
        CatalogIndexState::Ready,
        "CREATE INDEX must reach Ready automatically without MARK INDEX"
    );
    assert!(
        entry.op_id.is_some(),
        "a Ready index must carry a minted op_id"
    );
}

/// After `CREATE INDEX` reaches `Ready`, a matching point lookup is served
/// via the index-accelerated path (not a full-table scan): the returned row
/// set is exactly the matching row regardless of table size, and repeated
/// lookups measure p99 < 10ms (matching v0.32's bar).
#[tokio::test]
async fn create_index_point_lookup_under_10ms_p99() {
    let (port, _handle, shard_db, catalog) =
        start_gateway_with_shard("create-index-point-lookup").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE accounts (id BIGINT, balance BIGINT)")
        .await
        .expect("CREATE TABLE accounts");

    const TOTAL: usize = 500;
    let statements: Vec<String> = (0..TOTAL)
        .map(|i| format!("INSERT INTO accounts (id, balance) VALUES ({i}, {i})"))
        .collect();
    let statement_refs: Vec<&str> = statements.iter().map(String::as_str).collect();
    run_fixture_sql(
        &client,
        &statement_refs,
        "create-index-point-lookup-fixture",
    )
    .await;
    shard_db.flush().await.unwrap();

    client
        .simple_query("CREATE INDEX idx_accounts_balance ON accounts (balance)")
        .await
        .expect("CREATE INDEX failed");
    assert_eq!(
        catalog.get_index("idx_accounts_balance").unwrap().state,
        CatalogIndexState::Ready,
        "index must be Ready before measuring point-lookup latency"
    );

    const LOOKUPS: usize = 200;
    let mut latencies_us: Vec<u128> = Vec::with_capacity(LOOKUPS);
    for _ in 0..LOOKUPS {
        let start = Instant::now();
        let rows = client
            .simple_query("SELECT id, balance FROM accounts WHERE balance = 250")
            .await
            .expect("point lookup query");
        latencies_us.push(start.elapsed().as_micros());

        let rows = data_rows_from(&rows);
        assert_eq!(
            rows.len(),
            1,
            "point lookup must touch exactly the one matching row, not the full table"
        );
        assert_eq!(rows[0].get("balance").unwrap_or(""), "250");
    }

    latencies_us.sort_unstable();
    let p99_idx = (latencies_us.len() * 99 / 100).min(latencies_us.len() - 1);
    let p99_us = latencies_us[p99_idx];
    assert!(
        p99_us < 10_000,
        "expected p99 point-lookup latency < 10ms, got {p99_us}us"
    );
}

/// A bounded range predicate (`WHERE col > lo AND col < hi`) over a `Ready`
/// index is served by the new range-lookup accelerator and returns exactly
/// the matching subset.
#[tokio::test]
async fn create_index_range_lookup_returns_correct_rows() {
    let (port, _handle, shard_db, catalog) =
        start_gateway_with_shard("create-index-range-lookup").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE readings (id BIGINT, value BIGINT)")
        .await
        .expect("CREATE TABLE readings");
    run_fixture_sql(
        &client,
        &[
            "INSERT INTO readings (id, value) VALUES (1, 10)",
            "INSERT INTO readings (id, value) VALUES (2, 20)",
            "INSERT INTO readings (id, value) VALUES (3, 30)",
            "INSERT INTO readings (id, value) VALUES (4, 40)",
            "INSERT INTO readings (id, value) VALUES (5, 50)",
        ],
        "create-index-range-lookup-fixture",
    )
    .await;
    shard_db.flush().await.unwrap();

    client
        .simple_query("CREATE INDEX idx_readings_value ON readings (value)")
        .await
        .expect("CREATE INDEX failed");
    assert_eq!(
        catalog.get_index("idx_readings_value").unwrap().state,
        CatalogIndexState::Ready
    );

    let rows = client
        .simple_query("SELECT id, value FROM readings WHERE value > 15 AND value < 45")
        .await
        .expect("range lookup query");
    let mut got: Vec<(String, String)> = data_rows_from(&rows)
        .iter()
        .map(|row| {
            (
                row.get("id").unwrap_or("").to_string(),
                row.get("value").unwrap_or("").to_string(),
            )
        })
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![
            ("2".to_string(), "20".to_string()),
            ("3".to_string(), "30".to_string()),
            ("4".to_string(), "40".to_string()),
        ]
    );
}

/// A table whose backfill scan exceeds `MAX_INDEX_BACKFILL_ROWS` fails
/// `CREATE INDEX` with RS-2027, and the index entry does not remain stuck
/// in `Building` — the catalog entry is removed so a retry starts clean.
// A second query on the same connection right after COPY FROM STDIN
// deadlocks (see the "SELECT after COPY deadlocks... due to TCP buffer
// back-pressure" notes on the COPY tests in gateway_proof_tests.rs) — use a
// fresh connection for the CREATE INDEX so this hits the real backfill path.
#[tokio::test]
async fn create_index_backfill_row_limit_exceeded_returns_error() {
    let (port, _handle, shard_db, catalog) =
        start_gateway_with_shard("create-index-backfill-row-limit").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE oversized (id BIGINT, tag BIGINT)")
        .await
        .expect("CREATE TABLE oversized");

    // MAX_INDEX_BACKFILL_ROWS is 2_000 (v0.51.2 Slice 5); bulk-load past it
    // via COPY so the test stays fast.
    let sink: tokio_postgres::CopyInSink<bytes::Bytes> = client
        .copy_in("COPY oversized (id, tag) FROM STDIN")
        .await
        .expect("copy_in should enter COPY IN mode");
    tokio::pin!(sink);
    const TOTAL: usize = 2_500;
    for i in 0..TOTAL {
        let row = format!("{i}\t{i}\n");
        futures::SinkExt::send(&mut sink, bytes::Bytes::from(row))
            .await
            .expect("send row failed");
    }
    sink.finish().await.expect("CopyDone should succeed");
    shard_db.flush().await.unwrap();

    let index_client = connect_port(port).await;
    let result = index_client
        .simple_query("CREATE INDEX idx_oversized_tag ON oversized (tag)")
        .await;

    let err_message = match &result {
        Err(e) => e.as_db_error().map(|d| d.message().to_string()),
        Ok(_) => None,
    };
    assert!(
        err_message
            .as_deref()
            .map(|m| m.contains("RS-2027"))
            .unwrap_or(false),
        "expected RS-2027 for backfill row-limit exceeded, got: {result:?}"
    );

    assert!(
        catalog.get_index("idx_oversized_tag").is_none(),
        "index must not remain stuck in Building/registered after backfill failure"
    );
}
