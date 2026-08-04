//! v0.51.4 Slice 9: delta-proportional regression benchmark.
//!
//! Row-count based (not wall-clock — see the plan's note on v0.14's
//! wall-clock "≥10x speedup" proof having actually passed via a
//! "too-fast-to-measure" escape hatch, 1.09x not 10x). Populates a source
//! table with 1,000,000 rows across many commits, then issues one commit
//! that changes exactly one row, and asserts the per-commit
//! `COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL` delta for that last commit is
//! O(1) — not proportional to the table's full size — for one view per
//! operator family introduced in Slices 0-6: stateless project, aggregate,
//! join, and session window.
//!
//! This is the CI-gated regression check for Slice 0's own proportionality
//! property (already exit-tested at N=10,000 in
//! `unified_data_plane_tests.rs::commit_refresh_is_proportional_to_delta_not_table_size`)
//! at realistic scale and across every stateful operator family, so a
//! future regression back to full-table-rescan fails CI regardless of
//! which operator family it regresses in.

use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock};

use object_store::local::LocalFileSystem;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    server::COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use tempfile::TempDir;
use tokio_postgres::NoTls;

const TOTAL_ROWS: i64 = 1_000_000;
const CHUNK_SIZE: i64 = 50_000;

static VIEW_REFRESH_TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

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

/// Insert `total` rows into `table` in commits of `chunk` rows each, using
/// `row_sql` to render the `(...)` VALUES tuple for a given 1-based row
/// index. Returns the delta-rows counter's value immediately after the
/// last bulk chunk lands (the baseline the final single-row commit's own
/// delta is measured against).
async fn bulk_populate(
    client: &tokio_postgres::Client,
    table: &str,
    total: i64,
    chunk: i64,
    row_sql: impl Fn(i64) -> String,
) -> u64 {
    let mut row = 1i64;
    while row <= total {
        let end = (row + chunk - 1).min(total);
        let values: Vec<String> = (row..=end).map(&row_sql).collect();
        let insert_sql = format!("INSERT INTO {table} VALUES {}", values.join(","));
        client
            .simple_query(&insert_sql)
            .await
            .unwrap_or_else(|e| panic!("bulk INSERT into {table} rows {row}..={end} failed: {e}"));
        row = end + 1;
    }
    COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL.load(Ordering::Relaxed)
}

/// Assert that a single-row commit against a `total`-row table costs O(1)
/// delta rows, not O(total).
fn assert_o1_refresh(before: u64, after: u64, total: i64, family: &str) {
    let delta = after - before;
    assert!(
        delta < 100,
        "{family}: a single-row commit against a {total}-row table should cost O(1) delta rows, \
         got {delta} (a regression back to full-table-rescan would cost ~{total})"
    );
}

#[tokio::test]
async fn stateless_project_view_refresh_is_o1_at_1m_rows() {
    let _guard = VIEW_REFRESH_TEST_LOCK.lock().await;
    let catalog = Arc::new(CatalogStubs::new());
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("proportionality-project", store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
        shard_db,
    );
    let handler = server.handler().clone();
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("CREATE TABLE orders (id BIGINT, amount BIGINT)")
        .await
        .expect("CREATE TABLE should succeed");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW big_orders AS SELECT id, amount FROM orders WHERE amount > 100",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW should succeed");
    assert!(handler.has_compiled_view("big_orders"));

    let before = bulk_populate(&client, "orders", TOTAL_ROWS, CHUNK_SIZE, |i| {
        format!("({i}, 150)")
    })
    .await;

    client
        .simple_query("INSERT INTO orders (id, amount) VALUES (9999999, 150)")
        .await
        .expect("single-row INSERT should succeed");
    let after = COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL.load(Ordering::Relaxed);

    assert_o1_refresh(before, after, TOTAL_ROWS, "stateless project");
}

#[tokio::test]
async fn aggregate_view_refresh_is_o1_at_1m_rows() {
    let _guard = VIEW_REFRESH_TEST_LOCK.lock().await;
    let catalog = Arc::new(CatalogStubs::new());
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("proportionality-aggregate", store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
        shard_db,
    );
    let handler = server.handler().clone();
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("CREATE TABLE bid (id BIGINT, category BIGINT, price BIGINT)")
        .await
        .expect("CREATE TABLE should succeed");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW cat_sum AS SELECT category, SUM(price) FROM bid GROUP BY category",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW should succeed");
    assert!(handler.has_compiled_view("cat_sum"));

    let before = bulk_populate(&client, "bid", TOTAL_ROWS, CHUNK_SIZE, |i| {
        format!("({i}, {}, 100)", i % 1000)
    })
    .await;

    client
        .simple_query("INSERT INTO bid (id, category, price) VALUES (9999999, 1, 100)")
        .await
        .expect("single-row INSERT should succeed");
    let after = COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL.load(Ordering::Relaxed);

    assert_o1_refresh(before, after, TOTAL_ROWS, "aggregate");
}

#[tokio::test]
async fn join_view_refresh_is_o1_at_1m_rows() {
    let _guard = VIEW_REFRESH_TEST_LOCK.lock().await;
    let catalog = Arc::new(CatalogStubs::new());
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("proportionality-join", store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
        shard_db,
    );
    let handler = server.handler().clone();
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("CREATE TABLE a (id BIGINT, k BIGINT)")
        .await
        .expect("CREATE TABLE a should succeed");
    client
        .simple_query("CREATE TABLE b (id BIGINT, k BIGINT, val BIGINT)")
        .await
        .expect("CREATE TABLE b should succeed");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW a_join_b AS SELECT a.id, a.k, b.id, b.val FROM a JOIN b ON a.k = b.k",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW should succeed");
    assert!(handler.has_compiled_view("a_join_b"));

    // Seed a small right side (`b`) so left-side rows have something to
    // join against, then bulk-populate the left side (`a`) — the commit
    // whose cost is being measured for O(1)-ness.
    client
        .simple_query("INSERT INTO b (id, k, val) VALUES (1, 1, 1000)")
        .await
        .expect("seed INSERT INTO b should succeed");

    let before = bulk_populate(&client, "a", TOTAL_ROWS, CHUNK_SIZE, |i| {
        format!("({i}, {})", i % 1000)
    })
    .await;

    client
        .simple_query("INSERT INTO a (id, k) VALUES (9999999, 1)")
        .await
        .expect("single-row INSERT should succeed");
    let after = COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL.load(Ordering::Relaxed);

    assert_o1_refresh(before, after, TOTAL_ROWS, "join");
}

#[tokio::test]
async fn session_window_view_refresh_is_o1_at_1m_rows() {
    let _guard = VIEW_REFRESH_TEST_LOCK.lock().await;
    let catalog = Arc::new(CatalogStubs::new());
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("proportionality-session", store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
        shard_db,
    );
    let handler = server.handler().clone();
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("CREATE TABLE bid (bidder BIGINT, date_time BIGINT)")
        .await
        .expect("CREATE TABLE should succeed");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW q11 AS SELECT bidder, COUNT(*) as bid_count, \
             MIN(date_time) as starttime, MAX(date_time) as endtime FROM bid \
             GROUP BY bidder, SESSION(date_time, INTERVAL '10 seconds')",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW should succeed");
    assert!(handler.has_compiled_view("q11"));

    // Spread rows across many bidders/timestamps so sessions don't all
    // collapse into one giant open session, mirroring a realistic
    // Nexmark-shaped distribution.
    let before = bulk_populate(&client, "bid", TOTAL_ROWS, CHUNK_SIZE, |i| {
        format!("({}, {})", i % 10_000, i * 1000)
    })
    .await;

    client
        .simple_query("INSERT INTO bid (bidder, date_time) VALUES (1, 999999999999)")
        .await
        .expect("single-row INSERT should succeed");
    let after = COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL.load(Ordering::Relaxed);

    assert_o1_refresh(before, after, TOTAL_ROWS, "session window");
}
