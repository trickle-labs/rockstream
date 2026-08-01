//! v0.51.4 Slice 2 exit test: `compile_plan` compiles `PlanNode::TumbleWindow`
//! (composed with `Aggregate`, the `date_bin(...)` tumbling-window GROUP BY
//! shape used by Nexmark q7/q12/q15-17) through the live, gateway-submitted
//! SQL path into `TumbleWindowOp`, wired via `StatefulPipeline`.

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

async fn read_view_state(client: &tokio_postgres::Client, view: &str) -> HashMap<i64, i64> {
    let msgs = client
        .simple_query(&format!("SELECT * FROM {view}"))
        .await
        .expect("SELECT should succeed");
    let mut state = HashMap::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let window_start: i64 = row.get(0).unwrap().parse().unwrap();
            let sum: i64 = row.get(1).unwrap().parse().unwrap();
            state.insert(window_start, sum);
        }
    }
    state
}

fn oracle_state(rows: &HashMap<i64, (i64, i64)>) -> HashMap<i64, i64> {
    // `date_bin(INTERVAL '10 seconds', CAST(date_time AS TIMESTAMP))`: the
    // batch oracle's `CAST(.. AS TIMESTAMP)` treats the raw `date_time`
    // value as **seconds** since epoch regardless of destination `TimeUnit`
    // (Arrow/DataFusion numeric→Timestamp cast semantics — see
    // `rockstream-sql/src/lower.rs`'s `timestamp_display_scale` doc
    // comment), so the bucket width is 10 (raw seconds), and the
    // `CAST(date_bin(...) AS BIGINT)` displayed value is the bucket start
    // scaled up by the destination `TimeUnit`'s units-per-second (1e9 for
    // the default `Nanosecond`).
    const WINDOW_SIZE_SECONDS: i64 = 10;
    const DISPLAY_SCALE: i64 = 1_000_000_000;
    let mut state: HashMap<i64, i64> = HashMap::new();
    for (date_time, price) in rows.values() {
        let window_start = date_time.div_euclid(WINDOW_SIZE_SECONDS) * WINDOW_SIZE_SECONDS;
        *state.entry(window_start * DISPLAY_SCALE).or_insert(0) += price;
    }
    state
}

#[tokio::test]
async fn compiled_tumbling_view_matches_batch_oracle() {
    let catalog = Arc::new(CatalogStubs::new());
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("live-exec-tumble-window", store)
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
    let handler = server.handler().clone();
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("CREATE TABLE bid (id BIGINT, price BIGINT, date_time BIGINT)")
        .await
        .expect("CREATE TABLE should succeed");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW tumble_sum AS \
             SELECT CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start, \
             SUM(price) as total_price FROM bid \
             GROUP BY date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW should succeed");
    assert!(
        handler.has_compiled_view("tumble_sum"),
        "tumble_sum (q7-shaped) should be compiled through compile_plan, not the DataFusion materializer"
    );
    let op_id = catalog
        .get_view("tumble_sum")
        .expect("view registered")
        .op_id;
    assert!(
        op_id.is_some(),
        "CatalogView.op_id should be Some(_) for a compiled q7-shaped tumbling-window view"
    );

    let mut rows: HashMap<i64, (i64, i64)> = HashMap::new(); // id -> (date_time, price)

    // Commit 1: rows in two distinct 10s windows.
    client
        .simple_query(
            "INSERT INTO bid (id, price, date_time) VALUES \
             (1, 100, 1000), (2, 200, 5000), (3, 50, 15000)",
        )
        .await
        .expect("INSERT should succeed");
    rows.insert(1, (1000, 100));
    rows.insert(2, (5000, 200));
    rows.insert(3, (15000, 50));
    assert_eq!(
        read_view_state(&client, "tumble_sum").await,
        oracle_state(&rows),
        "after first insert commit"
    );

    // Commit 2: another row landing in the second window. Its timestamp
    // (16000) must not be older than the watermark established by commit
    // 1's latest event (15000) — `TumbleWindowOp`'s `LateDataPolicy::Drop`
    // correctly drops genuinely late rows (event time behind the
    // watermark), so this test (proving the *compiled-path wiring*, not
    // re-proving `TumbleWindowOp`'s own late-data semantics, already
    // oracle-proven since v0.20) keeps timestamps monotonically
    // non-decreasing across commits.
    client
        .simple_query("INSERT INTO bid (id, price, date_time) VALUES (4, 30, 16000)")
        .await
        .expect("INSERT should succeed");
    rows.insert(4, (16000, 30));
    assert_eq!(
        read_view_state(&client, "tumble_sum").await,
        oracle_state(&rows),
        "after second insert commit"
    );
}
