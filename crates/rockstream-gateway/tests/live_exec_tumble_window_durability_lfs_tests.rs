//! v0.51.4 Durability Slices (LFS): a compiled `TumbleWindow`+`Aggregate`
//! view's arrangement survives a gateway process restart against the same
//! SlateDB-on-LocalFileSystem backend, and post-restart commits into an
//! already-open window continue accumulating correctly on top of the
//! restored state.

use std::collections::HashMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
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

async fn start_gateway(
    shard_path: &str,
    store: Arc<dyn ObjectStore>,
    catalog: Arc<CatalogStubs>,
) -> (u16, tokio::task::JoinHandle<()>, Arc<ShardDb>) {
    let shard_db = Arc::new(ShardDb::builder(shard_path, store).build().await.unwrap());
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let (addr, handle) = server.serve_background().await.unwrap();
    (addr.port(), handle, shard_db)
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

#[tokio::test]
async fn compiled_tumble_window_state_persists_across_restart_lfs() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let shard_path = "live-exec-tumble-window-durability-lfs";

    let (port, handle, shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE bid (id BIGINT, price BIGINT, date_time BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW tumble_sum AS \
             SELECT CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start, \
             SUM(price) as total_price FROM bid \
             GROUP BY date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))",
        )
        .await
        .unwrap();
    // `date_bin(INTERVAL '10 seconds', CAST(date_time AS TIMESTAMP))`
    // treats the raw `date_time` value as **seconds** since epoch (Arrow/
    // DataFusion numeric→Timestamp cast semantics — see
    // `rockstream-sql/src/lower.rs`'s `timestamp_display_scale` doc
    // comment), so a 10-second bucket is 10 raw units wide, and the
    // displayed `CAST(date_bin(...) AS BIGINT)` value is the bucket start
    // scaled up by 1e9 (the default `Nanosecond` `TimeUnit`). Two rows in
    // the same open bucket (raw seconds 0-9), one in the next (10-19).
    client
        .simple_query(
            "INSERT INTO bid (id, price, date_time) VALUES (1, 100, 1), (2, 200, 5), (3, 50, 15)",
        )
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();
    handle.abort();

    let (port2, _handle2, shard_db2) = start_gateway(shard_path, store, catalog).await;
    let client2 = connect_port(port2).await;
    shard_db2.flush().await.unwrap();

    let restored = read_view_state(&client2, "tumble_sum").await;
    assert_eq!(
        restored,
        HashMap::from([(0, 300), (10_000_000_000, 50)]),
        "tumbling-window arrangement should survive restart with its pre-restart window totals"
    );

    // Window 0 was already finalized by the pre-restart commit itself (that
    // commit's own last row, at t=15, advanced the watermark past window
    // 0's close boundary) — `LateDataPolicy::Drop` correctly rejects any
    // further row landing in it, restart or not (see the non-durability
    // sibling test's identical comment). So the durability-proving
    // post-restart row must land in the still-open second window (raw
    // seconds 10..), with a monotonically-increasing timestamp, same as
    // that sibling test.
    client2
        .simple_query("INSERT INTO bid (id, price, date_time) VALUES (4, 30, 16)")
        .await
        .unwrap();
    client2.simple_query("COMMIT").await.unwrap();

    let after = read_view_state(&client2, "tumble_sum").await;
    assert_eq!(
        after,
        HashMap::from([(0, 300), (10_000_000_000, 80)]),
        "post-restart commit into an already-open window should accumulate on top of the persisted state"
    );
}
