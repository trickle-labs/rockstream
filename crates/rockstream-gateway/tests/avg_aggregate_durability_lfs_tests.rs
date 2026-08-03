//! v0.51.6 Slice 4 Durability (LFS): the compiled `AVG` view's row encoding
//! changed from `Int64` (truncating) to `Float64` (true division) — this
//! new `TAG_FLOAT64` row encoding (see `sink.rs`) must survive a gateway
//! process restart/reload against the same SlateDB-on-LocalFileSystem
//! backend, and post-restart commits continue producing the correct
//! fractional average on top of the restored `(sum, count)` state — not
//! reinterpreted as a stale `Int64` tag, and not recomputed from scratch.

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

async fn read_avg_state(client: &tokio_postgres::Client, view: &str) -> HashMap<i64, f64> {
    let msgs = client
        .simple_query(&format!("SELECT * FROM {view}"))
        .await
        .expect("SELECT should succeed");
    let mut state = HashMap::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let category: i64 = row.get(0).unwrap().parse().unwrap();
            let avg_qty: f64 = row.get(1).unwrap().parse().unwrap();
            state.insert(category, avg_qty);
        }
    }
    state
}

#[tokio::test]
async fn avg_aggregate_fractional_mean_persists_across_restart_lfs() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let shard_path = "avg-aggregate-durability-lfs";

    let (port, handle, shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE bid (id BIGINT, category BIGINT, price BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW cat_avg AS SELECT category, AVG(price) as avg_price FROM bid GROUP BY category",
        )
        .await
        .unwrap();
    // category 10: {100, 200} -> mean 150.0 (exact).
    // category 20: {50, 51} -> mean 50.5 (genuinely fractional).
    client
        .simple_query(
            "INSERT INTO bid (id, category, price) VALUES (1, 10, 100), (2, 10, 200), (3, 20, 50), (4, 20, 51)",
        )
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();
    handle.abort();

    // Restart: reopen the same on-disk shard, no in-memory state carried
    // over except the (unpersisted-by-design) catalog handle.
    let (port2, _handle2, shard_db2) = start_gateway(shard_path, store, catalog).await;
    let client2 = connect_port(port2).await;
    shard_db2.flush().await.unwrap();

    let restored = read_avg_state(&client2, "cat_avg").await;
    assert_eq!(
        restored,
        HashMap::from([(10, 150.0), (20, 50.5)]),
        "AVG's Float64 row encoding should survive restart with its pre-restart values, \
         not be reinterpreted as a stale Int64 tag"
    );

    // Post-restart commit must accumulate on top of the restored (sum,
    // count) state, not recompute from scratch: adding price=52 to category
    // 20 moves its true mean from 50.5 to (50+51+52)/3 = 51.0.
    client2
        .simple_query("INSERT INTO bid (id, category, price) VALUES (5, 20, 52)")
        .await
        .unwrap();
    client2.simple_query("COMMIT").await.unwrap();

    let after = read_avg_state(&client2, "cat_avg").await;
    assert_eq!(
        after,
        HashMap::from([(10, 150.0), (20, 51.0)]),
        "post-restart commit should accumulate on top of the persisted pre-restart state \
         and still produce a true floating-point average"
    );
}
