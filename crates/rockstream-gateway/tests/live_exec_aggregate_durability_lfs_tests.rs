//! v0.51.4 Durability Slices (LFS): a compiled `Aggregate` view's arrangement
//! (`AggregateOp`'s persisted group-by state) survives a gateway process
//! restart against the same SlateDB-on-LocalFileSystem backend, and
//! post-restart commits continue accumulating correctly on top of the
//! restored state — not from scratch.

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
            let category: i64 = row.get(0).unwrap().parse().unwrap();
            let sum: i64 = row.get(1).unwrap().parse().unwrap();
            state.insert(category, sum);
        }
    }
    state
}

#[tokio::test]
async fn compiled_aggregate_state_persists_across_restart_lfs() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let shard_path = "live-exec-aggregate-durability-lfs";

    let (port, handle, shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE bid (id BIGINT, category BIGINT, price BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW cat_sum AS SELECT category, SUM(price) FROM bid GROUP BY category",
        )
        .await
        .unwrap();
    client
        .simple_query(
            "INSERT INTO bid (id, category, price) VALUES (1, 10, 100), (2, 10, 200), (3, 20, 50)",
        )
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();
    handle.abort();

    // Restart: reopen the same on-disk shard, no in-memory state carried over
    // except the (unpersisted-by-design, see unified_data_plane_durability_
    // lfs_tests.rs) catalog handle.
    let (port2, _handle2, shard_db2) = start_gateway(shard_path, store, catalog).await;
    let client2 = connect_port(port2).await;
    shard_db2.flush().await.unwrap();

    let restored = read_view_state(&client2, "cat_sum").await;
    assert_eq!(
        restored,
        HashMap::from([(10, 300), (20, 50)]),
        "aggregate arrangement should survive restart with its pre-restart totals"
    );

    // Post-restart commit must accumulate on top of the restored state, not
    // recompute from scratch (which would also happen to produce the right
    // answer here only by coincidence if the delta-capture path were broken
    // — the real proof is Slice 0's proportionality test; this test proves
    // durability of the *state itself*).
    client2
        .simple_query("INSERT INTO bid (id, category, price) VALUES (4, 10, 400)")
        .await
        .unwrap();
    client2.simple_query("COMMIT").await.unwrap();

    let after = read_view_state(&client2, "cat_sum").await;
    assert_eq!(
        after,
        HashMap::from([(10, 700), (20, 50)]),
        "post-restart commit should accumulate on top of the persisted pre-restart state"
    );
}
