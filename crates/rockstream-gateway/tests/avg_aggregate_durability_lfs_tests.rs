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

async fn read_pairs(client: &tokio_postgres::Client, relation: &str) -> Vec<(i64, i64)> {
    let mut rows = client
        .simple_query(&format!("SELECT * FROM {relation}"))
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get(0).unwrap().parse().unwrap(),
                row.get(1).unwrap().parse().unwrap(),
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn view_output_failure_must_not_acknowledge_incomplete_commit() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, db) = start_gateway("view-failure", store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t VALUES (1, 10)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM t")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10)]);
    let before = db.last_epoch().load(std::sync::atomic::Ordering::SeqCst);
    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query("INSERT INTO t VALUES (2, 20)")
        .await
        .unwrap();
    db.fail_next_view_output_write();
    let result = client.simple_query("COMMIT").await;
    let frontier = db.last_epoch().load(std::sync::atomic::Ordering::SeqCst);
    let table = read_pairs(&client, "t").await;
    let view = read_pairs(&client, "mv").await;
    handle.abort();
    let (port2, _handle2, _db2) = start_gateway("view-failure", store, catalog).await;
    let client2 = connect_port(port2).await;
    let reopened_table = read_pairs(&client2, "t").await;
    let reopened_view = read_pairs(&client2, "mv").await;
    client2
        .simple_query("INSERT INTO t VALUES (3, 30)")
        .await
        .unwrap();
    let final_table = read_pairs(&client2, "t").await;
    let final_view = read_pairs(&client2, "mv").await;
    let initial = vec![(1, 10)];
    let committed = vec![(1, 10), (2, 20)];
    let valid_outcome = if result.is_ok() {
        frontier > before && table == committed
    } else {
        (table == initial && frontier == before) || (table == committed && frontier > before)
    };
    let mut expected_final = table.clone();
    expected_final.push((3, 30));
    assert!(
        valid_outcome && view == table && reopened_table == table && reopened_view == table
            && final_table == expected_final && final_view == expected_final,
        "commit={result:?}, frontier={before}->{frontier}, table={table:?}, view={view:?}, reopened table={reopened_table:?}, reopened view={reopened_view:?}, final table={final_table:?}, final view={final_view:?}"
    );
}

#[tokio::test]
async fn failed_atomic_commit_restores_aggregate_before_next_commit() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, db) =
        start_gateway("aggregate-view-failure", store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t VALUES (1, 10)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id, SUM(v) FROM t GROUP BY id")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10)]);
    let before = db.last_epoch().load(std::sync::atomic::Ordering::SeqCst);
    db.fail_next_view_output_write();
    assert!(client
        .simple_query("INSERT INTO t VALUES (2, 20)")
        .await
        .is_err());
    assert_eq!(
        db.last_epoch().load(std::sync::atomic::Ordering::SeqCst),
        before
    );
    assert_eq!(read_pairs(&client, "t").await, vec![(1, 10)]);
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10)]);
    client
        .simple_query("INSERT INTO t VALUES (3, 30)")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "t").await, vec![(1, 10), (3, 30)]);
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10), (3, 30)]);
    handle.abort();
    let (port, _handle, _db) = start_gateway("aggregate-view-failure", store, catalog).await;
    let client = connect_port(port).await;
    assert_eq!(read_pairs(&client, "t").await, vec![(1, 10), (3, 30)]);
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10), (3, 30)]);
}

#[tokio::test]
async fn failed_rollback_cannot_corrupt_a_later_commit() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, db) = start_gateway("failed-aggregate-rollback", store, catalog).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t VALUES (1, 10)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id, SUM(v) FROM t GROUP BY id")
        .await
        .unwrap();
    db.fail_next_view_output_write();
    db.set_fail_scans(true);
    assert!(client
        .simple_query("INSERT INTO t VALUES (2, 20)")
        .await
        .is_err());
    assert!(client.simple_query("SELECT * FROM mv").await.is_err());
    db.set_fail_scans(false);
    client
        .simple_query("INSERT INTO t VALUES (2, 30)")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "t").await, vec![(1, 10), (2, 30)]);
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10), (2, 30)]);
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
