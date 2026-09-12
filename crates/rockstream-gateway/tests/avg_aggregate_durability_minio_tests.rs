//! v0.51.6 Slice 4 Durability (MinIO/TC): the same scenario as
//! `avg_aggregate_durability_lfs_tests.rs`, but against a real S3-compatible
//! object store (MinIO via TestContainers) — confirming the new
//! `TAG_FLOAT64` row encoding (see `sink.rs`) round-trips through actual
//! object-store SST flush/read, not just local disk.

use std::collections::HashMap;
use std::sync::Arc;

use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
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

async fn run_avg_durability_case(store: Arc<dyn ObjectStore>, shard_path: &str) {
    let catalog = Arc::new(CatalogStubs::new());
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

    let (port2, _handle2, shard_db2) = start_gateway(shard_path, store, catalog).await;
    let client2 = connect_port(port2).await;
    shard_db2.flush().await.unwrap();

    let restored = read_avg_state(&client2, "cat_avg").await;
    assert_eq!(
        restored,
        HashMap::from([(10, 150.0), (20, 50.5)]),
        "AVG's Float64 row encoding should round-trip through a real S3-compatible \
         object store's SST flush/read, not be reinterpreted as a stale Int64 tag"
    );

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

#[tokio::test]
async fn avg_aggregate_fractional_mean_persists_across_restart_minio() {
    let bucket = "rockstream-avg-aggregate-durability-test";
    let (_container, port) = match rockstream_test_support::minio::start_minio(bucket).await {
        Some(m) => m,
        None => {
            eprintln!(
                "SKIP avg_aggregate_fractional_mean_persists_across_restart_minio: Docker not available"
            );
            return;
        }
    };
    run_avg_durability_case(
        Arc::new(rockstream_test_support::minio::minio_object_store(
            port, bucket,
        )),
        "avg-aggregate-durability-minio",
    )
    .await;
}
