//! v0.61.2 Durability Slices (MinIO/TC): verifies that aggregate delta-reduced
//! arrangements persist across gateway process restart against a real S3-compatible
//! object store (MinIO via TestContainers).

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

async fn run_aggregate_durability_case(store: Arc<dyn ObjectStore>, shard_path: &str) {
    let catalog = Arc::new(CatalogStubs::new());
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

    // Epoch 1
    client
        .simple_query(
            "INSERT INTO bid (id, category, price) VALUES (1, 10, 100), (2, 20, 50), (3, 30, 25)",
        )
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();

    // Epoch 2: Repeated updates
    client
        .simple_query(
            "INSERT INTO bid (id, category, price) VALUES (4, 10, 50), (5, 20, 10), (6, 20, -10)",
        )
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();

    shard_db.flush().await.unwrap();
    handle.abort();

    // Restart against same MinIO shard
    let (port2, _handle2, shard_db2) = start_gateway(shard_path, store, catalog).await;
    let client2 = connect_port(port2).await;
    shard_db2.flush().await.unwrap();

    let restored = read_view_state(&client2, "cat_sum").await;
    assert_eq!(
        restored,
        HashMap::from([(10, 150), (20, 50), (30, 25)]),
        "restored state on MinIO matches exact pre-restart multiset"
    );

    // Accumulate on top of restored state
    client2
        .simple_query("INSERT INTO bid (id, category, price) VALUES (7, 10, 200)")
        .await
        .unwrap();
    client2.simple_query("COMMIT").await.unwrap();

    let after = read_view_state(&client2, "cat_sum").await;
    assert_eq!(
        after,
        HashMap::from([(10, 350), (20, 50), (30, 25)]),
        "post-restart updates accumulate onto restored state on MinIO"
    );
}

#[tokio::test]
async fn test_v0612_aggregate_persists_across_restart_minio() {
    let bucket = "rockstream-v0612-aggregate-durability-test";
    let (_container, port) = match rockstream_test_support::minio::start_minio(bucket).await {
        Some(m) => m,
        None => {
            eprintln!(
                "SKIP test_v0612_aggregate_persists_across_restart_minio: Docker not available"
            );
            return;
        }
    };
    run_aggregate_durability_case(
        Arc::new(rockstream_test_support::minio::minio_object_store(
            port, bucket,
        )),
        "v0612-aggregate-durability-minio",
    )
    .await;
}
