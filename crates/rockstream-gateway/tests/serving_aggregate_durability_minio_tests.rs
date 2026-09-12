//! Serving aggregate durability MinIO tests (v0.51.8).
//!
//! Validates multi-type aggregate view state persistence and recovery
//! with MinIO S3 backend (via TestContainers).

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

const MINIO_BUCKET: &str = "rockstream-multi-type-aggregate-durability-test";

async fn run_multi_type_durability_case(store: Arc<dyn ObjectStore>, shard_path: &str) {
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE sales (cat text, val int)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_multi AS SELECT cat, SUM(val), AVG(val), COUNT(val), MIN(val), MAX(val) FROM sales GROUP BY cat",
        )
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO sales (cat, val) VALUES ('A', 10), ('A', 20), ('B', 5)")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();
    handle.abort();

    let (port2, _handle2, shard_db2) = start_gateway(shard_path, store, catalog).await;
    let client2 = connect_port(port2).await;
    shard_db2.flush().await.unwrap();

    let msgs = client2
        .simple_query("SELECT * FROM mv_multi")
        .await
        .expect("SELECT after restart should succeed");

    let mut results: HashMap<String, (i64, f64, i64, i64, i64)> = HashMap::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let cat = row.get(0).unwrap().to_string();
            let sum_v: i64 = row.get(1).unwrap().parse().unwrap();
            let avg_v: f64 = row.get(2).unwrap().parse().unwrap();
            let cnt_v: i64 = row.get(3).unwrap().parse().unwrap();
            let min_v: i64 = row.get(4).unwrap().parse().unwrap();
            let max_v: i64 = row.get(5).unwrap().parse().unwrap();
            results.insert(cat, (sum_v, avg_v, cnt_v, min_v, max_v));
        }
    }

    assert_eq!(results.get("A"), Some(&(30, 15.0, 2, 10, 20)));
    assert_eq!(results.get("B"), Some(&(5, 5.0, 1, 5, 5)));
}

#[tokio::test]
async fn multi_type_aggregate_persists_across_restart_minio() {
    let (_container, port) = match rockstream_test_support::minio::start_minio(MINIO_BUCKET).await {
        Some(m) => m,
        None => {
            eprintln!(
                "SKIP multi_type_aggregate_persists_across_restart_minio: Docker not available"
            );
            return;
        }
    };
    run_multi_type_durability_case(
        Arc::new(rockstream_test_support::minio::minio_object_store(
            port,
            MINIO_BUCKET,
        )),
        "multi-type-aggregate-durability-minio",
    )
    .await;
}
