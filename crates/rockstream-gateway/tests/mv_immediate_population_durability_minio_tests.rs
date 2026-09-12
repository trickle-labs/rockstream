//! v0.51.1 Slice 4 durability (MinIO/TC): the immediate view_output/{view}/ write performed synchronously by CREATE MATERIALIZED VIEW must be durable against the same SlateDB-on-S3 (MinIO) backend.

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

const MINIO_BUCKET: &str = "rockstream-mv-immediate-durability-test";

async fn run_mv_immediate_population_restart_case(store: Arc<dyn ObjectStore>, shard_path: &str) {
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, _shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, name TEXT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, name) VALUES (1, 'alice')")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();

    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM t")
        .await
        .unwrap();
    handle.abort();

    let (port2, _handle2, shard_db2) = start_gateway(shard_path, store, catalog).await;
    let client2 = connect_port(port2).await;
    shard_db2.flush().await.unwrap();
    let msgs = client2.simple_query("SELECT * FROM mv").await.unwrap();
    let rows: Vec<_> = msgs
        .iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(
        rows.len(),
        1,
        "expected mv output to survive restart with 1 row, got {}",
        rows.len()
    );
    assert_eq!(rows[0].get("id"), Some("1"));
    assert_eq!(rows[0].get("name"), Some("alice"));
}

#[tokio::test]
async fn mv_output_persisted_before_next_commit_minio() {
    let (_container, port) = match rockstream_test_support::minio::start_minio(MINIO_BUCKET).await {
        Some(m) => m,
        None => {
            eprintln!("SKIP mv_output_persisted_before_next_commit_minio: Docker not available");
            return;
        }
    };
    run_mv_immediate_population_restart_case(
        Arc::new(rockstream_test_support::minio::minio_object_store(
            port,
            MINIO_BUCKET,
        )),
        "mv-immediate-population-durability-minio",
    )
    .await;
}
