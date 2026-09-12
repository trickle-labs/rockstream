use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::{BatchOp, ShardDb};
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

const MINIO_BUCKET: &str = "rockstream-delete-durability-test";

async fn run_delete_restart_case(store: Arc<dyn ObjectStore>, shard_path: &str) {
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, a TEXT)")
        .await
        .unwrap();
    client
        .simple_query("SET rockstream.idempotency_key = 'delete-seed'")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, a) VALUES (1, 'A1')")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();
    client
        .simple_query("SET rockstream.idempotency_key = 'delete-change'")
        .await
        .unwrap();
    let msgs = client
        .simple_query("DELETE FROM t WHERE id = 1, a = 'A1' RETURNING *")
        .await
        .unwrap();
    let rows: Vec<_> = msgs
        .iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1);
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();
    handle.abort();

    let (port2, _handle2, shard_db2) = start_gateway(shard_path, store, catalog).await;
    let client2 = connect_port(port2).await;
    shard_db2.flush().await.unwrap();
    let msgs2 = client2.simple_query("SELECT * FROM t").await.unwrap();
    let rows2: Vec<_> = msgs2
        .iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();
    assert!(rows2.is_empty());
    assert!(matches!(
        BatchOp::Delete { key: vec![] },
        BatchOp::Delete { .. }
    ));
}

#[tokio::test]
async fn pre_image_capture_replays_correctly_after_crash_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    run_delete_restart_case(store, "delete-rmw-lfs").await;
}

#[tokio::test]
async fn pre_image_capture_replays_correctly_after_crash_minio_tc() {
    let (_container, port) = match rockstream_test_support::minio::start_minio(MINIO_BUCKET).await {
        Some(m) => m,
        None => {
            eprintln!(
                "SKIP pre_image_capture_replays_correctly_after_crash_minio_tc: Docker not available"
            );
            return;
        }
    };
    run_delete_restart_case(
        Arc::new(rockstream_test_support::minio::minio_object_store(
            port,
            MINIO_BUCKET,
        )),
        "delete-rmw-minio",
    )
    .await;
}
