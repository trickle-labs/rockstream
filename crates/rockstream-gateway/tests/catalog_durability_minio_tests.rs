//! Catalog durability tests on MinIO (v0.59.10 CAT-01 / Slice 5).

use std::sync::Arc;
use tokio_postgres::NoTls;

use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::{CatalogCheckpointEntry, CatalogNodeEntry, CatalogStubs, CatalogView},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

async fn simple_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .expect("query failed")
        .into_iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                let mut values = Vec::with_capacity(row.len());
                for i in 0..row.len() {
                    values.push(row.get(i).map(str::to_string));
                }
                Some(values)
            }
            _ => None,
        })
        .collect()
}

const MINIO_BUCKET: &str = "rockstream-catalog-durability-test";

async fn run_catalog_durability_case(store: Arc<dyn ObjectStore>, shard_path: &str) {
    let catalog1 = Arc::new(CatalogStubs::new());
    catalog1.add_node(CatalogNodeEntry {
        node_id: "node-minio-1".to_string(),
        worker_id: "worker-minio-200".to_string(),
        role: "worker".to_string(),
        address: "127.0.0.1:9092".to_string(),
        state: "READY".to_string(),
        lease_count: 5,
        memory_budget_bytes: 1024 * 1024 * 1024,
        last_heartbeat_at: "2026-08-24 11:30:00+00".to_string(),
    });
    catalog1.add_view(CatalogView {
        name: "minio_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: Some(777),
    });
    catalog1.record_checkpoint(CatalogCheckpointEntry {
        checkpoint_id: 999,
        committed_at: "2026-08-24 11:35:00+00".to_string(),
        epoch_number: 99,
        frontier: "[99]".to_string(),
        storage_path: format!("s3://{MINIO_BUCKET}/checkpoints/chk-999"),
        duration_ms: 60,
    });

    let (port1, handle1, db1) = start_gateway(shard_path, store.clone(), catalog1).await;
    let client1 = connect_port(port1).await;

    let node_rows1 = simple_rows(&client1, "SELECT worker_id FROM rockstream_catalog.nodes;").await;
    let view_rows1 = simple_rows(
        &client1,
        "SELECT arrangement_id FROM rockstream_catalog.views WHERE view_name = 'minio_view';",
    )
    .await;
    let op_rows1 = simple_rows(
        &client1,
        "SELECT operator_id FROM rockstream_catalog.operators WHERE view_name = 'minio_view';",
    )
    .await;
    let chk_rows1 = simple_rows(
        &client1,
        "SELECT checkpoint_id FROM rockstream_catalog.checkpoints;",
    )
    .await;

    assert_eq!(node_rows1[0][0].as_deref(), Some("worker-minio-200"));
    let pre_arr_id = view_rows1[0][0].clone().unwrap();
    let pre_op_id = op_rows1[0][0].clone().unwrap();
    assert_eq!(chk_rows1[0][0].as_deref(), Some("999"));

    db1.flush().await.unwrap();
    drop(client1);
    handle1.abort();
    drop(db1);

    // Restart gateway with new instance
    let catalog2 = Arc::new(CatalogStubs::new());
    catalog2.add_node(CatalogNodeEntry {
        node_id: "node-minio-1".to_string(),
        worker_id: "worker-minio-200".to_string(),
        role: "worker".to_string(),
        address: "127.0.0.1:9092".to_string(),
        state: "READY".to_string(),
        lease_count: 5,
        memory_budget_bytes: 1024 * 1024 * 1024,
        last_heartbeat_at: "2026-08-24 11:30:00+00".to_string(),
    });
    catalog2.add_view(CatalogView {
        name: "minio_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: Some(777),
    });
    catalog2.record_checkpoint(CatalogCheckpointEntry {
        checkpoint_id: 999,
        committed_at: "2026-08-24 11:35:00+00".to_string(),
        epoch_number: 99,
        frontier: "[99]".to_string(),
        storage_path: format!("s3://{MINIO_BUCKET}/checkpoints/chk-999"),
        duration_ms: 60,
    });

    let (port2, _handle2, db2) = start_gateway(shard_path, store, catalog2).await;
    let client2 = connect_port(port2).await;
    db2.flush().await.unwrap();

    let node_rows2 = simple_rows(&client2, "SELECT worker_id FROM rockstream_catalog.nodes;").await;
    let view_rows2 = simple_rows(
        &client2,
        "SELECT arrangement_id FROM rockstream_catalog.views WHERE view_name = 'minio_view';",
    )
    .await;
    let op_rows2 = simple_rows(
        &client2,
        "SELECT operator_id FROM rockstream_catalog.operators WHERE view_name = 'minio_view';",
    )
    .await;
    let chk_rows2 = simple_rows(
        &client2,
        "SELECT checkpoint_id FROM rockstream_catalog.checkpoints;",
    )
    .await;

    assert_eq!(
        node_rows2[0][0].as_deref(),
        Some("worker-minio-200"),
        "Worker ID must survive MinIO restart"
    );
    assert_eq!(
        view_rows2[0][0].as_ref().unwrap(),
        &pre_arr_id,
        "Arrangement ID must survive MinIO restart bit-identically"
    );
    assert_eq!(
        op_rows2[0][0].as_ref().unwrap(),
        &pre_op_id,
        "Operator ID must survive MinIO restart bit-identically"
    );
    assert_eq!(
        chk_rows2[0][0].as_deref(),
        Some("999"),
        "Checkpoint ID must survive MinIO restart"
    );
}

#[tokio::test]
async fn test_catalog_stable_identifiers_survive_restart_minio() {
    let _g = TEST_LOCK.lock().await;
    let (_container, port) = match rockstream_test_support::minio::start_minio(MINIO_BUCKET).await {
        Some(m) => m,
        None => {
            eprintln!(
                "SKIP test_catalog_stable_identifiers_survive_restart_minio: Docker not available"
            );
            return;
        }
    };
    run_catalog_durability_case(
        Arc::new(rockstream_test_support::minio::minio_object_store(
            port,
            MINIO_BUCKET,
        )),
        "catalog-durability-minio",
    )
    .await;
}
