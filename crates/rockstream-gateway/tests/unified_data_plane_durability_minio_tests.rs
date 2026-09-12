use std::sync::Arc;

use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_ops::read_view_output;
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
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

const MINIO_BUCKET: &str = "rockstream-unified-data-plane-durability-test";

async fn run_restart_case(store: Arc<dyn ObjectStore>, shard_path: &str) {
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, name TEXT)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM t")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, name) VALUES (1, 'alice')")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();
    let op_id = catalog
        .get_view("mv")
        .expect("view should be present in the catalog")
        .op_id
        .expect("compiled materialized view should carry an op_id");
    handle.abort();

    let reopened = Arc::new(ShardDb::builder(shard_path, store).build().await.unwrap());
    reopened.flush().await.unwrap();
    let rows = read_view_output(&reopened, OperatorId(op_id), 2)
        .await
        .expect("persisted compiled view output should be readable after restart");
    assert!(
        rows.iter().any(|(_, _, cols, weight)| {
            *weight > 0 && cols[0].as_i64() == Some(1) && cols[1].as_utf8() == Some("alice")
        }),
        "expected persisted compiled view_output for op_id {} after restart, got {:?}",
        op_id,
        rows
    );
}

#[tokio::test]
async fn op_id_and_shard_layout_survive_restart_minio() {
    let (_container, port) = match rockstream_test_support::minio::start_minio(MINIO_BUCKET).await {
        Some(m) => m,
        None => {
            eprintln!("SKIP op_id_and_shard_layout_survive_restart_minio: Docker not available");
            return;
        }
    };
    run_restart_case(
        Arc::new(rockstream_test_support::minio::minio_object_store(
            port,
            MINIO_BUCKET,
        )),
        "shards/0",
    )
    .await;
}
