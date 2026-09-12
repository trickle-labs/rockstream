//! Slice 7: Durability over MinIO (S3, via TestContainers) backend.
//!
//! Asserts that:
//! 1. Table creation, primary keys, and DML operations persist to SlateDB on S3 (MinIO).
//! 2. After process restart on the same MinIO bucket, all DML state is recovered.

use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::catalog::DurableCatalogStore;
use rockstream_storage::ShardDb;

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
    .expect("connect failed");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {e}");
        }
    });
    client
}

async fn rows(client: &tokio_postgres::Client, query: &str) -> Vec<Vec<String>> {
    let msgs = client.simple_query(query).await.expect("query failed");
    msgs.into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                let count = r.columns().len();
                let mut v = Vec::with_capacity(count);
                for i in 0..count {
                    v.push(r.get(i).unwrap_or("").to_string());
                }
                Some(v)
            }
            _ => None,
        })
        .collect()
}

const MINIO_BUCKET: &str = "rockstream-dml-durability-test";

#[tokio::test]
async fn test_dml_persistence_minio_lifecycle() {
    let (_container, minio_port) = match rockstream_test_support::minio::start_minio(MINIO_BUCKET)
        .await
    {
        Some(m) => m,
        None => {
            eprintln!("SKIP test_dml_persistence_minio_lifecycle: Docker is not available locally");
            return;
        }
    };

    let store = Arc::new(rockstream_test_support::minio::minio_object_store(
        minio_port,
        MINIO_BUCKET,
    ));
    let shard_path = "dml-durability-minio-shard";
    let prefix = "catalog-durability-minio";

    // ── Phase 1: Create table, execute DML ──────────────────────────────────
    let (port1, handle1, shard_db1) = {
        let shard_db = Arc::new(
            ShardDb::builder(shard_path, store.clone())
                .build()
                .await
                .unwrap(),
        );
        let durable = Arc::new(DurableCatalogStore::new(store.clone(), prefix));
        let catalog = Arc::new(CatalogStubs::new());
        catalog.set_durable_store(durable);

        let view_reader = Arc::new(NoopViewReader);
        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db.clone());
        let (local_addr, handle) = server.serve_background().await.unwrap();
        (local_addr.port(), handle, shard_db)
    };

    let client1 = connect_port(port1).await;
    client1
        .simple_query("CREATE TABLE s3_products (id BIGINT PRIMARY KEY, title TEXT, price INT);")
        .await
        .unwrap();

    client1
        .simple_query(
            "INSERT INTO s3_products (id, title, price) VALUES \
             (1, 'book', 25), \
             (2, 'pen', 5), \
             (3, 'ruler', 8);",
        )
        .await
        .unwrap();

    client1
        .simple_query("UPDATE s3_products SET price = 30 WHERE id = 1;")
        .await
        .unwrap();

    client1
        .simple_query("DELETE FROM s3_products WHERE id = 2;")
        .await
        .unwrap();

    let mut pre_rows = rows(&client1, "SELECT id, title, price FROM s3_products;").await;
    pre_rows.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(
        pre_rows,
        vec![
            vec!["1".to_string(), "book".to_string(), "30".to_string()],
            vec!["3".to_string(), "ruler".to_string(), "8".to_string()],
        ]
    );

    shard_db1.flush().await.unwrap();
    handle1.abort();

    // ── Phase 2: Restart against MinIO and recover state ────────────────────
    let (port2, handle2, shard_db2) = {
        let shard_db = Arc::new(
            ShardDb::builder(shard_path, store.clone())
                .build()
                .await
                .unwrap(),
        );
        let durable = Arc::new(
            DurableCatalogStore::recover(store.clone(), prefix)
                .await
                .expect("DurableCatalogStore::recover must succeed"),
        );
        let catalog = Arc::new(CatalogStubs::new());
        catalog.set_durable_store(durable);
        catalog.sync_from_durable_store().await.unwrap();

        let view_reader = Arc::new(NoopViewReader);
        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db.clone());
        let (local_addr, handle) = server.serve_background().await.unwrap();
        (local_addr.port(), handle, shard_db)
    };

    let client2 = connect_port(port2).await;
    let mut post_rows = rows(&client2, "SELECT id, title, price FROM s3_products;").await;
    post_rows.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(
        post_rows,
        vec![
            vec!["1".to_string(), "book".to_string(), "30".to_string()],
            vec!["3".to_string(), "ruler".to_string(), "8".to_string()],
        ],
        "Recovered MinIO state must match pre-restart state"
    );

    shard_db2.flush().await.unwrap();
    handle2.abort();
}
