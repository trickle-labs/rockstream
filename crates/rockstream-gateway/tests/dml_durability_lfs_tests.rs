//! Slice 7: Durability over LocalFileSystem (LFS) backend.
//!
//! Asserts that:
//! 1. Table creation, primary keys, and DML operations persist to SlateDB on LocalFileSystem.
//! 2. After process restart on identical LocalFileSystem store, all DML state is recovered.

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
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

#[tokio::test]
async fn test_dml_persistence_lfs_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_path = "dml-durability-lfs-shard";
    let prefix = "catalog-durability-lfs";

    // ── Phase 1: Create, insert, update, and delete ────────────────────────
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
        .simple_query("CREATE TABLE lfs_items (id BIGINT PRIMARY KEY, name TEXT, stock INT);")
        .await
        .unwrap();

    client1
        .simple_query(
            "INSERT INTO lfs_items (id, name, stock) VALUES \
             (1, 'widget', 100), \
             (2, 'gadget', 200), \
             (3, 'sprocket', 300);",
        )
        .await
        .unwrap();

    // UPDATE row 1
    client1
        .simple_query("UPDATE lfs_items SET stock = 150 WHERE id = 1;")
        .await
        .unwrap();

    // DELETE row 3
    client1
        .simple_query("DELETE FROM lfs_items WHERE id = 3;")
        .await
        .unwrap();

    let mut pre_rows = rows(&client1, "SELECT id, name, stock FROM lfs_items;").await;
    pre_rows.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(
        pre_rows,
        vec![
            vec!["1".to_string(), "widget".to_string(), "150".to_string()],
            vec!["2".to_string(), "gadget".to_string(), "200".to_string()],
        ]
    );

    shard_db1.flush().await.unwrap();
    handle1.abort();

    // ── Phase 2: Restart from same LocalFileSystem directory ──────────────
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
    let mut post_rows = rows(&client2, "SELECT id, name, stock FROM lfs_items;").await;
    post_rows.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(
        post_rows,
        vec![
            vec!["1".to_string(), "widget".to_string(), "150".to_string()],
            vec!["2".to_string(), "gadget".to_string(), "200".to_string()],
        ],
        "Recovered LFS state must match pre-restart committed state"
    );

    // Verify further DML operations work on restarted node
    client2
        .simple_query("INSERT INTO lfs_items (id, name, stock) VALUES (4, 'bolt', 400);")
        .await
        .unwrap();

    let mut final_rows = rows(&client2, "SELECT id, name, stock FROM lfs_items;").await;
    final_rows.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(
        final_rows,
        vec![
            vec!["1".to_string(), "widget".to_string(), "150".to_string()],
            vec!["2".to_string(), "gadget".to_string(), "200".to_string()],
            vec!["4".to_string(), "bolt".to_string(), "400".to_string()],
        ]
    );

    shard_db2.flush().await.unwrap();
    handle2.abort();
}
