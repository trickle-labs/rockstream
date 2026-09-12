//! Type Completeness: All Types Arrangement Durability Tests across LFS & MinIO (v0.59.20 Slice 7 / Phase 3b).

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

async fn start_gateway(
    path: &str,
    store: Arc<dyn ObjectStore>,
    catalog: Arc<CatalogStubs>,
) -> (u16, tokio::task::JoinHandle<()>, Arc<ShardDb>) {
    let db = Arc::new(ShardDb::builder(path, store).build().await.unwrap());
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        db.clone(),
    );
    let (addr, handle) = server.serve_background().await.unwrap();
    (addr.port(), handle, db)
}

async fn connect_client(port: u16) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test]
async fn test_all_types_arrangement_durability_lfs() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());

    // Phase 1: Initialize table with text, temporal, uuid, decimal keys and insert records
    let (port1, handle1, db1) =
        start_gateway("type-durability-lfs", store.clone(), catalog.clone()).await;
    let client1 = connect_client(port1).await;

    client1
        .simple_query(
            "CREATE TABLE t_all_types (\
                id BIGINT, \
                name TEXT, \
                d DATE, \
                ts TIMESTAMP, \
                uid UUID, \
                amount DECIMAL(18, 4)\
            )",
        )
        .await
        .unwrap();

    client1
        .simple_query(
            "INSERT INTO t_all_types VALUES (\
                1, \
                'rockstream_v1', \
                '2026-09-01', \
                '2026-09-01 12:00:00', \
                'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', \
                9999.5000\
            )",
        )
        .await
        .unwrap();

    db1.flush().await.unwrap();
    handle1.abort();

    // Phase 2: Restart gateway on same LFS store and verify persistent state
    let (port2, handle2, _db2) = start_gateway("type-durability-lfs", store, catalog).await;
    let client2 = connect_client(port2).await;

    let res = client2
        .simple_query("SELECT id, name, d, uid, amount FROM t_all_types WHERE id = 1")
        .await
        .unwrap();

    let mut row_found = false;
    for msg in res {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0).unwrap(), "1");
            assert_eq!(row.get(1).unwrap(), "rockstream_v1");
            assert_eq!(row.get(2).unwrap(), "2026-09-01");
            assert_eq!(row.get(3).unwrap(), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
            assert_eq!(row.get(4).unwrap(), "9999.5000");
            row_found = true;
        }
    }
    assert!(row_found, "Row must persist across ShardDb restart on LFS");

    handle2.abort();
}

#[tokio::test]
async fn test_all_types_arrangement_durability_minio() {
    let bucket = "rockstream-type-completeness-durability";
    let (_container, port) = match rockstream_test_support::minio::start_minio(bucket).await {
        Some(m) => m,
        None => {
            eprintln!(
                "SKIP test_all_types_arrangement_durability_minio: Docker is not available locally"
            );
            return;
        }
    };
    let store = Arc::new(rockstream_test_support::minio::minio_object_store(
        port, bucket,
    ));
    let catalog = Arc::new(CatalogStubs::new());

    let (port1, handle1, db1) =
        start_gateway("type-durability-minio", store.clone(), catalog.clone()).await;
    let client1 = connect_client(port1).await;

    client1
        .simple_query(
            "CREATE TABLE t_minio (\
                id BIGINT, \
                tag TEXT, \
                event_date DATE\
            )",
        )
        .await
        .unwrap();

    client1
        .simple_query("INSERT INTO t_minio VALUES (42, 'minio_persisted', '2026-09-01')")
        .await
        .unwrap();

    db1.flush().await.unwrap();
    handle1.abort();

    let (port2, handle2, _db2) = start_gateway("type-durability-minio", store, catalog).await;
    let client2 = connect_client(port2).await;

    let res = client2
        .simple_query("SELECT id, tag, event_date FROM t_minio WHERE id = 42")
        .await
        .unwrap();

    let mut found = false;
    for msg in res {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0).unwrap(), "42");
            assert_eq!(row.get(1).unwrap(), "minio_persisted");
            assert_eq!(row.get(2).unwrap(), "2026-09-01");
            found = true;
        }
    }
    assert!(found, "Row must persist across ShardDb restart on MinIO");

    handle2.abort();
}
