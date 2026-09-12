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

async fn gateway(
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

async fn client(port: u16) -> tokio_postgres::Client {
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
async fn compiled_join_state_persists_across_restart_minio() {
    let bucket = "rockstream-core-join-durability";
    let (_container, port) = match rockstream_test_support::minio::start_minio(bucket).await {
        Some(m) => m,
        None => {
            eprintln!("SKIP compiled_join_state_persists_across_restart_minio: Docker is not available locally");
            return;
        }
    };
    let store = Arc::new(rockstream_test_support::minio::minio_object_store(
        port, bucket,
    ));
    let catalog = Arc::new(CatalogStubs::new());
    let (port1, handle1, db1) = gateway("core-join-minio", store.clone(), catalog.clone()).await;
    let client1 = client(port1).await;
    client1
        .batch_execute(
            "CREATE TABLE a (id BIGINT, k BIGINT); \
             CREATE TABLE b (id BIGINT, k BIGINT, val BIGINT); \
             CREATE VIEW joined AS SELECT a.id, a.k, b.id, b.val FROM a JOIN b ON a.k = b.k; \
             INSERT INTO a VALUES (1, 100);",
        )
        .await
        .unwrap();
    db1.flush().await.unwrap();
    handle1.abort();

    let (port2, _handle2, db2) = gateway("core-join-minio", store, catalog).await;
    let client2 = client(port2).await;
    client2
        .simple_query("INSERT INTO b VALUES (2, 100, 999)")
        .await
        .unwrap();
    db2.flush().await.unwrap();
    let rows = client2
        .simple_query("SELECT * FROM joined")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).unwrap_or("").to_owned())
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![vec!["1", "100", "2", "999"]]);
}

#[tokio::test]
async fn compiled_tumble_window_state_persists_across_restart_minio() {
    let bucket = "rockstream-core-window-durability";
    let (_container, port) = match rockstream_test_support::minio::start_minio(bucket).await {
        Some(m) => m,
        None => {
            eprintln!("SKIP compiled_tumble_window_state_persists_across_restart_minio: Docker is not available locally");
            return;
        }
    };
    let store = Arc::new(rockstream_test_support::minio::minio_object_store(
        port, bucket,
    ));
    let catalog = Arc::new(CatalogStubs::new());
    let (port1, handle1, db1) = gateway("core-window-minio", store.clone(), catalog.clone()).await;
    let client1 = client(port1).await;
    client1
        .batch_execute(
            "CREATE TABLE events (id BIGINT, price BIGINT, date_time BIGINT); \
             CREATE MATERIALIZED VIEW windows AS \
             SELECT CAST(date_bin(INTERVAL '10 seconds', CAST(date_time AS TIMESTAMP)) AS BIGINT), \
             SUM(price) FROM events GROUP BY date_bin(INTERVAL '10 seconds', CAST(date_time AS TIMESTAMP)); \
             INSERT INTO events VALUES (1, 100, 1), (2, 50, 15);",
        )
        .await
        .unwrap();
    db1.flush().await.unwrap();
    handle1.abort();

    let (port2, _handle2, db2) = gateway("core-window-minio", store, catalog).await;
    let client2 = client(port2).await;
    db2.flush().await.unwrap();
    let mut results = HashMap::new();
    for message in client2.simple_query("SELECT * FROM windows").await.unwrap() {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            results.insert(
                row.get(0).unwrap().parse::<i64>().unwrap(),
                row.get(1).unwrap().parse::<i64>().unwrap(),
            );
        }
    }
    assert_eq!(results, HashMap::from([(0, 100), (10_000_000_000, 50)]));
}
