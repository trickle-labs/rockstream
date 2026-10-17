//! Regression transcripts for durable, whole-epoch subscription publication.
//! These exercise the actual PgWire COMMIT path and its bounded replay log;
//! full durability under process destruction still needs release qualification.

use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_gateway::catalog_stubs::CatalogStubs;
use rockstream_gateway::view_reader::{ViewReadStrategy, ViewReader};
use rockstream_gateway::{GatewayError, GatewayServer};
use rockstream_storage::ShardDb;
use tokio_postgres::NoTls;

struct EmptyViewReader;

#[async_trait::async_trait]
impl ViewReader for EmptyViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(Vec::new())
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

async fn open_client(port: u16) -> tokio_postgres::Client {
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
async fn acknowledged_commit_publishes_complete_epoch_with_exact_rows() {
    let storage = Arc::new(InMemory::new());
    let shard = Arc::new(
        ShardDb::builder("v072-durable-publication-once", storage)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(EmptyViewReader),
        Arc::clone(&shard),
    );
    let registry = server.handler().subscribe_registry().clone();
    let (address, task) = server.serve_background().await.unwrap();
    let client = open_client(address.port()).await;
    client
        .simple_query("CREATE TABLE v072_orders (id BIGINT, amount BIGINT)")
        .await
        .unwrap();
    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query("INSERT INTO v072_orders (id, amount) VALUES (1, 10)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO v072_orders (id, amount) VALUES (2, 20)")
        .await
        .unwrap();
    assert_eq!(registry.since_epoch("v072_orders", 0), None);
    client.simple_query("COMMIT").await.unwrap();

    let entries = registry.since_epoch("v072_orders", 0).unwrap();
    assert_eq!(entries.len(), 2);
    let committed_epoch = entries[0].epoch;
    assert!(committed_epoch > 0);
    assert_eq!(
        entries
            .iter()
            .map(|entry| (entry.epoch, entry.mz_diff, entry.encoded_row.clone()))
            .collect::<Vec<_>>(),
        vec![
            (committed_epoch, 1, bytes::Bytes::from_static(b"1\t10")),
            (committed_epoch, 1, bytes::Bytes::from_static(b"2\t20")),
        ]
    );
    assert_eq!(registry.latest_epoch("v072_orders"), Some(committed_epoch));
    let results = client
        .simple_query("SELECT * FROM v072_orders ORDER BY id")
        .await
        .unwrap();
    let rows: Vec<_> = results
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get("id").unwrap_or("").to_owned(),
                row.get("amount").unwrap_or("").to_owned(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            ("1".to_owned(), "10".to_owned()),
            ("2".to_owned(), "20".to_owned())
        ]
    );
    task.abort();
}

#[tokio::test]
async fn concurrent_commits_publish_only_whole_ordered_epochs() {
    let storage = Arc::new(InMemory::new());
    let shard = Arc::new(
        ShardDb::builder("v072-durable-publication-order", storage)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(EmptyViewReader),
        shard,
    );
    let registry = server.handler().subscribe_registry().clone();
    let (address, task) = server.serve_background().await.unwrap();
    let first = open_client(address.port()).await;
    let second = open_client(address.port()).await;
    first
        .simple_query("CREATE TABLE v072_orders_concurrent (id BIGINT, amount BIGINT)")
        .await
        .unwrap();
    first.simple_query("BEGIN").await.unwrap();
    second.simple_query("BEGIN").await.unwrap();
    first
        .simple_query("INSERT INTO v072_orders_concurrent (id, amount) VALUES (1, 10)")
        .await
        .unwrap();
    second
        .simple_query("INSERT INTO v072_orders_concurrent (id, amount) VALUES (2, 20)")
        .await
        .unwrap();
    let (one, two) = tokio::join!(first.simple_query("COMMIT"), second.simple_query("COMMIT"));
    one.unwrap();
    two.unwrap();

    let entries = registry.since_epoch("v072_orders_concurrent", 0).unwrap();
    assert_eq!(entries.len(), 2);
    assert!(entries[0].epoch < entries[1].epoch);
    assert_eq!(entries[0].mz_diff, 1);
    assert_eq!(entries[1].mz_diff, 1);
    let mut rows = entries
        .iter()
        .map(|item| item.encoded_row.clone())
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            bytes::Bytes::from_static(b"1\t10"),
            bytes::Bytes::from_static(b"2\t20")
        ]
    );
    task.abort();
}
