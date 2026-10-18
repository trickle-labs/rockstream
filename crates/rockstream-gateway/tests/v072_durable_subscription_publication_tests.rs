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
    assert!(registry.since_epoch("v072_orders", 0).is_none());
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

struct PausedSnapshotReader {
    entered: Arc<tokio::sync::Notify>,
    resume: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl ViewReader for PausedSnapshotReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        self.entered.notify_one();
        self.resume.notified().await;
        Ok(Vec::new())
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

#[tokio::test]
async fn local_snapshot_scan_blocks_competing_commit_until_epoch_fence_releases() {
    let storage = Arc::new(InMemory::new());
    let shard = Arc::new(
        ShardDb::builder("v072-local-snapshot-epoch-fence", storage)
            .build()
            .await
            .unwrap(),
    );
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let reader = Arc::new(PausedSnapshotReader {
        entered: Arc::clone(&entered),
        resume: Arc::clone(&resume),
    });
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        reader,
        shard,
    );
    let registry = server.handler().subscribe_registry().clone();
    let (address, server_task) = server.serve_background().await.unwrap();
    let writer = open_client(address.port()).await;
    let snapshot_client = open_client(address.port()).await;
    writer
        .simple_query("CREATE TABLE v072_epoch_fence (id BIGINT)")
        .await
        .unwrap();

    let snapshot_task = tokio::spawn(async move {
        // SUBSCRIBE is intentionally long-lived; the test only needs the
        // initial snapshot admission and its lock boundary.
        let _ = snapshot_client
            .simple_query("SUBSCRIBE v072_epoch_fence AS OF NOW WITH SNAPSHOT")
            .await;
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .expect("snapshot must enter the controlled producer reader");

    writer.simple_query("BEGIN").await.unwrap();
    writer
        .simple_query("INSERT INTO v072_epoch_fence (id) VALUES (7)")
        .await
        .unwrap();
    let (commit_started_tx, commit_started_rx) = tokio::sync::oneshot::channel();
    let mut committing = tokio::spawn(async move {
        let _ = commit_started_tx.send(());
        writer.simple_query("COMMIT").await
    });
    commit_started_rx.await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), &mut committing)
            .await
            .is_err(),
        "a local COMMIT must not pass the snapshot epoch/read boundary"
    );
    assert_eq!(registry.latest_epoch("v072_epoch_fence"), None);
    resume.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(10), &mut committing)
        .await
        .expect("commit must resume once the snapshot finishes")
        .unwrap()
        .unwrap();
    let entries = registry.since_epoch("v072_epoch_fence", 0).unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| (entry.mz_diff, entry.encoded_row.clone()))
            .collect::<Vec<_>>(),
        vec![(1, bytes::Bytes::from_static(b"7"))]
    );
    snapshot_task.abort();
    server_task.abort();
}

#[tokio::test]
async fn failed_storage_flush_cannot_publish_subscription_epoch() {
    let store = Arc::new(InMemory::new());
    let shard = Arc::new(
        ShardDb::builder("v072-durable-publication-failed-flush", store.clone())
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
        .simple_query("CREATE TABLE v072_failed_flush (id BIGINT)")
        .await
        .unwrap();
    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query("INSERT INTO v072_failed_flush (id) VALUES (7)")
        .await
        .unwrap();
    // Inject at the exact ShardDb::flush boundary used by PgWire COMMIT.
    // SlateDB need not write a "/compacted/" SST for this small transaction,
    // so an ObjectStore path filter would not reliably exercise this failure.
    shard.set_fail_flushes(true);
    let preflight = shard.flush().await.unwrap_err();
    assert!(
        preflight.to_string().contains("injected storage flush failure"),
        "test must actively reject a durable flush"
    );
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        client.simple_query("COMMIT"),
    )
    .await
    .expect("flush should return the injected error without an unbounded stall");
    assert!(
        outcome.is_err(),
        "a rejected durable flush must not acknowledge COMMIT"
    );
    // Failure was injected at flush, after batch staging but before replay
    // publication. A failed commit must never advertise a committed epoch.
    shard.set_fail_flushes(false);
    assert_eq!(registry.latest_epoch("v072_failed_flush"), None);
    assert!(registry.since_epoch("v072_failed_flush", 0).is_none());
    task.abort();
}
