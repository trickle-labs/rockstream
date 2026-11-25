use std::sync::Arc;
use std::time::Duration;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_storage::compactor::{
    inspect_compaction_debt, CompactionHorizonGuard, ShardCompactorHandle,
    StandaloneCompactorConfig,
};
use rockstream_storage::reader::{ServingReaderOptions, SharedServingCache};
use rockstream_storage::{ShardDb, ShardReader, WriteBatch};
use rockstream_types::ids::ShardId;

#[tokio::test]
async fn remote_compactor_compacts_without_inline_compaction_and_survives_crash() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(temp.path()).expect("local store"));

    // Stateful worker opens ShardDb with inline compaction disabled.
    let db = ShardDb::builder("db", Arc::clone(&store))
        .with_inline_compaction(false)
        .build()
        .await
        .expect("build shard db without inline compaction");

    // Write and flush several epochs to accumulate L0 SSTs.
    for epoch in 1..=4u64 {
        let mut batch = WriteBatch::new();
        batch.put(
            format!("view_output/orders/{epoch}").as_bytes(),
            format!("{epoch}\titem_{epoch}\t{}", epoch * 10).as_bytes(),
        );
        db.write_batch(batch).await.expect("write batch");
        db.flush_memtable().await.expect("flush memtable to L0 SST");
    }

    let debt_before = inspect_compaction_debt("db", Arc::clone(&store))
        .await
        .expect("inspect debt before");
    assert!(
        debt_before.l0_sst_count >= 1,
        "expected uncompacted L0 SSTs when inline compaction is disabled, got {debt_before:?}"
    );

    // Spawn dedicated standalone compactor against the same object store.
    let compactor = ShardCompactorHandle::spawn(
        ShardId(0),
        "db",
        Arc::clone(&store),
        StandaloneCompactorConfig {
            max_concurrent_compactions: 2,
            poll_interval: Duration::from_millis(50),
            manifest_poll_interval: Duration::from_millis(50),
            max_sst_size: 64 * 1024 * 1024,
            filter_bits_per_key: Some(10),
        },
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(compactor.is_running());
    assert!(compactor.last_error().is_none());

    // Simulate compactor crash / stop mid-run.
    compactor.stop().await;
    assert!(!compactor.is_running());

    // Stateful worker ingestion continues uninterrupted even with no compactor running.
    let mut batch = WriteBatch::new();
    batch.put(b"view_output/orders/5", b"5\titem_5\t50");
    db.write_batch(batch)
        .await
        .expect("write batch after compactor stop");
    db.flush_memtable()
        .await
        .expect("flush memtable after compactor stop");

    let debt_during_outage = inspect_compaction_debt("db", Arc::clone(&store))
        .await
        .expect("inspect debt during outage");
    assert!(debt_during_outage.l0_sst_count >= 1);

    // Restart a new compactor and verify all 5 rows are intact and exact.
    let compactor2 = ShardCompactorHandle::spawn(
        ShardId(0),
        "db",
        Arc::clone(&store),
        StandaloneCompactorConfig {
            poll_interval: Duration::from_millis(50),
            ..StandaloneCompactorConfig::default()
        },
    );
    tokio::time::sleep(Duration::from_millis(200)).await;

    let reader = ShardReader::open("db", Arc::clone(&store))
        .await
        .expect("open ShardReader");
    let rows = reader
        .scan_prefix(b"view_output/orders/")
        .await
        .expect("scan rows");
    let mut decoded: Vec<(String, String)> = rows
        .into_iter()
        .map(|(k, v)| {
            (
                String::from_utf8_lossy(&k).into_owned(),
                String::from_utf8_lossy(&v).into_owned(),
            )
        })
        .collect();
    decoded.sort();

    assert_eq!(
        decoded,
        vec![
            (
                "view_output/orders/1".to_owned(),
                "1\titem_1\t10".to_owned()
            ),
            (
                "view_output/orders/2".to_owned(),
                "2\titem_2\t20".to_owned()
            ),
            (
                "view_output/orders/3".to_owned(),
                "3\titem_3\t30".to_owned()
            ),
            (
                "view_output/orders/4".to_owned(),
                "4\titem_4\t40".to_owned()
            ),
            (
                "view_output/orders/5".to_owned(),
                "5\titem_5\t50".to_owned()
            ),
        ]
    );

    compactor2.stop().await;
    db.close().await.expect("close db");
}

#[tokio::test]
async fn compaction_horizon_guard_respects_readers_backups_and_replay() {
    let guard = CompactionHorizonGuard::new();
    assert_eq!(guard.safe_compaction_horizon(), None);
    assert!(guard.can_reclaim_epoch(10));

    guard.pin_horizon("reader-gw-1", 50);
    guard.pin_horizon("as-of-epoch-query", 30);
    guard.pin_horizon("online-backup-406", 40);
    guard.set_replay_horizon(Some(25));

    assert_eq!(guard.safe_compaction_horizon(), Some(25));
    assert!(guard.can_reclaim_epoch(24));
    assert!(!guard.can_reclaim_epoch(25));
    assert!(!guard.can_reclaim_epoch(30));

    // Advance replay horizon; AS OF EPOCH query (30) is now the tightest bound.
    guard.set_replay_horizon(Some(60));
    assert_eq!(guard.safe_compaction_horizon(), Some(30));
    assert!(guard.can_reclaim_epoch(29));
    assert!(!guard.can_reclaim_epoch(30));

    // Release AS OF EPOCH query; backup (40) becomes the tightest bound.
    guard.release_horizon("as-of-epoch-query");
    assert_eq!(guard.safe_compaction_horizon(), Some(40));

    // Verify ManagedCheckpoint on ShardReader registers a real SlateDB checkpoint.
    let temp = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(temp.path()).expect("local store"));
    let db = ShardDb::builder("db", Arc::clone(&store))
        .with_inline_compaction(false)
        .build()
        .await
        .expect("build db");
    db.flush().await.expect("initial flush");

    let cache = SharedServingCache::new("test-gw", 4 * 1024 * 1024);
    let reader = ShardReader::open_for_serving(
        "db",
        Arc::clone(&store),
        ServingReaderOptions {
            manifest_poll_interval: Duration::from_millis(50),
            checkpoint_lifetime: Duration::from_secs(30),
            use_managed_checkpoint: true,
            ..ServingReaderOptions::default()
        },
        Some(&cache),
    )
    .await
    .expect("open_for_serving");

    let debt = inspect_compaction_debt("db", Arc::clone(&store))
        .await
        .expect("inspect debt");
    assert!(
        debt.active_checkpoints >= 1,
        "expected ManagedCheckpoint reader to register a SlateDB checkpoint, got {debt:?}"
    );
    drop(reader);
    db.close().await.expect("close db");
}
