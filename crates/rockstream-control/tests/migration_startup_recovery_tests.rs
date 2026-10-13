//! Restart regression tests for issue #107: exercise production ControlService::start.
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::ObjectStoreExt;
use rockstream_control::topology::TopologyCatalog;
use rockstream_control::{
    ControlService, MigrationLoadError, MigrationPersistentStore, MigrationRecoveryBlockReason,
    MigrationShard, MigrationStartupStatus, ShardManager,
};
use rockstream_storage::ShardDb;
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::migration::{BucketSet, MigrationRecord, MigrationState};

fn record(state: MigrationState, id: &str) -> MigrationRecord {
    let mut record = MigrationRecord::new(
        id,
        vec![ShardId(1)],
        ShardId(2),
        BucketSet::new([7, 8]),
        42,
        9,
    );
    for next in [
        MigrationState::Snapshotting,
        MigrationState::Copying,
        MigrationState::DualWriting,
        MigrationState::CatchingUp,
        MigrationState::FencingOld,
        MigrationState::Cutover,
        MigrationState::Verifying,
        MigrationState::GcEligible,
        MigrationState::Done,
    ] {
        if record.state == state {
            break;
        }
        record.apply_transition(next).unwrap();
    }
    record
}

async fn shard(store: Arc<InMemory>, id: u64, path: &str) -> MigrationShard {
    let db = ShardDb::builder(path.to_owned(), store.clone())
        .build()
        .await
        .unwrap();
    MigrationShard {
        shard_id: ShardId(id),
        path: path.to_owned(),
        object_store: store,
        db,
        frontier: 42,
    }
}

async fn keys(db: &ShardDb) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut result: Vec<_> = db
        .scan_prefix(b"bucket/")
        .await
        .unwrap()
        .into_iter()
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect();
    result.sort();
    result
}

async fn start(
    persistent: &MigrationPersistentStore,
    store: Arc<InMemory>,
    manager: ShardManager,
    donor: Option<MigrationShard>,
) -> rockstream_control::ControlServiceHandle {
    let mut service = ControlService::new(TopologyCatalog::new())
        .with_shard_manager(manager)
        .with_migration_store(Arc::new(MigrationPersistentStore::new(store)));
    if let Some(donor) = donor {
        service = service.with_migration_recovery_donor(donor);
    }
    let handle = service.start("127.0.0.1:0").await.unwrap();
    assert_eq!(
        persistent.load_active().await.unwrap().len(),
        handle
            .migration_recovery
            .iter()
            .filter(|outcome| matches!(outcome.status, MigrationStartupStatus::Blocked(_)))
            .count()
    );
    handle
}

#[tokio::test]
async fn startup_blocks_unsafe_phases_without_changing_record_data_or_leases() {
    for state in [
        MigrationState::Copying,
        MigrationState::FencingOld,
        MigrationState::Verifying,
        MigrationState::GcEligible,
    ] {
        let store = Arc::new(InMemory::new());
        let persistent = MigrationPersistentStore::new(store.clone());
        let donor = shard(store.clone(), 1, "restart/donor-blocked").await;
        let recipient = shard(store.clone(), 2, "restart/recipient-blocked").await;
        donor.db.put(b"bucket/7/a", b"donor").await.unwrap();
        donor.db.put(b"bucket/8/b", b"donor-b").await.unwrap();
        donor.db.put(b"bucket/99/keep", b"unrelated").await.unwrap();
        recipient.db.put(b"bucket/7/a", b"recipient").await.unwrap();
        donor.db.flush().await.unwrap();
        recipient.db.flush().await.unwrap();
        let before_donor = keys(&donor.db).await;
        let before_recipient = keys(&recipient.db).await;
        let record = record(state, "blocked");
        persistent.save(&record).await.unwrap();
        let manager = ShardManager::new();
        manager.acquire(ShardId(1), WorkerId(10)).unwrap();
        manager.acquire(ShardId(2), WorkerId(20)).unwrap();
        let leases = manager.snapshot();

        for _ in 0..2 {
            let handle = start(
                &persistent,
                store.clone(),
                manager.clone(),
                Some(donor.clone()),
            )
            .await;
            let outcome = &handle.migration_recovery[0];
            assert_eq!(outcome.recovery.record, record);
            assert_eq!(
                outcome.status,
                MigrationStartupStatus::Blocked(if state == MigrationState::GcEligible {
                    MigrationRecoveryBlockReason::CleanupNotStarted
                } else {
                    MigrationRecoveryBlockReason::NeedsOnlineDriver
                })
            );
            assert_eq!(persistent.load("blocked").await.unwrap(), record);
            assert_eq!(
                persistent.load_history("blocked").await,
                Err(MigrationLoadError::Missing)
            );
            assert_eq!(keys(&donor.db).await, before_donor);
            assert_eq!(keys(&recipient.db).await, before_recipient);
            assert_eq!(manager.snapshot(), leases);
            handle.shutdown();
        }
    }
}

#[tokio::test]
async fn startup_replays_partial_cleanup_and_then_is_idempotent() {
    let store = Arc::new(InMemory::new());
    let persistent = MigrationPersistentStore::new(store.clone());
    let donor = shard(store.clone(), 1, "restart/donor-cleanup").await;
    let recipient = shard(store.clone(), 2, "restart/recipient-cleanup").await;
    for (key, val) in [
        (&b"bucket/7/a"[..], &b"first"[..]),
        (&b"bucket/7/b"[..], &b"second"[..]),
        (&b"bucket/8/c"[..], &b"third"[..]),
        (&b"bucket/99/keep"[..], &b"unrelated"[..]),
    ] {
        donor.db.put(key, val).await.unwrap();
        recipient.db.put(key, val).await.unwrap();
    }
    // Crash after one donor delete, before finishing the other buckets.
    donor.db.delete(b"bucket/7/a").await.unwrap();
    donor.db.flush().await.unwrap();
    recipient.db.flush().await.unwrap();
    let recipient_before = keys(&recipient.db).await;
    let manager = ShardManager::new();
    manager.acquire(ShardId(1), WorkerId(10)).unwrap();
    manager.acquire(ShardId(2), WorkerId(20)).unwrap();
    let leases = manager.snapshot();
    let mut record = record(MigrationState::GcEligible, "partial-cleanup");
    record.cleanup_intent = true;
    record.verification_progress = Some(100);
    persistent.save(&record).await.unwrap();

    let handle = start(
        &persistent,
        store.clone(),
        manager.clone(),
        Some(donor.clone()),
    )
    .await;
    assert_eq!(
        handle.migration_recovery[0].status,
        MigrationStartupStatus::ReplayedCleanup { deleted_keys: 2 }
    );
    assert_eq!(
        persistent.load("partial-cleanup").await,
        Err(MigrationLoadError::Missing)
    );
    let history = persistent.load_history("partial-cleanup").await.unwrap();
    assert_eq!(history.state, MigrationState::Done);
    assert!(history.cleanup_intent);
    assert_eq!(
        keys(&donor.db).await,
        vec![(b"bucket/99/keep".to_vec(), b"unrelated".to_vec())]
    );
    assert_eq!(keys(&recipient.db).await, recipient_before);
    assert_eq!(manager.snapshot(), leases);
    handle.shutdown();

    let handle2 = start(&persistent, store, manager.clone(), Some(donor.clone())).await;
    assert!(handle2.migration_recovery.is_empty());
    assert_eq!(
        persistent.load_history("partial-cleanup").await.unwrap(),
        history
    );
    assert_eq!(
        keys(&donor.db).await,
        vec![(b"bucket/99/keep".to_vec(), b"unrelated".to_vec())]
    );
    assert_eq!(keys(&recipient.db).await, recipient_before);
    assert_eq!(manager.snapshot(), leases);
    handle2.shutdown();
}

#[tokio::test]
async fn startup_archives_done_after_history_persistence_interruption() {
    for history_already_persisted in [false, true] {
        let store = Arc::new(InMemory::new());
        let persistent = MigrationPersistentStore::new(store.clone());
        let manager = ShardManager::new();
        manager.acquire(ShardId(1), WorkerId(10)).unwrap();
        manager.acquire(ShardId(2), WorkerId(20)).unwrap();
        let leases = manager.snapshot();
        let record = {
            let mut r = record(MigrationState::Done, "done");
            r.cleanup_intent = true;
            r.verification_progress = Some(100);
            r
        };
        persistent.save(&record).await.unwrap();
        if history_already_persisted {
            // Reproduce the durable history-write / active-delete gap.
            store
                .put(
                    &object_store::path::Path::from("topology/migration_history/done.json"),
                    serde_json::to_vec(&record).unwrap().into(),
                )
                .await
                .unwrap();
        }
        let handle = start(&persistent, store.clone(), manager.clone(), None).await;
        assert_eq!(
            handle.migration_recovery[0].status,
            MigrationStartupStatus::ArchivedDone
        );
        assert_eq!(
            persistent.load("done").await,
            Err(MigrationLoadError::Missing)
        );
        assert_eq!(persistent.load_history("done").await.unwrap(), record);
        assert_eq!(manager.snapshot(), leases);
        handle.shutdown();
        let twice = start(&persistent, store, manager.clone(), None).await;
        assert!(twice.migration_recovery.is_empty());
        assert_eq!(persistent.load_history("done").await.unwrap(), record);
        assert_eq!(manager.snapshot(), leases);
        twice.shutdown();
    }
}
