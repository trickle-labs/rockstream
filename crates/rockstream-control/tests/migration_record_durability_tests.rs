use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::{ObjectStore, ObjectStoreExt};
use rockstream_control::{MigrationLoadError, MigrationPersistentStore, ShardManager};
use rockstream_test_support::docker_available;
use rockstream_test_support::minio::{minio_object_store, start_minio};
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::migration::{
    BucketSet, MigrationRecord, MigrationState, MIGRATION_RECORD_VERSION,
};

fn make_record() -> MigrationRecord {
    let mut record = MigrationRecord::new(
        "durable-migration",
        vec![ShardId(1)],
        ShardId(2),
        BucketSet::new([7]),
        42,
        9,
    );
    record
        .apply_transition(MigrationState::Snapshotting)
        .unwrap();
    record.apply_transition(MigrationState::Copying).unwrap();
    record
        .apply_transition(MigrationState::DualWriting)
        .unwrap();
    record
}

const MINIO_BUCKET: &str = "rockstream-migration-durability-test";

#[tokio::test]
async fn migration_record_load_distinguishes_missing_corrupt_and_unsupported() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let persistent = MigrationPersistentStore::new(store.clone());

    assert_eq!(
        persistent.load("missing").await,
        Err(MigrationLoadError::Missing)
    );

    store
        .put(
            &object_store::path::Path::from("topology/migration/corrupt.json"),
            b"not-json".to_vec().into(),
        )
        .await
        .unwrap();
    assert!(matches!(
        persistent.load("corrupt").await,
        Err(MigrationLoadError::Corrupt(_))
    ));

    let mut unsupported = make_record();
    unsupported.record_version = 99;
    persistent.save(&unsupported).await.unwrap();
    assert_eq!(
        persistent.load(&unsupported.migration_id).await,
        Err(MigrationLoadError::UnsupportedVersion {
            found: 99,
            expected: MIGRATION_RECORD_VERSION,
        })
    );
}

#[tokio::test]
async fn active_migrations_load_in_stable_order_after_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let persistent = MigrationPersistentStore::new(store);
    let first = MigrationRecord::new(
        "migration-b",
        vec![ShardId(1)],
        ShardId(2),
        BucketSet::new([8]),
        42,
        9,
    );
    let second = MigrationRecord::new(
        "migration-a",
        vec![ShardId(3)],
        ShardId(4),
        BucketSet::new([9]),
        43,
        10,
    );
    persistent.save(&first).await.unwrap();
    persistent.save(&second).await.unwrap();

    assert_eq!(
        persistent.load_active().await.unwrap(),
        vec![second.clone(), first.clone()]
    );
    persistent.archive(&first, None).await.unwrap();
    assert_eq!(persistent.load_active().await.unwrap(), vec![second]);
}

#[tokio::test]
async fn active_migration_recovery_captures_current_leases() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let persistent = MigrationPersistentStore::new(store);
    let record = make_record();
    persistent.save(&record).await.unwrap();
    let manager = ShardManager::new();
    let donor_lease = manager.acquire(ShardId(1), WorkerId(10)).unwrap();
    let recipient_lease = manager.acquire(ShardId(2), WorkerId(20)).unwrap();

    assert_eq!(
        persistent.recover_active(&manager).await.unwrap(),
        vec![rockstream_control::MigrationRecovery {
            record,
            current_leases: std::collections::BTreeMap::from([
                (ShardId(1), Some(donor_lease)),
                (ShardId(2), Some(recipient_lease)),
            ]),
        }]
    );
}

#[tokio::test]
async fn migration_record_survives_restart_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    store
        .put(
            &object_store::path::Path::from("unrelated.json"),
            b"unrelated".to_vec().into(),
        )
        .await
        .unwrap();
    let persistent_a = MigrationPersistentStore::new(store.clone());
    let state = make_record();
    persistent_a.save(&state).await.unwrap();

    let persistent_b = MigrationPersistentStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    assert_eq!(persistent_b.load(&state.migration_id).await.unwrap(), state);
}

#[tokio::test]
async fn migration_record_survives_restart_minio_tc() {
    if !docker_available() {
        eprintln!("SKIP migration_record_survives_restart_minio_tc: Docker not available");
        return;
    }
    let (_container, port) = match start_minio(MINIO_BUCKET).await {
        Some(res) => res,
        None => return,
    };

    let persistent_a = MigrationPersistentStore::new(minio_object_store(port, MINIO_BUCKET));
    let state = make_record();
    persistent_a.save(&state).await.unwrap();
    let persistent_b = MigrationPersistentStore::new(minio_object_store(port, MINIO_BUCKET));
    assert_eq!(persistent_b.load(&state.migration_id).await.unwrap(), state);
}

#[tokio::test]
async fn test_interrupted_migration_progress_survives_restart_lfs_and_minio() {
    let mut record = make_record().with_work_estimates(Some(20_000_000), Some(100_000));
    record.record_progress(8_000_000, 40_000);

    // 1. LFS
    let dir = tempfile::tempdir().unwrap();
    let store_lfs: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let persistent_lfs_a = MigrationPersistentStore::new(store_lfs.clone());
    persistent_lfs_a.save(&record).await.unwrap();

    let persistent_lfs_b = MigrationPersistentStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    let mut loaded = persistent_lfs_b.load(&record.migration_id).await.unwrap();
    assert_eq!(loaded.progress_phase(), record.progress_phase());
    assert_eq!(loaded.bytes_remaining(), Some(0)); // dual_writing has 0 bytes remaining
    assert_eq!(loaded.rows_remaining(), Some(0));
    assert_eq!(loaded.estimated_remaining_ms(), None);

    // Advance loaded record after restart
    loaded.apply_transition(MigrationState::CatchingUp).unwrap();
    assert_eq!(loaded.progress_phase(), "catching_up");
    loaded.apply_transition(MigrationState::FencingOld).unwrap();
    loaded.apply_transition(MigrationState::Cutover).unwrap();
    loaded.apply_transition(MigrationState::Verifying).unwrap();
    loaded.apply_transition(MigrationState::GcEligible).unwrap();
    loaded.apply_transition(MigrationState::Done).unwrap();
    assert_eq!(loaded.progress_phase(), "done");
    assert_eq!(loaded.bytes_remaining(), Some(0));
    assert_eq!(loaded.rows_remaining(), Some(0));
    assert_eq!(loaded.estimated_remaining_ms(), Some(0));

    // 2. MinIO (if docker available)
    if docker_available() {
        if let Some((_container, port)) = start_minio(MINIO_BUCKET).await {
            let persistent_minio_a =
                MigrationPersistentStore::new(minio_object_store(port, MINIO_BUCKET));
            persistent_minio_a.save(&record).await.unwrap();

            let persistent_minio_b =
                MigrationPersistentStore::new(minio_object_store(port, MINIO_BUCKET));
            let loaded_minio = persistent_minio_b.load(&record.migration_id).await.unwrap();
            assert_eq!(loaded_minio.progress_phase(), record.progress_phase());
            assert_eq!(loaded_minio.bytes_remaining(), Some(0));
        }
    }
}
