use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_control::audit::FileAuditLog;
use rockstream_control::MigrationPersistentStore;
use rockstream_types::ids::ShardId;
use rockstream_types::migration::{BucketSet, MigrationRecord, MigrationState};

fn record() -> MigrationRecord {
    MigrationRecord::new(
        "migration-1",
        vec![ShardId(1)],
        ShardId(2),
        BucketSet::new([7, 8]),
        42,
        9,
    )
}

#[tokio::test]
async fn migration_record_roundtrips_all_states_and_legal_transitions() {
    let store = MigrationPersistentStore::new(Arc::new(InMemory::new()));
    let dir = tempfile::tempdir().unwrap();
    let audit = FileAuditLog::open(dir.path().join("audit.jsonl")).unwrap();

    let mut record = record();
    store.save(&record).await.unwrap();
    let states = [
        MigrationState::Snapshotting,
        MigrationState::Copying,
        MigrationState::DualWriting,
        MigrationState::CatchingUp,
        MigrationState::FencingOld,
        MigrationState::Cutover,
        MigrationState::Verifying,
        MigrationState::GcEligible,
        MigrationState::Done,
    ];
    for state in states {
        store
            .transition(&mut record, state, Some(&audit))
            .await
            .unwrap();
        let loaded = store.load(&record.migration_id).await.unwrap();
        assert_eq!(loaded.state, state);
    }

    let events = audit.read_all().unwrap();
    assert_eq!(events.len(), states.len());
    assert!(events
        .iter()
        .all(|event| event.action == "migration.transition"));
}

#[tokio::test]
async fn migration_record_idempotent_reapplication_is_noop() {
    let store = MigrationPersistentStore::new(Arc::new(InMemory::new()));
    let mut record = record();
    store.save(&record).await.unwrap();

    assert!(store
        .transition(&mut record, MigrationState::Snapshotting, None)
        .await
        .unwrap());
    let updated_at_ms = record.updated_at_ms;
    assert!(!store
        .transition(&mut record, MigrationState::Snapshotting, None)
        .await
        .unwrap());
    assert_eq!(record.updated_at_ms, updated_at_ms);
}

#[tokio::test]
async fn migration_record_illegal_transition_rejected_with_rs_code() {
    let store = MigrationPersistentStore::new(Arc::new(InMemory::new()));
    let mut record = record();
    let err = store
        .transition(&mut record, MigrationState::Cutover, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("RS-5030"));
    assert_eq!(record.state, MigrationState::Planned);
}

#[tokio::test]
async fn migration_record_aborted_roundtrips() {
    let store = MigrationPersistentStore::new(Arc::new(InMemory::new()));
    let mut record = record();
    store
        .transition(&mut record, MigrationState::Aborted, None)
        .await
        .unwrap();
    let loaded = store.load(&record.migration_id).await.unwrap();
    assert_eq!(loaded.state, MigrationState::Aborted);
}
