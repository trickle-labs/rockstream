use std::sync::Arc;
use std::time::Instant;

use object_store::memory::InMemory;
use rockstream_control::{
    CheckpointCoordinator, MigrationCoordinator, MigrationShard, PhaseClocks,
};
use rockstream_runtime::{DualWriteRouter, RoutedWrite};
use rockstream_storage::ShardDb;
use rockstream_types::ids::ShardId;
use rockstream_types::migration::{BucketSet, MigrationRecord, MigrationState};

fn make_record() -> MigrationRecord {
    MigrationRecord::new(
        "dual-write-1",
        vec![ShardId(1)],
        ShardId(2),
        BucketSet::new([7]),
        42,
        9,
    )
}

fn make_key(bucket: u64, suffix: &str) -> Vec<u8> {
    format!("bucket/{bucket}/{suffix}").into_bytes()
}

#[test]
fn migration_epoch_routes_old_and_new_writes_exactly_once() {
    let mut record = make_record().with_migration_epoch(100);
    record
        .apply_transition(MigrationState::Snapshotting)
        .unwrap();
    record.apply_transition(MigrationState::Copying).unwrap();
    record
        .apply_transition(MigrationState::DualWriting)
        .unwrap();
    let router = DualWriteRouter::new(record.clone());

    assert_eq!(
        router.route_targets_at_epoch(7, 9, 99).unwrap(),
        vec![ShardId(1)]
    );
    assert_eq!(
        router.route_targets_at_epoch(7, 9, 100).unwrap(),
        vec![ShardId(1), ShardId(2)]
    );

    record.apply_transition(MigrationState::CatchingUp).unwrap();
    record.apply_transition(MigrationState::FencingOld).unwrap();
    record.apply_transition(MigrationState::Cutover).unwrap();
    record.cutover_epoch = Some(120);
    let router = DualWriteRouter::new(record);
    assert_eq!(
        router.route_targets_at_epoch(7, 9, 119).unwrap(),
        vec![ShardId(1)]
    );
    assert_eq!(
        router.route_targets_at_epoch(7, 9, 120).unwrap(),
        vec![ShardId(2)]
    );
}

async fn make_shard(
    shard_id: u64,
    path: &str,
    store: Arc<InMemory>,
    frontier: u64,
) -> MigrationShard {
    let db = ShardDb::builder(path.to_string(), store.clone())
        .build()
        .await
        .unwrap();
    MigrationShard {
        shard_id: ShardId(shard_id),
        path: path.to_string(),
        object_store: store,
        db,
        frontier,
    }
}

async fn scan_bucket(db: &ShardDb, bucket: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let prefix = format!("bucket/{bucket}/").into_bytes();
    let mut entries: Vec<_> = db
        .scan_prefix(&prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

#[tokio::test]
async fn dual_write_zero_loss_tests() {
    let store = Arc::new(InMemory::new());
    let donor = make_shard(1, "dual-write/donor", store.clone(), 42).await;
    let recipient = make_shard(2, "dual-write/recipient", store.clone(), 42).await;
    donor.db.put(&make_key(7, "seed"), b"seed").await.unwrap();
    donor.db.flush().await.unwrap();

    let checkpoints = CheckpointCoordinator::new(vec![ShardId(1)]);
    let mut record = make_record();
    let coordinator = MigrationCoordinator::new();
    coordinator
        .drive_planned_to_copying(
            &mut record,
            std::slice::from_ref(&donor),
            &recipient,
            &checkpoints,
            PhaseClocks {
                snapshotting_started_at: Instant::now(),
                copying_started_at: Instant::now(),
            },
            None,
        )
        .await
        .unwrap();
    coordinator.begin_dual_writing(&mut record, None).unwrap();
    assert_eq!(record.state, MigrationState::DualWriting);

    let router = DualWriteRouter::new(record.clone());
    for i in 0..5u64 {
        let write = RoutedWrite {
            bucket: 7,
            bucket_map_version: 9,
            key: make_key(7, &format!("stream-{i}")),
            value: format!("v{i}").into_bytes(),
        };
        assert_eq!(
            router
                .apply_write(&write, &donor.db, &recipient.db)
                .await
                .unwrap(),
            2
        );
    }

    coordinator
        .advance_to_catching_up(&mut record, None)
        .unwrap();
    assert!(coordinator
        .advance_to_fencing_old_if_caught_up(&mut record, 42, 42, None)
        .unwrap());

    assert_eq!(
        scan_bucket(&donor.db, 7).await,
        scan_bucket(&recipient.db, 7).await
    ); // M6-S2
}
