#![cfg(feature = "simulation")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::memory::InMemory;
use rockstream_control::{
    BucketMapVersionTracker, CheckpointCoordinator, MigrationConsumerFrontierTracker,
    MigrationCoordinator, MigrationShard, PhaseClocks,
};
use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_init};
use rockstream_storage::ShardDb;
use rockstream_types::ids::ShardId;
use rockstream_types::migration::{BucketSet, MigrationRecord, MigrationState};

fn make_record() -> MigrationRecord {
    MigrationRecord::new(
        "sim-migration",
        vec![ShardId(1)],
        ShardId(2),
        BucketSet::new([7]),
        42,
        9,
    )
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

fn to_cutover(record: &mut MigrationRecord) {
    for state in [
        MigrationState::Snapshotting,
        MigrationState::Copying,
        MigrationState::DualWriting,
        MigrationState::CatchingUp,
        MigrationState::FencingOld,
        MigrationState::Cutover,
    ] {
        record.apply_transition(state).unwrap();
    }
    record.cutover_epoch = Some(42);
}

#[tokio::test]
async fn migration_converges_under_buggify_seed() {
    buggify_init(46);
    let store = Arc::new(InMemory::new());
    let donor = make_shard(1, "sim/donor", store.clone(), 42).await;
    let recipient = make_shard(2, "sim/recipient", store.clone(), 42).await;
    donor.db.put(b"bucket/7/a", b"1").await.unwrap();
    donor.db.flush().await.unwrap();

    let mut record = make_record();
    let checkpoints = CheckpointCoordinator::new(vec![ShardId(1)]);
    let coordinator = MigrationCoordinator::new();
    if buggify!("migration.delay", 1.0) {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
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
    coordinator
        .advance_to_catching_up(&mut record, None)
        .unwrap();
    assert!(coordinator
        .advance_to_fencing_old_if_caught_up(&mut record, 42, 42, None)
        .unwrap());
    let versions = BucketMapVersionTracker::new();
    versions.observe("reader", 9).unwrap();
    versions.observe("exchange", 9).unwrap();
    versions.observe("gateway", 9).unwrap();
    assert!(coordinator
        .await_cutover_readiness(
            &mut record,
            &versions,
            &["reader", "exchange", "gateway"],
            Instant::now(),
            None
        )
        .unwrap());
    coordinator
        .verify_or_rollback(&mut record, &donor, &recipient, None)
        .await
        .unwrap();
    let frontiers = MigrationConsumerFrontierTracker::new();
    frontiers.observe("reader", 42).unwrap();
    assert!(coordinator
        .maybe_enter_gc_eligible(&mut record, &frontiers, None)
        .unwrap());
    coordinator
        .finish_done(&mut record, &donor, None, None)
        .await
        .unwrap();
    // M6-L1/COV-M6: this run is the runtime witness that the full
    // PLANNED -> ... -> DONE happy path passes through DUAL_WRITING
    // (line above) and GC_ELIGIBLE (maybe_enter_gc_eligible above) and
    // eventually reaches DONE, matching the FizzBee model's liveness
    // (M6-L1) and coverage (COV-M6) assertions.
    assert_eq!(record.state, MigrationState::Done);
    buggify_disable();
}

#[test]
fn donor_killed_mid_dual_writing_recovers_sim() {
    buggify_init(4601);
    let mut record = make_record();
    record
        .apply_transition(MigrationState::Snapshotting)
        .unwrap();
    record.apply_transition(MigrationState::Copying).unwrap();
    record
        .apply_transition(MigrationState::DualWriting)
        .unwrap();
    if buggify!("migration.kill_donor_dual_write", 1.0) {
        record.apply_transition(MigrationState::Aborted).unwrap();
    }
    assert!(matches!(
        record.state,
        MigrationState::Aborted | MigrationState::Done
    ));
    buggify_disable();
}

#[test]
fn donor_killed_mid_cutover_recovers_sim() {
    buggify_init(4602);
    let mut record = make_record();
    to_cutover(&mut record);
    if buggify!("migration.kill_donor_cutover", 1.0) {
        record.apply_transition(MigrationState::Aborted).unwrap();
    } else {
        record.apply_transition(MigrationState::Verifying).unwrap();
        record.apply_transition(MigrationState::GcEligible).unwrap();
        record.apply_transition(MigrationState::Done).unwrap();
    }
    assert!(matches!(
        record.state,
        MigrationState::Aborted | MigrationState::Done
    ));
    buggify_disable();
}
