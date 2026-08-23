use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::memory::InMemory;
use rockstream_control::{
    BucketMapVersionTracker, CheckpointCoordinator, MigrationConsumerFrontierTracker,
    MigrationCoordinator, MigrationPersistentStore, MigrationShard, PhaseClocks,
};
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::ids::ShardId;
use rockstream_types::migration::{BucketSet, MigrationRecord, MigrationState};

fn make_record() -> MigrationRecord {
    MigrationRecord::new(
        "migration-42",
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
    db.scan_prefix(&prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect()
}

fn step_to_cutover(record: &mut MigrationRecord) {
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
    record.cutover_epoch = Some(record.planned_frontier);
}

#[tokio::test]
async fn snapshotting_pins_donor_checkpoint_at_f_plan() {
    let store = Arc::new(InMemory::new());
    let donor = make_shard(1, "migration/donor-a", store.clone(), 42).await;
    let recipient = make_shard(2, "migration/recipient-a", store.clone(), 42).await;
    donor
        .db
        .put(&make_key(7, "before"), b"value")
        .await
        .unwrap();
    donor.db.flush().await.unwrap();

    let mut record = make_record();
    let checkpoints = CheckpointCoordinator::new(vec![ShardId(1)]);
    let coordinator = MigrationCoordinator::new();
    let manifest = coordinator
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

    assert_eq!(record.planned_frontier, 42);
    assert_eq!(record.state, MigrationState::Copying);
    assert!(record.donor_checkpoints.contains_key(&ShardId(1)));
    assert_eq!(
        manifest.shards[&ShardId(1)].shard_checkpoint_id,
        record.donor_checkpoints[&ShardId(1)]
    );
}

#[tokio::test]
async fn copying_recipient_matches_donor_checkpoint() {
    let store = Arc::new(InMemory::new());
    let donor = make_shard(1, "migration/donor-b", store.clone(), 42).await;
    let recipient = make_shard(2, "migration/recipient-b", store.clone(), 42).await;
    donor.db.put(&make_key(7, "a"), b"1").await.unwrap();
    donor.db.put(&make_key(7, "b"), b"2").await.unwrap();
    donor.db.flush().await.unwrap();

    let mut record = make_record();
    let checkpoints = CheckpointCoordinator::new(vec![ShardId(1)]);
    MigrationCoordinator::new()
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

    assert_eq!(
        scan_bucket(&donor.db, 7).await,
        scan_bucket(&recipient.db, 7).await
    );
    assert_eq!(record.donor_checkpoint_snapshots.len(), 1);
    assert_eq!(
        record.copied_rows,
        Some(donor.db.scan_prefix(b"").await.unwrap().len() as u64)
    );
}

#[tokio::test]
async fn bounded_copy_chunks_report_exact_limits_and_output() {
    let store = Arc::new(InMemory::new());
    let donor = make_shard(1, "migration/chunks-donor", store.clone(), 42).await;
    let recipient = make_shard(2, "migration/chunks-recipient", store.clone(), 42).await;
    let mut batch = WriteBatch::new();
    for i in 0..300 {
        batch.put(&make_key(7, &format!("k{i:04}")), b"value");
    }
    donor.db.write_batch(batch).await.unwrap();
    donor.db.flush().await.unwrap();

    let mut record = make_record();
    record
        .apply_transition(MigrationState::Snapshotting)
        .unwrap();
    record.apply_transition(MigrationState::Copying).unwrap();
    let source_rows = donor.db.scan_prefix(b"").await.unwrap().len();
    let stats = MigrationCoordinator::new()
        .copy_bounded_chunks(&mut record, &[donor], &recipient)
        .await
        .unwrap();

    assert_eq!(stats.copied_rows, source_rows as u64);
    assert_eq!(stats.chunks, 2);
    assert!(stats.max_chunk_rows <= 256);
    assert!(stats.max_chunk_bytes <= 1024 * 1024);
    assert_eq!(scan_bucket(&recipient.db, 7).await.len(), 300);
}

#[tokio::test]
async fn state_timeout_transitions_to_aborted() {
    let store = Arc::new(InMemory::new());
    let donor = make_shard(1, "migration/donor-timeout", store.clone(), 42).await;
    let recipient = make_shard(2, "migration/recipient-timeout", store.clone(), 42).await;

    let mut record = make_record();
    let checkpoints = CheckpointCoordinator::new(vec![ShardId(1)]);
    let coordinator = MigrationCoordinator::new().with_timeouts(
        Duration::from_millis(1),
        Duration::from_secs(300),
        Duration::from_secs(60),
    );
    let err = coordinator
        .drive_planned_to_copying(
            &mut record,
            &[donor],
            &recipient,
            &checkpoints,
            PhaseClocks {
                snapshotting_started_at: Instant::now() - Duration::from_secs(1),
                copying_started_at: Instant::now(),
            },
            None,
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("RS-1030"));
    assert_eq!(record.state, MigrationState::Aborted);
}

#[tokio::test]
async fn verifying_scan_compare_detects_divergence_and_aborts() {
    let store = Arc::new(InMemory::new());
    let donor = make_shard(1, "migration/donor-verify", store.clone(), 42).await;
    let recipient = make_shard(2, "migration/recipient-verify", store.clone(), 42).await;
    donor.db.put(&make_key(7, "x"), b"left").await.unwrap();
    recipient.db.put(&make_key(7, "x"), b"right").await.unwrap();

    let mut record = make_record();
    step_to_cutover(&mut record);
    let err = MigrationCoordinator::new()
        .verify_or_rollback(&mut record, &donor, &recipient, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("RS-5034"));
    assert_eq!(record.state, MigrationState::DualWriting);
}

#[tokio::test]
async fn gc_eligible_blocked_until_consumer_frontier_passes_cutover() {
    let tracker = MigrationConsumerFrontierTracker::new();
    let mut record = make_record();
    step_to_cutover(&mut record);
    MigrationCoordinator::new()
        .verify_or_rollback(
            &mut record,
            &make_shard(1, "migration/donor-gc", Arc::new(InMemory::new()), 42).await,
            &make_shard(2, "migration/recipient-gc", Arc::new(InMemory::new()), 42).await,
            None,
        )
        .await
        .unwrap();
    tracker.observe("reader-a", 41).unwrap();
    tracker.observe("gateway-a", 42).unwrap();

    assert!(!MigrationCoordinator::new()
        .maybe_enter_gc_eligible(&mut record, &tracker, None)
        .unwrap());
    assert_eq!(record.state, MigrationState::Verifying);

    tracker.observe("reader-a", 42).unwrap();
    assert!(MigrationCoordinator::new()
        .maybe_enter_gc_eligible(&mut record, &tracker, None)
        .unwrap());
    assert_eq!(record.state, MigrationState::GcEligible); // M6-S3
}

#[tokio::test]
async fn done_cleanup_is_scan_and_delete_never_range_delete() {
    let store = Arc::new(InMemory::new());
    let donor = make_shard(1, "migration/donor-cleanup", store.clone(), 42).await;
    donor.db.put(&make_key(7, "gone"), b"1").await.unwrap();
    donor.db.put(&make_key(99, "stay"), b"1").await.unwrap();
    donor.db.flush().await.unwrap();
    let mut record = make_record();
    for state in [
        MigrationState::Snapshotting,
        MigrationState::Copying,
        MigrationState::DualWriting,
        MigrationState::CatchingUp,
        MigrationState::FencingOld,
        MigrationState::Cutover,
        MigrationState::Verifying,
        MigrationState::GcEligible,
    ] {
        record.apply_transition(state).unwrap();
    }
    record.cutover_epoch = Some(42);

    let persistent = MigrationPersistentStore::new(store.clone());
    persistent.save(&record).await.unwrap();

    let recipient = make_shard(2, "migration/recipient-cleanup", store.clone(), 42).await;
    let stats = MigrationCoordinator::new()
        .finish_done(&mut record, &donor, Some(&persistent), None)
        .await
        .unwrap();
    assert_eq!(record.state, MigrationState::Done);
    assert_eq!(stats.deleted_keys, 1);
    assert!(scan_bucket(&donor.db, 7).await.is_empty());
    assert_eq!(scan_bucket(&donor.db, 99).await.len(), 1);
    assert!(persistent.load(&record.migration_id).await.is_none());
    assert_eq!(
        persistent
            .load_history(&record.migration_id)
            .await
            .unwrap()
            .state,
        MigrationState::Done
    );

    let source =
        std::fs::read_to_string(format!("{}/src/migration.rs", env!("CARGO_MANIFEST_DIR")))
            .unwrap();
    assert!(source.contains("scan_prefix"));
    assert!(source.contains("batch.delete"));
    assert!(!source.contains("range_delete"));
    drop(recipient);
}

#[tokio::test]
async fn verify_scan_window_is_bounded_with_fill_level_metric() {
    let store = Arc::new(InMemory::new());
    let donor = make_shard(1, "migration/donor-window", store.clone(), 42).await;
    let recipient = make_shard(2, "migration/recipient-window", store.clone(), 42).await;
    let mut donor_batch = WriteBatch::new();
    let mut recipient_batch = WriteBatch::new();
    for i in 0..1025usize {
        let key = make_key(7, &format!("k{i:04}"));
        donor_batch.put(&key, b"v");
        recipient_batch.put(&key, b"v");
    }
    donor.db.write_batch(donor_batch).await.unwrap();
    recipient.db.write_batch(recipient_batch).await.unwrap();
    donor.db.flush().await.unwrap();
    recipient.db.flush().await.unwrap();

    let mut record = make_record();
    step_to_cutover(&mut record);
    let coordinator = MigrationCoordinator::new();
    let fill = coordinator.verify_scan_fill_level(1025);
    assert_eq!(fill.used, 1025);
    let err = coordinator
        .verify_or_rollback(&mut record, &donor, &recipient, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("RS-5031"));
}

#[tokio::test]
async fn cutover_waits_for_all_observers_before_verifying() {
    let tracker = BucketMapVersionTracker::new();
    let mut record = make_record();
    for state in [
        MigrationState::Snapshotting,
        MigrationState::Copying,
        MigrationState::DualWriting,
        MigrationState::CatchingUp,
        MigrationState::FencingOld,
    ] {
        record.apply_transition(state).unwrap();
    }

    tracker.observe("reader", 9).unwrap();
    tracker.observe("exchange", 9).unwrap();
    let coordinator = MigrationCoordinator::new();
    assert!(!coordinator
        .await_cutover_readiness(
            &mut record,
            &tracker,
            &["reader", "exchange", "gateway"],
            Instant::now(),
            None,
        )
        .unwrap());
    assert_eq!(record.state, MigrationState::Cutover);
    tracker.observe("gateway", 9).unwrap();
    assert!(coordinator
        .await_cutover_readiness(
            &mut record,
            &tracker,
            &["reader", "exchange", "gateway"],
            Instant::now(),
            None,
        )
        .unwrap());
}

#[test]
fn cutover_requires_a_committed_frontier() {
    let tracker = BucketMapVersionTracker::new();
    let mut record = make_record();
    for state in [
        MigrationState::Snapshotting,
        MigrationState::Copying,
        MigrationState::DualWriting,
        MigrationState::CatchingUp,
        MigrationState::FencingOld,
    ] {
        record.apply_transition(state).unwrap();
    }
    for component in ["reader", "exchange", "gateway"] {
        tracker.observe(component, 9).unwrap();
    }

    let coordinator = MigrationCoordinator::new();
    assert!(!coordinator
        .await_cutover_readiness_at_frontier(
            &mut record,
            &tracker,
            &["reader", "exchange", "gateway"],
            41,
            Instant::now(),
            None,
        )
        .unwrap());
    assert_eq!(record.state, MigrationState::FencingOld);
    assert!(coordinator
        .await_cutover_readiness_at_frontier(
            &mut record,
            &tracker,
            &["reader", "exchange", "gateway"],
            42,
            Instant::now(),
            None,
        )
        .unwrap());
    assert_eq!(record.cutover_epoch, Some(42));
}

#[tokio::test]
async fn donor_reclamation_is_rejected_before_frontier_gate() {
    let store = Arc::new(InMemory::new());
    let donor = make_shard(1, "migration/early-gc-donor", store, 42).await;
    donor.db.put(&make_key(7, "keep"), b"value").await.unwrap();
    donor.db.flush().await.unwrap();
    let mut record = make_record();
    step_to_cutover(&mut record);

    let err = MigrationCoordinator::new()
        .finish_done(&mut record, &donor, None, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("RS-5033"));
    assert_eq!(scan_bucket(&donor.db, 7).await.len(), 1);
}

#[test]
fn test_migration_progress_monotonic_all_phases() {
    let mut record = make_record().with_work_estimates(Some(10_000_000), Some(50_000));
    assert_eq!(record.progress_phase(), "planned");
    assert_eq!(record.bytes_remaining(), Some(10_000_000));
    assert_eq!(record.rows_remaining(), Some(50_000));
    assert!(record.estimated_remaining_ms().is_some());

    record
        .apply_transition(MigrationState::Snapshotting)
        .unwrap();
    assert_eq!(record.progress_phase(), "snapshotting");
    assert_eq!(record.bytes_remaining(), Some(10_000_000));
    assert_eq!(record.rows_remaining(), Some(50_000));

    record.apply_transition(MigrationState::Copying).unwrap();
    assert_eq!(record.progress_phase(), "copying");
    assert_eq!(record.bytes_remaining(), Some(10_000_000));

    // Monotonic copy updates
    record.record_progress(4_000_000, 20_000);
    assert_eq!(record.bytes_remaining(), Some(6_000_000));
    assert_eq!(record.rows_remaining(), Some(30_000));
    assert!(record.estimated_remaining_ms().unwrap() > 0);

    record.record_progress(8_000_000, 40_000);
    assert_eq!(record.bytes_remaining(), Some(2_000_000));
    assert_eq!(record.rows_remaining(), Some(10_000));

    record.record_progress(10_000_000, 50_000);
    assert_eq!(record.bytes_remaining(), Some(0));
    assert_eq!(record.rows_remaining(), Some(0));

    for next_state in [
        MigrationState::DualWriting,
        MigrationState::CatchingUp,
        MigrationState::FencingOld,
        MigrationState::Cutover,
        MigrationState::Verifying,
        MigrationState::GcEligible,
    ] {
        record.apply_transition(next_state).unwrap();
        assert_eq!(record.progress_phase(), next_state.to_string());
        assert_eq!(record.bytes_remaining(), Some(0));
        assert_eq!(record.rows_remaining(), Some(0));
        assert!(record.estimated_remaining_ms().is_some());
    }

    record.apply_transition(MigrationState::Done).unwrap();
    assert_eq!(record.progress_phase(), "done");
    assert_eq!(record.bytes_remaining(), Some(0));
    assert_eq!(record.rows_remaining(), Some(0));
    assert_eq!(record.estimated_remaining_ms(), Some(0));
}
