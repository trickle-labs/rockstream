use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutOptions,
    PutPayload, PutResult,
};
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

#[derive(Debug)]
struct FailOnPutStore {
    inner: InMemory,
    put_count: AtomicUsize,
    fail_at: AtomicUsize,
}

impl FailOnPutStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            put_count: AtomicUsize::new(0),
            fail_at: AtomicUsize::new(usize::MAX),
        }
    }

    fn fail_at(&self, put: usize) {
        self.fail_at.store(put, Ordering::SeqCst);
    }
}

impl std::fmt::Display for FailOnPutStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "FailOnPutStore")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for FailOnPutStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        bytes: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let put = self.put_count.fetch_add(1, Ordering::SeqCst) + 1;
        if put == self.fail_at.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "FailOnPutStore",
                source: "simulated storage failure".into(),
            });
        }
        self.inner.put_opts(location, bytes, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
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
    let persistent = Arc::new(MigrationPersistentStore::new(store.clone()));
    let coordinator = MigrationCoordinator::new().with_migration_store(persistent.clone());
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
    assert_eq!(record.donor_frontier, Some(42));
    assert_eq!(record.recipient_frontier, Some(42));
    assert_eq!(record.lag, Some(0));
    assert_eq!(record.state, MigrationState::Copying);
    assert!(record.donor_checkpoints.contains_key(&ShardId(1)));
    assert_eq!(
        manifest.shards[&ShardId(1)].shard_checkpoint_id,
        record.donor_checkpoints[&ShardId(1)]
    );
    let persisted = persistent.load(&record.migration_id).await.unwrap();
    assert_eq!(persisted.state, MigrationState::Copying);
    assert_eq!(persisted.donor_checkpoints, record.donor_checkpoints);
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
        Some(scan_bucket(&donor.db, 7).await.len() as u64)
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
    batch.put(&make_key(99, "not-migrating"), b"value");
    donor.db.write_batch(batch).await.unwrap();
    donor.db.flush().await.unwrap();

    let mut record = make_record();
    record
        .apply_transition(MigrationState::Snapshotting)
        .unwrap();
    record.apply_transition(MigrationState::Copying).unwrap();
    let source_rows = scan_bucket(&donor.db, 7).await.len();
    let stats = MigrationCoordinator::new()
        .copy_bounded_chunks(&mut record, &[donor], &recipient)
        .await
        .unwrap();

    assert_eq!(stats.copied_rows, source_rows as u64);
    assert_eq!(stats.chunks, 2);
    assert!(stats.max_chunk_rows <= 256);
    assert!(stats.max_chunk_bytes <= 1024 * 1024);
    assert_eq!(scan_bucket(&recipient.db, 7).await.len(), 300);
    assert_eq!(scan_bucket(&recipient.db, 99).await, Vec::new());
}

#[tokio::test]
async fn copy_write_failure_requires_a_durable_intent_and_replays_exactly_once() {
    let donor = make_shard(1, "migration/replay-donor", Arc::new(InMemory::new()), 42).await;
    let recipient = make_shard(
        2,
        "migration/replay-recipient",
        Arc::new(InMemory::new()),
        42,
    )
    .await;
    donor.db.put(&make_key(7, "once"), b"value").await.unwrap();
    donor.db.flush().await.unwrap();

    let failing_store = Arc::new(FailOnPutStore::new());
    let persistent = Arc::new(MigrationPersistentStore::new(failing_store.clone()));
    let coordinator = MigrationCoordinator::new().with_migration_store(persistent.clone());
    let mut record = make_record();
    record
        .apply_transition(MigrationState::Snapshotting)
        .unwrap();
    record.apply_transition(MigrationState::Copying).unwrap();
    persistent.save(&record).await.unwrap();

    let before_intent_failure = record.clone();
    failing_store.fail_at(2);
    assert!(matches!(
        coordinator
            .copy_bounded_chunks(&mut record, std::slice::from_ref(&donor), &recipient)
            .await,
        Err(rockstream_control::MigrationError::Storage(_))
    ));
    assert_eq!(record, before_intent_failure);
    assert_eq!(scan_bucket(&recipient.db, 7).await, Vec::new());

    failing_store.fail_at(4);
    assert!(matches!(
        coordinator
            .copy_bounded_chunks(&mut record, std::slice::from_ref(&donor), &recipient)
            .await,
        Err(rockstream_control::MigrationError::Storage(_))
    ));
    assert_eq!(
        record.copy_intents,
        std::collections::BTreeMap::from([(ShardId(1), 0)])
    );
    assert_eq!(record.copy_cursors, std::collections::BTreeMap::new());
    assert_eq!(
        scan_bucket(&recipient.db, 7).await,
        vec![(make_key(7, "once"), b"value".to_vec())]
    );

    failing_store.fail_at(usize::MAX);
    assert_eq!(
        coordinator
            .copy_bounded_chunks(&mut record, std::slice::from_ref(&donor), &recipient)
            .await
            .unwrap(),
        rockstream_control::MigrationCopyStats {
            chunks: 1,
            copied_rows: 1,
            copied_bytes: 18,
            max_chunk_rows: 1,
            max_chunk_bytes: 18,
        }
    );
    assert_eq!(
        record.copy_cursors,
        std::collections::BTreeMap::from([(ShardId(1), 1)])
    );
    assert_eq!(record.copy_intents, std::collections::BTreeMap::new());
    assert_eq!(record.copied_rows, Some(1));
    assert_eq!(record.copied_bytes, Some(18));
    assert_eq!(record.progress().estimated_rows, None);
    assert_eq!(
        scan_bucket(&recipient.db, 7).await,
        vec![(make_key(7, "once"), b"value".to_vec())]
    );
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
    donor.db.flush().await.unwrap();
    recipient.db.flush().await.unwrap();

    let mut record = make_record();
    step_to_cutover(&mut record);
    let err = MigrationCoordinator::new()
        .verify_or_rollback(&mut record, &donor, &recipient, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("RS-5034"));
    assert_eq!(record.state, MigrationState::DualWriting);
    assert_eq!(record.verification_progress, Some(0));
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
    assert_eq!(record.verification_progress, Some(100));
    tracker.observe("reader-a", 41).unwrap();
    tracker.observe("gateway-a", 42).unwrap();

    assert!(!MigrationCoordinator::new()
        .maybe_enter_gc_eligible(&mut record, &tracker, None)
        .await
        .unwrap());
    assert_eq!(record.state, MigrationState::Verifying);

    tracker.observe("reader-a", 42).unwrap();
    assert!(MigrationCoordinator::new()
        .maybe_enter_gc_eligible(&mut record, &tracker, None)
        .await
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
    assert!(record.cleanup_intent);
    assert_eq!(stats.deleted_keys, 1);
    assert!(scan_bucket(&donor.db, 7).await.is_empty());
    assert_eq!(scan_bucket(&donor.db, 99).await.len(), 1);
    assert_eq!(
        persistent.load(&record.migration_id).await,
        Err(rockstream_control::MigrationLoadError::Missing)
    );
    assert_eq!(
        persistent
            .load_history(&record.migration_id)
            .await
            .unwrap()
            .state,
        MigrationState::Done
    );
    assert!(
        persistent
            .load_history(&record.migration_id)
            .await
            .unwrap()
            .cleanup_intent
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
        .await
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
        .await
        .unwrap());
}

#[tokio::test]
async fn cutover_requires_a_committed_frontier() {
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
        .await
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
        .await
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
    assert_eq!(record.estimated_remaining_ms(), None);

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
    assert_eq!(record.estimated_remaining_ms(), None);

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
        assert_eq!(record.estimated_remaining_ms(), None);
    }

    record.apply_transition(MigrationState::Done).unwrap();
    assert_eq!(record.progress_phase(), "done");
    assert_eq!(record.bytes_remaining(), Some(0));
    assert_eq!(record.rows_remaining(), Some(0));
    assert_eq!(record.estimated_remaining_ms(), Some(0));
}
