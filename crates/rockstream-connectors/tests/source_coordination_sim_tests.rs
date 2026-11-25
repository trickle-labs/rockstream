use async_trait::async_trait;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use object_store::memory::InMemory;
use rockstream_connectors::{
    BackfillCursor, BackfillLifecycle, BackfillPhase, OffsetToken, PollDeltaResult,
    SnapshotDeltaFence, SnapshotStream, SourceCheckpoint, SourceCheckpointStore, SourceConnector,
    SourceError, SourceRuntimeCoordinator,
};
use rockstream_storage::{
    keys::{ShardKeyEncoder, ShardPrefix},
    ShardDb, WriteBatch,
};
use rockstream_types::connector::PartitionFilter;
use rockstream_types::ids::ConnectorId;
use rockstream_types::timestamp::Epoch;

const SEED: u64 = 0x5115_0003;

fn m3_batch(epoch: u64) -> WriteBatch {
    let mut batch = WriteBatch::new();
    batch.put(&[ShardPrefix::OpState.as_byte(), 1], b"state");
    batch.put(&[ShardPrefix::ViewOutput.as_byte(), 1], b"output");
    batch.put(&ShardKeyEncoder::frontier_key(), &epoch.to_be_bytes());
    batch
}

#[derive(Default)]
struct RecordingSource {
    /// Complete ACK transcript, including failed broker attempts.
    acknowledgements: Vec<(Epoch, OffsetToken)>,
    poll_calls: usize,
    pause_calls: Vec<String>,
    resume_calls: usize,
    fail_next_ack: Arc<AtomicBool>,
}

#[async_trait]
impl SourceConnector for RecordingSource {
    fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
        Ok(Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Int64,
            false,
        )])))
    }

    async fn start_snapshot(
        &mut self,
        _fence: &SnapshotDeltaFence,
        _after: Option<OffsetToken>,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError> {
        Ok(SnapshotStream::new(vec![]))
    }

    async fn poll_delta(
        &mut self,
        after: OffsetToken,
        _max_bytes: usize,
        _credits_available: usize,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        self.poll_calls += 1;
        Ok(PollDeltaResult {
            batches: vec![],
            new_offset: after,
            watermark: None,
        })
    }

    async fn commit_offset(
        &mut self,
        epoch: Epoch,
        offset: OffsetToken,
    ) -> Result<(), SourceError> {
        self.acknowledgements.push((epoch, offset));
        if self.fail_next_ack.swap(false, Ordering::SeqCst) {
            return Err(SourceError::CommitOffsetFailed {
                epoch,
                reason: "injected transient Kafka broker ACK failure".to_string(),
            });
        }
        Ok(())
    }

    async fn pause(&mut self, reason: String) -> Result<(), SourceError> {
        self.pause_calls.push(reason);
        Ok(())
    }

    async fn resume(&mut self) -> Result<(), SourceError> {
        self.resume_calls += 1;
        Ok(())
    }
}

async fn runtime(connector_id: ConnectorId) -> SourceRuntimeCoordinator<RecordingSource> {
    let db = Arc::new(
        ShardDb::builder("source-coordination", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap(),
    );
    SourceRuntimeCoordinator::new(
        RecordingSource::default(),
        connector_id,
        OffsetToken::new(vec![]),
        SourceCheckpointStore::new(db, 0, connector_id),
    )
}

#[tokio::test]
async fn seeded_competing_owners_commit_one_fenced_token_per_epoch() {
    rockstream_sim::buggify::buggify_init(SEED);
    let _owner_acquire_fault = rockstream_sim::buggify!("source.owner_acquire", 1.0);
    let connector_id = ConnectorId(5115);
    let mut coordinator = runtime(connector_id).await;
    coordinator.recover().await.unwrap();
    let stale_owner = coordinator.acquire_owner("owner-a").unwrap();
    let current_owner = coordinator.acquire_owner("owner-b").unwrap();

    let stale = coordinator
        .commit_epoch(
            &stale_owner,
            1,
            OffsetToken::new(b"offset-01".to_vec()),
            m3_batch(1),
        )
        .await
        .unwrap_err();
    coordinator
        .commit_epoch(
            &current_owner,
            1,
            OffsetToken::new(b"offset-01".to_vec()),
            m3_batch(1),
        )
        .await
        .unwrap();

    assert_eq!(
        (stale.to_string(), coordinator.into_inner().acknowledgements),
        (
            "RS-4001: source I/O error: RS-4013: source owner lease is fenced or inactive; next steps: recover the checkpoint and acquire a new owner lease".to_string(),
            vec![(1, OffsetToken::new(b"offset-01".to_vec()))],
        )
    );
    rockstream_sim::buggify::buggify_disable();
}

#[tokio::test]
async fn backfill_fence_simruntime_restarts_preserve_exactly_once() {
    for seed in [0x52_01_u64, 0x52_02, 0x52_03] {
        rockstream_sim::buggify::buggify_init(seed);
        let _snapshot_capture = rockstream_sim::buggify!("backfill.snapshot_capture", 1.0);
        let _m3_prepare = rockstream_sim::buggify!("backfill.m3_prepare", 1.0);
        let _interleave_drain = rockstream_sim::buggify!("backfill.interleave_drain", 1.0);
        let _publish = rockstream_sim::buggify!("backfill.publish", 1.0);
        let connector_id = ConnectorId(5201);
        let db = Arc::new(
            ShardDb::builder(
                format!("backfill-fence-sim-{seed}"),
                Arc::new(InMemory::new()),
            )
            .build()
            .await
            .unwrap(),
        );
        let store = SourceCheckpointStore::new(db.clone(), 0, connector_id);
        let fence = SnapshotDeltaFence::new(
            OffsetToken::new(b"snapshot-at-2".to_vec()),
            OffsetToken::new(b"live-at-2".to_vec()),
        );
        let records = [b"snapshot-1".as_slice(), b"snapshot-2", b"live-3"];
        let mut coordinator = SourceRuntimeCoordinator::new(
            RecordingSource::default(),
            connector_id,
            OffsetToken::new(vec![]),
            store.clone(),
        );
        coordinator.recover().await.unwrap();
        let mut owner = coordinator.acquire_owner("worker-a").unwrap();

        for (index, record) in records.into_iter().enumerate() {
            let epoch = index as u64 + 1;
            let cursor = BackfillCursor::new("orders_mv", 0, record.to_vec(), fence.clone(), epoch);
            let phase = if epoch == 1 {
                BackfillPhase::Snapshotting
            } else if epoch == 2 {
                BackfillPhase::CatchingUp
            } else {
                BackfillPhase::Running
            };
            let lifecycle = BackfillLifecycle::new(
                phase,
                cursor,
                (records.len() - index - 1) as u64,
                records.len() as u64,
                if epoch == 3 { record.len() as u64 } else { 0 },
                (epoch == 3).then_some(epoch),
            );
            let mut batch = WriteBatch::new();
            batch.put(&[ShardPrefix::OpState.as_byte(), 1], b"state");
            batch.put(format!("view_output/orders_mv/{epoch}").as_bytes(), record);
            batch.put(&ShardKeyEncoder::frontier_key(), &epoch.to_be_bytes());
            coordinator
                .commit_backfill_epoch(
                    &owner,
                    epoch,
                    OffsetToken::new(format!("offset-{epoch}").into_bytes()),
                    lifecycle,
                    batch,
                )
                .await
                .unwrap();

            if epoch == 1 || epoch == 2 {
                let mut recovered = SourceRuntimeCoordinator::new(
                    RecordingSource::default(),
                    connector_id,
                    OffsetToken::new(vec![]),
                    store.clone(),
                );
                assert_eq!(
                    recovered.recover().await.unwrap(),
                    Some(
                        SourceCheckpoint::prepared(
                            connector_id,
                            epoch,
                            OffsetToken::new(format!("offset-{epoch}").into_bytes()),
                        )
                        .committed()
                    )
                );
                owner = recovered.acquire_owner("worker-b").unwrap();
                // Recovery fences input until the exact durable token is
                // acknowledged, even if the old owner already ACKed it.
                recovered.acknowledge_recovered(&owner).await.unwrap();
                coordinator = recovered;
            }
        }

        let output = db
            .scan_prefix(b"view_output/orders_mv/")
            .await
            .unwrap()
            .into_iter()
            .map(|(_, value)| value.to_vec())
            .collect::<Vec<_>>();
        assert_eq!(
            (
                output,
                store
                    .backfill_lifecycle("orders_mv")
                    .await
                    .unwrap(),
                db.get(&ShardKeyEncoder::frontier_key())
                    .await
                    .unwrap()
                    .map(|bytes| bytes.to_vec()),
            ),
            (
                records.into_iter().map(Vec::from).collect::<Vec<_>>(),
                Some(BackfillLifecycle::new(
                    BackfillPhase::Running,
                    BackfillCursor::new(
                        "orders_mv",
                        0,
                        b"live-3".to_vec(),
                        fence,
                        3,
                    ),
                    0,
                    3,
                    b"live-3".len() as u64,
                    Some(3),
                )),
                Some(3_u64.to_be_bytes().to_vec()),
            ),
            "seed={seed}: a committed cursor must resume without replaying snapshot or delta output"
        );
        rockstream_sim::buggify::buggify_disable();
    }
}

#[tokio::test]
async fn seeded_owner_crash_after_prepare_retries_from_last_committed_token() {
    rockstream_sim::buggify::buggify_init(SEED + 1);
    let _post_prepare_fault = rockstream_sim::buggify!("source.post_prepare", 1.0);
    let connector_id = ConnectorId(5116);
    let db = Arc::new(
        ShardDb::builder("source-coordination-prepared", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap(),
    );
    let store = SourceCheckpointStore::new(db, 0, connector_id);
    store
        .prepare(&SourceCheckpoint::prepared(
            connector_id,
            1,
            OffsetToken::new(b"offset-01".to_vec()),
        ))
        .await
        .unwrap();
    let mut coordinator = SourceRuntimeCoordinator::new(
        RecordingSource::default(),
        connector_id,
        OffsetToken::new(vec![]),
        store,
    );
    coordinator.recover().await.unwrap();
    let owner = coordinator.acquire_owner("owner-a").unwrap();
    coordinator
        .commit_epoch(
            &owner,
            1,
            OffsetToken::new(b"offset-01".to_vec()),
            m3_batch(1),
        )
        .await
        .unwrap();

    assert_eq!(
        (
            coordinator.committed_offset().clone(),
            coordinator.into_inner().acknowledgements
        ),
        (
            OffsetToken::new(b"offset-01".to_vec()),
            vec![(1, OffsetToken::new(b"offset-01".to_vec()))],
        )
    );
    rockstream_sim::buggify::buggify_disable();
}

#[tokio::test]
async fn seeded_owner_crash_after_m3_commit_acks_once_after_recovery() {
    rockstream_sim::buggify::buggify_init(SEED + 2);
    let _post_m3_commit_fault = rockstream_sim::buggify!("source.post_m3_commit", 1.0);
    let connector_id = ConnectorId(5117);
    let db = Arc::new(
        ShardDb::builder("source-coordination-recovery", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap(),
    );
    let store = SourceCheckpointStore::new(db, 0, connector_id);
    let prepared =
        SourceCheckpoint::prepared(connector_id, 1, OffsetToken::new(b"offset-01".to_vec()));
    store.prepare(&prepared).await.unwrap();
    let mut m3_input = m3_batch(1);
    store.append_committed(&mut m3_input, &prepared).unwrap();
    store.commit_m3(m3_input).await.unwrap();

    let mut coordinator = SourceRuntimeCoordinator::new(
        RecordingSource::default(),
        connector_id,
        OffsetToken::new(vec![]),
        store,
    );
    coordinator.recover().await.unwrap();
    let owner = coordinator.acquire_owner("owner-b").unwrap();
    coordinator.acknowledge_recovered(&owner).await.unwrap();

    assert_eq!(
        coordinator.into_inner().acknowledgements,
        vec![(1, OffsetToken::new(b"offset-01".to_vec()))]
    );
    rockstream_sim::buggify::buggify_disable();
}

#[tokio::test]
async fn seeded_webhook_retry_after_owner_failover_is_exactly_once() {
    rockstream_sim::buggify::buggify_init(SEED + 3);
    let _pre_ack_fault = rockstream_sim::buggify!("source.pre_ack", 1.0);
    let connector_id = ConnectorId(5123);
    let mut coordinator = runtime(connector_id).await;
    coordinator.recover().await.unwrap();
    let first_owner = coordinator.acquire_owner("owner-a").unwrap();
    coordinator
        .commit_epoch(
            &first_owner,
            1,
            OffsetToken::new(b"delivery-01".to_vec()),
            m3_batch(1),
        )
        .await
        .unwrap();
    let second_owner = coordinator.acquire_owner("owner-b").unwrap();
    let retry = coordinator
        .commit_epoch(
            &second_owner,
            1,
            OffsetToken::new(b"delivery-01".to_vec()),
            m3_batch(1),
        )
        .await
        .unwrap_err();

    assert_eq!(
        (retry.to_string(), coordinator.into_inner().acknowledgements),
        (
            "RS-4001: source I/O error: RS-4015: source epoch 1 is not the next fenced epoch 2; next steps: recover the committed checkpoint and retry".to_string(),
            vec![(1, OffsetToken::new(b"delivery-01".to_vec()))],
        )
    );
    rockstream_sim::buggify::buggify_disable();
}

#[tokio::test]
async fn seeded_lifecycle_race_never_revives_dropped_or_paused_source() {
    rockstream_sim::buggify::buggify_init(SEED + 4);
    let _cleanup_fence_fault = rockstream_sim::buggify!("source.cleanup_fence", 1.0);
    let connector_id = ConnectorId(5124);
    let mut coordinator = runtime(connector_id).await;
    coordinator.recover().await.unwrap();
    let owner = coordinator.acquire_owner("owner-a").unwrap();
    coordinator.pause("paused by operator").await.unwrap();
    let paused_commit = coordinator
        .commit_epoch(
            &owner,
            1,
            OffsetToken::new(b"offset-01".to_vec()),
            m3_batch(1),
        )
        .await
        .unwrap_err();
    coordinator.resume().await.unwrap();
    let owner = coordinator.acquire_owner("owner-b").unwrap();
    coordinator
        .commit_epoch(
            &owner,
            1,
            OffsetToken::new(b"offset-01".to_vec()),
            m3_batch(1),
        )
        .await
        .unwrap();
    let removed = coordinator.drop_source().await.unwrap();
    let dropped_commit = coordinator
        .commit_epoch(
            &owner,
            2,
            OffsetToken::new(b"offset-02".to_vec()),
            m3_batch(2),
        )
        .await
        .unwrap_err();

    assert_eq!(
        (
            paused_commit.to_string(),
            removed,
            dropped_commit.to_string(),
            coordinator.blocked_reason().map(str::to_string),
        ),
        (
            "RS-4001: source I/O error: RS-4013: source owner lease is fenced or inactive; next steps: recover the checkpoint and acquire a new owner lease".to_string(),
            1,
            "RS-4001: source I/O error: RS-4013: source owner lease is fenced or inactive; next steps: recover the checkpoint and acquire a new owner lease".to_string(),
            Some("source dropped".to_string()),
        )
    );
    rockstream_sim::buggify::buggify_disable();
}

#[tokio::test]
async fn recovery_uses_only_committed_checkpoint_and_fences_replaced_owner() {
    let connector_id = ConnectorId(5125);
    let db = Arc::new(
        ShardDb::builder(
            "source-coordination-recovery-owner",
            Arc::new(InMemory::new()),
        )
        .build()
        .await
        .unwrap(),
    );
    let store = SourceCheckpointStore::new(db, 0, connector_id);
    store
        .prepare(&SourceCheckpoint::prepared(
            connector_id,
            9,
            OffsetToken::new(b"prepared-only".to_vec()),
        ))
        .await
        .unwrap();
    let committed =
        SourceCheckpoint::prepared(connector_id, 2, OffsetToken::new(b"committed-02".to_vec()));
    store.prepare(&committed).await.unwrap();
    let mut batch = m3_batch(2);
    store.append_committed(&mut batch, &committed).unwrap();
    store.commit_m3(batch).await.unwrap();

    let mut coordinator = SourceRuntimeCoordinator::new(
        RecordingSource::default(),
        connector_id,
        OffsetToken::new(vec![]),
        store,
    );
    let before_recovery = coordinator.acquire_owner("owner-a").unwrap_err();
    let recovered = coordinator.recover().await.unwrap();
    let replaced_owner = coordinator.acquire_owner("owner-a").unwrap();
    let owner = coordinator.acquire_owner("owner-b").unwrap();
    let stale = coordinator
        .acknowledge_recovered(&replaced_owner)
        .await
        .unwrap_err();
    coordinator.acknowledge_recovered(&owner).await.unwrap();

    assert_eq!(
        (
            before_recovery.to_string(),
            recovered,
            replaced_owner,
            owner,
            stale.to_string(),
            coordinator.committed_offset().clone(),
            coordinator.into_inner().acknowledgements,
        ),
        (
            "RS-4001: source I/O error: RS-4012: source owner cannot become active before checkpoint recovery; next steps: run recovery and retry owner registration".to_string(),
            Some(committed.committed()),
            rockstream_connectors::SourceOwnerLease {
                owner_id: "owner-a".to_string(),
                fence_token: 1,
            },
            rockstream_connectors::SourceOwnerLease {
                owner_id: "owner-b".to_string(),
                fence_token: 2,
            },
            "RS-4001: source I/O error: RS-4013: source owner lease is fenced or inactive; next steps: recover the checkpoint and acquire a new owner lease".to_string(),
            OffsetToken::new(b"committed-02".to_vec()),
            vec![(2, OffsetToken::new(b"committed-02".to_vec()))],
        )
    );
}

#[tokio::test]
async fn globally_allocated_epoch_gap_commits_exact_runtime_state() {
    let mut coordinator = runtime(ConnectorId(5126)).await;
    coordinator.recover().await.unwrap();
    let owner = coordinator.acquire_owner("owner-a").unwrap();
    coordinator
        .commit_epoch(
            &owner,
            2,
            OffsetToken::new(b"offset-02".to_vec()),
            m3_batch(2),
        )
        .await
        .unwrap();

    assert_eq!(
        (
            coordinator.committed_epoch(),
            coordinator.committed_offset().clone(),
            coordinator.blocked_reason().map(str::to_string),
            coordinator.metrics().await.unwrap(),
            coordinator.into_inner().acknowledgements,
        ),
        (
            2,
            OffsetToken::new(b"offset-02".to_vec()),
            None,
            rockstream_connectors::SourceRuntimeMetrics {
                source_runtime_in_flight_epochs: 0,
                source_checkpoint_history_entries: 1,
                source_cleanup_scan_pages: 0,
            },
            vec![(2, OffsetToken::new(b"offset-02".to_vec()))],
        )
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replayable_commit_writes_one_visibility_batch_then_acknowledges_exact_lsn() {
    let connector_id = ConnectorId(5127);
    let db = Arc::new(
        ShardDb::builder("source-replayable", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap(),
    );
    let store = SourceCheckpointStore::new(Arc::clone(&db), 0, connector_id);
    let mut coordinator = SourceRuntimeCoordinator::new(
        RecordingSource::default(),
        connector_id,
        OffsetToken::new(vec![]),
        store.clone(),
    );
    coordinator.recover().await.unwrap();
    let owner = coordinator.acquire_owner("owner-pgoutput").unwrap();
    let offset = OffsetToken::new(b"0/52".to_vec());
    let lifecycle = BackfillLifecycle::new(
        BackfillPhase::Running,
        BackfillCursor::new(
            "view_a",
            0,
            offset.as_bytes().to_vec(),
            SnapshotDeltaFence::new(OffsetToken::new(b"0/1".to_vec()), offset.clone()),
            7,
        ),
        0,
        3,
        0,
        Some(7),
    );
    let mut batch = m3_batch(7);
    batch.put(b"visible/base-row", b"row-1");
    coordinator
        .commit_replayable_epoch(
            &owner,
            7,
            offset.clone(),
            std::slice::from_ref(&lifecycle),
            batch,
        )
        .await
        .unwrap();

    assert_eq!(
        (
            db.get(b"visible/base-row").await.unwrap(),
            coordinator.backfill_lifecycle("view_a").await.unwrap(),
            store.highest_committed().await.unwrap(),
            coordinator.metrics().await.unwrap(),
            coordinator.into_inner().acknowledgements,
        ),
        (
            Some(bytes::Bytes::from_static(b"row-1")),
            Some(lifecycle),
            Some(SourceCheckpoint::prepared(connector_id, 7, offset.clone()).committed()),
            rockstream_connectors::SourceRuntimeMetrics {
                source_runtime_in_flight_epochs: 0,
                source_checkpoint_history_entries: 1,
                source_cleanup_scan_pages: 0,
            },
            vec![(7, offset)],
        )
    );
}

#[tokio::test]
async fn pending_ack_blocks_poll_until_idle_recovery_acks_exact_durable_checkpoint() {
    let connector_id = ConnectorId(5130);
    let db = Arc::new(
        ShardDb::builder("source-idle-ack-recovery", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap(),
    );
    let store = SourceCheckpointStore::new(Arc::clone(&db), 0, connector_id);
    let fail_next_ack = Arc::new(AtomicBool::new(true));
    let mut coordinator = SourceRuntimeCoordinator::new(
        RecordingSource {
            fail_next_ack: Arc::clone(&fail_next_ack),
            ..Default::default()
        },
        connector_id,
        OffsetToken::new(vec![]),
        store.clone(),
    );
    coordinator.recover().await.unwrap();
    let owner = coordinator.acquire_owner("same-worker").unwrap();
    let durable = OffsetToken::new(b"durable-01".to_vec());

    let error = coordinator
        .commit_epoch(&owner, 1, durable.clone(), m3_batch(1))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("RS-4016"));
    assert_eq!(coordinator.committed_epoch(), 1);
    assert_eq!(coordinator.committed_offset(), &durable);
    assert!(coordinator.has_pending_ack());
    assert!(coordinator.blocked_reason().is_some());
    assert_eq!(
        coordinator
            .metrics()
            .await
            .unwrap()
            .source_runtime_in_flight_epochs,
        1
    );
    // No further read or M3 write is allowed before the broker ACK.
    assert!(coordinator.poll_delta(1024, 1).await.is_err());
    assert!(coordinator
        .commit_epoch(
            &owner,
            2,
            OffsetToken::new(b"unread-02".to_vec()),
            m3_batch(2)
        )
        .await
        .is_err());
    assert_eq!(
        store.highest_committed().await.unwrap(),
        Some(SourceCheckpoint::prepared(connector_id, 1, durable.clone()).committed())
    );

    // The injected fault is one-shot. Recovery must work without any poll.
    assert!(!fail_next_ack.load(Ordering::SeqCst));
    coordinator.acknowledge_recovered(&owner).await.unwrap();
    assert!(!coordinator.has_pending_ack());
    assert_eq!(coordinator.blocked_reason(), None);
    assert_eq!(
        coordinator
            .metrics()
            .await
            .unwrap()
            .source_runtime_in_flight_epochs,
        0
    );
    assert!(coordinator
        .poll_delta(1024, 1)
        .await
        .unwrap()
        .batches
        .is_empty());
    let next = OffsetToken::new(b"healthy-02".to_vec());
    coordinator
        .commit_epoch(&owner, 2, next.clone(), m3_batch(2))
        .await
        .unwrap();
    assert_eq!(
        store.highest_committed().await.unwrap(),
        Some(SourceCheckpoint::prepared(connector_id, 2, next.clone()).committed())
    );
    assert_eq!(
        db.get(&[ShardPrefix::ViewOutput.as_byte(), 1])
            .await
            .unwrap()
            .as_deref(),
        Some(b"output".as_slice()),
    );
    let source = coordinator.into_inner();
    assert_eq!(
        source.acknowledgements,
        vec![(1, durable.clone()), (1, durable), (2, next)]
    );
    assert_eq!(
        source.poll_calls, 1,
        "the blocked poll never reached the connector"
    );
    assert_eq!(source.pause_calls.len(), 1, "async pause must actually run");
    assert_eq!(source.resume_calls, 1, "recovery must resume the source");
}

#[tokio::test]
async fn repeated_backfill_ack_failures_do_not_leak_in_flight_epochs_or_replay_m3() {
    const COUNT: u64 = 70; // More than SOURCE_RUNTIME_MAX_IN_FLIGHT_EPOCHS.
    let connector_id = ConnectorId(5131);
    let db = Arc::new(
        ShardDb::builder("source-repeated-ack-recovery", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap(),
    );
    let store = SourceCheckpointStore::new(Arc::clone(&db), 0, connector_id);
    let fail_next_ack = Arc::new(AtomicBool::new(false));
    let mut coordinator = SourceRuntimeCoordinator::new(
        RecordingSource {
            fail_next_ack: Arc::clone(&fail_next_ack),
            ..Default::default()
        },
        connector_id,
        OffsetToken::new(vec![]),
        store.clone(),
    );
    coordinator.recover().await.unwrap();
    let owner = coordinator
        .acquire_owner("same-worker-over-64-failures")
        .unwrap();
    let fence = SnapshotDeltaFence::new(
        OffsetToken::new(b"snapshot-fence".to_vec()),
        OffsetToken::new(b"live-fence".to_vec()),
    );
    let mut transcript = Vec::new();
    for epoch in 1..=COUNT {
        let token = OffsetToken::new(format!("offset-{epoch:03}").into_bytes());
        let lifecycle = BackfillLifecycle::new(
            BackfillPhase::Running,
            BackfillCursor::new(
                "repeat_view",
                0,
                token.as_bytes().to_vec(),
                fence.clone(),
                epoch,
            ),
            0,
            COUNT,
            0,
            Some(epoch),
        );
        let mut batch = m3_batch(epoch);
        batch.put(
            format!("view_output/repeat_view/{epoch}").as_bytes(),
            b"once",
        );
        fail_next_ack.store(true, Ordering::SeqCst);
        assert!(coordinator
            .commit_backfill_epoch(&owner, epoch, token.clone(), lifecycle, batch)
            .await
            .unwrap_err()
            .to_string()
            .contains("RS-4016"));
        assert!(coordinator.has_pending_ack());
        assert_eq!(
            coordinator
                .metrics()
                .await
                .unwrap()
                .source_runtime_in_flight_epochs,
            1,
        );
        assert!(coordinator.poll_delta(1024, 1).await.is_err());
        coordinator.acknowledge_recovered(&owner).await.unwrap();
        assert!(!coordinator.has_pending_ack());
        assert_eq!(coordinator.blocked_reason(), None);
        assert_eq!(
            coordinator
                .metrics()
                .await
                .unwrap()
                .source_runtime_in_flight_epochs,
            0,
        );
        transcript.push((epoch, token.clone())); // first attempt failed
        transcript.push((epoch, token)); // exact checkpoint retried
    }
    // Healthy input still commits normally after a lifetime of transient errors.
    let healthy = OffsetToken::new(b"offset-healthy".to_vec());
    coordinator
        .commit_epoch(&owner, COUNT + 1, healthy.clone(), m3_batch(COUNT + 1))
        .await
        .unwrap();
    transcript.push((COUNT + 1, healthy.clone()));
    assert_eq!(
        store.highest_committed().await.unwrap(),
        Some(SourceCheckpoint::prepared(connector_id, COUNT + 1, healthy).committed())
    );
    let output = db.scan_prefix(b"view_output/repeat_view/").await.unwrap();
    assert_eq!(
        output.len(),
        COUNT as usize,
        "no M3 output can be reapplied by retrying ACK"
    );
    assert!(output.iter().all(|(_, value)| value.as_ref() == b"once"));
    assert_eq!(
        coordinator
            .metrics()
            .await
            .unwrap()
            .source_runtime_in_flight_epochs,
        0
    );
    let source = coordinator.into_inner();
    assert_eq!(
        source.acknowledgements, transcript,
        "complete broker ACK transcript"
    );
    assert_eq!(source.poll_calls, 0, "all blocked polls were fenced");
    assert_eq!(source.pause_calls.len(), COUNT as usize);
    assert_eq!(source.resume_calls, COUNT as usize);
}
