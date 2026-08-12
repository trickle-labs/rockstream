use async_trait::async_trait;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use object_store::memory::InMemory;
use rockstream_connectors::{
    BackfillCursor, BackfillLifecycle, BackfillPhase, OffsetToken, PollDeltaResult,
    SnapshotDeltaFence, SnapshotStream, SourceCheckpoint, SourceCheckpointStore, SourceConnector,
    SourceError, SourceRuntimeCoordinator,
};
use rockstream_storage::{keys::ShardKeyEncoder, ShardDb, WriteBatch};
use rockstream_types::connector::PartitionFilter;
use rockstream_types::ids::ConnectorId;
use rockstream_types::timestamp::Epoch;

const SEED: u64 = 0x5115_0003;

struct RecordingSource {
    acknowledgements: Vec<(Epoch, OffsetToken)>,
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
        _after: OffsetToken,
        _max_bytes: usize,
        _credits_available: usize,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        unreachable!("coordination tests commit explicit source input")
    }

    async fn commit_offset(
        &mut self,
        epoch: Epoch,
        offset: OffsetToken,
    ) -> Result<(), SourceError> {
        self.acknowledgements.push((epoch, offset));
        Ok(())
    }

    async fn pause(&mut self, _reason: String) -> Result<(), SourceError> {
        Ok(())
    }

    async fn resume(&mut self) -> Result<(), SourceError> {
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
        RecordingSource {
            acknowledgements: vec![],
        },
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
            WriteBatch::new(),
        )
        .await
        .unwrap_err();
    coordinator
        .commit_epoch(
            &current_owner,
            1,
            OffsetToken::new(b"offset-01".to_vec()),
            WriteBatch::new(),
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
            RecordingSource {
                acknowledgements: vec![],
            },
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
                    RecordingSource {
                        acknowledgements: vec![],
                    },
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
        RecordingSource {
            acknowledgements: vec![],
        },
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
            WriteBatch::new(),
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
    let mut m3_input = WriteBatch::new();
    store.append_committed(&mut m3_input, &prepared).unwrap();
    store.commit_m3(m3_input).await.unwrap();

    let mut coordinator = SourceRuntimeCoordinator::new(
        RecordingSource {
            acknowledgements: vec![],
        },
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
            WriteBatch::new(),
        )
        .await
        .unwrap();
    let second_owner = coordinator.acquire_owner("owner-b").unwrap();
    let retry = coordinator
        .commit_epoch(
            &second_owner,
            1,
            OffsetToken::new(b"delivery-01".to_vec()),
            WriteBatch::new(),
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
            WriteBatch::new(),
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
            WriteBatch::new(),
        )
        .await
        .unwrap();
    let removed = coordinator.drop_source().await.unwrap();
    let dropped_commit = coordinator
        .commit_epoch(
            &owner,
            2,
            OffsetToken::new(b"offset-02".to_vec()),
            WriteBatch::new(),
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
    let mut batch = WriteBatch::new();
    store.append_committed(&mut batch, &committed).unwrap();
    store.commit_m3(batch).await.unwrap();

    let mut coordinator = SourceRuntimeCoordinator::new(
        RecordingSource {
            acknowledgements: vec![],
        },
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
            WriteBatch::new(),
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

#[tokio::test]
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
        RecordingSource {
            acknowledgements: vec![],
        },
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
    let mut batch = WriteBatch::new();
    batch.put(b"visible/base-row", b"row-1");
    coordinator
        .commit_replayable_epoch(&owner, 7, offset.clone(), &[lifecycle.clone()], batch)
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
