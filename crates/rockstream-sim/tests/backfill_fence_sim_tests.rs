//! Seeded crash/restart proof for the source-side backfill M3 coordinator.

#![cfg(feature = "simulation")]

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use object_store::memory::InMemory;
use rockstream_connectors::{
    BackfillCursor, BackfillLifecycle, BackfillPhase, OffsetToken, PollDeltaResult,
    SnapshotDeltaFence, SnapshotStream, SourceCheckpoint, SourceCheckpointStore, SourceConnector,
    SourceError, SourceRuntimeCoordinator,
};
use rockstream_sim::{buggify, buggify::buggify_init, SimRuntime};
use rockstream_storage::{keys::ShardKeyEncoder, ShardDb, WriteBatch};
use rockstream_types::{connector::PartitionFilter, ids::ConnectorId, timestamp::Epoch};

struct RecordingSource;

#[async_trait]
impl SourceConnector for RecordingSource {
    fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
        Ok(Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
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
        Ok(PollDeltaResult {
            batches: vec![],
            new_offset: after,
            watermark: None,
        })
    }

    async fn commit_offset(
        &mut self,
        _epoch: Epoch,
        _offset: OffsetToken,
    ) -> Result<(), SourceError> {
        Ok(())
    }

    async fn pause(&mut self, _reason: String) -> Result<(), SourceError> {
        Ok(())
    }

    async fn resume(&mut self) -> Result<(), SourceError> {
        Ok(())
    }
}

#[tokio::test]
async fn backfill_fence_simruntime_restarts_preserve_exactly_once() {
    for seed in [0x52_01_u64, 0x52_02, 0x52_03] {
        let _runtime = SimRuntime::new(seed);
        buggify_init(seed);
        let _snapshot_capture = buggify!("backfill.snapshot_capture", 1.0);
        let _m3_prepare = buggify!("backfill.m3_prepare", 1.0);
        let _interleave_drain = buggify!("backfill.interleave_drain", 1.0);
        let _publish = buggify!("backfill.publish", 1.0);
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
            RecordingSource,
            connector_id,
            OffsetToken::new(vec![]),
            store.clone(),
        );
        coordinator.recover().await.unwrap();
        let mut owner = coordinator.acquire_owner("worker-a").unwrap();

        for (index, record) in records.into_iter().enumerate() {
            let epoch = index as u64 + 1;
            let phase = match epoch {
                1 => BackfillPhase::Snapshotting,
                2 => BackfillPhase::CatchingUp,
                _ => BackfillPhase::Running,
            };
            let lifecycle = BackfillLifecycle::new(
                phase,
                BackfillCursor::new("orders_mv", 0, record.to_vec(), fence.clone(), epoch),
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

            if epoch < 3 {
                let mut recovered = SourceRuntimeCoordinator::new(
                    RecordingSource,
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

        assert_eq!(
            (
                db.scan_prefix(b"view_output/orders_mv/")
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|(_, value)| value.to_vec())
                    .collect::<Vec<_>>(),
                store.backfill_lifecycle("orders_mv").await.unwrap(),
                db.get(&ShardKeyEncoder::frontier_key())
                    .await
                    .unwrap()
                    .map(|bytes| bytes.to_vec()),
            ),
            (
                records.into_iter().map(Vec::from).collect::<Vec<_>>(),
                Some(BackfillLifecycle::new(
                    BackfillPhase::Running,
                    BackfillCursor::new("orders_mv", 0, b"live-3".to_vec(), fence, 3),
                    0,
                    3,
                    b"live-3".len() as u64,
                    Some(3),
                )),
                Some(3_u64.to_be_bytes().to_vec()),
            ),
            "seed={seed}: recovery must resume from committed cursors without gaps or overlap"
        );
        rockstream_sim::buggify::buggify_disable();
    }
}
