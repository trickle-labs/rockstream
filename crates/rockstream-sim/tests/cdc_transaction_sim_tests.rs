use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use object_store::memory::InMemory;
use rockstream_connectors::{
    BackfillCursor, BackfillLifecycle, BackfillPhase, OffsetToken, PollDeltaResult,
    SnapshotDeltaFence, SnapshotStream, SourceCheckpoint, SourceCheckpointStore, SourceConnector,
    SourceError, SourceRuntimeCoordinator,
};
use rockstream_sim::{buggify, SimRuntime};
use rockstream_storage::{keys::ShardKeyEncoder, ShardDb, WriteBatch};
use rockstream_types::{connector::PartitionFilter, ids::ConnectorId, timestamp::Epoch};

struct AckSource;

#[async_trait]
impl SourceConnector for AckSource {
    fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
        Ok(Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )])))
    }

    async fn start_snapshot(
        &mut self,
        _: &SnapshotDeltaFence,
        _: Option<OffsetToken>,
        _: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError> {
        Ok(SnapshotStream::new(vec![]))
    }

    async fn poll_delta(
        &mut self,
        _: OffsetToken,
        _: usize,
        _: usize,
        _: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        unreachable!("simulation commits explicit envelopes")
    }

    async fn commit_offset(&mut self, _: Epoch, _: OffsetToken) -> Result<(), SourceError> {
        Ok(())
    }

    async fn pause(&mut self, _: String) -> Result<(), SourceError> {
        Ok(())
    }

    async fn resume(&mut self) -> Result<(), SourceError> {
        Ok(())
    }
}

#[tokio::test]
async fn shared_pgoutput_transaction_simruntime_is_atomic_under_faults() {
    for seed in 0x5220..0x5240 {
        let _runtime = SimRuntime::new(seed);
        rockstream_sim::buggify::buggify_init(seed);
        let _begin = buggify!("pgoutput.begin_acceptance", 0.25);
        let _spill = buggify!("pgoutput.spill_handoff", 0.25);
        let _m3 = buggify!("pgoutput.m3_commit", 0.25);
        let _ack = buggify!("pgoutput.upstream_acknowledgement", 0.25);
        let _disconnect = buggify!("pgoutput.connection_disconnect", 0.25);
        let _relation = buggify!("pgoutput.relation_classification", 0.25);
        let connector_id = ConnectorId(seed);
        let db = Arc::new(
            ShardDb::builder(
                format!("cdc-transaction-sim-{seed}"),
                Arc::new(InMemory::new()),
            )
            .build()
            .await
            .unwrap(),
        );
        let store = SourceCheckpointStore::new(Arc::clone(&db), 0, connector_id);
        let mut coordinator = SourceRuntimeCoordinator::new(
            AckSource,
            connector_id,
            OffsetToken::new(vec![]),
            store.clone(),
        );
        coordinator.recover().await.unwrap();
        let lease = coordinator.acquire_owner("sim-owner").unwrap();
        let offset = OffsetToken::new(b"0/52".to_vec());
        let lifecycle = BackfillLifecycle::new(
            BackfillPhase::Running,
            BackfillCursor::new(
                "orders_and_payments",
                0,
                offset.as_bytes().to_vec(),
                SnapshotDeltaFence::new(OffsetToken::new(b"0/1".to_vec()), offset.clone()),
                1,
            ),
            0,
            2,
            0,
            Some(1),
        );
        let mut batch = WriteBatch::new();
        batch.put(b"source_input/orders/0001", b"1");
        batch.put(b"source_input/payments/0001", b"2");
        batch.put(&ShardKeyEncoder::frontier_key(), &1_u64.to_be_bytes());
        coordinator
            .commit_replayable_epoch(
                &lease,
                1,
                offset.clone(),
                std::slice::from_ref(&lifecycle),
                batch,
            )
            .await
            .unwrap();

        assert_eq!(
            (
                db.scan_prefix(b"source_input/")
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|(key, value)| (key.to_vec(), value.to_vec()))
                    .collect::<Vec<_>>(),
                db.get(&ShardKeyEncoder::frontier_key())
                    .await
                    .unwrap()
                    .map(|value| value.to_vec()),
                store.highest_committed().await.unwrap(),
                store
                    .backfill_lifecycle("orders_and_payments")
                    .await
                    .unwrap(),
                coordinator.committed_epoch(),
                coordinator.committed_offset().clone(),
            ),
            (
                vec![
                    (b"source_input/orders/0001".to_vec(), b"1".to_vec()),
                    (b"source_input/payments/0001".to_vec(), b"2".to_vec()),
                ],
                Some(1_u64.to_be_bytes().to_vec()),
                Some(SourceCheckpoint::prepared(connector_id, 1, offset.clone()).committed()),
                Some(lifecycle),
                1,
                offset,
            ),
            "seed={seed}: an upstream transaction is visible only as one complete epoch"
        );
        rockstream_sim::buggify::buggify_disable();
    }
}
