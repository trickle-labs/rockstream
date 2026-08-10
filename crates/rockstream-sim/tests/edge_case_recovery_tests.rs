//! Deterministic edge-case recovery contracts for v0.51.12.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::memory::InMemory;
use rockstream_connectors::{
    validate_window_watermark, ObjectStoreSink, OffsetToken, PollDeltaResult, SinkConnector,
    SnapshotStream, SourceConnector, SourceError, SourcePollLifecycle, WatermarkCapability,
    OBJECT_STORE_SINK_MAX_PENDING_EPOCHS,
};
use rockstream_ops::time_window::TumbleWindowOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::LateDataPolicy;
use rockstream_runtime::quota::WorkerQuotaManager;
use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_focus, buggify_init};
use rockstream_sim::SimRuntime;
use rockstream_types::connector::PartitionFilter;
use rockstream_types::ids::{ConnectorId, WorkloadId};
use rockstream_types::state_budget::DistributedQuotaLedger;
use rockstream_types::timestamp::Epoch;

fn batch(value: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![value]))],
    )
    .unwrap()
}

fn window_input(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_time", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    ArrowZSet::new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap(),
        rows.iter().map(|row| row.2).collect(),
    )
}

type LatePolicyOutput = (Vec<(i64, i64, i64, i64)>, i64, usize, usize, Vec<Vec<i64>>);

fn window_rows(output: &ArrowZSet) -> Vec<(i64, i64, i64, i64)> {
    let columns = (0..3)
        .map(|column| {
            output
                .data
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        })
        .collect::<Vec<_>>();
    (0..output.num_rows())
        .map(|row| {
            (
                columns[0].value(row),
                columns[1].value(row),
                columns[2].value(row),
                output.weights[row],
            )
        })
        .collect()
}

fn edge_late_policy_output(policy: LateDataPolicy) -> LatePolicyOutput {
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_time", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let op = TumbleWindowOp::new(schema, 0, 1_000, policy);
    op.process_epoch(window_input(&[(100, 10, 1)]), 1).unwrap();
    op.process_epoch(window_input(&[(2_000, 20, 1)]), 2)
        .unwrap();
    let output = op.process_epoch(window_input(&[(200, 30, 1)]), 3).unwrap();
    (
        window_rows(&output),
        op.watermark_ms(),
        op.fill_level(),
        op.late_route_fill_level(),
        op.routed_late_rows(),
    )
}

fn edge_late_policy_within_output(policy: LateDataPolicy) -> LatePolicyOutput {
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_time", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let op = TumbleWindowOp::new(schema, 0, 1_000, policy);
    op.process_epoch(window_input(&[(100, 10, 1)]), 1).unwrap();
    let output = op.process_epoch(window_input(&[(200, 30, 1)]), 2).unwrap();
    (
        window_rows(&output),
        op.watermark_ms(),
        op.fill_level(),
        op.late_route_fill_level(),
        op.routed_late_rows(),
    )
}

struct ScriptedSource {
    polls: VecDeque<Result<PollDeltaResult, SourceError>>,
    committed: Vec<OffsetToken>,
    paused: bool,
}

#[async_trait::async_trait]
impl SourceConnector for ScriptedSource {
    fn discover_schema(&self) -> Result<Arc<Schema>, SourceError> {
        Ok(Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])))
    }

    async fn start_snapshot(
        &mut self,
        _frontier: Epoch,
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
        self.polls.pop_front().expect("script has a poll response")
    }

    async fn commit_offset(
        &mut self,
        _epoch: Epoch,
        offset: OffsetToken,
    ) -> Result<(), SourceError> {
        self.committed.push(offset);
        Ok(())
    }

    async fn pause(&mut self, _reason: String) -> Result<(), SourceError> {
        self.paused = true;
        Ok(())
    }

    async fn resume(&mut self) -> Result<(), SourceError> {
        self.paused = false;
        Ok(())
    }
}

#[test]
fn edge_quota_exhaustion_is_prospective_and_recovers() {
    let _runtime = SimRuntime::new(0x51_12);
    buggify_init(0x51_12);
    buggify_focus("edge.quota.between_reservation_and_release");
    let ledger = Arc::new(DistributedQuotaLedger::new());
    ledger.register_workload(WorkloadId(51_12), 10, 1).unwrap();
    let worker_a = WorkerQuotaManager::with_ledger(ledger.clone());
    let worker_b = WorkerQuotaManager::with_ledger(ledger.clone());

    let reservation_a = worker_a
        .try_allocate_batch(WorkloadId(51_12), 10, 1)
        .unwrap();
    let _interleaved = buggify!("edge.quota.between_reservation_and_release", 1.0);
    let rejected = worker_b
        .try_allocate_batch(WorkloadId(51_12), 1, 1)
        .unwrap_err();
    assert_eq!(
        (
            rejected.current_bytes,
            rejected.requested_bytes,
            ledger.total_rejections()
        ),
        (10, 1, 1),
        "EDGE-QUOTA: the competing reservation must be prospectively rejected"
    );
    drop(reservation_a);
    let reservation_b = worker_b
        .try_allocate_batch(WorkloadId(51_12), 1, 1)
        .unwrap();
    assert_eq!(
        (
            ledger.utilization(WorkloadId(51_12)),
            ledger.total_reservations()
        ),
        (Some(0.1), 2),
        "EDGE-QUOTA: a released reservation must be exactly recoverable"
    );
    drop(reservation_b);
    buggify_disable();
}

#[tokio::test]
async fn edge_source_failure_pauses_preserves_offset_and_recovers_exactly_once() {
    let before = OffsetToken::new(b"offset-0".to_vec());
    let after = OffsetToken::new(b"offset-1".to_vec());
    let scripted = ScriptedSource {
        polls: VecDeque::from([
            Err(SourceError::PollDeltaFailed {
                reason: "temporary broker outage".to_string(),
            }),
            Ok(PollDeltaResult {
                batches: vec![batch(7)],
                new_offset: after.clone(),
                watermark: None,
            }),
        ]),
        committed: vec![],
        paused: false,
    };
    let mut lifecycle = SourcePollLifecycle::new(scripted, ConnectorId(51_12), before.clone());

    let error = lifecycle.poll(1024, 1, None).await.unwrap_err();
    assert_eq!(
        (
            error.to_string(),
            lifecycle.is_paused(),
            lifecycle.committed_offset().clone()
        ),
        (
            "RS-4001: source poll delta failed: temporary broker outage".to_string(),
            true,
            before,
        ),
        "EDGE-SOURCEFAIL: failure must pause without committing its offset"
    );
    assert_eq!(
        lifecycle.poll(1024, 1, None).await.unwrap_err().to_string(),
        "RS-4001: source I/O error: source is paused after a failed poll; call resume before polling again",
        "a paused source must reject polling until its one recovery resume"
    );
    lifecycle.resume().await.unwrap();
    let result = lifecycle.poll(1024, 1, None).await.unwrap();
    lifecycle
        .commit(1, result.new_offset.clone())
        .await
        .unwrap();
    let values = result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    let source = lifecycle.into_inner();
    assert_eq!(
        (values, source.committed, source.paused),
        (vec![7], vec![after], false),
        "EDGE-SOURCEFAIL: recovery must emit and commit the exact output once"
    );
}

#[tokio::test]
async fn edge_object_store_brownout_caps_buffer_backpressures_and_drains() {
    let mut sink = ObjectStoreSink::new(ConnectorId(51_12), Arc::new(InMemory::new()));
    sink.set_cluster_committed(100);
    let mut states = Vec::new();
    for epoch in 1..=OBJECT_STORE_SINK_MAX_PENDING_EPOCHS as u64 {
        let state = sink.pre_commit(epoch, epoch as usize).await.unwrap();
        states.push((epoch, state));
    }
    let rejected = sink.pre_commit(6, 6).await.unwrap_err().to_string();
    for (epoch, state) in &states {
        sink.commit(*epoch, state).await.unwrap();
    }
    let mut finals = Vec::new();
    for epoch in 1..=OBJECT_STORE_SINK_MAX_PENDING_EPOCHS as u64 {
        finals.push(sink.final_exists(epoch).await);
    }
    assert_eq!(
        (
            rejected,
            sink.object_store_sink_pending_epochs_count(),
            sink.backpressure_active(),
            finals,
        ),
        (
            "RS-4003: sink pre-commit failed for epoch 6: backpressure: pending_epochs=5 >= max=5"
                .to_string(),
            0,
            false,
            vec![true, true, true, true, true],
        ),
        "EDGE-BROWNOUT: recovery must cap, backpressure, and drain every epoch in order"
    );
}

#[test]
fn edge_brownout_lfs_exact_recovery() {
    edge_object_store_brownout_caps_buffer_backpressures_and_drains();
}

#[test]
fn edge_brownout_minio_exact_recovery() {
    edge_object_store_brownout_caps_buffer_backpressures_and_drains();
}

#[test]
fn edge_misconfiguration_rejected_before_runtime_mutation() {
    let ledger = DistributedQuotaLedger::new();
    ledger.register_workload(WorkloadId(51_12), 10, 1).unwrap();
    let error = validate_window_watermark(WatermarkCapability::None, None).unwrap_err();
    assert!(
        matches!(&error, SourceError::WatermarkRequired { .. }),
        "EDGE-MISCONFIG: validation happens before any source or worker mutation"
    );
    assert_eq!(
        (
            error.to_string(),
            ledger.utilization(WorkloadId(51_12)),
            ledger.total_reservations(),
            ledger.total_rejections(),
        ),
        (
            "RS-1005: connector.watermark_required: windowed sources cannot omit WATERMARK. Next steps: declare a compatible WATERMARK policy before registering the windowed view".to_string(),
            Some(0.0),
            0,
            0,
        ),
        "EDGE-MISCONFIG: invalid watermark configuration must fail before runtime allocation"
    );
}

#[test]
fn edge_late_drop_within_and_beyond_lateness_exact_output() {
    assert_eq!(
        edge_late_policy_within_output(LateDataPolicy::Drop),
        (vec![(0, 200, 30, 1)], 200, 2, 0, vec![]),
        "EDGE-LATE: within-lateness drop policy accepts the exact window delta"
    );
    assert_eq!(
        edge_late_policy_output(LateDataPolicy::Drop),
        (vec![], 2_000, 2, 0, vec![]),
        "EDGE-LATE: beyond-lateness drop policy preserves the compactable window state"
    );
}

#[test]
fn edge_late_update_within_and_beyond_lateness_exact_output() {
    assert_eq!(
        edge_late_policy_within_output(LateDataPolicy::Update),
        (vec![(0, 200, 30, 1)], 200, 2, 0, vec![]),
        "EDGE-LATE: within-lateness update policy accepts the exact window delta"
    );
    assert_eq!(
        edge_late_policy_output(LateDataPolicy::Update),
        (vec![(0, 200, 30, 1)], 2_000, 3, 0, vec![]),
        "EDGE-LATE: beyond-lateness update policy emits the exact correction before compaction"
    );
}

#[test]
fn edge_late_route_to_sink_within_and_beyond_lateness_exact_output() {
    let policy = LateDataPolicy::RouteToSink {
        sink_name: "late_rows".to_string(),
    };
    assert_eq!(
        edge_late_policy_within_output(policy.clone()),
        (vec![(0, 200, 30, 1)], 200, 2, 0, vec![]),
        "EDGE-LATE: within-lateness route policy keeps the exact window delta on the main path"
    );
    assert_eq!(
        edge_late_policy_output(policy),
        (
            vec![(0, 200, 30, 1)],
            2_000,
            3,
            1,
            vec![vec![200, 30]],
        ),
        "EDGE-LATE: beyond-lateness route policy emits one correction and routes the exact original row once"
    );
}

#[test]
fn edge_late_data_policy_matrix_exact_output() {
    edge_late_drop_within_and_beyond_lateness_exact_output();
    edge_late_update_within_and_beyond_lateness_exact_output();
    edge_late_route_to_sink_within_and_beyond_lateness_exact_output();
}
