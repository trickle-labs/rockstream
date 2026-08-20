use rockstream_types::data_plane::{RuntimeExchangeMessage, RuntimeRow};
use rockstream_types::ids::{LeaseToken, OperatorId, ShardId, WorkerId, WorkloadId};
use rockstream_types::metrics::{
    self, R1ExecutionCounters, R1ExecutionKey, R1ExecutionStrategy, R1WorkerActivity,
};

#[test]
fn encoded_exchange_counter_equals_fixture_serialization() {
    metrics::reset_all();
    let frame = RuntimeExchangeMessage {
        version: 1,
        request_id: "request-9".into(),
        workload_id: WorkloadId(7),
        shard_id: ShardId(2),
        epoch: 12,
        operator_id: OperatorId(3),
        lease_token: LeaseToken(13),
        source: "orders".into(),
        rows: vec![RuntimeRow {
            values_tsv: "42".into(),
            weight: -1,
        }],
    };
    let encoded = serde_json::to_vec(&frame).unwrap();
    let bytes = frame
        .record_encoded_exchange(WorkerId(5), R1ExecutionStrategy::Classic)
        .unwrap();

    assert_eq!(bytes, encoded.len() as u64);
    assert_eq!(
        metrics::r1_execution_snapshot(),
        vec![(
            R1ExecutionKey {
                worker_id: WorkerId(5),
                workload_id: WorkloadId(7),
                shard_id: ShardId(2),
                operator_id: OperatorId(3),
                strategy: R1ExecutionStrategy::Classic,
            },
            R1ExecutionCounters {
                encoded_exchange_bytes: encoded.len() as u64,
                ..Default::default()
            },
        )]
    );
    assert_eq!(
        metrics::r1_worker_snapshot(),
        vec![(
            WorkerId(5),
            R1WorkerActivity {
                exchange_bytes: encoded.len() as u64,
                ..Default::default()
            },
        )]
    );
}
