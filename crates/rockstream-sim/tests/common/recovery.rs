use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    error::KafkaError,
    ClientConfig, Message, Offset, TopicPartitionList,
};
use rockstream_connectors::{SourceConnector, SourceRuntimeCoordinator};
use rockstream_ops::sink::{materialize_view_state, read_view_output, ColumnValue};
use rockstream_runtime::exchange::persistence::committed_frontier;
use rockstream_sim::qualification::worker_failure::WorkerObservation;
use rockstream_storage::ShardDb;
use rockstream_types::ids::{OperatorId, ShardId};
use serde_json::{json, Value};
use std::{
    fs,
    path::Path,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
const ID: u64 = 126;

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

pub fn save(path: &Path, value: &impl serde::Serialize) {
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(value).unwrap()).unwrap();
    fs::rename(temporary, path).unwrap();
}

pub fn typed(values: &[ColumnValue]) -> Vec<Value> {
    values
        .iter()
        .map(|v| match v {
            ColumnValue::Int64(n) => json!({"Int64":n}),
            ColumnValue::Utf8(s) => json!({"Utf8":s}),
            other => panic!("unexpected scenario type {other:?}"),
        })
        .collect()
}

pub async fn observe<S: SourceConnector>(
    db: &ShardDb,
    worker: &rockstream_runtime::WorkerClientHandle,
    source: &SourceRuntimeCoordinator<S>,
) -> WorkerObservation {
    let output = read_view_output(db, OperatorId(ID), 2).await.unwrap();
    let weighted_output = json!(output
        .iter()
        .map(|(epoch, row, values, weight)| json!([epoch, row, typed(values), weight]))
        .collect::<Vec<_>>());
    let relation = json!(materialize_view_state(output, &[])
        .into_values()
        .map(|(values, weight)| json!([typed(&values), weight]))
        .collect::<Vec<_>>());
    let metrics = source.metrics().await.unwrap();
    WorkerObservation {
        observed_at_ms: now_ms(),
        process_id: std::process::id(),
        lease: worker
            .leases()
            .into_iter()
            .find(|l| l.shard_id == ShardId(ID))
            .unwrap(),
        source_epoch: source.committed_epoch(),
        source_offsets: serde_json::from_slice(source.committed_offset().as_bytes()).unwrap(),
        view_frontier: committed_frontier(db).await.unwrap(),
        weighted_output,
        relation,
        in_flight_epochs: metrics.source_runtime_in_flight_epochs,
        checkpoint_history_entries: metrics.source_checkpoint_history_entries,
        sink_progress: serde_json::from_slice(
            &db.get(b"\x06kafka-view/126").await.unwrap().unwrap(),
        )
        .unwrap(),
    }
}

pub fn consume(bootstrap: &str) -> Value {
    consume_topic(bootstrap, "worker-failure-output")
}

pub fn consume_topic(bootstrap: &str, topic: &str) -> Value {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "worker-recovery-oracle")
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .set("enable.partition.eof", "true")
        .create()
        .unwrap();
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(topic, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&assignment).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut records = Vec::new();
    while Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => records.push(json!([
                message.key_view::<str>().unwrap().unwrap(),
                serde_json::from_slice::<Value>(message.payload().unwrap()).unwrap()
            ])),
            Some(Err(KafkaError::PartitionEOF(0))) => return json!(records),
            Some(Err(error)) => panic!("sink consumer: {error}"),
            None => {}
        }
    }
    panic!("sink progress deadline");
}
