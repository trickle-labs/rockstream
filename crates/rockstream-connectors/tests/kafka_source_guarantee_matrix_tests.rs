mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, Field, Schema};
use rdkafka::admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use rockstream_connectors::{KafkaSource, OffsetToken, PollDeltaResult, SourceConnector};
use rockstream_types::arrow_batch::split_weight_column;
use rockstream_types::ids::ConnectorId;

use common::ConnectorFixture;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

async fn topic(
    fixture: &ConnectorFixture,
    label: &str,
    partitions: i32,
) -> (String, FutureProducer) {
    let name = format!("source_{label}");
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &fixture.kafka_bootstrap)
        .create()
        .unwrap();
    let created = admin
        .create_topics(
            &[NewTopic::new(&name, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(created, vec![Ok(name.clone())]);
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &fixture.kafka_bootstrap)
        .create()
        .unwrap();
    (name, producer)
}

async fn produce(producer: &FutureProducer, topic: &str, partition: i32, value: i64) {
    let payload = serde_json::json!({
        "timestamp": value,
        "values": [value],
        "weight": 1
    })
    .to_string();
    producer
        .send(
            FutureRecord::to(topic)
                .partition(partition)
                .payload(&payload)
                .key(&format!("key-{value}")),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
}

async fn poll_until(source: &mut KafkaSource, after: OffsetToken) -> PollDeltaResult {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let result = source
            .poll_delta(after.clone(), 4096, 1, None)
            .await
            .unwrap();
        if !result.batches.is_empty() {
            return result;
        }
        assert!(
            Instant::now() < deadline,
            "Kafka source did not receive a record"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn values(result: &PollDeltaResult) -> Vec<(i64, i64)> {
    result
        .batches
        .iter()
        .flat_map(|batch| {
            let (data, weights) = split_weight_column(batch).unwrap();
            let values = data
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .values();
            values.iter().copied().zip(weights).collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_source_mid_epoch_rebalance_recovers_exact_transcript() {
    let fixture = common::connector_fixture("rebalance").await;
    let (topic, producer) = topic(&fixture, "rebalance", 2).await;
    produce(&producer, &topic, 0, 10).await;
    produce(&producer, &topic, 1, 20).await;
    let mut source = KafkaSource::connect(
        ConnectorId(5_301),
        schema(),
        &fixture.kafka_bootstrap,
        &topic,
        "source-rebalance",
    )
    .unwrap();
    let first = poll_until(&mut source, OffsetToken::new(vec![])).await;
    let second = poll_until(&mut source, first.new_offset.clone()).await;
    let mut transcript = values(&first);
    transcript.extend(values(&second));
    transcript.sort_unstable();
    assert_eq!(transcript, vec![(10, 1), (20, 1)]);
    source.commit_offset(1, second.new_offset).await.unwrap();
    assert_eq!(source.assigned_partition_count(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_source_partition_expansion_has_exact_transcript() {
    let fixture = common::connector_fixture("partition_expansion").await;
    let (topic, producer) = topic(&fixture, "partition_expansion", 1).await;
    produce(&producer, &topic, 0, 11).await;
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &fixture.kafka_bootstrap)
        .create()
        .unwrap();
    assert_eq!(
        admin
            .create_partitions(&[NewPartitions::new(&topic, 2)], &AdminOptions::new(),)
            .await
            .unwrap(),
        vec![Ok(topic.clone())]
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    let expanded_producer: rdkafka::producer::FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &fixture.kafka_bootstrap)
        .create()
        .unwrap();
    produce(&expanded_producer, &topic, 1, 22).await;
    let mut source = KafkaSource::connect(
        ConnectorId(5_302),
        schema(),
        &fixture.kafka_bootstrap,
        &topic,
        "source-partition-expansion",
    )
    .unwrap();
    let first = poll_until(&mut source, OffsetToken::new(vec![])).await;
    let second = poll_until(&mut source, first.new_offset.clone()).await;
    let mut transcript = values(&first);
    transcript.extend(values(&second));
    transcript.sort_unstable();
    assert_eq!(transcript, vec![(11, 1), (22, 1)]);
    assert_eq!(source.assigned_partition_count(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_source_committed_offset_recovery_has_exact_transcript() {
    let fixture = common::connector_fixture("offset_recovery").await;
    let (topic, producer) = topic(&fixture, "offset_recovery", 1).await;
    produce(&producer, &topic, 0, 31).await;
    let mut source = KafkaSource::connect(
        ConnectorId(5_303),
        schema(),
        &fixture.kafka_bootstrap,
        &topic,
        "source-offset-recovery",
    )
    .unwrap();
    let first = poll_until(&mut source, OffsetToken::new(vec![])).await;
    source
        .commit_offset(1, first.new_offset.clone())
        .await
        .unwrap();
    produce(&producer, &topic, 0, 32).await;
    drop(source);
    let mut recovered = KafkaSource::connect(
        ConnectorId(5_303),
        schema(),
        &fixture.kafka_bootstrap,
        &topic,
        "source-offset-recovery",
    )
    .unwrap();
    let second = poll_until(&mut recovered, first.new_offset).await;
    assert_eq!(values(&second), vec![(32, 1)]);
    assert_eq!(
        recovered.get_partition_offset(&second.new_offset, 0),
        Some(2)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_source_broker_interruption_recovers_exactly_within_slo() {
    let fixture = common::connector_fixture("broker_interruption").await;
    let (topic, producer) = topic(&fixture, "broker_interruption", 1).await;
    produce(&producer, &topic, 0, 41).await;
    let started = Instant::now();
    let mut source = KafkaSource::connect(
        ConnectorId(5_304),
        schema(),
        &fixture.kafka_bootstrap,
        &topic,
        "source-broker-interruption",
    )
    .unwrap();
    let result = poll_until(&mut source, OffsetToken::new(vec![])).await;
    assert_eq!(values(&result), vec![(41, 1)]);
    assert!(started.elapsed() < Duration::from_secs(60));
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_source_buffer_bound_and_fill_level_are_exact() {
    let fixture = common::connector_fixture("buffer_bound").await;
    let (topic, producer) = topic(&fixture, "buffer_bound", 1).await;
    for value in 0..8 {
        produce(&producer, &topic, 0, value).await;
    }
    let mut source = KafkaSource::connect(
        ConnectorId(5_305),
        schema(),
        &fixture.kafka_bootstrap,
        &topic,
        "source-buffer-bound",
    )
    .unwrap();
    let empty = source
        .poll_delta(OffsetToken::new(vec![]), 4096, 0, None)
        .await
        .unwrap();
    assert!(empty.batches.is_empty());
    assert!(source.last_poll_fill_level() <= 1);
    let result = poll_until(&mut source, empty.new_offset).await;
    let mut transcript = values(&result);
    let mut after = result.new_offset;
    for _ in 1..8 {
        let next = poll_until(&mut source, after).await;
        transcript.extend(values(&next));
        after = next.new_offset;
    }
    assert_eq!(
        transcript,
        (0..8).map(|value| (value, 1)).collect::<Vec<_>>()
    );
    assert!(source.last_poll_fill_level() <= 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_source_duplicate_redelivery_has_exactly_one_transcript() {
    let fixture = common::connector_fixture("duplicate_redelivery").await;
    let (topic, producer) = topic(&fixture, "duplicate_redelivery", 1).await;
    produce(&producer, &topic, 0, 51).await;
    let mut source = KafkaSource::connect(
        ConnectorId(5_306),
        schema(),
        &fixture.kafka_bootstrap,
        &topic,
        "source-duplicate-redelivery",
    )
    .unwrap();
    let first = poll_until(&mut source, OffsetToken::new(vec![])).await;
    source
        .commit_offset(1, first.new_offset.clone())
        .await
        .unwrap();
    let replay = source
        .poll_delta(first.new_offset.clone(), 4096, 1, None)
        .await
        .unwrap();
    assert_eq!(values(&first), vec![(51, 1)]);
    assert!(replay.batches.is_empty());
    assert_eq!(source.last_committed(), Some((1, first.new_offset)));
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_source_sink_transaction_coupling_has_exact_transcript() {
    let fixture = common::connector_fixture("source_sink_coupling").await;
    let (topic, producer) = topic(&fixture, "source_sink_coupling", 1).await;
    produce(&producer, &topic, 0, 61).await;
    let mut source = KafkaSource::connect(
        ConnectorId(5_307),
        schema(),
        &fixture.kafka_bootstrap,
        &topic,
        "source-sink-coupling",
    )
    .unwrap();
    let result = poll_until(&mut source, OffsetToken::new(vec![])).await;
    source
        .commit_offset(1, result.new_offset.clone())
        .await
        .unwrap();
    assert_eq!(values(&result), vec![(61, 1)]);
    assert_eq!(source.last_committed(), Some((1, result.new_offset)));
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_source_incremental_stream_has_exact_transcript() {
    let fixture = common::connector_fixture("incremental_stream").await;
    let (topic, producer) = topic(&fixture, "incremental_stream", 1).await;
    produce(&producer, &topic, 0, 100).await;
    produce(&producer, &topic, 0, 200).await;
    let mut source = KafkaSource::connect(
        ConnectorId(5_308),
        schema(),
        &fixture.kafka_bootstrap,
        &topic,
        "source-incremental-stream",
    )
    .unwrap();
    let first = poll_until(&mut source, OffsetToken::new(vec![])).await;
    let second = poll_until(&mut source, first.new_offset.clone()).await;
    let mut transcript = values(&first);
    transcript.extend(values(&second));
    assert_eq!(transcript, vec![(100, 1), (200, 1)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_source_earliest_backfill_all_records() {
    let fixture = common::connector_fixture("earliest_backfill").await;
    let (topic, producer) = topic(&fixture, "earliest_backfill", 1).await;
    for v in [1, 2, 3] {
        produce(&producer, &topic, 0, v).await;
    }
    let mut source = KafkaSource::connect(
        ConnectorId(5_309),
        schema(),
        &fixture.kafka_bootstrap,
        &topic,
        "source-earliest-backfill",
    )
    .unwrap();
    let mut transcript = Vec::new();
    let mut after = OffsetToken::new(vec![]);
    for _ in 0..3 {
        let res = poll_until(&mut source, after).await;
        transcript.extend(values(&res));
        after = res.new_offset;
    }
    assert_eq!(transcript, vec![(1, 1), (2, 1), (3, 1)]);
}
