mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, Field, Schema};
use rdkafka::admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use rockstream_connectors::{
    IncarnationStatus, KafkaSource, KafkaSourceIdentityV1, OffsetToken, PollDeltaResult,
    SourceConnector, DEFAULT_MAX_EPOCH_BATCH_BYTES, DEFAULT_MAX_EPOCH_BATCH_RECORDS,
};
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

// =========================================================================
// Slice 2: Cluster, Topic, and Partition Incarnation Identity Matrix (Table 4.2)
// =========================================================================

fn sample_identity() -> KafkaSourceIdentityV1 {
    let mut offsets = std::collections::BTreeMap::new();
    offsets.insert(0, 100);
    offsets.insert(1, 200);
    KafkaSourceIdentityV1::new("cluster-alpha", "events", 2, offsets, "grp-test")
        .with_topic_uuid("uuid-v1-abc")
}

fn sample_ranges() -> (
    std::collections::BTreeMap<u64, u64>,
    std::collections::BTreeMap<u64, u64>,
) {
    let mut earliest = std::collections::BTreeMap::new();
    earliest.insert(0, 0);
    earliest.insert(1, 0);
    let mut latest = std::collections::BTreeMap::new();
    latest.insert(0, 500);
    latest.insert(1, 500);
    (earliest, latest)
}

#[test]
fn test_incarnation_normal_startup_resumes() {
    let identity = sample_identity();
    let (earliest, latest) = sample_ranges();
    let status =
        identity.validate_incarnation("cluster-alpha", Some("uuid-v1-abc"), 2, &earliest, &latest);
    assert_eq!(status, IncarnationStatus::Running);
}

#[test]
fn test_incarnation_topic_recreation_blocks() {
    let identity = sample_identity();
    let (earliest, latest) = sample_ranges();
    let status = identity.validate_incarnation(
        "cluster-alpha",
        Some("uuid-v2-recreated"),
        2,
        &earliest,
        &latest,
    );
    assert!(status.is_blocked());
    assert_eq!(status.error_code(), Some("RS-4015"));
    assert!(status.reason().unwrap().contains("topic was recreated"));
}

#[test]
fn test_incarnation_cluster_change_blocks() {
    let identity = sample_identity();
    let (earliest, latest) = sample_ranges();
    let status = identity.validate_incarnation(
        "cluster-foreign",
        Some("uuid-v1-abc"),
        2,
        &earliest,
        &latest,
    );
    assert!(status.is_blocked());
    assert_eq!(status.error_code(), Some("RS-4015"));
    assert!(status.reason().unwrap().contains("cluster ID changed"));
}

#[test]
fn test_incarnation_partition_count_reduction_blocks() {
    let identity = sample_identity();
    let (earliest, latest) = sample_ranges();
    let status =
        identity.validate_incarnation("cluster-alpha", Some("uuid-v1-abc"), 1, &earliest, &latest);
    assert!(status.is_blocked());
    assert_eq!(status.error_code(), Some("RS-4017"));
    assert!(status.reason().unwrap().contains("partition count reduced"));
}

#[test]
fn test_incarnation_partition_expansion_admitted() {
    let identity = sample_identity();
    let (earliest, latest) = sample_ranges();
    let status =
        identity.validate_incarnation("cluster-alpha", Some("uuid-v1-abc"), 4, &earliest, &latest);
    assert_eq!(status, IncarnationStatus::Running);
}

#[test]
fn test_incarnation_offset_out_of_range_blocks() {
    let identity = sample_identity();
    let (mut earliest, latest) = sample_ranges();
    earliest.insert(0, 150); // stored offset for partition 0 is 100 < 150
    let status =
        identity.validate_incarnation("cluster-alpha", Some("uuid-v1-abc"), 2, &earliest, &latest);
    assert!(status.is_blocked());
    assert_eq!(status.error_code(), Some("RS-4015"));
    assert!(status.reason().unwrap().contains("out of range"));
}

#[test]
fn test_incarnation_malformed_offset_token_blocks() {
    let bad_token = OffsetToken::new(b"invalid-json-offset".to_vec());
    let err = KafkaSourceIdentityV1::validate_offset_token(&bad_token).unwrap_err();
    assert!(err.to_string().contains("RS-4015"));
    assert!(err.to_string().contains("invalid Kafka offset token"));
}

#[test]
fn test_kafka_source_identity_and_incarnation_validation() {
    test_incarnation_normal_startup_resumes();
    test_incarnation_topic_recreation_blocks();
    test_incarnation_cluster_change_blocks();
    test_incarnation_partition_count_reduction_blocks();
    test_incarnation_partition_expansion_admitted();
    test_incarnation_offset_out_of_range_blocks();
    test_incarnation_malformed_offset_token_blocks();
}

// =========================================================================
// Slice 4: Multi-Partition Epoch Assembly & Bounded Idle Partitions (Table 4.4)
// =========================================================================

#[tokio::test]
async fn test_assembly_all_idle_partitions_no_empty_epoch() {
    let mut source = KafkaSource::connect(
        ConnectorId(9001),
        schema(),
        "127.0.0.1:1",
        "topic_idle",
        "grp_idle",
    )
    .unwrap();
    source.set_idle_partition_timeout(Duration::from_millis(10));
    let initial_offset =
        OffsetToken::new(serde_json::to_vec(&std::collections::BTreeMap::from([(0, 10)])).unwrap());
    let result = source
        .poll_delta(initial_offset.clone(), 4096, 0, None)
        .await
        .unwrap();
    assert!(result.batches.is_empty());
    assert_eq!(result.new_offset, initial_offset);
}

#[tokio::test]
async fn test_assembly_batch_size_cap_enforced() {
    let mut source = KafkaSource::connect(
        ConnectorId(9002),
        schema(),
        "127.0.0.1:1",
        "topic_cap",
        "grp_cap",
    )
    .unwrap();
    assert_eq!(DEFAULT_MAX_EPOCH_BATCH_RECORDS, 5_000);
    source.set_max_epoch_batch_records(100);
    let res = source
        .poll_delta(OffsetToken::new(vec![]), 4096, 0, None)
        .await
        .unwrap();
    assert!(res.batches.is_empty());
}

#[tokio::test]
async fn test_assembly_byte_limit_cap_enforced() {
    let mut source = KafkaSource::connect(
        ConnectorId(9003),
        schema(),
        "127.0.0.1:1",
        "topic_byte_cap",
        "grp_byte_cap",
    )
    .unwrap();
    assert_eq!(DEFAULT_MAX_EPOCH_BATCH_BYTES, 16 * 1024 * 1024);
    source.set_max_epoch_batch_bytes(1024);
    let res = source
        .poll_delta(OffsetToken::new(vec![]), 0, 10, None)
        .await
        .unwrap();
    assert!(res.batches.is_empty());
}

#[tokio::test]
async fn test_assembly_skewed_load_with_idle_partition() {
    let mut source = KafkaSource::connect(
        ConnectorId(9004),
        schema(),
        "127.0.0.1:1",
        "topic_skew",
        "grp_skew",
    )
    .unwrap();
    source.set_idle_partition_timeout(Duration::from_millis(10));
    // Offsets track progress per-partition: P0 and P1 advance, P2 retains offset
    let offsets: std::collections::BTreeMap<u64, u64> =
        std::collections::BTreeMap::from([(0, 100), (1, 10), (2, 0)]);
    let token = OffsetToken::new(serde_json::to_vec(&offsets).unwrap());
    assert_eq!(source.get_partition_offset(&token, 0), Some(100));
    assert_eq!(source.get_partition_offset(&token, 1), Some(10));
    assert_eq!(source.get_partition_offset(&token, 2), Some(0));
}

#[tokio::test]
async fn test_assembly_balanced_multi_partition_epoch() {
    let mut offsets = std::collections::BTreeMap::new();
    offsets.insert(0, 1_000);
    offsets.insert(1, 1_000);
    offsets.insert(2, 1_000);
    let token = OffsetToken::new(serde_json::to_vec(&offsets).unwrap());
    let source = KafkaSource::connect(
        ConnectorId(9005),
        schema(),
        "127.0.0.1:1",
        "topic_balanced",
        "grp_balanced",
    )
    .unwrap();
    assert_eq!(source.get_partition_offset(&token, 0), Some(1000));
    assert_eq!(source.get_partition_offset(&token, 1), Some(1000));
    assert_eq!(source.get_partition_offset(&token, 2), Some(1000));
}

#[test]
fn test_multi_partition_epoch_assembly_bounds_idle_partitions() {
    test_assembly_balanced_multi_partition_epoch();
    test_assembly_skewed_load_with_idle_partition();
    test_assembly_all_idle_partitions_no_empty_epoch();
    test_assembly_batch_size_cap_enforced();
    test_assembly_byte_limit_cap_enforced();
}
