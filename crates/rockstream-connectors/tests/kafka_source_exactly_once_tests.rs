//! E2E proof: a real Kafka consumer group polls, revokes, and commits exact offsets.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, Field, Schema};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Offset, TopicPartitionList,
};
use rockstream_connectors::{KafkaSource, OffsetToken, PollDeltaResult, SourceConnector};
use rockstream_types::arrow_batch::split_weight_column;
use rockstream_types::ids::ConnectorId;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, KAFKA_PORT};

async fn poll_until(
    source: &mut KafkaSource,
    after: OffsetToken,
    credits: usize,
) -> PollDeltaResult {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let result = source
            .poll_delta(after.clone(), 4096, credits, None)
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

#[tokio::test(flavor = "multi_thread")]
async fn real_broker_source_assignment_poll_and_commit() {
    let broker = apache::Kafka::default().start().await.unwrap();
    let bootstrap = format!(
        "127.0.0.1:{}",
        broker.get_host_port_ipv4(KAFKA_PORT).await.unwrap()
    );
    let topic = "source-exactly-once";
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();
    let created = admin
        .create_topics(
            &[NewTopic::new(topic, 2, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(created, vec![Ok(topic.to_string())]);
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();

    for (partition, timestamp, value) in [(0, 100, 10), (1, 200, 20)] {
        let payload =
            serde_json::json!({"timestamp": timestamp, "values": [value], "weight": 1}).to_string();
        producer
            .send(
                FutureRecord::to(topic)
                    .partition(partition)
                    .payload(&payload)
                    .key(&format!("key-{partition}")),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
    }

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let mut source =
        KafkaSource::connect(ConnectorId(101), schema, &bootstrap, topic, "source-proof").unwrap();
    let first = poll_until(&mut source, OffsetToken::new(vec![]), 1).await;
    let second = poll_until(&mut source, first.new_offset.clone(), 1).await;
    source.commit_offset(7, second.new_offset.clone()).unwrap();
    let committed = second.new_offset.clone();

    let mut values = [first, second]
        .into_iter()
        .flat_map(|result| {
            let (data, weights) = split_weight_column(&result.batches[0]).unwrap();
            assert_eq!(weights, vec![1]);
            data.column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    values.sort_unstable();

    assert_eq!(source.assigned_partition_count(), 2);
    assert_eq!(values, vec![10, 20]);
    assert_eq!(source.get_partition_offset(&committed, 0), Some(1));
    assert_eq!(source.get_partition_offset(&committed, 1), Some(1));
    assert_eq!(source.last_committed(), Some((7, committed)));
    assert_eq!(source.last_poll_fill_level(), 0);

    let verifier: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "source-proof")
        .create()
        .unwrap();
    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(topic, 0);
    partitions.add_partition(topic, 1);
    let committed_offsets = verifier
        .committed_offsets(partitions, Duration::from_secs(5))
        .unwrap();
    assert_eq!(
        committed_offsets
            .elements()
            .into_iter()
            .map(|partition| (partition.partition(), partition.offset()))
            .collect::<Vec<_>>(),
        vec![(0, Offset::Offset(1)), (1, Offset::Offset(1))]
    );
}
