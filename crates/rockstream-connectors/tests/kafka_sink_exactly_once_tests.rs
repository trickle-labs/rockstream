//! Real-broker proof for the transactional Kafka sink.

use std::time::{Duration, Instant};

use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message,
};
use rockstream_connectors::{KafkaSink, SinkConnector};
use rockstream_types::{ids::ConnectorId, sink::RecoveryAction};
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::kafka::apache::{self, KAFKA_PORT};

async fn read_epoch_payloads(bootstrap: &str, topic: &str) -> Vec<String> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("sink-proof-{}", std::process::id()))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .create()
        .unwrap();
    consumer.subscribe(&[topic]).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut payloads = Vec::new();
    while Instant::now() < deadline {
        if let Ok(Ok(message)) =
            tokio::time::timeout(Duration::from_millis(100), consumer.recv()).await
        {
            payloads.push(message.payload_view::<str>().unwrap().unwrap().to_owned());
            if payloads.len() == 2 {
                break;
            }
        }
    }
    payloads
}

#[tokio::test(flavor = "multi_thread")]
async fn transactional_commit_and_recovery_have_one_payload_per_epoch() {
    let broker = apache::Kafka::default()
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .start()
        .await
        .unwrap();
    let bootstrap = format!(
        "127.0.0.1:{}",
        broker.get_host_port_ipv4(KAFKA_PORT).await.unwrap()
    );
    let topic = "sink-exactly-once";
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();
    assert_eq!(
        admin
            .create_topics(
                &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .unwrap(),
        vec![Ok(topic.to_string())]
    );

    let mut sink = KafkaSink::connect(ConnectorId(102), &bootstrap, topic).unwrap();
    sink.set_cluster_committed(2);
    let first = sink.pre_commit(1, 3).await.unwrap();
    sink.commit(1, &first).await.unwrap();
    let second = sink.pre_commit(2, 5).await.unwrap();
    sink.commit(2, &second).await.unwrap();

    let mut recovered = KafkaSink::connect(ConnectorId(102), &bootstrap, topic).unwrap();
    recovered.set_cluster_committed(2);
    recovered
        .recover(RecoveryAction::RerunCommit {
            epoch: 2,
            profile: recovered.idempotency_profile(),
            pending_handle: second.pending_handle().to_vec(),
        })
        .await
        .unwrap();

    assert_eq!(
        read_epoch_payloads(&bootstrap, topic).await,
        vec!["{\"epoch\":1,\"rows\":3}", "{\"epoch\":2,\"rows\":5}"]
    );
}

trait PendingHandle {
    fn pending_handle(&self) -> &[u8];
}

impl PendingHandle for rockstream_types::sink::SinkState {
    fn pending_handle(&self) -> &[u8] {
        match self {
            Self::PreCommitted { pending_handle, .. } => pending_handle,
            Self::Idle | Self::Committed => &[],
        }
    }
}
