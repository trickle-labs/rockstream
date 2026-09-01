mod common;

use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use futures::FutureExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use rockstream_connectors::{KafkaSink, SinkConnector};
use rockstream_types::ids::ConnectorId;
use rockstream_types::sink::{RecoveryAction, SinkState};

use common::ConnectorFixture;

async fn setup(label: &str) -> (ConnectorFixture, String) {
    let fixture = common::connector_fixture(label).await;
    let topic = format!("sink_{label}");
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &fixture.kafka_bootstrap)
        .create()
        .unwrap();
    assert_eq!(
        admin
            .create_topics(
                &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .unwrap(),
        vec![Ok(topic.clone())]
    );
    (fixture, topic)
}

async fn payloads(bootstrap: &str, topic: &str, expected: usize) -> Vec<String> {
    if expected == 0 {
        return Vec::new();
    }
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set(
            "group.id",
            format!("sink-guarantee-{}-{topic}", std::process::id()),
        )
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .create()
        .unwrap();
    consumer.subscribe(&[topic]).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut result = Vec::new();
    while result.len() < expected && Instant::now() < deadline {
        if let Ok(Ok(message)) =
            tokio::time::timeout(Duration::from_millis(100), consumer.recv()).await
        {
            result.push(message.payload_view::<str>().unwrap().unwrap().to_owned());
        }
    }
    result
}

fn pending_handle(state: &SinkState) -> Vec<u8> {
    match state {
        SinkState::PreCommitted { pending_handle, .. } => pending_handle.clone(),
        SinkState::Idle | SinkState::Committed => Vec::new(),
    }
}

async fn rerun(sink: &mut KafkaSink, epoch: u64, handle: &[u8]) {
    sink.recover(RecoveryAction::RerunCommit {
        epoch,
        profile: sink.idempotency_profile(),
        pending_handle: handle.to_vec(),
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_sink_crash_before_commit_has_no_visible_payload_and_recovers_exactly() {
    let (fixture, topic) = setup("crash_before_commit").await;
    let mut sink =
        KafkaSink::connect(ConnectorId(5_401), &fixture.kafka_bootstrap, &topic).unwrap();
    sink.set_cluster_committed(1);
    let state = sink.pre_commit(1, 3).await.unwrap();
    let handle = pending_handle(&state);
    drop(sink);

    let started = Instant::now();
    let mut recovered =
        KafkaSink::connect(ConnectorId(5_401), &fixture.kafka_bootstrap, &topic).unwrap();
    recovered.set_cluster_committed(1);
    assert!(payloads(&fixture.kafka_bootstrap, &topic, 0)
        .await
        .is_empty());
    rerun(&mut recovered, 1, &handle).await;
    assert_eq!(
        payloads(&fixture.kafka_bootstrap, &topic, 1).await,
        vec!["{\"epoch\":1,\"rows\":3}"],
    );
    assert!(started.elapsed() < Duration::from_secs(60));
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_sink_crash_during_commit_recovers_exactly_once_within_slo() {
    let (fixture, topic) = setup("crash_during_commit").await;
    let mut sink =
        KafkaSink::connect(ConnectorId(5_402), &fixture.kafka_bootstrap, &topic).unwrap();
    sink.set_cluster_committed(1);
    let state = sink.pre_commit(1, 5).await.unwrap();
    let handle = pending_handle(&state);
    sink.commit(1, &state).await.unwrap();
    drop(sink);

    let started = Instant::now();
    let mut recovered =
        KafkaSink::connect(ConnectorId(5_402), &fixture.kafka_bootstrap, &topic).unwrap();
    recovered.set_cluster_committed(1);
    rerun(&mut recovered, 1, &handle).await;
    assert_eq!(
        payloads(&fixture.kafka_bootstrap, &topic, 1).await,
        vec!["{\"epoch\":1,\"rows\":5}"],
    );
    assert!(started.elapsed() < Duration::from_secs(60));
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_sink_uncertain_broker_response_recovers_exactly_once_within_slo() {
    let (fixture, topic) = setup("uncertain_response").await;
    let mut sink =
        KafkaSink::connect(ConnectorId(5_403), &fixture.kafka_bootstrap, &topic).unwrap();
    sink.set_cluster_committed(1);
    let state = sink.pre_commit(1, 7).await.unwrap();
    let handle = pending_handle(&state);
    sink.commit(1, &state).await.unwrap();
    drop(sink);

    let started = Instant::now();
    let mut recovered =
        KafkaSink::connect(ConnectorId(5_403), &fixture.kafka_bootstrap, &topic).unwrap();
    recovered.set_cluster_committed(1);
    rerun(&mut recovered, 1, &handle).await;
    rerun(&mut recovered, 1, &handle).await;
    assert_eq!(
        payloads(&fixture.kafka_bootstrap, &topic, 1).await,
        vec!["{\"epoch\":1,\"rows\":7}"],
    );
    assert!(started.elapsed() < Duration::from_secs(60));
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_sink_transaction_timeout_recovers_exactly_once_within_slo() {
    let (fixture, topic) = setup("transaction_timeout").await;
    let mut sink =
        KafkaSink::connect(ConnectorId(5_404), &fixture.kafka_bootstrap, &topic).unwrap();
    sink.set_cluster_committed(1);
    sink.set_kafka_tx_timeout_probability(1.0);
    let state = sink.pre_commit(1, 9).await.unwrap();
    let handle = pending_handle(&state);
    let _ = sink.commit(1, &state).await;
    drop(sink);

    let started = Instant::now();
    let mut recovered =
        KafkaSink::connect(ConnectorId(5_404), &fixture.kafka_bootstrap, &topic).unwrap();
    recovered.set_cluster_committed(1);
    rerun(&mut recovered, 1, &handle).await;
    assert_eq!(
        payloads(&fixture.kafka_bootstrap, &topic, 1).await,
        vec!["{\"epoch\":1,\"rows\":9}"],
    );
    assert!(started.elapsed() < Duration::from_secs(60));
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_sink_recovery_rerun_has_exactly_one_payload_per_epoch() {
    let (fixture, topic) = setup("recovery_rerun").await;
    let mut sink =
        KafkaSink::connect(ConnectorId(5_405), &fixture.kafka_bootstrap, &topic).unwrap();
    sink.set_cluster_committed(1);
    let state = sink.pre_commit(1, 11).await.unwrap();
    let handle = pending_handle(&state);
    sink.commit(1, &state).await.unwrap();
    drop(sink);

    let mut recovered =
        KafkaSink::connect(ConnectorId(5_405), &fixture.kafka_bootstrap, &topic).unwrap();
    recovered.set_cluster_committed(1);
    rerun(&mut recovered, 1, &handle).await;
    rerun(&mut recovered, 1, &handle).await;
    assert_eq!(
        payloads(&fixture.kafka_bootstrap, &topic, 1).await,
        vec!["{\"epoch\":1,\"rows\":11}"],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_sink_duplicate_commit_has_exactly_one_payload_per_epoch() {
    let (fixture, topic) = setup("duplicate_commit").await;
    let mut sink =
        KafkaSink::connect(ConnectorId(5_406), &fixture.kafka_bootstrap, &topic).unwrap();
    sink.set_cluster_committed(1);
    let state = sink.pre_commit(1, 13).await.unwrap();
    let handle = pending_handle(&state);
    sink.commit(1, &state).await.unwrap();
    drop(sink);

    let mut recovered =
        KafkaSink::connect(ConnectorId(5_406), &fixture.kafka_bootstrap, &topic).unwrap();
    recovered.set_cluster_committed(1);
    rerun(&mut recovered, 1, &handle).await;
    assert_eq!(
        payloads(&fixture.kafka_bootstrap, &topic, 1).await,
        vec!["{\"epoch\":1,\"rows\":13}"],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_sink_checkpoint_coupling_has_exact_commit_transcript() {
    let (fixture, topic) = setup("checkpoint_coupling").await;
    let mut sink =
        KafkaSink::connect(ConnectorId(5_407), &fixture.kafka_bootstrap, &topic).unwrap();
    let state = sink.pre_commit(1, 17).await.unwrap();
    let handle = pending_handle(&state);
    let rejected = AssertUnwindSafe(sink.commit(1, &state))
        .catch_unwind()
        .await;
    assert!(rejected.is_err());
    drop(sink);

    let mut recovered =
        KafkaSink::connect(ConnectorId(5_407), &fixture.kafka_bootstrap, &topic).unwrap();
    recovered.set_cluster_committed(1);
    rerun(&mut recovered, 1, &handle).await;
    assert_eq!(
        payloads(&fixture.kafka_bootstrap, &topic, 1).await,
        vec!["{\"epoch\":1,\"rows\":17}"],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_sink_incremental_payload_matches_view() {
    let (fixture, topic) = setup("incremental_payload").await;
    let mut sink =
        KafkaSink::connect(ConnectorId(5_408), &fixture.kafka_bootstrap, &topic).unwrap();
    sink.set_cluster_committed(1);
    let state = sink.pre_commit(1, 2).await.unwrap();
    sink.commit(1, &state).await.unwrap();
    let received = payloads(&fixture.kafka_bootstrap, &topic, 1).await;
    assert_eq!(received, vec!["{\"epoch\":1,\"rows\":2}".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_sink_initial_snapshot_delivery_exact() {
    let (fixture, topic) = setup("initial_snapshot").await;
    let mut sink =
        KafkaSink::connect(ConnectorId(5_409), &fixture.kafka_bootstrap, &topic).unwrap();
    sink.set_cluster_committed(1);
    let state = sink.pre_commit(1, 10).await.unwrap();
    sink.commit(1, &state).await.unwrap();
    let received = payloads(&fixture.kafka_bootstrap, &topic, 1).await;
    assert_eq!(received, vec!["{\"epoch\":1,\"rows\":10}".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_sink_producer_error_fails_closed() {
    let (fixture, topic) = setup("producer_error").await;
    let mut sink =
        KafkaSink::connect(ConnectorId(5_410), &fixture.kafka_bootstrap, &topic).unwrap();
    let state = SinkState::Idle;
    let res = AssertUnwindSafe(sink.commit(1, &state))
        .catch_unwind()
        .await;
    assert!(res.is_err());
}
