mod common;
#[path = "common/kafka_fault_proxy.rs"]
mod kafka_fault_proxy;

use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use futures::FutureExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use rockstream_connectors::{KafkaSink, SinkConnector, SinkError};
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

async fn payloads(bootstrap: &str, topic: &str) -> Vec<String> {
    records(bootstrap, topic)
        .await
        .into_iter()
        .map(|(_, payload)| String::from_utf8(payload).unwrap())
        .collect()
}

async fn records(bootstrap: &str, topic: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set(
            "group.id",
            format!("sink-guarantee-{}-{topic}", std::process::id()),
        )
        .set("auto.offset.reset", "earliest")
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
    let mut result = Vec::new();
    while Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => result.push((
                message.key().unwrap().to_vec(),
                message.payload().unwrap().to_vec(),
            )),
            Some(Err(KafkaError::PartitionEOF(0))) => return result,
            Some(Err(error)) => panic!("broker oracle failed: {error}"),
            None => {}
        }
    }
    panic!("broker oracle did not reach end of topic: {result:?}")
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_sink_crash_before_commit_has_no_visible_payload_and_recovers_exactly() {
    interrupted_recovery(2).await;
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
        payloads(&fixture.kafka_bootstrap, &topic).await,
        vec!["{\"epoch\":1,\"rows\":5}"],
    );
    assert!(started.elapsed() < Duration::from_secs(60));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_sink_uncertain_broker_response_recovers_exactly_once_within_slo() {
    interrupted_recovery(1).await;
}

async fn interrupted_recovery(commit_fault: u8) {
    let broker = kafka_fault_proxy::KafkaFaultProxy::start().await;
    let topic = "sink_interrupted_recovery";
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &broker.bootstrap)
        .create()
        .unwrap();
    assert_eq!(
        admin
            .create_topics(
                &[
                    NewTopic::new(topic, 1, TopicReplication::Fixed(1)),
                    NewTopic::new("sink_producer_probe", 1, TopicReplication::Fixed(1)),
                ],
                &AdminOptions::new(),
            )
            .await
            .unwrap(),
        vec![Ok(topic.to_owned()), Ok("sink_producer_probe".to_owned())]
    );
    let mut sink = KafkaSink::connect(ConnectorId(5_403), &broker.bootstrap, topic).unwrap();
    sink.set_cluster_committed(1);
    let state = sink.pre_commit(1, 7).await.unwrap();
    let handle = pending_handle(&state);
    assert_eq!(
        state,
        SinkState::PreCommitted {
            staged_rows: 7,
            pending_handle: b"kafka-txn-5403-epoch-1:7".to_vec(),
        }
    );
    broker.faults.commit.store(commit_fault, Ordering::SeqCst);
    let error = sink.commit(1, &state).await.unwrap_err();
    assert!(
        matches!(error, SinkError::CommitFailed { epoch: 1, .. }),
        "{error}"
    );
    tokio::time::timeout(Duration::from_secs(5), broker.faults.interrupted.notified())
        .await
        .unwrap();
    assert_eq!(
        *broker.faults.commit_response.lock().unwrap(),
        if commit_fault == 1 { Some(0) } else { None }
    );
    assert_eq!(sink.delivered_epochs_for_test(), &BTreeSet::new());
    assert_eq!(sink.kafka_sink_staged_epochs_count(), 1);
    drop(sink);
    broker.faults.commit.store(0, Ordering::SeqCst);
    let started = Instant::now();
    let mut recovered = KafkaSink::connect(ConnectorId(5_403), &broker.bootstrap, topic).unwrap();
    let before = if commit_fault == 1 {
        vec![(b"1".to_vec(), b"{\"epoch\":1,\"rows\":7}".to_vec())]
    } else {
        vec![]
    };
    assert_eq!(records(&broker.bootstrap, topic).await, before);
    let action = RecoveryAction::RerunCommit {
        epoch: 1,
        profile: recovered.idempotency_profile(),
        pending_handle: handle,
    };
    broker
        .faults
        .lookup_unavailable
        .store(true, Ordering::SeqCst);
    // A second transactional producer can initialize and publish while the
    // lookup-only Kafka APIs are unavailable.
    let mut probe =
        KafkaSink::connect(ConnectorId(5_404), &broker.bootstrap, "sink_producer_probe").unwrap();
    let probe_state = probe.pre_commit(2, 9).await.unwrap();
    probe.set_cluster_committed(2);
    probe.commit(2, &probe_state).await.unwrap();
    assert_eq!(
        payloads(&broker.bootstrap, "sink_producer_probe").await,
        vec!["{\"epoch\":2,\"rows\":9}"]
    );
    let mut expected = before;
    for _ in 0..2 {
        let result = recovered.recover(action.clone()).await;
        assert_eq!(records(&broker.bootstrap, topic).await, expected);
        let error = result.unwrap_err();
        match &error {
            SinkError::DeliveryUnknown { epoch, reason } => {
                assert_eq!(*epoch, 1);
                assert_eq!(reason, "consumer setup failed: Meta data fetch error: OperationTimedOut (Local: Timed out)");
            }
            other => panic!("expected retryable unknown outcome, got {other:?}"),
        }
        assert_eq!(error.to_string(), "RS-4030: sink delivery outcome unknown for epoch 1: consumer setup failed: Meta data fetch error: OperationTimedOut (Local: Timed out); retry recovery");
        assert_eq!(recovered.delivered_epochs_for_test(), &BTreeSet::new());
        assert_eq!(recovered.kafka_sink_staged_epochs_count(), 0);
    }
    broker
        .faults
        .lookup_unavailable
        .store(false, Ordering::SeqCst);
    if commit_fault == 2 {
        expected.push((b"1".to_vec(), b"{\"epoch\":1,\"rows\":7}".to_vec()));
    }
    for _ in 0..2 {
        recovered.recover(action.clone()).await.unwrap();
        assert_eq!(records(&broker.bootstrap, topic).await, expected);
        assert_eq!(recovered.delivered_epochs_for_test(), &BTreeSet::from([1]));
        assert_eq!(recovered.kafka_sink_staged_epochs_count(), 0);
    }
    assert!(started.elapsed() < Duration::from_secs(60));
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_sink_lookup_timeout_does_not_confirm_absence_before_stable_end() {
    let (fixture, topic) = setup("lookup_timeout").await;
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &fixture.kafka_bootstrap)
        .set("transactional.id", "unresolved-marker")
        .create()
        .unwrap();
    producer.init_transactions(Duration::from_secs(15)).unwrap();
    producer.begin_transaction().unwrap();
    producer
        .send(
            BaseRecord::to(&topic)
                .key("1")
                .payload("{\"epoch\":1,\"rows\":7}"),
        )
        .unwrap();
    producer.flush(Duration::from_secs(5)).unwrap();
    let mut sink =
        KafkaSink::connect(ConnectorId(5_410), &fixture.kafka_bootstrap, &topic).unwrap();
    let action = RecoveryAction::RerunCommit {
        epoch: 1,
        profile: sink.idempotency_profile(),
        pending_handle: b"kafka-txn-5410-epoch-1:7".to_vec(),
    };
    let error = sink.recover(action.clone()).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "RS-4030: sink delivery outcome unknown for epoch 1: lookup timed out; retry recovery"
    );
    assert_eq!(sink.delivered_epochs_for_test(), &BTreeSet::new());
    assert_eq!(
        payloads(&fixture.kafka_bootstrap, &topic).await,
        Vec::<String>::new()
    );
    producer
        .commit_transaction(Duration::from_secs(15))
        .unwrap();
    for _ in 0..2 {
        sink.recover(action.clone()).await.unwrap();
        assert_eq!(
            payloads(&fixture.kafka_bootstrap, &topic).await,
            vec!["{\"epoch\":1,\"rows\":7}"]
        );
        assert_eq!(sink.delivered_epochs_for_test(), &BTreeSet::from([1]));
        assert_eq!(sink.kafka_sink_staged_epochs_count(), 0);
    }
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
        payloads(&fixture.kafka_bootstrap, &topic).await,
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
        payloads(&fixture.kafka_bootstrap, &topic).await,
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
        payloads(&fixture.kafka_bootstrap, &topic).await,
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
        payloads(&fixture.kafka_bootstrap, &topic).await,
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
    let received = payloads(&fixture.kafka_bootstrap, &topic).await;
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
    let received = payloads(&fixture.kafka_bootstrap, &topic).await;
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
