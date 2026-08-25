//! v0.59.17 Slice 6 — durability (MinIO): the same proof as
//! `connector_scenario_durability_lfs_tests.rs`, but the `sink.kafka`
//! recovery checkpoint is persisted to and restored from a real MinIO
//! (S3-compatible) object store instead of a local filesystem.

mod common;

use object_store::ObjectStore;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use rockstream_connectors::{KafkaSink, SinkConnector};
use rockstream_types::ids::ConnectorId;
use rockstream_types::sink::{RecoveryAction, SinkState};
use std::time::{Duration, Instant};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

fn pending_handle(state: &SinkState) -> Vec<u8> {
    match state {
        SinkState::PreCommitted { pending_handle, .. } => pending_handle.clone(),
        SinkState::Idle | SinkState::Committed => Vec::new(),
    }
}

async fn payloads(bootstrap: &str, topic: &str, expected: usize) -> Vec<String> {
    if expected == 0 {
        return Vec::new();
    }
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set(
            "group.id",
            format!("durability-minio-{}-{topic}", std::process::id()),
        )
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
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

#[tokio::test(flavor = "multi_thread")]
async fn kafka_sink_checkpoint_restart_minio_has_byte_identical_commit() {
    if !common::docker_available() {
        eprintln!(
            "SKIP kafka_sink_checkpoint_restart_minio_has_byte_identical_commit: Docker is not available locally"
        );
        return;
    }
    let (fixture, topic) = {
        let fixture = common::connector_fixture("sink_ckpt_minio").await;
        let topic = "sink_ckpt_minio".to_string();
        let admin: rdkafka::admin::AdminClient<_> = ClientConfig::new()
            .set("bootstrap.servers", &fixture.kafka_bootstrap)
            .create()
            .expect("admin client");
        admin
            .create_topics(
                &[rdkafka::admin::NewTopic::new(
                    &topic,
                    1,
                    rdkafka::admin::TopicReplication::Fixed(1),
                )],
                &rdkafka::admin::AdminOptions::new(),
            )
            .await
            .expect("create topic");
        (fixture, topic)
    };

    let minio = MinIO::default()
        .start()
        .await
        .expect("minio container start");
    let minio_port = minio
        .get_host_port_ipv4(9000)
        .await
        .expect("minio container port");
    let bucket = "sink-checkpoint-durability";
    common::create_minio_bucket(minio_port, bucket).await;
    let store = common::build_minio_store(minio_port, bucket);
    let checkpoint_path = object_store::path::Path::from("sink_checkpoint/epoch-1.bin");

    // Mid-scenario: pre-commit stages the epoch's payload, producing the
    // recovery checkpoint a real gateway would persist durably before
    // crossing the crash window between pre-commit and commit.
    let mut sink =
        KafkaSink::connect(ConnectorId(9_002), &fixture.kafka_bootstrap, &topic).unwrap();
    sink.set_cluster_committed(1);
    let state = sink.pre_commit(1, 4).await.unwrap();
    let handle = pending_handle(&state);

    // Persist the checkpoint to a real MinIO-backed object store.
    store
        .put(&checkpoint_path, handle.clone().into())
        .await
        .unwrap();

    // Simulated crash: the in-process sink (and its ephemeral state) is gone.
    drop(sink);

    // Simulated restart: recover the checkpoint from the MinIO backend
    // before reconnecting — proves the checkpoint round-trips byte-identical
    // across the backend, not just that recovery happens to succeed.
    let restored_handle = store
        .get(&checkpoint_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec();
    assert_eq!(
        restored_handle, handle,
        "checkpoint persisted to MinIO must round-trip byte-identical"
    );

    let mut recovered =
        KafkaSink::connect(ConnectorId(9_002), &fixture.kafka_bootstrap, &topic).unwrap();
    recovered.set_cluster_committed(1);
    recovered
        .recover(RecoveryAction::RerunCommit {
            epoch: 1,
            profile: recovered.idempotency_profile(),
            pending_handle: restored_handle,
        })
        .await
        .unwrap();

    // The committed output visible after the checkpoint/restart is exactly
    // what was staged before the crash — no more, no less.
    assert_eq!(
        payloads(&fixture.kafka_bootstrap, &topic, 1).await,
        vec!["{\"epoch\":1,\"rows\":4}".to_string()],
    );
}
