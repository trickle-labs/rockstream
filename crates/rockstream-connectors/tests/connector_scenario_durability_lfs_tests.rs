//! v0.59.17 Slice 6 — durability (LFS): `sink.kafka`'s recovery checkpoint
//! (the `pending_handle` a caller must persist between `pre_commit` and a
//! confirmed `commit`) survives being written to and read back from a real
//! `object_store::local::LocalFileSystem` backend across a simulated
//! gateway-process restart, and the committed output produced after
//! restoring that checkpoint is byte-identical to the payload staged before
//! the (simulated) crash.
//!
//! `connector.postgres-cdc` and `connector.kafka-source` already have
//! equivalent checkpoint/restart proofs in their own guarantee-matrix
//! suites (`postgres_cdc_each_commit_boundary_recovers_exactly_once`,
//! `kafka_source_committed_offset_recovery_has_exact_transcript`) — see
//! `capabilities.toml`, which points their `checkpoint_recovery` behavior at
//! those existing tests rather than duplicating this shape for all three.

mod common;

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use rockstream_connectors::{KafkaSink, SinkConnector};
use rockstream_types::ids::ConnectorId;
use rockstream_types::sink::{RecoveryAction, SinkState};
use std::time::{Duration, Instant};
use tempfile::TempDir;

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
            format!("durability-lfs-{}-{topic}", std::process::id()),
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
async fn kafka_sink_checkpoint_restart_lfs_has_byte_identical_commit() {
    if !common::docker_available() {
        eprintln!(
            "SKIP kafka_sink_checkpoint_restart_lfs_has_byte_identical_commit: Docker is not available locally"
        );
        return;
    }
    let (fixture, topic) = {
        let fixture = common::connector_fixture("sink_ckpt_lfs").await;
        let topic = "sink_ckpt_lfs".to_string();
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

    // Mid-scenario: pre-commit stages the epoch's payload, producing the
    // recovery checkpoint (`pending_handle`) a real gateway would persist
    // durably before crossing the crash window between pre-commit and commit.
    let mut sink =
        KafkaSink::connect(ConnectorId(9_001), &fixture.kafka_bootstrap, &topic).unwrap();
    sink.set_cluster_committed(1);
    let state = sink.pre_commit(1, 4).await.unwrap();
    let handle = pending_handle(&state);

    // Persist the checkpoint to a real LocalFileSystem-backed object store.
    let checkpoint_dir = TempDir::new().unwrap();
    let store = LocalFileSystem::new_with_prefix(checkpoint_dir.path()).unwrap();
    let checkpoint_path = ObjectPath::from("sink_checkpoint/epoch-1.bin");
    store
        .put(&checkpoint_path, handle.clone().into())
        .await
        .unwrap();

    // Simulated crash: the in-process sink (and its ephemeral state) is gone.
    drop(sink);

    // Simulated restart: recover the checkpoint from the LFS backend before
    // reconnecting — proves the checkpoint itself round-trips byte-identical
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
        "checkpoint persisted to LocalFileSystem must round-trip byte-identical"
    );

    let mut recovered =
        KafkaSink::connect(ConnectorId(9_001), &fixture.kafka_bootstrap, &topic).unwrap();
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
