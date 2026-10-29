//! #126: one real worker loss, with independently checked view and Kafka output.
#![cfg(feature = "docker_tests")]

use arrow::datatypes::{DataType, Field, Schema};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    consumer::{BaseConsumer, Consumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use rockstream_connectors::{
    KafkaSink, KafkaSource, OffsetToken, SourceCheckpointStore, SourceRuntimeCoordinator,
};
use rockstream_control::{
    audit::FileAuditLog, frontier::FrontierAggregator, ControlService, ShardManager,
    TopologyCatalog,
};
use rockstream_gateway::view_reader::{HotOnlyViewReader, ViewReadStrategy, ViewReader};
use rockstream_ops::{
    compile_plan_with_sink_id, kafka_sink::KafkaViewSink, sink::write_view_directory_entry,
    ArrowZSet,
};
use rockstream_plan::PlanNode;
use rockstream_runtime::start_worker_client;
use rockstream_sim::qualification::worker_failure::{
    qualification_records, validate, RecoveryFailure, WorkerObservation, WorkerRecoveryEvidence,
    MANIFEST,
};
use rockstream_storage::{keys::ShardKeyEncoder, ShardReader, WriteBatch};
use rockstream_test_support::minio::{minio_object_store, start_minio, MINIO_PASS, MINIO_USER};
use rockstream_types::{
    arrow_batch::split_weight_column,
    ids::{ConnectorId, OperatorId, ShardId, WorkerId},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs,
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::kafka::apache::{self, KAFKA_PORT};

#[path = "common/recovery.rs"]
mod recovery;
use recovery::{consume, now_ms, observe, save};

const ID: u64 = 126;
const WAIT: Duration = Duration::from_secs(60);
const STALE_OWNER_MANIFEST: &str =
    include_str!("../../../docs/testing/worker-stale-owner-kafka-minio-v1.json");

fn committed_group_offset(bootstrap: &str, group: &str, topic: &str) -> i64 {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .create()
        .unwrap();
    let offsets = consumer.committed(Duration::from_secs(10)).unwrap();
    match offsets
        .elements()
        .iter()
        .find(|partition| partition.topic() == topic && partition.partition() == 0)
        .map(|partition| partition.offset())
    {
        Some(Offset::Offset(offset)) => offset,
        other => panic!("missing committed offset for {group}/{topic}: {other:?}"),
    }
}

fn consume_topic(bootstrap: &str, topic: &str) -> Value {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "worker-stale-owner-oracle")
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
            Some(Err(rdkafka::error::KafkaError::PartitionEOF(0))) => return json!(records),
            Some(Err(error)) => panic!("sink consumer: {error}"),
            None => {}
        }
    }
    panic!("sink progress deadline for {topic}");
}

async fn read_observation(path: &Path, child: &mut Child) -> WorkerObservation {
    serde_json::from_value(read_json(path, child).await).unwrap()
}

async fn read_json(path: &Path, child: &mut Child) -> Value {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Ok(bytes) = fs::read(path) {
            return serde_json::from_slice(&bytes).unwrap();
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "worker exited before {} (see worker log)",
            path.display()
        );
        assert!(
            Instant::now() < deadline,
            "progress deadline waiting for {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

struct Workers(Vec<Child>);
impl Drop for Workers {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

// A subprocess entry point around production APIs, not an alternative source,
// storage, view, lease manager, or sink implementation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_process_fixture() {
    let Ok(root) = std::env::var("ROCKSTREAM_RECOVERY_WORKER_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let id: u64 = std::env::var("ROCKSTREAM_RECOVERY_WORKER_ID")
        .unwrap()
        .parse()
        .unwrap();
    let control = std::env::var("ROCKSTREAM_RECOVERY_CONTROL").unwrap();
    let bootstrap = std::env::var("ROCKSTREAM_RECOVERY_KAFKA").unwrap();
    let stale_owner_mode = std::env::var_os("ROCKSTREAM_RECOVERY_STALE_OWNER").is_some();
    let source_topic = std::env::var("ROCKSTREAM_RECOVERY_SOURCE_TOPIC")
        .unwrap_or_else(|_| "worker-failure-input".to_owned());
    let sink_topic = std::env::var("ROCKSTREAM_RECOVERY_SINK_TOPIC")
        .unwrap_or_else(|_| "worker-failure-output".to_owned());
    let source_group = std::env::var("ROCKSTREAM_RECOVERY_SOURCE_GROUP")
        .unwrap_or_else(|_| "worker-failure-source".to_owned());
    let (worker, _task) = start_worker_client(id, &control, &root.join(format!("worker-{id}")))
        .await
        .unwrap();
    let deadline = Instant::now() + WAIT;
    while worker.worker_id().is_none() {
        assert!(Instant::now() < deadline, "registration deadline");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if id == 1 {
        worker.request_shard(ShardId(ID)).await.unwrap();
    } else if stale_owner_mode && id == 2 {
        let deadline = Instant::now() + Duration::from_secs(180);
        while !root.join("request-worker-2").exists() {
            assert!(Instant::now() < deadline, "recipient request deadline");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        worker.request_shard(ShardId(ID)).await.unwrap();
    }
    let db = loop {
        if let Some(db) = worker.get_shard_db(ShardId(ID)) {
            break Arc::new(db);
        }
        assert!(Instant::now() < deadline, "ownership deadline");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    save(
        &root.join(format!("worker-{id}-lease.json")),
        &json!({"at_ms":now_ms(),"lease":worker.leases()}),
    );
    let deadline = Instant::now() + WAIT;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let source = KafkaSource::connect(
        ConnectorId(ID),
        schema.clone(),
        &bootstrap,
        &source_topic,
        &source_group,
    )
    .unwrap();
    let mut source = SourceRuntimeCoordinator::new(
        source,
        ConnectorId(ID),
        OffsetToken::new(Vec::new()),
        SourceCheckpointStore::new(db.clone(), ID as u128, ConnectorId(ID)),
    );
    source.recover().await.unwrap();
    let owner = source.acquire_owner(format!("worker-{id}")).unwrap();
    let plan = PlanNode::ViewSink {
        view_name: "letters".into(),
        pk: vec![],
        child: Box::new(PlanNode::Source {
            name: "letters_source".into(),
        }),
    };
    let compiled = compile_plan_with_sink_id(
        &plan,
        db.clone(),
        &HashMap::from([("letters_source".into(), schema)]),
        OperatorId(ID),
    )
    .unwrap();
    compiled.pipeline.restore(&db).await.unwrap();
    write_view_directory_entry(&db, "letters", OperatorId(ID), 2, &[])
        .await
        .unwrap();
    let mut sink = KafkaViewSink::new(
        KafkaSink::connect(ConnectorId(ID), &bootstrap, &sink_topic).unwrap(),
        db.clone(),
        OperatorId(ID),
        2,
    );
    if id == 2 && !stale_owner_mode {
        assert!(
            sink.deliver_next().await.unwrap(),
            "recover durable staged update"
        );
    }
    while source.committed_epoch() < 5 {
        assert!(Instant::now() < deadline, "source progress deadline");
        let delta = source.poll_delta(64 * 1024, 1).await.unwrap();
        if delta.batches.is_empty() {
            continue;
        }
        let epoch = source.next_epoch().unwrap();
        let lease = worker
            .leases()
            .into_iter()
            .find(|l| l.shard_id == ShardId(ID))
            .unwrap();
        let mut writes = WriteBatch::new();
        for batch in delta.batches {
            let (data, weights) = split_weight_column(&batch).unwrap();
            let output = compiled
                .pipeline
                .process(ArrowZSet::new(data, weights))
                .unwrap();
            compiled.sink.append_epoch(&mut writes, &output, epoch);
        }
        compiled
            .pipeline
            .append_state(&db, &mut writes)
            .await
            .unwrap();
        writes.put(&ShardKeyEncoder::frontier_key(), &epoch.to_be_bytes());
        if stale_owner_mode && id == 1 && epoch == 3 {
            save(
                &root.join("stale-ready.json"),
                &json!({
                    "at_ms": now_ms(),
                    "process_id": std::process::id(),
                    "worker_id": id,
                    "lease": lease.clone(),
                    "epoch": epoch,
                    "offset": delta.new_offset.clone(),
                    "before": observe(&db, &worker, &source).await,
                }),
            );
            let deadline = Instant::now() + Duration::from_secs(180);
            while !root.join("release-stale-owner").exists() {
                assert!(Instant::now() < deadline, "stale owner release deadline");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        let fence_valid = worker
            .check_fence_write(ShardId(ID), lease.lease_token)
            .await
            .unwrap();
        if stale_owner_mode && id == 1 && epoch == 3 {
            let source_commit = if fence_valid {
                source
                    .commit_epoch(&owner, epoch, delta.new_offset, writes)
                    .await
                    .map(|()| "committed".to_owned())
                    .unwrap_or_else(|error| format!("error:{error}"))
            } else {
                "not_attempted".to_owned()
            };
            let frontier_report_error = worker
                .report_shard_frontier(ShardId(ID), 99)
                .await
                .unwrap_err();
            assert_eq!(
                frontier_report_error,
                "RS-8004: frontier report lacks the current shard lease"
            );
            let fence_after_report = worker
                .check_fence_write(ShardId(ID), lease.lease_token)
                .await
                .unwrap();
            save(
                &root.join("stale-owner-result.json"),
                &json!({
                    "at_ms": now_ms(),
                    "process_id": std::process::id(),
                    "worker_id": id,
                    "lease_token": lease.lease_token,
                    "epoch": epoch,
                    "fence_valid": fence_valid,
                    "source_commit": source_commit,
                    "stale_frontier_report_epoch": 99,
                    "frontier_report_error": frontier_report_error,
                    "fence_after_report": fence_after_report,
                }),
            );
            return;
        }
        assert!(fence_valid, "current owner must pass the write fence");
        source
            .commit_epoch(&owner, epoch, delta.new_offset, writes)
            .await
            .unwrap();
        if stale_owner_mode {
            assert_eq!(
                worker
                    .report_shard_frontier(ShardId(ID), epoch)
                    .await
                    .unwrap(),
                epoch
            );
        }
        if id == 1 && epoch == 3 {
            assert!(sink.stage_next().await.unwrap());
            save(
                &root.join("staged.json"),
                &observe(&db, &worker, &source).await,
            );
            // Keep the production transaction active until the parent kills us.
            std::future::pending::<()>().await;
        }
        assert!(sink.deliver_next().await.unwrap());
    }
    save(
        &root.join("recovered.json"),
        &observe(&db, &worker, &source).await,
    );
    if stale_owner_mode && id == 2 {
        let deadline = Instant::now() + Duration::from_secs(60);
        while !root.join("observe-after-stale").exists() {
            assert!(Instant::now() < deadline, "post-stale observation deadline");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        save(
            &root.join("after-stale.json"),
            &observe(&db, &worker, &source).await,
        );
    }
    std::future::pending::<()>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_failure_recovery_from_view_and_kafka() {
    let docker_started = Instant::now();
    let info = Command::new("docker")
        .arg("info")
        .output()
        .expect("Docker required");
    assert!(
        info.status.success(),
        "Docker required: {}",
        String::from_utf8_lossy(&info.stderr)
    );
    let docker_command_ms = docker_started.elapsed().as_millis() as u64;
    let artifact_dir = std::env::var_os("ROCKSTREAM_CHAOS_ARTIFACT_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/real-cluster-chaos")
        });
    fs::create_dir_all(&artifact_dir).unwrap();
    let root = tempfile::tempdir().unwrap();
    let m: Value = serde_json::from_str(MANIFEST).unwrap();
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
    let (_minio, port) = start_minio("worker-recovery")
        .await
        .expect("MinIO required");
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();
    for topic in ["worker-failure-input", "worker-failure-output"] {
        assert_eq!(
            admin
                .create_topics(
                    &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
                    &AdminOptions::new()
                )
                .await
                .unwrap(),
            vec![Ok(topic.to_string())]
        );
    }
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();
    for row in m["history"].as_array().unwrap() {
        producer
            .send(
                FutureRecord::to("worker-failure-input")
                    .key("")
                    .payload(&row.to_string()),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
    }
    let manager = ShardManager::new();
    let audit = Arc::new(FileAuditLog::open(root.path().join("audit.jsonl")).unwrap());
    let catalog = TopologyCatalog::new();
    let control = ControlService::new(catalog.clone())
        .with_shard_manager(manager.clone())
        .with_audit(audit.clone())
        .start("127.0.0.1:0")
        .await
        .unwrap();
    let mut workers = Workers(Vec::new());
    for id in [1, 2] {
        let log = fs::File::create(artifact_dir.join(format!("worker-{id}.log"))).unwrap();
        workers.0.push(
            Command::new(std::env::current_exe().unwrap())
                .args(["worker_process_fixture", "--exact", "--nocapture"])
                .env("ROCKSTREAM_RECOVERY_WORKER_ROOT", root.path())
                .env("ROCKSTREAM_RECOVERY_WORKER_ID", id.to_string())
                .env("ROCKSTREAM_RECOVERY_CONTROL", control.addr.to_string())
                .env("ROCKSTREAM_RECOVERY_KAFKA", &bootstrap)
                .env(
                    "ROCKSTREAM_OBJECT_STORE_ENDPOINT",
                    format!("http://127.0.0.1:{port}"),
                )
                .env("ROCKSTREAM_OBJECT_STORE_BUCKET", "worker-recovery")
                .env("ROCKSTREAM_OBJECT_STORE_REGION", "us-east-1")
                .env("ROCKSTREAM_OBJECT_STORE_ACCESS_KEY", MINIO_USER)
                .env("ROCKSTREAM_OBJECT_STORE_SECRET_KEY", MINIO_PASS)
                .stdout(Stdio::from(log.try_clone().unwrap()))
                .stderr(Stdio::from(log))
                .spawn()
                .unwrap(),
        );
    }
    let before = read_observation(&root.path().join("staged.json"), &mut workers.0[0]).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while catalog.get(WorkerId(2)).is_none() {
        assert!(
            Instant::now() < deadline,
            "replacement worker must be registered before fault"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    save(&artifact_dir.join("before-fault.json"), &before);
    let sink_records_before = consume(&bootstrap);
    assert_eq!(
        sink_records_before,
        json!(&m["expected_sink"].as_array().unwrap()[..2]),
        "staged update must be invisible"
    );
    let fault_at_ms = now_ms();
    let kill_started = Instant::now();
    workers.0[0].kill().unwrap();
    workers.0[0].wait().unwrap();
    let worker_kill_command_ms = kill_started.elapsed().as_millis();
    let after = read_observation(&root.path().join("recovered.json"), &mut workers.0[1]).await;
    save(&artifact_dir.join("after-recovery.json"), &after);
    let store = Arc::new(object_store::prefix::PrefixStore::new(
        minio_object_store(port, "worker-recovery"),
        "shards/126",
    ));
    let reader = Arc::new(ShardReader::open("db", store).await.unwrap());
    let frontier = reader
        .get(&ShardKeyEncoder::frontier_key())
        .await
        .unwrap()
        .unwrap();
    let query_frontier = u64::from_be_bytes(frontier.as_ref().try_into().unwrap());
    let view_reader = HotOnlyViewReader {
        shard_reader: reader,
        frontier_epoch: Some(query_frontier),
    };
    let query_rows = view_reader
        .read_view("letters", None, ViewReadStrategy::HotOnly)
        .await
        .unwrap()
        .into_iter()
        .map(|row| String::from_utf8(row).unwrap())
        .collect();
    let first_correct_query_at_ms = now_ms();
    let detection = audit
        .read_all()
        .unwrap()
        .into_iter()
        .find(|e| e.action == "worker.disconnected")
        .expect("control must detect real disconnect");
    let lease_event: Value =
        serde_json::from_slice(&fs::read(root.path().join("worker-2-lease.json")).unwrap())
            .unwrap();
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(revision.status.success());
    let evidence = WorkerRecoveryEvidence {
        scenario_id: m["scenario_id"].as_str().unwrap().into(),
        manifest_sha256: format!("{:x}", Sha256::digest(MANIFEST)),
        revision: String::from_utf8(revision.stdout).unwrap().trim().into(),
        sample_count: 1,
        fault_at_ms,
        detection,
        ownership_observed_at_ms: lease_event["at_ms"].as_u64().unwrap(),
        first_correct_query_at_ms,
        query_frontier,
        query_rows,
        docker_command_ms,
        before,
        after,
        sink_records_before,
        control_lease: manager
            .leases()
            .into_iter()
            .find(|l| l.shard_id == ShardId(ID))
            .unwrap(),
        sink_records: consume(&bootstrap),
    };
    save(
        &artifact_dir.join("worker-failure-kafka-minio-v1.json"),
        &evidence,
    );
    assert_eq!(validate(&evidence), Ok(()));
    if let Ok(candidate) = std::env::var("ROCKSTREAM_QUALIFICATION_CANDIDATE") {
        let candidate = serde_json::from_str(&candidate).unwrap();
        let records = qualification_records(&evidence, &candidate);
        assert_eq!(records["application"]["status"], "PASSED");
        for (name, reason) in [
            ("missing_event", "MissingObservation"),
            ("constant_timings", "Timings"),
            ("stale_query", "Progress"),
            ("changed_value", "ViewOutput"),
            ("duplicate_output", "SinkOutput"),
            ("helper_only_status", "Ownership"),
        ] {
            assert_eq!(records["negative_controls"][name]["status"], "FAILED");
            assert_eq!(records["negative_controls"][name]["error"], reason);
        }
        save(&artifact_dir.join("qualification-records.json"), &records);
    }
    // Negative controls mutate the actual returned observations, one at a time.
    let mut wrong = evidence.clone();
    wrong.after.weighted_output[3][2][1]["Utf8"] = json!("wrong");
    assert_eq!(validate(&wrong), Err(RecoveryFailure::ViewOutput));
    let mut duplicate = evidence.clone();
    duplicate
        .sink_records
        .as_array_mut()
        .unwrap()
        .push(evidence.sink_records[2].clone());
    assert_eq!(validate(&duplicate), Err(RecoveryFailure::SinkOutput));
    let mut stalled = evidence.clone();
    stalled.after.view_frontier = 4;
    assert_eq!(validate(&stalled), Err(RecoveryFailure::Progress));
    let mut late = evidence.clone();
    late.first_correct_query_at_ms = late.fault_at_ms + 60_001;
    late.after.observed_at_ms = late.first_correct_query_at_ms;
    assert_eq!(validate(&late), Err(RecoveryFailure::QueryDeadline));
    fs::write(artifact_dir.join("real-cluster-chaos-summary.md"),format!("# Worker recovery: {}\n\nRevision: `{}`; manifest SHA-256: `{}`.\n\nSamples: 1 (no percentile estimate).\n\nFault to control disconnect detection: {} ms.\nFault to replacement lease observation: {} ms.\nFault to correct view query at frontier 5: {} ms.\nDocker info command: {} ms. Worker kill/wait command: {} ms.\n\nComplete observations: `worker-failure-kafka-minio-v1.json`.\nAll four negative controls rejected for their intended reasons.\n",evidence.scenario_id,evidence.revision,evidence.manifest_sha256,evidence.detection.timestamp_ms-fault_at_ms,evidence.ownership_observed_at_ms-fault_at_ms,first_correct_query_at_ms-fault_at_ms,docker_command_ms,worker_kill_command_ms)).unwrap();
    drop(workers);
    control.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_worker_write_and_frontier_report_are_rejected_after_reassignment() {
    let info = Command::new("docker")
        .arg("info")
        .output()
        .expect("Docker required");
    assert!(info.status.success(), "Docker required");
    let artifact_dir = std::env::var_os("ROCKSTREAM_CHAOS_ARTIFACT_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/real-cluster-chaos")
        });
    fs::create_dir_all(&artifact_dir).unwrap();
    let root = tempfile::tempdir().unwrap();
    let manifest: Value = serde_json::from_str(STALE_OWNER_MANIFEST).unwrap();
    let source_topic = manifest["source_topic"].as_str().unwrap();
    let sink_topic = manifest["sink_topic"].as_str().unwrap();
    let stale_source_group = manifest["stale_source_group"].as_str().unwrap();
    let recipient_source_group = manifest["recipient_source_group"].as_str().unwrap();
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
    let bucket = "worker-stale-owner";
    let (_minio, port) = start_minio(bucket).await.expect("MinIO required");
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();
    for topic in [source_topic, sink_topic] {
        assert_eq!(
            admin
                .create_topics(
                    &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
                    &AdminOptions::new()
                )
                .await
                .unwrap(),
            vec![Ok(topic.to_owned())]
        );
    }
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();
    for row in manifest["history"].as_array().unwrap() {
        producer
            .send(
                FutureRecord::to(source_topic)
                    .key("")
                    .payload(&row.to_string()),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
    }
    let manager = ShardManager::new();
    let audit = Arc::new(FileAuditLog::open(root.path().join("audit.jsonl")).unwrap());
    let catalog = TopologyCatalog::new();
    let frontier = Arc::new(FrontierAggregator::new());
    let control = ControlService::new(catalog.clone())
        .with_shard_manager(manager.clone())
        .with_frontier(frontier.clone())
        .with_audit(audit)
        .start("127.0.0.1:0")
        .await
        .unwrap();
    let mut workers = Workers(Vec::new());
    for id in [1, 2] {
        let log =
            fs::File::create(artifact_dir.join(format!("stale-owner-worker-{id}.log"))).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap());
        child
            .args(["worker_process_fixture", "--exact", "--nocapture"])
            .env("ROCKSTREAM_RECOVERY_WORKER_ROOT", root.path())
            .env("ROCKSTREAM_RECOVERY_WORKER_ID", id.to_string())
            .env("ROCKSTREAM_RECOVERY_CONTROL", control.addr.to_string())
            .env("ROCKSTREAM_RECOVERY_KAFKA", &bootstrap)
            .env("ROCKSTREAM_RECOVERY_SOURCE_TOPIC", source_topic)
            .env("ROCKSTREAM_RECOVERY_SINK_TOPIC", sink_topic)
            .env(
                "ROCKSTREAM_RECOVERY_SOURCE_GROUP",
                if id == 1 {
                    stale_source_group
                } else {
                    recipient_source_group
                },
            )
            .env("ROCKSTREAM_RECOVERY_STALE_OWNER", "1")
            .env(
                "ROCKSTREAM_OBJECT_STORE_ENDPOINT",
                format!("http://127.0.0.1:{port}"),
            )
            .env("ROCKSTREAM_OBJECT_STORE_BUCKET", bucket)
            .env("ROCKSTREAM_OBJECT_STORE_REGION", "us-east-1")
            .env("ROCKSTREAM_OBJECT_STORE_ACCESS_KEY", MINIO_USER)
            .env("ROCKSTREAM_OBJECT_STORE_SECRET_KEY", MINIO_PASS)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log));
        workers.0.push(child.spawn().unwrap());
    }
    let stale_ready = read_json(&root.path().join("stale-ready.json"), &mut workers.0[0]).await;
    let before: WorkerObservation = serde_json::from_value(stale_ready["before"].clone()).unwrap();
    assert_eq!(before.source_epoch, 2);
    let old_token = before.lease.lease_token.0;
    let stale_group_offset_at_gate =
        committed_group_offset(&bootstrap, stale_source_group, source_topic);
    assert_eq!(stale_group_offset_at_gate, 2);

    let (forced_lease, evicted) = manager.force_acquire(ShardId(ID), WorkerId(2));
    assert_eq!(evicted, Some(WorkerId(1)));
    assert!(forced_lease.lease_token.0 > old_token);
    let reassigned_at_ms = now_ms();
    let recipient_request_at_ms = now_ms();
    save(
        &root.path().join("request-worker-2"),
        &json!({"at_ms": recipient_request_at_ms}),
    );
    let recipient_lease =
        read_json(&root.path().join("worker-2-lease.json"), &mut workers.0[1]).await;
    let new_token = recipient_lease["lease"][0]["lease_token"]
        .as_u64()
        .or_else(|| recipient_lease["lease"]["lease_token"].as_u64())
        .unwrap();
    assert!(new_token > forced_lease.lease_token.0);
    assert!(new_token > old_token);
    let after_b: WorkerObservation =
        read_observation(&root.path().join("recovered.json"), &mut workers.0[1]).await;
    assert_eq!(after_b.source_epoch, 5);
    assert_eq!(after_b.view_frontier, 5);
    assert!(after_b.observed_at_ms >= recipient_lease["at_ms"].as_u64().unwrap());
    assert_eq!(
        after_b.weighted_output,
        manifest["expected_weighted_output"]
    );
    assert_eq!(after_b.relation, manifest["expected_relation"]);
    tokio::time::timeout(Duration::from_secs(15), async {
        while frontier.cluster_frontier().epoch != Some(5) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("recipient frontier report deadline");
    let recipient_group_offset_after_b =
        committed_group_offset(&bootstrap, recipient_source_group, source_topic);
    assert_eq!(recipient_group_offset_after_b, 5);

    let fault_at_ms = now_ms();
    assert!(after_b.observed_at_ms <= fault_at_ms);
    save(
        &root.path().join("release-stale-owner"),
        &json!({"at_ms": fault_at_ms}),
    );
    let stale_result = read_json(
        &root.path().join("stale-owner-result.json"),
        &mut workers.0[0],
    )
    .await;
    assert_eq!(stale_result["fence_valid"], false);
    assert_eq!(stale_result["source_commit"], "not_attempted");
    assert_eq!(
        stale_result["frontier_report_error"],
        "RS-8004: frontier report lacks the current shard lease"
    );
    assert!(stale_result["at_ms"].as_u64().unwrap() >= fault_at_ms);
    assert_eq!(stale_result["stale_frontier_report_epoch"], 99);
    assert_eq!(stale_result["fence_after_report"], false);
    assert_eq!(frontier.cluster_frontier().epoch, Some(5));
    let stale_group_offset_after_attempt =
        committed_group_offset(&bootstrap, stale_source_group, source_topic);
    assert_eq!(stale_group_offset_after_attempt, 2);
    let recipient_group_offset_after_stale =
        committed_group_offset(&bootstrap, recipient_source_group, source_topic);
    assert_eq!(recipient_group_offset_after_stale, 5);

    save(
        &root.path().join("observe-after-stale"),
        &json!({"at_ms": now_ms()}),
    );
    let after_stale: WorkerObservation =
        read_observation(&root.path().join("after-stale.json"), &mut workers.0[1]).await;
    assert_eq!(after_stale.source_epoch, after_b.source_epoch);
    assert_eq!(after_stale.source_offsets, after_b.source_offsets);
    assert_eq!(after_stale.view_frontier, after_b.view_frontier);
    assert_eq!(after_stale.weighted_output, after_b.weighted_output);
    assert_eq!(after_stale.relation, after_b.relation);
    assert_eq!(after_stale.sink_progress, after_b.sink_progress);
    let current_lease = manager.get(ShardId(ID)).unwrap();
    assert_eq!(current_lease.worker_id, WorkerId(2));
    assert_eq!(current_lease.lease_token.0, new_token);

    let store = Arc::new(object_store::prefix::PrefixStore::new(
        minio_object_store(port, bucket),
        format!("shards/{ID}"),
    ));
    let reader = Arc::new(ShardReader::open("db", store).await.unwrap());
    let frontier_bytes = reader
        .get(&ShardKeyEncoder::frontier_key())
        .await
        .unwrap()
        .unwrap();
    let durable_frontier = u64::from_be_bytes(frontier_bytes.as_ref().try_into().unwrap());
    assert_eq!(durable_frontier, 5);
    let query_rows = HotOnlyViewReader {
        shard_reader: reader,
        frontier_epoch: Some(durable_frontier),
    }
    .read_view("letters", None, ViewReadStrategy::HotOnly)
    .await
    .unwrap()
    .into_iter()
    .map(|row| String::from_utf8(row).unwrap())
    .collect::<Vec<_>>();
    let expected_query_rows: Vec<String> =
        serde_json::from_value(manifest["expected_query_rows"].clone()).unwrap();
    assert_eq!(query_rows, expected_query_rows);
    let sink_records = consume_topic(&bootstrap, sink_topic);
    assert_eq!(sink_records, manifest["expected_sink"]);

    let revision = std::env::var("ROCKSTREAM_QUALIFICATION_REVISION")
        .or_else(|_| std::env::var("GITHUB_SHA"))
        .unwrap_or_else(|_| {
            let revision = Command::new("git")
                .args(["rev-parse", "HEAD"])
                .output()
                .unwrap();
            assert!(revision.status.success());
            String::from_utf8(revision.stdout)
                .unwrap()
                .trim()
                .to_owned()
        });
    let evidence = json!({
        "scenario_id": manifest["scenario_id"],
        "replay_seed": manifest["replay_seed"],
        "manifest_sha256": format!("{:x}", Sha256::digest(STALE_OWNER_MANIFEST)),
        "revision": revision,
        "control_service_revision": revision,
        "kafka_image": manifest["kafka_image"],
        "minio_image": manifest["minio_image"],
        "source_topic": source_topic,
        "sink_topic": sink_topic,
        "stale_source_group": stale_source_group,
        "recipient_source_group": recipient_source_group,
        "fault_at_ms": fault_at_ms,
        "owner_a": {
            "worker_id": 1,
            "process_id": stale_ready["process_id"],
            "lease_token": old_token,
            "prepared_epoch": stale_ready["epoch"],
            "at_ms": stale_ready["at_ms"],
        },
        "owner_b": {
            "worker_id": 2,
            "process_id": after_b.process_id,
            "lease_token": new_token,
            "forced_lease_token": forced_lease.lease_token.0,
            "observed_at_ms": recipient_lease["at_ms"],
        },
        "control_transition": {
            "evicted_worker_id": evicted,
            "forced_lease_token": forced_lease.lease_token.0,
            "reassigned_at_ms": reassigned_at_ms,
            "recipient_request_at_ms": recipient_request_at_ms,
            "recipient_lease_token": new_token,
            "recipient_committed_at_ms": after_b.observed_at_ms,
            "stale_attempt_released_at_ms": fault_at_ms,
            "stale_attempt_observed_at_ms": stale_result["at_ms"],
        },
        "stale_group_offset_at_gate": stale_group_offset_at_gate,
        "stale_group_offset_after_attempt": stale_group_offset_after_attempt,
        "recipient_group_offset_after_owner_b": recipient_group_offset_after_b,
        "recipient_group_offset_after_stale_attempt": recipient_group_offset_after_stale,
        "stale_attempt": stale_result,
        "fence_response": {
            "type": "FenceAck",
            "shard_id": ID,
            "lease_token": old_token,
            "valid": false,
        },
        "frontier_after_owner_b": 5,
        "frontier_after_stale_report": frontier.cluster_frontier().epoch,
        "durable_frontier_after_stale_attempt": durable_frontier,
        "before": before,
        "after_owner_b": after_b,
        "after_stale_attempt": after_stale,
        "query_rows": query_rows,
        "sink_records": sink_records,
        "worker_log_sha256": {
            "worker_1": format!("{:x}", Sha256::digest(fs::read(artifact_dir.join("stale-owner-worker-1.log")).unwrap())),
            "worker_2": format!("{:x}", Sha256::digest(fs::read(artifact_dir.join("stale-owner-worker-2.log")).unwrap())),
        },
    });
    let evidence_path = artifact_dir.join("worker-stale-owner-kafka-minio-v1.json");
    save(&evidence_path, &evidence);
    fs::write(
        artifact_dir.join("stale-owner-summary.md"),
        format!(
            "# Stale owner qualification\n\nScenario: `{}`. Revision: `{}`.\n\nWorker A (PID {}) held token {}; worker B (PID {}) acquired token {}. Worker A's delayed epoch-3 write was rejected, its source ACK was not attempted, and frontier 99 was rejected after B committed through epoch 5. A's source group stayed at offset {}; B's independent group reached offset {}.\n\nEvidence: `{}`. Worker logs are retained with SHA-256 values in the evidence.\n\nScope: real Kafka + MinIO, two worker processes, control-plane forced reassignment. The workers use separate Kafka groups to allow concurrent source progress; this does not exercise consumer-group rebalance behavior or claim all split-brain schedules.\n",
            manifest["scenario_id"],
            evidence["revision"],
            stale_ready["process_id"],
            old_token,
            after_b.process_id,
            new_token,
            stale_group_offset_after_attempt,
            recipient_group_offset_after_stale,
            evidence_path.file_name().unwrap().to_string_lossy(),
        ),
    )
    .unwrap();
    drop(workers);
    control.shutdown();
}
