//! #126: one real worker loss, with independently checked view and Kafka output.
#![cfg(feature = "docker_tests")]

use arrow::datatypes::{DataType, Field, Schema};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use rockstream_connectors::{
    KafkaSink, KafkaSource, OffsetToken, SourceCheckpointStore, SourceRuntimeCoordinator,
};
use rockstream_control::{audit::FileAuditLog, ControlService, ShardManager, TopologyCatalog};
use rockstream_gateway::view_reader::{HotOnlyViewReader, ViewReadStrategy, ViewReader};
use rockstream_ops::{
    compile_plan_with_sink_id, kafka_sink::KafkaViewSink, sink::write_view_directory_entry,
    ArrowZSet,
};
use rockstream_plan::PlanNode;
use rockstream_runtime::start_worker_client;
use rockstream_sim::qualification::worker_failure::{
    validate, RecoveryFailure, WorkerObservation, WorkerRecoveryEvidence, MANIFEST,
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

async fn read_observation(path: &Path, child: &mut Child) -> WorkerObservation {
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
        "worker-failure-input",
        "worker-failure-source",
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
        KafkaSink::connect(ConnectorId(ID), &bootstrap, "worker-failure-output").unwrap(),
        db.clone(),
        OperatorId(ID),
        2,
    );
    if id == 2 {
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
        assert!(worker
            .check_fence_write(ShardId(ID), lease.lease_token)
            .await
            .unwrap());
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
        source
            .commit_epoch(&owner, epoch, delta.new_offset, writes)
            .await
            .unwrap();
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
