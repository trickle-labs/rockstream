//! #126: one real worker loss, with independently checked view and Kafka output.
#![cfg(feature = "docker_tests")]

use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    consumer::{BaseConsumer, Consumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Offset, TopicPartitionList,
};
use rockstream_connectors::{
    KafkaSink, KafkaSource, OffsetToken, PollDeltaResult, SnapshotDeltaFence, SnapshotStream,
    SourceCheckpointStore, SourceConnector, SourceError, SourceRuntimeCoordinator,
    WatermarkCapability,
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
    qualification_records, validate, RecoveryFailure, WorkerObservation, WorkerRecoveryEvidence,
    MANIFEST,
};
use rockstream_storage::{keys::ShardKeyEncoder, ShardReader, WriteBatch};
use rockstream_test_support::minio::{minio_object_store, start_minio, MINIO_PASS, MINIO_USER};
use rockstream_types::{
    arrow_batch::split_weight_column,
    connector::PartitionFilter,
    ids::{ConnectorId, OperatorId, ShardId, WorkerId},
    timestamp::Epoch,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::kafka::apache::{self, KAFKA_PORT};

#[path = "common/recovery.rs"]
mod recovery;
use recovery::{consume, consume_topic, now_ms, observe, save};

const ID: u64 = 126;
const WAIT: Duration = Duration::from_secs(60);
const ACK_GAP_MANIFEST: &str =
    include_str!("../../../docs/testing/worker-source-ack-gap-kafka-minio-v1.json");

struct AckBoundarySource {
    inner: KafkaSource,
    root: PathBuf,
    worker_id: u64,
    source_group: String,
    source_topic: String,
    enabled: bool,
    skip_recovered_offset: bool,
}

#[async_trait]
impl SourceConnector for AckBoundarySource {
    fn discover_schema(&self) -> Result<arrow::datatypes::SchemaRef, SourceError> {
        self.inner.discover_schema()
    }

    async fn capture_snapshot_delta_fence(
        &mut self,
        partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotDeltaFence, SourceError> {
        self.inner
            .capture_snapshot_delta_fence(partition_filter)
            .await
    }

    async fn start_snapshot(
        &mut self,
        fence: &SnapshotDeltaFence,
        after: Option<OffsetToken>,
        partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError> {
        self.inner
            .start_snapshot(fence, after, partition_filter)
            .await
    }

    async fn start_snapshot_bounded(
        &mut self,
        fence: &SnapshotDeltaFence,
        after: Option<OffsetToken>,
        partition_filter: Option<PartitionFilter>,
        max_rows: usize,
    ) -> Result<SnapshotStream, SourceError> {
        self.inner
            .start_snapshot_bounded(fence, after, partition_filter, max_rows)
            .await
    }

    async fn poll_delta(
        &mut self,
        after: OffsetToken,
        max_bytes: usize,
        credits_available: usize,
        partition_filter: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        self.inner
            .poll_delta(after, max_bytes, credits_available, partition_filter)
            .await
    }

    async fn commit_offset(
        &mut self,
        epoch: Epoch,
        offset: OffsetToken,
    ) -> Result<(), SourceError> {
        if self.enabled && self.worker_id == 1 && epoch == 3 {
            save(
                &self.root.join("ack-gap-pending.json"),
                &json!({"at_ms":now_ms(),"epoch":epoch,"offset":serde_json::from_slice::<Value>(offset.as_bytes()).unwrap(),"worker_id":self.worker_id,"process_id":std::process::id(),"source_group":self.source_group,"source_topic":self.source_topic}),
            );
            std::future::pending().await
        }
        if self.enabled && self.worker_id == 2 && epoch == 3 {
            let committed = if self.skip_recovered_offset {
                let mut offsets: std::collections::BTreeMap<u64, u64> =
                    serde_json::from_slice(offset.as_bytes()).unwrap();
                *offsets.get_mut(&0).unwrap() += 1;
                OffsetToken::new(serde_json::to_vec(&offsets).unwrap())
            } else {
                offset.clone()
            };
            self.inner.commit_offset(epoch, committed.clone()).await?;
            save(
                &self.root.join("ack-gap-recovered-ack.json"),
                &json!({"at_ms":now_ms(),"epoch":epoch,"requested_offset":serde_json::from_slice::<Value>(offset.as_bytes()).unwrap(),"committed_offset":serde_json::from_slice::<Value>(committed.as_bytes()).unwrap(),"worker_id":self.worker_id,"process_id":std::process::id(),"source_group":self.source_group,"source_topic":self.source_topic}),
            );
            if self.skip_recovered_offset {
                std::future::pending().await
            }
            while !self.root.join("ack-gap-ack-continue").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            return Ok(());
        }
        self.inner.commit_offset(epoch, offset).await
    }

    async fn pause(&mut self, _reason: String) -> Result<(), SourceError> {
        self.inner.pause();
        Ok(())
    }

    async fn resume(&mut self) -> Result<(), SourceError> {
        self.inner.resume();
        Ok(())
    }

    fn partition_filter_support(&self) -> bool {
        self.inner.partition_filter_support()
    }

    fn watermark_capability(&self) -> WatermarkCapability {
        self.inner.watermark_capability()
    }
}

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
    let ack_gap = std::env::var_os("ROCKSTREAM_RECOVERY_ACK_GAP").is_some();
    let ack_gap_manifest: Value = serde_json::from_str(ACK_GAP_MANIFEST).unwrap();
    let source_topic = std::env::var("ROCKSTREAM_ACK_GAP_SOURCE_TOPIC")
        .unwrap_or_else(|_| "worker-failure-input".into());
    let sink_topic = std::env::var("ROCKSTREAM_ACK_GAP_SINK_TOPIC")
        .unwrap_or_else(|_| "worker-failure-output".into());
    let source_group = std::env::var("ROCKSTREAM_ACK_GAP_SOURCE_GROUP")
        .unwrap_or_else(|_| "worker-failure-source".into());
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
        &source_topic,
        &source_group,
    )
    .unwrap();
    let mut source = SourceRuntimeCoordinator::new(
        AckBoundarySource {
            inner: source,
            root: root.to_path_buf(),
            worker_id: id,
            source_group: source_group.clone(),
            source_topic: source_topic.clone(),
            enabled: ack_gap,
            skip_recovered_offset: std::env::var_os("ROCKSTREAM_ACK_GAP_SKIP_OFFSET").is_some(),
        },
        ConnectorId(ID),
        OffsetToken::new(Vec::new()),
        SourceCheckpointStore::new(db.clone(), ID as u128, ConnectorId(ID)),
    );
    source.recover().await.unwrap();
    let owner = source.acquire_owner(format!("worker-{id}")).unwrap();
    if ack_gap && id == 1 {
        let observer = KafkaSource::connect(
            ConnectorId(ID),
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            &bootstrap,
            &source_topic,
            &format!("{source_group}-observer"),
        )
        .unwrap();
        let mut observer = SourceRuntimeCoordinator::new(
            observer,
            ConnectorId(ID),
            OffsetToken::new(Vec::new()),
            SourceCheckpointStore::new(db.clone(), ID as u128, ConnectorId(ID)),
        );
        let observed_db = db.clone();
        let observed_worker = worker.clone();
        let observed_root = root.to_path_buf();
        tokio::spawn(async move {
            while !observed_root.join("ack-gap-pending.json").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            observer.recover().await.unwrap();
            save(
                &observed_root.join("ack-gap-before.json"),
                &observe(&observed_db, &observed_worker, &observer).await,
            );
        });
    }
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
    if id == 2 {
        assert!(
            sink.deliver_next().await.unwrap(),
            "recover durable staged update"
        );
    }
    if ack_gap && id == 2 {
        source.acknowledge_recovered(&owner).await.unwrap();
    }
    let final_epoch = if ack_gap {
        ack_gap_manifest["required_frontier"].as_u64().unwrap()
    } else {
        5
    };
    while source.committed_epoch() < final_epoch {
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

#[derive(serde::Serialize)]
struct SourceAckGapEvidence {
    scenario_id: String,
    manifest_sha256: String,
    revision: String,
    kafka_image: String,
    minio_image: String,
    run_kind: String,
    source_type: String,
    sink_type: String,
    source_topic: String,
    sink_topic: String,
    source_group: String,
    fault_at_ms: u64,
    fault_boundary: Value,
    recovered_ack: Value,
    before: WorkerObservation,
    after: Option<WorkerObservation>,
    group_offset_before_fault: i64,
    group_offset_after_recovered_ack: i64,
    group_offset_after_recovery: Option<i64>,
    sink_records_before: Value,
    sink_records: Option<Value>,
    query_frontier: Option<u64>,
    query_rows: Option<Vec<String>>,
    oracle: Value,
}

#[derive(Debug, PartialEq, Eq)]
enum AckGapFailure {
    Identity,
    SourceAcknowledgement,
    Ownership,
    SourceProgress,
    BeforeView,
    AfterView,
    SinkOutput,
}

fn validate_source_ack_gap(
    e: &SourceAckGapEvidence,
    manifest: &Value,
) -> Result<(), AckGapFailure> {
    if e.scenario_id != manifest["scenario_id"]
        || e.manifest_sha256 != format!("{:x}", Sha256::digest(ACK_GAP_MANIFEST))
        || e.source_topic
            .strip_prefix(manifest["source_topic"].as_str().unwrap())
            .is_none()
        || e.sink_topic
            .strip_prefix(manifest["sink_topic"].as_str().unwrap())
            .is_none()
        || e.source_group
            .strip_prefix(manifest["source_group"].as_str().unwrap())
            .is_none()
        || e.fault_boundary["source_group"] != e.source_group
        || e.fault_boundary["source_topic"] != e.source_topic
        || e.recovered_ack["source_group"] != e.source_group
        || e.recovered_ack["source_topic"] != e.source_topic
        || e.oracle
            != json!({"history":manifest["history"],"expected_weighted_output":manifest["expected_weighted_output"],"expected_relation":manifest["expected_relation"],"expected_sink":manifest["expected_sink"]})
    {
        return Err(AckGapFailure::Identity);
    }
    if e.fault_boundary["epoch"] != 3
        || e.fault_boundary["offset"]["0"] != 3
        || e.fault_boundary["worker_id"] != 1
        || e.recovered_ack["requested_offset"]["0"] != 3
        || e.recovered_ack["committed_offset"]["0"] != 3
        || e.recovered_ack["epoch"] != 3
        || e.recovered_ack["worker_id"] != 2
        || e.group_offset_before_fault != 2
        || e.group_offset_after_recovered_ack != 3
        || e.group_offset_after_recovery != Some(6)
    {
        return Err(AckGapFailure::SourceAcknowledgement);
    }
    let Some(after) = &e.after else {
        return Err(AckGapFailure::SourceProgress);
    };
    if e.before.lease.worker_id.0 != 1
        || after.lease.worker_id.0 != 2
        || e.before.lease.lease_token.0 != 1
        || after.lease.lease_token.0 != 2
        || e.before.process_id == after.process_id
        || e.fault_boundary["process_id"] != e.before.process_id
        || e.recovered_ack["process_id"] != after.process_id
        || e.recovered_ack["at_ms"].as_u64().unwrap() < e.fault_at_ms
        || after.observed_at_ms < e.recovered_ack["at_ms"].as_u64().unwrap()
    {
        return Err(AckGapFailure::Ownership);
    }
    if e.before.source_epoch != 3
        || e.before.source_offsets != std::collections::BTreeMap::from([(0, 3)])
        || e.before.view_frontier != 3
        || e.after.as_ref().unwrap().source_epoch != 6
        || after.source_offsets != std::collections::BTreeMap::from([(0, 6)])
        || after.view_frontier != 6
        || e.query_frontier != Some(6)
    {
        return Err(AckGapFailure::SourceProgress);
    }
    if e.before.weighted_output
        != json!(&manifest["expected_weighted_output"].as_array().unwrap()[..3])
        || e.before.relation != json!([[[{"Int64":2},{"Utf8":"b"}],1]])
    {
        return Err(AckGapFailure::BeforeView);
    }
    if after.weighted_output != manifest["expected_weighted_output"]
        || after.relation != manifest["expected_relation"]
        || e.query_rows.as_deref() != Some(&[String::from("1\tc"), String::from("1\tc")])
    {
        return Err(AckGapFailure::AfterView);
    }
    let expected_before_sink = json!(&manifest["expected_sink"].as_array().unwrap()[..2]);
    if e.sink_records_before != expected_before_sink
        || e.sink_records.as_ref() != Some(&manifest["expected_sink"])
        || after.sink_progress
            != json!({"view":126,"columns":2,"topic":e.sink_topic,"snapshot_complete":true,"view_epoch":6,"transaction":6,"pending":null})
        || e.before.sink_progress
            != json!({"view":126,"columns":2,"topic":e.sink_topic,"snapshot_complete":true,"view_epoch":2,"transaction":2,"pending":null})
    {
        return Err(AckGapFailure::SinkOutput);
    }
    Ok(())
}

fn kafka_committed_offset(bootstrap: &str, group: &str, topic: &str) -> i64 {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(topic, 0);
    match consumer
        .committed_offsets(partitions, Duration::from_secs(5))
        .unwrap()
        .find_partition(topic, 0)
        .unwrap()
        .offset()
    {
        Offset::Offset(offset) => offset,
        Offset::Invalid => -1,
        offset => panic!("unexpected committed Kafka offset {offset:?}"),
    }
}

async fn wait_for_json(path: &Path, child: &mut Child) -> Value {
    let deadline = Instant::now() + WAIT;
    loop {
        if path.exists() {
            return serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "worker exited before {}",
            path.display()
        );
        assert!(
            Instant::now() < deadline,
            "deadline waiting for {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn run_source_ack_gap_case(
    bootstrap: &str,
    manifest: &Value,
    artifact_root: &Path,
    suffix: &str,
    skip_recovered_offset: bool,
) -> SourceAckGapEvidence {
    let source_topic = format!("{}-{suffix}", manifest["source_topic"].as_str().unwrap());
    let sink_topic = format!("{}-{suffix}", manifest["sink_topic"].as_str().unwrap());
    let source_group = format!("{}-{suffix}", manifest["source_group"].as_str().unwrap());
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    for topic in [&source_topic, &sink_topic] {
        assert_eq!(
            admin
                .create_topics(
                    &[NewTopic::new(topic.as_str(), 1, TopicReplication::Fixed(1))],
                    &AdminOptions::new()
                )
                .await
                .unwrap(),
            vec![Ok(topic.to_string())]
        );
    }
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    for row in manifest["history"].as_array().unwrap() {
        producer
            .send(
                FutureRecord::to(&source_topic)
                    .key("")
                    .payload(&row.to_string()),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
    }
    let bucket = format!("worker-source-ack-gap-{suffix}");
    let (_minio, port) = start_minio(&bucket).await.expect("MinIO required");
    let root = tempfile::tempdir().unwrap();
    let audit = Arc::new(FileAuditLog::open(root.path().join("audit.jsonl")).unwrap());
    let manager = ShardManager::new();
    let catalog = TopologyCatalog::new();
    let control = ControlService::new(catalog.clone())
        .with_shard_manager(manager.clone())
        .with_audit(audit.clone())
        .start("127.0.0.1:0")
        .await
        .unwrap();
    let artifact_dir = artifact_root.join(format!("source-ack-gap-{suffix}-{}", now_ms()));
    fs::create_dir_all(&artifact_dir).unwrap();
    let mut workers = Workers(Vec::new());
    for id in [1, 2] {
        let log = fs::File::create(artifact_dir.join(format!("worker-{id}.log"))).unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["worker_process_fixture", "--exact", "--nocapture"])
            .env("ROCKSTREAM_RECOVERY_WORKER_ROOT", root.path())
            .env("ROCKSTREAM_RECOVERY_WORKER_ID", id.to_string())
            .env("ROCKSTREAM_RECOVERY_CONTROL", control.addr.to_string())
            .env("ROCKSTREAM_RECOVERY_KAFKA", bootstrap)
            .env("ROCKSTREAM_RECOVERY_ACK_GAP", "1")
            .env("ROCKSTREAM_ACK_GAP_SOURCE_TOPIC", &source_topic)
            .env("ROCKSTREAM_ACK_GAP_SINK_TOPIC", &sink_topic)
            .env("ROCKSTREAM_ACK_GAP_SOURCE_GROUP", &source_group)
            .env_remove("ROCKSTREAM_ACK_GAP_SKIP_OFFSET")
            .env(
                "ROCKSTREAM_OBJECT_STORE_ENDPOINT",
                format!("http://127.0.0.1:{port}"),
            )
            .env("ROCKSTREAM_OBJECT_STORE_BUCKET", &bucket)
            .env("ROCKSTREAM_OBJECT_STORE_REGION", "us-east-1")
            .env("ROCKSTREAM_OBJECT_STORE_ACCESS_KEY", MINIO_USER)
            .env("ROCKSTREAM_OBJECT_STORE_SECRET_KEY", MINIO_PASS)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log));
        if skip_recovered_offset {
            command.env("ROCKSTREAM_ACK_GAP_SKIP_OFFSET", "1");
        }
        workers.0.push(command.spawn().unwrap());
    }
    let fault_boundary =
        wait_for_json(&root.path().join("ack-gap-pending.json"), &mut workers.0[0]).await;
    let before =
        read_observation(&root.path().join("ack-gap-before.json"), &mut workers.0[0]).await;
    let group_offset_before_fault = kafka_committed_offset(bootstrap, &source_group, &source_topic);
    let sink_records_before = consume_topic(bootstrap, &sink_topic);
    let fault_at_ms = fault_boundary["at_ms"].as_u64().unwrap();
    workers.0[0].kill().unwrap();
    workers.0[0].wait().unwrap();
    let recovered_ack = wait_for_json(
        &root.path().join("ack-gap-recovered-ack.json"),
        &mut workers.0[1],
    )
    .await;
    let group_offset_after_recovered_ack =
        kafka_committed_offset(bootstrap, &source_group, &source_topic);
    let after = if skip_recovered_offset {
        None
    } else {
        fs::write(root.path().join("ack-gap-ack-continue"), b"continue").unwrap();
        Some(read_observation(&root.path().join("recovered.json"), &mut workers.0[1]).await)
    };
    let (group_offset_after_recovery, sink_records, query_frontier, query_rows) = if after.is_some()
    {
        let offset = kafka_committed_offset(bootstrap, &source_group, &source_topic);
        let sink_records = consume_topic(bootstrap, &sink_topic);
        let store = Arc::new(object_store::prefix::PrefixStore::new(
            minio_object_store(port, &bucket),
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
        (
            Some(offset),
            Some(sink_records),
            Some(query_frontier),
            Some(query_rows),
        )
    } else {
        (None, None, None, None)
    };
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(revision.status.success());
    let evidence = SourceAckGapEvidence {
        scenario_id: manifest["scenario_id"].as_str().unwrap().into(),
        manifest_sha256: format!("{:x}", Sha256::digest(ACK_GAP_MANIFEST)),
        revision: String::from_utf8(revision.stdout).unwrap().trim().into(),
        kafka_image: "apache/kafka:3.8.0".into(),
        minio_image: "codemowers/minio:RELEASE.2025-09-07T16-13-09Z".into(),
        run_kind: if skip_recovered_offset {
            "unsafe_skip_offset_control"
        } else {
            "positive"
        }
        .into(),
        source_type: "KafkaSource".into(),
        sink_type: "KafkaViewSink with transactional KafkaSink".into(),
        source_topic,
        sink_topic,
        source_group,
        fault_at_ms,
        fault_boundary: fault_boundary.clone(),
        recovered_ack: recovered_ack.clone(),
        before,
        after,
        group_offset_before_fault,
        group_offset_after_recovered_ack,
        group_offset_after_recovery,
        sink_records_before,
        sink_records,
        query_frontier,
        query_rows,
        oracle: json!({"history":manifest["history"],"expected_weighted_output":manifest["expected_weighted_output"],"expected_relation":manifest["expected_relation"],"expected_sink":manifest["expected_sink"]}),
    };
    save(&artifact_dir.join("evidence.json"), &evidence);
    save(&artifact_dir.join("fault-boundary.json"), &fault_boundary);
    save(&artifact_dir.join("recovered-ack.json"), &recovered_ack);
    let evidence_digest = format!(
        "{:x}",
        Sha256::digest(fs::read(artifact_dir.join("evidence.json")).unwrap())
    );
    let log_digests = [1, 2].map(|id| {
        let path = artifact_dir.join(format!("worker-{id}.log"));
        (
            path.file_name().unwrap().to_string_lossy().to_string(),
            format!("{:x}", Sha256::digest(fs::read(path).unwrap())),
        )
    });
    save(
        &artifact_dir.join("artifact-digests.json"),
        &json!({"evidence_sha256":evidence_digest,"worker_log_sha256":log_digests}),
    );
    evidence
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_ack_gap_recovery_from_kafka_and_minio() {
    let info = Command::new("docker")
        .arg("info")
        .output()
        .expect("Docker required");
    assert!(
        info.status.success(),
        "Docker required: {}",
        String::from_utf8_lossy(&info.stderr)
    );
    let artifact_root = std::env::var_os("ROCKSTREAM_CHAOS_ARTIFACT_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/real-cluster-chaos")
        });
    fs::create_dir_all(&artifact_root).unwrap();
    let manifest: Value = serde_json::from_str(ACK_GAP_MANIFEST).unwrap();
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
    let positive =
        run_source_ack_gap_case(&bootstrap, &manifest, &artifact_root, "positive", false).await;
    assert_eq!(validate_source_ack_gap(&positive, &manifest), Ok(()));
    let mutant =
        run_source_ack_gap_case(&bootstrap, &manifest, &artifact_root, "mutation", true).await;
    assert_eq!(
        validate_source_ack_gap(&mutant, &manifest),
        Err(AckGapFailure::SourceAcknowledgement)
    );
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
