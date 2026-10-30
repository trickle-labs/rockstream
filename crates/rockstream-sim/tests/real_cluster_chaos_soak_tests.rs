//! #126: one real worker loss, with independently checked view and Kafka output.
#![cfg(feature = "docker_tests")]
#![recursion_limit = "256"]

use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    consumer::{BaseConsumer, Consumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use rockstream_connectors::{
    KafkaSink, KafkaSource, OffsetToken, PollDeltaResult, SnapshotDeltaFence, SnapshotStream,
    SourceCheckpointStore, SourceConnector, SourceError, SourceRuntimeCoordinator,
    WatermarkCapability,
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
use rockstream_test_support::minio::{
    create_minio_bucket, minio_object_store, start_minio, MINIO_PASS, MINIO_USER,
};
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
const STALE_OWNER_MANIFEST: &str =
    include_str!("../../../docs/testing/worker-stale-owner-kafka-minio-v1.json");

fn committed_group_offset(bootstrap: &str, group: &str, topic: &str) -> i64 {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .create()
        .unwrap();
    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(topic, 0);
    let offsets = consumer
        .committed_offsets(partitions, Duration::from_secs(10))
        .unwrap();
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

fn consume_stale_owner_topic(bootstrap: &str, topic: &str) -> Value {
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
const ACK_GAP_MANIFEST: &str =
    include_str!("../../../docs/testing/worker-source-ack-gap-kafka-minio-v1.json");
const SINK_CURSOR_GAP_MANIFEST: &str =
    include_str!("../../../docs/testing/worker-sink-cursor-gap-kafka-minio-v1.json");
const ENOSPC_MANIFEST: &str =
    include_str!("../../../docs/testing/worker-state-enospc-kafka-minio-v1.json");
const ENOSPC_MINIO_IMAGE: &str = "codemowers/minio:RELEASE.2025-09-07T16-13-09Z";
const ENOSPC_TMPFS_BYTES: u64 = 64 * 1024 * 1024;

struct BoundedMinio {
    id: String,
    port: u16,
    log_path: PathBuf,
}

impl Drop for BoundedMinio {
    fn drop(&mut self) {
        if let Ok(logs) = Command::new("docker")
            .args(["logs", self.id.as_str()])
            .output()
        {
            let _ = fs::write(
                &self.log_path,
                format!(
                    "{}{}",
                    String::from_utf8_lossy(&logs.stdout),
                    String::from_utf8_lossy(&logs.stderr)
                ),
            );
        }
        let _ = Command::new("docker")
            .args(["rm", "--force", self.id.as_str()])
            .output();
    }
}

async fn start_bounded_minio(bucket: &str, run_id: &str, log_path: &Path) -> BoundedMinio {
    let name = format!("rockstream-enospc-minio-{run_id}");
    let tmpfs = format!("/data:rw,size={ENOSPC_TMPFS_BYTES},mode=0777");
    let output = Command::new("docker")
        .args([
            "run",
            "--detach",
            "--rm",
            "--name",
            name.as_str(),
            "--publish",
            "127.0.0.1::9000",
            "--tmpfs",
            tmpfs.as_str(),
            "--env",
            "MINIO_ROOT_USER=minioadmin",
            "--env",
            "MINIO_ROOT_PASSWORD=minioadmin",
            ENOSPC_MINIO_IMAGE,
            "server",
            "/data",
        ])
        .output()
        .expect("start bounded MinIO container");
    assert!(
        output.status.success(),
        "start bounded MinIO: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let id = String::from_utf8(output.stdout).unwrap().trim().to_owned();
    let mut container = BoundedMinio {
        id,
        port: 0,
        log_path: log_path.to_path_buf(),
    };
    let port_output = Command::new("docker")
        .args(["port", container.id.as_str(), "9000/tcp"])
        .output()
        .expect("read bounded MinIO port");
    assert!(
        port_output.status.success(),
        "read bounded MinIO port: {}",
        String::from_utf8_lossy(&port_output.stderr)
    );
    container.port = String::from_utf8(port_output.stdout)
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .rsplit(':')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let client = reqwest::Client::new();
    let ready_url = format!("http://127.0.0.1:{}/minio/health/ready", container.port);
    let deadline = Instant::now() + WAIT;
    loop {
        if client
            .get(&ready_url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "bounded MinIO readiness deadline"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    create_minio_bucket(container.port, bucket).await;
    container
}

fn docker_exec(container_id: &str, command: &[&str]) -> std::process::Output {
    let mut args = vec!["exec", container_id];
    args.extend_from_slice(command);
    Command::new("docker")
        .args(args)
        .output()
        .expect("run bounded MinIO container command")
}

fn minio_data_available_kib(df: &str) -> u64 {
    df.lines()
        .last()
        .unwrap()
        .split_whitespace()
        .nth(3)
        .unwrap()
        .parse()
        .unwrap()
}

fn fill_minio_data_tmpfs(container_id: &str) -> Value {
    let fill = docker_exec(
        container_id,
        &[
            "sh",
            "-c",
            "dd if=/dev/zero of=/data/.rockstream-enospc-pressure bs=1M",
        ],
    );
    let stdout = String::from_utf8_lossy(&fill.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&fill.stderr).into_owned();
    assert!(
        !fill.status.success() && stderr.contains("No space left on device"),
        "bounded MinIO tmpfs did not produce ENOSPC: status={:?}, stderr={stderr}",
        fill.status.code()
    );
    let df = docker_exec(container_id, &["df", "-Pk", "/data"]);
    assert!(df.status.success(), "df /data failed");
    let df = String::from_utf8_lossy(&df.stdout).into_owned();
    assert_eq!(
        minio_data_available_kib(&df),
        0,
        "MinIO data tmpfs has space"
    );
    json!({
        "path":"/data/.rockstream-enospc-pressure",
        "tmpfs_bytes":ENOSPC_TMPFS_BYTES,
        "fill_exit_code":fill.status.code(),
        "fill_stdout":stdout,
        "fill_stderr":stderr,
        "df_full":df
    })
}

fn restore_minio_data_tmpfs(container_id: &str) -> Value {
    let remove = docker_exec(
        container_id,
        &["rm", "-f", "/data/.rockstream-enospc-pressure"],
    );
    assert!(
        remove.status.success(),
        "remove MinIO tmpfs pressure file: {}",
        String::from_utf8_lossy(&remove.stderr)
    );
    let df = docker_exec(container_id, &["df", "-Pk", "/data"]);
    assert!(df.status.success(), "df /data after recovery failed");
    let df = String::from_utf8_lossy(&df.stdout).into_owned();
    let available_kib = minio_data_available_kib(&df);
    assert!(
        available_kib > 8 * 1024,
        "MinIO capacity was not restored: {df}"
    );
    json!({"available_kib":available_kib,"df_after_restore":df})
}

fn bounded_minio_logs(container_id: &str) -> String {
    let output = Command::new("docker")
        .args(["logs", container_id])
        .output()
        .expect("read bounded MinIO logs");
    assert!(output.status.success(), "read MinIO logs failed");
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

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
    let ack_gap = std::env::var_os("ROCKSTREAM_RECOVERY_ACK_GAP").is_some();
    let sink_cursor_gap = std::env::var_os("ROCKSTREAM_RECOVERY_SINK_CURSOR_GAP").is_some();
    let enospc_mode = std::env::var_os("ROCKSTREAM_RECOVERY_ENOSPC").is_some();
    let ack_gap_manifest: Value = serde_json::from_str(ACK_GAP_MANIFEST).unwrap();
    let sink_cursor_gap_manifest: Value = serde_json::from_str(SINK_CURSOR_GAP_MANIFEST).unwrap();
    let enospc_manifest: Value = serde_json::from_str(ENOSPC_MANIFEST).unwrap();
    let (source_topic, sink_topic, source_group) = if ack_gap {
        (
            std::env::var("ROCKSTREAM_ACK_GAP_SOURCE_TOPIC")
                .unwrap_or_else(|_| "worker-failure-input".into()),
            std::env::var("ROCKSTREAM_ACK_GAP_SINK_TOPIC")
                .unwrap_or_else(|_| "worker-failure-output".into()),
            std::env::var("ROCKSTREAM_ACK_GAP_SOURCE_GROUP")
                .unwrap_or_else(|_| "worker-failure-source".into()),
        )
    } else {
        (
            std::env::var("ROCKSTREAM_RECOVERY_SOURCE_TOPIC")
                .unwrap_or_else(|_| "worker-failure-input".to_owned()),
            std::env::var("ROCKSTREAM_RECOVERY_SINK_TOPIC")
                .unwrap_or_else(|_| "worker-failure-output".to_owned()),
            std::env::var("ROCKSTREAM_RECOVERY_SOURCE_GROUP")
                .unwrap_or_else(|_| "worker-failure-source".to_owned()),
        )
    };
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
    } else if enospc_mode && id == 2 {
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
    if id == 2 && !stale_owner_mode && !enospc_mode {
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
    } else if enospc_mode {
        enospc_manifest["required_frontier"].as_u64().unwrap()
    } else {
        5
    };
    let records_per_epoch = if enospc_mode {
        enospc_manifest["records_per_epoch"].as_u64().unwrap() as usize
    } else {
        1
    };
    let enospc_fault_epoch = enospc_manifest["fault_epoch"].as_u64().unwrap();
    while source.committed_epoch() < final_epoch {
        assert!(Instant::now() < deadline, "source progress deadline");
        let delta = source
            .poll_delta(64 * 1024, records_per_epoch)
            .await
            .unwrap();
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
        if enospc_mode && id == 1 && epoch == enospc_fault_epoch {
            save(
                &root.join("enospc-ready.json"),
                &json!({
                    "at_ms":now_ms(),
                    "process_id":std::process::id(),
                    "worker_id":id,
                    "epoch":epoch,
                    "attempted_source_offset":serde_json::from_slice::<Value>(delta.new_offset.as_bytes()).unwrap(),
                    "before":observe(&db,&worker,&source).await
                }),
            );
            let deadline = Instant::now() + Duration::from_secs(180);
            while !root.join("release-enospc").exists() {
                assert!(Instant::now() < deadline, "ENOSPC release deadline");
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
            assert!(
                frontier_report_error == "RS-8004: frontier report lacks the current shard lease",
                "M4-S5: stale frontier report returned {frontier_report_error}"
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
        let commit_result = source
            .commit_epoch(&owner, epoch, delta.new_offset, writes)
            .await;
        if enospc_mode && id == 1 && epoch == enospc_fault_epoch {
            let (outcome, error) = match commit_result {
                Ok(()) => ("unexpected_success", None),
                Err(error) => ("error", Some(error.to_string())),
            };
            save(
                &root.join("enospc-failure.json"),
                &json!({
                    "at_ms":now_ms(),
                    "process_id":std::process::id(),
                    "worker_id":id,
                    "epoch":epoch,
                    "outcome":outcome,
                    "error":error,
                    "after":observe(&db,&worker,&source).await
                }),
            );
            std::future::pending::<()>().await;
            return;
        }
        commit_result.unwrap();
        if stale_owner_mode {
            assert_eq!(
                worker
                    .report_shard_frontier(ShardId(ID), epoch)
                    .await
                    .unwrap(),
                epoch
            );
        }
        if id == 1 && epoch == 3 && !sink_cursor_gap {
            assert!(sink.stage_next().await.unwrap());
            save(
                &root.join("staged.json"),
                &observe(&db, &worker, &source).await,
            );
            // Keep the production transaction active until the parent kills us.
            std::future::pending::<()>().await;
        }
        if sink_cursor_gap
            && id == 1
            && epoch
                == sink_cursor_gap_manifest["fault_after_source_epoch"]
                    .as_u64()
                    .unwrap()
        {
            save(
                &root.join("sink-cursor-gap-before.json"),
                &observe(&db, &worker, &source).await,
            );
            assert!(sink.deliver_next().await.unwrap());
            panic!("sink cursor gap hook did not hold the worker");
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

async fn read_minio_view(port: u16, bucket: &str) -> (u64, Vec<String>, Value) {
    let store = Arc::new(object_store::prefix::PrefixStore::new(
        minio_object_store(port, bucket),
        "shards/126",
    ));
    let reader = Arc::new(ShardReader::open("db", store).await.unwrap());
    let frontier = reader
        .get(&ShardKeyEncoder::frontier_key())
        .await
        .unwrap()
        .unwrap();
    let frontier = u64::from_be_bytes(frontier.as_ref().try_into().unwrap());
    let sink_progress = reader.get(b"\x06kafka-view/126").await.unwrap().unwrap();
    let sink_progress = serde_json::from_slice(&sink_progress).unwrap();
    let rows = HotOnlyViewReader {
        shard_reader: reader,
        frontier_epoch: Some(frontier),
    }
    .read_view("letters", None, ViewReadStrategy::HotOnly)
    .await
    .unwrap()
    .into_iter()
    .map(|row| String::from_utf8(row).unwrap())
    .collect();
    (frontier, rows, sink_progress)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sink_transaction_cursor_gap_recovers_exactly_once() {
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
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/real-cluster-chaos")
        });
    fs::create_dir_all(&artifact_root).unwrap();
    let manifest: Value = serde_json::from_str(SINK_CURSOR_GAP_MANIFEST).unwrap();
    let suffix = now_ms().to_string();
    let source_topic = format!("{}-{suffix}", manifest["source_topic"].as_str().unwrap());
    let sink_topic = format!("{}-{suffix}", manifest["sink_topic"].as_str().unwrap());
    let source_group = format!("{}-{suffix}", manifest["source_group"].as_str().unwrap());
    let bucket = format!("worker-sink-cursor-gap-{suffix}");
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
    let (_minio, port) = start_minio(&bucket).await.expect("MinIO required");
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
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
        .set("bootstrap.servers", &bootstrap)
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
    let root = tempfile::tempdir().unwrap();
    let audit = Arc::new(FileAuditLog::open(root.path().join("audit.jsonl")).unwrap());
    let manager = ShardManager::new();
    let control = ControlService::new(TopologyCatalog::new())
        .with_shard_manager(manager.clone())
        .with_audit(audit.clone())
        .start("127.0.0.1:0")
        .await
        .unwrap();
    let artifact_dir = artifact_root.join(format!("sink-cursor-gap-{suffix}"));
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
            .env("ROCKSTREAM_RECOVERY_KAFKA", &bootstrap)
            .env("ROCKSTREAM_RECOVERY_SOURCE_TOPIC", &source_topic)
            .env("ROCKSTREAM_RECOVERY_SINK_TOPIC", &sink_topic)
            .env("ROCKSTREAM_RECOVERY_SOURCE_GROUP", &source_group)
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
        if id == 1 {
            command
                .env("ROCKSTREAM_RECOVERY_SINK_CURSOR_GAP", "1")
                .env("ROCKSTREAM_TEST_SINK_CURSOR_GAP_ROOT", root.path())
                .env(
                    "ROCKSTREAM_TEST_SINK_CURSOR_GAP_EPOCH",
                    manifest["fault_sink_transaction_epoch"]
                        .as_u64()
                        .unwrap()
                        .to_string(),
                );
        } else {
            command
                .env_remove("ROCKSTREAM_RECOVERY_SINK_CURSOR_GAP")
                .env_remove("ROCKSTREAM_TEST_SINK_CURSOR_GAP_ROOT")
                .env_remove("ROCKSTREAM_TEST_SINK_CURSOR_GAP_EPOCH");
        }
        workers.0.push(command.spawn().unwrap());
    }
    let fault_boundary = wait_for_json(
        &root.path().join("sink-transaction-committed.json"),
        &mut workers.0[0],
    )
    .await;
    let before = read_observation(
        &root.path().join("sink-cursor-gap-before.json"),
        &mut workers.0[0],
    )
    .await;
    let fault_at_ms = fault_boundary["committed_at_ms"].as_u64().unwrap();
    let fault_group_offset = kafka_committed_offset(&bootstrap, &source_group, &source_topic);
    let sink_records_at_fault = consume_topic(&bootstrap, &sink_topic);
    let (fault_query_frontier, fault_query_rows, fault_sink_progress) =
        read_minio_view(port, &bucket).await;
    let expected_fault_query_rows: Vec<String> = manifest["query_rows_at_fault"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row.as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        fault_boundary["epoch"],
        manifest["fault_sink_transaction_epoch"]
    );
    assert_eq!(fault_boundary["process_id"], before.process_id);
    assert_eq!(before.lease.worker_id.0, 1);
    assert_eq!(before.source_epoch, manifest["fault_after_source_epoch"]);
    assert_eq!(
        before.source_offsets,
        std::collections::BTreeMap::from([(0, 3)])
    );
    assert_eq!(before.view_frontier, manifest["frontier_at_fault"]);
    assert_eq!(
        before.weighted_output,
        json!(&manifest["expected_weighted_output"].as_array().unwrap()[..3])
    );
    assert_eq!(before.relation, manifest["relation_at_fault"]);
    assert_eq!(fault_sink_progress["view"], manifest["view_id"]);
    assert_eq!(fault_sink_progress["columns"], 2);
    assert_eq!(fault_sink_progress["topic"], sink_topic);
    assert_eq!(fault_sink_progress["snapshot_complete"], true);
    assert_eq!(
        fault_sink_progress["transaction"],
        manifest["sink_cursor_at_fault"]["transaction"]
    );
    assert_eq!(
        fault_sink_progress["view_epoch"],
        manifest["sink_cursor_at_fault"]["view_epoch"]
    );
    assert!(!fault_sink_progress["pending"].is_null());
    assert_eq!(fault_group_offset, manifest["source_offset_at_fault"]);
    assert_eq!(fault_query_frontier, manifest["frontier_at_fault"]);
    assert_eq!(fault_query_rows, expected_fault_query_rows);
    assert_eq!(sink_records_at_fault, manifest["sink_records_at_fault"]);

    let kill_started = Instant::now();
    workers.0[0].kill().unwrap();
    workers.0[0].wait().unwrap();
    let worker_kill_ms = kill_started.elapsed().as_millis() as u64;
    let after = read_observation(&root.path().join("recovered.json"), &mut workers.0[1]).await;
    let group_offset_after_recovery =
        kafka_committed_offset(&bootstrap, &source_group, &source_topic);
    let sink_records_after_recovery = consume_topic(&bootstrap, &sink_topic);
    let (query_frontier, query_rows, recovered_sink_progress) =
        read_minio_view(port, &bucket).await;
    let expected_query_rows: Vec<String> = manifest["query_rows_after_recovery"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row.as_str().unwrap().to_owned())
        .collect();
    let query_at_ms = now_ms();
    let lease_event: Value =
        serde_json::from_slice(&fs::read(root.path().join("worker-2-lease.json")).unwrap())
            .unwrap();
    let detection = audit
        .read_all()
        .unwrap()
        .into_iter()
        .find(|event| event.action == "worker.disconnected")
        .expect("control must detect killed worker");
    let detection_ms = detection.timestamp_ms.saturating_sub(fault_at_ms);
    let ownership_ms = lease_event["at_ms"]
        .as_u64()
        .unwrap()
        .saturating_sub(fault_at_ms);
    let query_ms = query_at_ms.saturating_sub(fault_at_ms);
    assert_eq!(after.lease.worker_id.0, 2);
    assert!(after.lease.lease_token.0 > before.lease.lease_token.0);
    assert_ne!(after.process_id, before.process_id);
    assert_eq!(after.source_epoch, manifest["required_frontier"]);
    assert_eq!(
        after.source_offsets,
        std::collections::BTreeMap::from([(0, 5)])
    );
    assert_eq!(after.view_frontier, manifest["required_frontier"]);
    assert_eq!(after.weighted_output, manifest["expected_weighted_output"]);
    assert_eq!(after.relation, manifest["expected_relation"]);
    assert_eq!(query_frontier, manifest["required_frontier"]);
    assert_eq!(query_rows, expected_query_rows);
    assert_eq!(
        group_offset_after_recovery,
        manifest["source_offset_after_recovery"]
    );
    assert_eq!(
        sink_records_after_recovery, manifest["expected_sink"],
        "P1-SINK-CURSOR-GAP: recovered Kafka sink output mismatch"
    );
    assert_eq!(
        after.sink_progress,
        json!({"view":126,"columns":2,"topic":sink_topic,"snapshot_complete":true,"view_epoch":5,"transaction":5,"pending":null})
    );
    assert_eq!(recovered_sink_progress, after.sink_progress);
    for (name, elapsed) in [
        ("detection", detection_ms),
        ("ownership", ownership_ms),
        ("query", query_ms),
    ] {
        assert!(
            elapsed <= manifest["limits_ms"][name].as_u64().unwrap(),
            "{name} deadline: {elapsed} ms"
        );
    }
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(revision.status.success());
    let evidence = json!({
        "scenario_id":manifest["scenario_id"],
        "manifest_sha256":format!("{:x}", Sha256::digest(SINK_CURSOR_GAP_MANIFEST)),
        "revision":String::from_utf8(revision.stdout).unwrap().trim(),
        "kafka_image":"apache/kafka:3.8.0",
        "minio_image":"codemowers/minio:RELEASE.2025-09-07T16-13-09Z",
        "source_type":"KafkaSource",
        "sink_type":"KafkaViewSink with transactional KafkaSink",
        "source_topic":source_topic,
        "sink_topic":sink_topic,
        "source_group":source_group,
        "bucket":bucket,
        "run_id":suffix,
        "replay_seed":manifest["replay_seed"],
        "fault_at_ms":fault_at_ms,
        "fault_boundary":fault_boundary,
        "detection":detection,
        "lease_event":lease_event,
        "before":before,
        "after":after,
        "worker_kill_ms":worker_kill_ms,
        "fault_group_offset":fault_group_offset,
        "group_offset_after_recovery":group_offset_after_recovery,
        "sink_records_at_fault":sink_records_at_fault,
        "sink_records_after_recovery":sink_records_after_recovery,
        "fault_query_frontier":fault_query_frontier,
        "fault_query_rows":fault_query_rows,
        "fault_sink_progress":fault_sink_progress,
        "query_frontier":query_frontier,
        "query_rows":query_rows,
        "recovered_sink_progress":recovered_sink_progress,
        "query_at_ms":query_at_ms,
        "elapsed_ms":{"detection":detection_ms,"ownership":ownership_ms,"query":query_ms},
        "oracle":{"history":manifest["history"],"expected_weighted_output":manifest["expected_weighted_output"],"expected_relation":manifest["expected_relation"],"expected_sink":manifest["expected_sink"]},
        "scope":manifest["scope"]
    });
    save(&artifact_dir.join("evidence.json"), &evidence);
    save(&artifact_dir.join("fault-boundary.json"), &fault_boundary);
    save(
        &artifact_dir.join("fault-sink-cursor.json"),
        &fault_sink_progress,
    );
    save(&artifact_dir.join("before-fault.json"), &before);
    save(&artifact_dir.join("after-recovery.json"), &after);
    let evidence_sha256 = format!(
        "{:x}",
        Sha256::digest(fs::read(artifact_dir.join("evidence.json")).unwrap())
    );
    let worker_log_sha256 = [1, 2].map(|id| {
        let path = artifact_dir.join(format!("worker-{id}.log"));
        (
            path.file_name().unwrap().to_string_lossy().to_string(),
            format!("{:x}", Sha256::digest(fs::read(path).unwrap())),
        )
    });
    let fault_artifact_sha256 = ["fault-boundary.json", "fault-sink-cursor.json"].map(|name| {
        (
            name,
            format!(
                "{:x}",
                Sha256::digest(fs::read(artifact_dir.join(name)).unwrap())
            ),
        )
    });
    save(
        &artifact_dir.join("artifact-digests.json"),
        &json!({"evidence_sha256":evidence_sha256,"fault_artifact_sha256":fault_artifact_sha256,"worker_log_sha256":worker_log_sha256}),
    );
    fs::write(
        artifact_dir.join("sink-cursor-gap-summary.md"),
        format!(
            "# Sink cursor gap recovery: {}\n\nRevision: `{}`; manifest SHA-256: `{}`; replay seed: {}.\n\nFault: Kafka transaction epoch {} committed before the MinIO cursor cleared its pending state. Replacement ownership and first correct query: {} ms.\n\nSource group offset: {} at fault, {} after recovery. Final frontier: {}; sink records match the independent oracle.\n\nKafka: `apache/kafka:3.8.0`; MinIO: `codemowers/minio:RELEASE.2025-09-07T16-13-09Z`. Scope: {}.\n\nComplete observations: `evidence.json`. Worker logs and fault-boundary records are SHA-256 indexed in `artifact-digests.json`.\n",
            manifest["scenario_id"].as_str().unwrap(),
            evidence["revision"].as_str().unwrap(),
            evidence["manifest_sha256"].as_str().unwrap(),
            manifest["replay_seed"],
            manifest["fault_sink_transaction_epoch"],
            query_ms,
            fault_group_offset,
            group_offset_after_recovery,
            query_frontier,
            manifest["scope"].as_str().unwrap()
        ),
    )
    .unwrap();
    drop(workers);
    control.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_state_enospc_withholds_source_ack_and_recovers_exactly() {
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
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/real-cluster-chaos")
        });
    fs::create_dir_all(&artifact_root).unwrap();
    let manifest: Value = serde_json::from_str(ENOSPC_MANIFEST).unwrap();
    let suffix = now_ms().to_string();
    let artifact_dir = artifact_root.join(format!("worker-state-enospc-{suffix}"));
    fs::create_dir_all(&artifact_dir).unwrap();
    let source_topic = format!("{}-{suffix}", manifest["source_topic"].as_str().unwrap());
    let sink_topic = format!("{}-{suffix}", manifest["sink_topic"].as_str().unwrap());
    let source_group = format!("{}-{suffix}", manifest["source_group"].as_str().unwrap());
    let bucket = format!("worker-state-enospc-{suffix}");
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
    let minio = start_bounded_minio(&bucket, &suffix, &artifact_dir.join("minio.log")).await;
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
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
        .set("bootstrap.servers", &bootstrap)
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
    let root = tempfile::tempdir().unwrap();
    let audit = Arc::new(FileAuditLog::open(root.path().join("audit.jsonl")).unwrap());
    let control = ControlService::new(TopologyCatalog::new())
        .with_shard_manager(ShardManager::new())
        .with_audit(audit.clone())
        .start("127.0.0.1:0")
        .await
        .unwrap();
    let spawn_worker = |id: u64, log_path: &Path| {
        let log = fs::File::create(log_path).unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["worker_process_fixture", "--exact", "--nocapture"])
            .env("ROCKSTREAM_RECOVERY_WORKER_ROOT", root.path())
            .env("ROCKSTREAM_RECOVERY_WORKER_ID", id.to_string())
            .env("ROCKSTREAM_RECOVERY_CONTROL", control.addr.to_string())
            .env("ROCKSTREAM_RECOVERY_KAFKA", &bootstrap)
            .env("ROCKSTREAM_RECOVERY_SOURCE_TOPIC", &source_topic)
            .env("ROCKSTREAM_RECOVERY_SINK_TOPIC", &sink_topic)
            .env("ROCKSTREAM_RECOVERY_SOURCE_GROUP", &source_group)
            .env("ROCKSTREAM_RECOVERY_ENOSPC", "1")
            .env(
                "ROCKSTREAM_OBJECT_STORE_ENDPOINT",
                format!("http://127.0.0.1:{}", minio.port),
            )
            .env("ROCKSTREAM_OBJECT_STORE_BUCKET", &bucket)
            .env("ROCKSTREAM_OBJECT_STORE_REGION", "us-east-1")
            .env("ROCKSTREAM_OBJECT_STORE_ACCESS_KEY", MINIO_USER)
            .env("ROCKSTREAM_OBJECT_STORE_SECRET_KEY", MINIO_PASS)
            .env_remove("ROCKSTREAM_RECOVERY_STALE_OWNER")
            .env_remove("ROCKSTREAM_RECOVERY_ACK_GAP")
            .env_remove("ROCKSTREAM_RECOVERY_SINK_CURSOR_GAP")
            .env_remove("ROCKSTREAM_ACK_GAP_SKIP_OFFSET")
            .env_remove("ROCKSTREAM_TEST_SINK_CURSOR_GAP_ROOT")
            .env_remove("ROCKSTREAM_TEST_SINK_CURSOR_GAP_EPOCH")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log));
        if id == 1 {
            command.env("ROCKSTREAM_TEST_ENOSPC_ROOT", root.path());
        }
        command.spawn().unwrap()
    };
    let mut workers = Workers(vec![spawn_worker(1, &artifact_dir.join("worker-1.log"))]);
    let ready = wait_for_json(&root.path().join("enospc-ready.json"), &mut workers.0[0]).await;
    let ready_group_offset = kafka_committed_offset(&bootstrap, &source_group, &source_topic);
    assert_eq!(ready["epoch"], manifest["fault_epoch"]);
    assert_eq!(
        ready["attempted_source_offset"],
        manifest["attempted_source_offset"]
    );
    assert_eq!(
        ready["before"]["source_epoch"],
        manifest["fault_epoch"].as_u64().unwrap() - 1
    );
    assert_eq!(ready_group_offset, manifest["source_offset_at_fault"]);

    let enospc_trigger = fill_minio_data_tmpfs(&minio.id);
    let enospc_at_ms = now_ms();
    save(&artifact_dir.join("enospc-trigger.json"), &enospc_trigger);
    fs::write(root.path().join("release-enospc"), b"ENOSPC confirmed").unwrap();
    let failure_path = root.path().join("enospc-failure.json");
    let failure_deadline = Instant::now() + Duration::from_secs(60);
    let failure: Value = loop {
        if let Ok(bytes) = fs::read(&failure_path) {
            if let Ok(failure) = serde_json::from_slice(&bytes) {
                break failure;
            }
        }
        if let Some(status) = workers.0[0].try_wait().unwrap() {
            panic!("worker exited before reporting ENOSPC: {status}");
        }
        assert!(
            Instant::now() < failure_deadline,
            "MinIO WAL write did not report ENOSPC before deadline"
        );
        let minio_log = bounded_minio_logs(&minio.id);
        if minio_log.contains(&format!(
            "API: PutObject(bucket={bucket}, object=shards/{ID}/db/wal/"
        )) && minio_log.contains(&format!("dstPath: shards/{ID}/db/wal/"))
            && minio_log.contains("drive path full")
        {
            break json!({
                "at_ms":now_ms(),
                "worker_process_id":workers.0[0].id(),
                "outcome":"pending_enospc",
                "error":"MinIO rejected the worker WAL PutObject with drive path full",
                "worker_pending":true
            });
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let fault_at_ms = failure["at_ms"].as_u64().unwrap();
    let fault_group_offset = kafka_committed_offset(&bootstrap, &source_group, &source_topic);
    let sink_records_at_fault = consume_topic(&bootstrap, &sink_topic);
    let fault_after: Option<WorkerObservation> = failure
        .get("after")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .unwrap();
    let before: WorkerObservation = serde_json::from_value(ready["before"].clone()).unwrap();
    let expected_fault_rows: Vec<String> = manifest["query_rows_before_fault"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row.as_str().unwrap().to_owned())
        .collect();
    let expected_fault_sink_progress = json!({
        "view":ID,
        "columns":2,
        "topic":sink_topic,
        "snapshot_complete":true,
        "view_epoch":manifest["sink_cursor_at_fault"]["view_epoch"],
        "transaction":manifest["sink_cursor_at_fault"]["transaction"],
        "pending":null
    });
    save(&artifact_dir.join("before-fault.json"), &ready);

    match failure["outcome"].as_str().unwrap() {
        "error" => {
            let storage_error = failure["error"].as_str().unwrap_or_default();
            assert!(
                storage_error.contains("No space left on device")
                    || storage_error.contains("Insufficient Storage")
                    || storage_error.contains("507")
                    || storage_error.contains("drive path full"),
                "source checkpoint failure did not expose ENOSPC: {storage_error}"
            );
        }
        "pending_enospc" => assert_eq!(failure["worker_pending"], true),
        outcome => panic!("unexpected ENOSPC outcome: {outcome}"),
    }
    assert_eq!(ready_group_offset, fault_group_offset);
    assert_eq!(fault_group_offset, manifest["source_offset_at_fault"]);
    assert_eq!(
        before.source_epoch,
        manifest["fault_epoch"].as_u64().unwrap() - 1
    );
    assert_eq!(
        before.source_offsets,
        std::collections::BTreeMap::from([(
            0,
            manifest["source_offset_at_fault"].as_u64().unwrap()
        )])
    );
    assert_eq!(before.view_frontier, manifest["frontier_before_fault"]);
    assert_eq!(
        before.weighted_output,
        manifest["weighted_output_before_fault"]
    );
    assert_eq!(before.relation, manifest["relation_before_fault"]);
    assert_eq!(sink_records_at_fault, manifest["sink_records_at_fault"]);
    if let Some(fault_after) = &fault_after {
        assert_eq!(fault_after.source_epoch, before.source_epoch);
        assert_eq!(fault_after.source_offsets, before.source_offsets);
        assert_eq!(fault_after.view_frontier, before.view_frontier);
        assert_eq!(fault_after.weighted_output, before.weighted_output);
        assert_eq!(fault_after.relation, before.relation);
        assert_eq!(fault_after.sink_progress, expected_fault_sink_progress);
    }

    let kill_started = Instant::now();
    workers.0[0].kill().unwrap();
    workers.0[0].wait().unwrap();
    let worker_kill_ms = kill_started.elapsed().as_millis() as u64;
    let capacity_restored = restore_minio_data_tmpfs(&minio.id);
    save(
        &artifact_dir.join("capacity-restored.json"),
        &capacity_restored,
    );
    let (fault_frontier, fault_rows, fault_sink_progress) =
        read_minio_view(minio.port, &bucket).await;
    assert_eq!(fault_frontier, manifest["frontier_before_fault"]);
    assert_eq!(fault_rows, expected_fault_rows);
    assert_eq!(fault_sink_progress, expected_fault_sink_progress);
    save(
        &artifact_dir.join("fault-state.json"),
        &json!({
            "failure":failure,
            "ready_group_offset":ready_group_offset,
            "fault_group_offset":fault_group_offset,
            "sink_records":sink_records_at_fault,
            "frontier":fault_frontier,
            "rows":fault_rows,
            "sink_progress":fault_sink_progress
        }),
    );
    workers
        .0
        .push(spawn_worker(2, &artifact_dir.join("worker-2.log")));
    let after: WorkerObservation = serde_json::from_value(
        wait_for_json(&root.path().join("recovered.json"), &mut workers.0[1]).await,
    )
    .unwrap();
    let group_offset_after_recovery =
        kafka_committed_offset(&bootstrap, &source_group, &source_topic);
    let sink_records_after_recovery = consume_topic(&bootstrap, &sink_topic);
    let (query_frontier, query_rows, recovered_sink_progress) =
        read_minio_view(minio.port, &bucket).await;
    let expected_query_rows: Vec<String> = manifest["query_rows_after_recovery"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row.as_str().unwrap().to_owned())
        .collect();
    let lease_event: Value =
        serde_json::from_slice(&fs::read(root.path().join("worker-2-lease.json")).unwrap())
            .unwrap();
    let detection = audit
        .read_all()
        .unwrap()
        .into_iter()
        .find(|event| event.action == "worker.disconnected")
        .expect("control must detect worker killed after ENOSPC");
    let detection_ms = detection.timestamp_ms.saturating_sub(fault_at_ms);
    let ownership_ms = lease_event["at_ms"]
        .as_u64()
        .unwrap()
        .saturating_sub(fault_at_ms);
    let query_at_ms = now_ms();
    let query_ms = query_at_ms.saturating_sub(fault_at_ms);
    let minio_log = bounded_minio_logs(&minio.id);
    fs::write(artifact_dir.join("minio.log"), &minio_log).unwrap();
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(revision.status.success());
    let expected_final_sink_progress = json!({
        "view":ID,
        "columns":2,
        "topic":sink_topic,
        "snapshot_complete":true,
        "view_epoch":manifest["required_frontier"],
        "transaction":manifest["required_frontier"],
        "pending":null
    });
    let evidence = json!({
        "scenario_id":manifest["scenario_id"],
        "manifest_sha256":format!("{:x}",Sha256::digest(ENOSPC_MANIFEST)),
        "revision":String::from_utf8(revision.stdout).unwrap().trim(),
        "kafka_image":"apache/kafka:3.8.0",
        "minio_image":ENOSPC_MINIO_IMAGE,
        "minio_container_id":minio.id,
        "minio_data_tmpfs_bytes":ENOSPC_TMPFS_BYTES,
        "source_type":"KafkaSource",
        "sink_type":"KafkaViewSink with transactional KafkaSink",
        "state_store":"MinIO-backed ShardDb",
        "source_topic":source_topic,
        "sink_topic":sink_topic,
        "source_group":source_group,
        "bucket":bucket,
        "run_id":suffix,
        "replay_seed":manifest["replay_seed"],
        "ready_at_ms":ready["at_ms"],
        "enospc_at_ms":enospc_at_ms,
        "fault_at_ms":fault_at_ms,
        "before":before,
        "failure":failure,
        "fault_group_offset":fault_group_offset,
        "sink_records_at_fault":sink_records_at_fault,
        "fault_frontier":fault_frontier,
        "fault_rows":fault_rows,
        "fault_sink_progress":fault_sink_progress,
        "enospc_trigger":enospc_trigger,
        "capacity_restored":capacity_restored,
        "detection":detection,
        "lease_event":lease_event,
        "after":after,
        "worker_kill_ms":worker_kill_ms,
        "group_offset_after_recovery":group_offset_after_recovery,
        "sink_records_after_recovery":sink_records_after_recovery,
        "query_frontier":query_frontier,
        "query_rows":query_rows,
        "recovered_sink_progress":recovered_sink_progress,
        "query_at_ms":query_at_ms,
        "elapsed_ms":{"detection":detection_ms,"ownership":ownership_ms,"query":query_ms},
        "oracle":{
            "history":manifest["history"],
            "weighted_output_before_fault":manifest["weighted_output_before_fault"],
            "relation_before_fault":manifest["relation_before_fault"],
            "expected_weighted_output":manifest["expected_weighted_output"],
            "expected_relation":manifest["expected_relation"],
            "expected_sink":manifest["expected_sink"]
        },
        "minio_log_sha256":format!("{:x}",Sha256::digest(minio_log.as_bytes())),
        "scope":manifest["scope"]
    });
    save(&artifact_dir.join("evidence.json"), &evidence);

    assert_eq!(after.lease.worker_id.0, 2);
    assert!(after.lease.lease_token.0 > before.lease.lease_token.0);
    assert_ne!(after.process_id, before.process_id);
    assert_eq!(after.source_epoch, manifest["required_frontier"]);
    assert_eq!(
        after.source_offsets,
        std::collections::BTreeMap::from([(0, 5)])
    );
    assert_eq!(after.view_frontier, manifest["required_frontier"]);
    assert_eq!(after.weighted_output, manifest["expected_weighted_output"]);
    assert_eq!(after.relation, manifest["expected_relation"]);
    assert_eq!(query_frontier, manifest["required_frontier"]);
    assert_eq!(query_rows, expected_query_rows);
    assert_eq!(
        group_offset_after_recovery,
        manifest["source_offset_after_recovery"]
    );
    assert_eq!(sink_records_after_recovery, manifest["expected_sink"]);
    assert_eq!(after.sink_progress, expected_final_sink_progress);
    assert_eq!(recovered_sink_progress, after.sink_progress);
    for (name, elapsed) in [("ownership", ownership_ms), ("query", query_ms)] {
        assert!(
            elapsed <= manifest["limits_ms"][name].as_u64().unwrap(),
            "{name} deadline: {elapsed} ms"
        );
    }
    workers.0[1].kill().unwrap();
    workers.0[1].wait().unwrap();
    let evidence_sha256 = format!(
        "{:x}",
        Sha256::digest(fs::read(artifact_dir.join("evidence.json")).unwrap())
    );
    let artifact_names = [
        "evidence.json",
        "before-fault.json",
        "fault-state.json",
        "enospc-trigger.json",
        "capacity-restored.json",
        "worker-1.log",
        "worker-2.log",
        "minio.log",
    ];
    let artifact_sha256 = artifact_names.map(|name| {
        (
            name,
            format!(
                "{:x}",
                Sha256::digest(fs::read(artifact_dir.join(name)).unwrap())
            ),
        )
    });
    save(
        &artifact_dir.join("artifact-digests.json"),
        &json!({"evidence_sha256":evidence_sha256,"artifact_sha256":artifact_sha256}),
    );
    fs::write(
        artifact_dir.join("worker-state-enospc-summary.md"),
        format!(
            "# Worker state ENOSPC recovery: {}\n\nRevision: `{}`; manifest SHA-256: `{}`; replay seed: {}.\n\nThe bounded MinIO `/data` tmpfs reached OS ENOSPC before source epoch {} checkpoint prepare. MinIO rejected the worker WAL write, and the worker did not advance Kafka offset {} or committed frontier {} before it was killed. After restoring capacity, worker 2 recovered frontier {} and the exact Kafka sink, MinIO view, and cursor matched the independent oracle.\n\nKafka: `apache/kafka:3.8.0`; MinIO: `{}`; tmpfs: {} bytes. Scope: {}.\n\nComplete observations and logs are indexed in `artifact-digests.json`.\n",
            manifest["scenario_id"],
            evidence["revision"].as_str().unwrap(),
            evidence["manifest_sha256"].as_str().unwrap(),
            manifest["replay_seed"],
            manifest["fault_epoch"],
            fault_group_offset,
            fault_frontier,
            query_frontier,
            ENOSPC_MINIO_IMAGE,
            ENOSPC_TMPFS_BYTES,
            manifest["scope"]
        ),
    )
    .unwrap();
    drop(workers);
    control.shutdown();
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
    let sink_records = consume_stale_owner_topic(&bootstrap, sink_topic);
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
