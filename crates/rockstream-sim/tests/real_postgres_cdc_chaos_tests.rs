//! Real PostgreSQL pgoutput -> durable M3 -> transactional Kafka recovery.
#![cfg(feature = "docker_tests")]
#![recursion_limit = "256"]

#[path = "common/recovery.rs"]
mod recovery;
use recovery::save;

use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    ClientConfig,
};
use rockstream_connectors::{
    OffsetToken, PgLsn, PgOutputConfig, PollDeltaResult, PostgresCdcSource, SnapshotDeltaFence,
    SnapshotStream, SourceCheckpointStore, SourceConnector, SourceError, SourceRuntimeCoordinator,
    WatermarkCapability,
};
use rockstream_control::{audit::FileAuditLog, ControlService, ShardManager, TopologyCatalog};
use rockstream_gateway::view_reader::{HotOnlyViewReader, ViewReadStrategy, ViewReader};
use rockstream_ops::{
    compile_plan_with_sink_id,
    kafka_sink::KafkaViewSink,
    sink::write_view_directory_entry,
    sink::{materialize_view_state, read_view_output},
    ArrowZSet,
};
use rockstream_plan::PlanNode;
use rockstream_runtime::{
    exchange::persistence::committed_frontier, start_worker_client, WorkerClientHandle,
};
use rockstream_storage::{keys::ShardKeyEncoder, ShardReader, WriteBatch};
use rockstream_test_support::minio::{minio_object_store, start_minio, MINIO_PASS, MINIO_USER};
use rockstream_types::{
    arrow_batch::split_weight_column,
    connector::PartitionFilter,
    ids::{ConnectorId, OperatorId, ShardId, WorkerId},
};
use serde::{Deserialize, Serialize};
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
use testcontainers::{core::WaitFor, runners::AsyncRunner, GenericImage, ImageExt};
use testcontainers_modules::kafka::apache::{self, KAFKA_PORT};
use tokio_postgres::{Client, NoTls};

const ID: u64 = 126;
const FAULT_EPOCH: u64 = 3;
const WAIT: Duration = Duration::from_secs(180);
const MANIFEST: &str = include_str!("../../../docs/testing/postgres-cdc-kafka-minio-v1.json");

#[derive(Debug, PartialEq, Eq)]
enum QualificationFailure {
    Identity,
    SourceAcknowledgement,
    Ownership,
    SourceProgress,
    ViewOutput,
    SinkOutput,
    MutationNotDetected,
}

#[derive(Deserialize, Serialize, Clone)]
struct PostgresObservation {
    observed_at_ms: u64,
    process_id: u32,
    worker_id: u64,
    lease_token: u64,
    source_epoch: u64,
    source_checkpoint_lsn: String,
    view_frontier: u64,
    weighted_output: Value,
    relation: Value,
    in_flight_epochs: usize,
    checkpoint_history_entries: usize,
    sink_progress: Value,
}

#[derive(Serialize)]
struct PostgresCdcEvidence {
    scenario_id: String,
    manifest_sha256: String,
    source_revision: String,
    postgres_image: String,
    postgres_server_version: String,
    kafka_image: String,
    minio_image: String,
    run_kind: String,
    table: String,
    publication: String,
    slot: String,
    sink_topic: String,
    fault_boundary: Value,
    checkpoint_before_fault: PostgresObservation,
    slot_lsn_before_fault: String,
    checkpoint_at_fault: PostgresObservation,
    slot_lsn_at_fault: String,
    recovered_ack: Option<Value>,
    checkpoint_after_recovered_ack: Option<PostgresObservation>,
    slot_lsn_after_recovered_ack: Option<String>,
    checkpoint_at_completion: Option<PostgresObservation>,
    slot_lsn_at_completion: Option<String>,
    frontier_at_completion: Option<u64>,
    rows_at_completion: Option<Vec<String>>,
    sink_progress_at_completion: Option<Value>,
    sink_records_at_fault: Value,
    sink_records_at_completion: Option<Value>,
    unsafe_ack: Option<Value>,
    oracle: Value,
}

struct PgAckBoundarySource {
    inner: PostgresCdcSource,
    root: PathBuf,
    worker_id: u64,
    unsafe_future_ack: bool,
}

#[async_trait]
impl SourceConnector for PgAckBoundarySource {
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
            .open_pgoutput(PgLsn::from_offset_token(&after)? != PgLsn::ZERO)
            .await?;
        self.inner
            .poll_delta(after, max_bytes, credits_available, partition_filter)
            .await
    }

    async fn commit_offset(&mut self, epoch: u64, offset: OffsetToken) -> Result<(), SourceError> {
        let checkpoint_lsn = PgLsn::from_offset_token(&offset)?.to_string();
        if self.worker_id == 1 && epoch == FAULT_EPOCH {
            save(
                &self.root.join("postgres-ack-pending.json"),
                &json!({
                    "at_ms": recovery::now_ms(),
                    "process_id": std::process::id(),
                    "worker_id": self.worker_id,
                    "epoch": epoch,
                    "checkpoint_lsn": checkpoint_lsn,
                }),
            );
            if self.unsafe_future_ack {
                let target = wait_for_child_json(&self.root.join("unsafe-slot-lsn.json")).await;
                let target_lsn = target["lsn"].as_str().unwrap();
                let target_offset = PgLsn::parse(target_lsn)?.to_offset_token();
                self.inner.open_pgoutput(true).await?;
                self.inner.commit_offset(epoch, target_offset).await?;
                let confirmed_flush_lsn = self.inner.confirmed_flush_lsn().await?;
                save(
                    &self.root.join("postgres-unsafe-ack.json"),
                    &json!({
                        "at_ms": recovery::now_ms(),
                        "process_id": std::process::id(),
                        "worker_id": self.worker_id,
                        "epoch": epoch,
                        "checkpoint_lsn": checkpoint_lsn,
                        "requested_lsn": target_lsn,
                        "confirmed_flush_lsn": confirmed_flush_lsn.map(|lsn| lsn.to_string()),
                    }),
                );
                std::future::pending::<()>().await;
            }
            std::future::pending::<()>().await;
        }
        if self.worker_id == 2 && epoch == FAULT_EPOCH {
            self.inner.open_pgoutput(true).await?;
            self.inner.commit_offset(epoch, offset).await?;
            let confirmed_flush_lsn = self.inner.confirmed_flush_lsn().await?;
            save(
                &self.root.join("postgres-recovered-ack.json"),
                &json!({
                    "at_ms": recovery::now_ms(),
                    "process_id": std::process::id(),
                    "worker_id": self.worker_id,
                    "epoch": epoch,
                    "checkpoint_lsn": checkpoint_lsn,
                    "confirmed_flush_lsn": confirmed_flush_lsn.map(|lsn| lsn.to_string()),
                }),
            );
            wait_for_child_file(&self.root.join("continue-recovered-ack")).await;
            return Ok(());
        }
        self.inner.open_pgoutput(epoch > 0).await?;
        self.inner.commit_offset(epoch, offset).await
    }

    async fn pause(&mut self, reason: String) -> Result<(), SourceError> {
        self.inner.pause(reason).await
    }

    async fn resume(&mut self) -> Result<(), SourceError> {
        self.inner.resume().await
    }

    fn partition_filter_support(&self) -> bool {
        self.inner.partition_filter_support()
    }

    fn watermark_capability(&self) -> WatermarkCapability {
        self.inner.watermark_capability()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_cdc_worker_process_fixture() {
    let Ok(root) = std::env::var("ROCKSTREAM_PG_CDC_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let id: u64 = std::env::var("ROCKSTREAM_PG_CDC_WORKER_ID")
        .unwrap()
        .parse()
        .unwrap();
    let control_addr = std::env::var("ROCKSTREAM_PG_CDC_CONTROL").unwrap();
    let bootstrap = std::env::var("ROCKSTREAM_PG_CDC_KAFKA").unwrap();
    let pg_config = pg_config_from_env();
    let unsafe_future_ack = std::env::var_os("ROCKSTREAM_PG_CDC_UNSAFE_FUTURE_ACK").is_some();
    let (worker, _task) =
        start_worker_client(id, &control_addr, &root.join(format!("worker-{id}")))
            .await
            .unwrap();
    let deadline = Instant::now() + WAIT;
    while worker.worker_id().is_none() {
        assert!(Instant::now() < deadline, "registration deadline");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if id == 1 {
        worker.request_shard(ShardId(ID)).await.unwrap();
    } else {
        wait_for_child_file(&root.join("request-worker-2")).await;
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
        &json!({"at_ms":recovery::now_ms(),"lease":worker.leases()}),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let observer_schema = schema.clone();
    let source =
        PostgresCdcSource::configured_pgoutput(ConnectorId(ID), schema.clone(), pg_config.clone())
            .unwrap();
    let mut source = SourceRuntimeCoordinator::new(
        PgAckBoundarySource {
            inner: source,
            root: root.clone(),
            worker_id: id,
            unsafe_future_ack,
        },
        ConnectorId(ID),
        PgLsn::ZERO.to_offset_token(),
        SourceCheckpointStore::new(db.clone(), ID as u128, ConnectorId(ID)),
    );
    source.recover().await.unwrap();
    let owner = source.acquire_owner(format!("worker-{id}")).unwrap();
    if id == 1 {
        let observed_db = db.clone();
        let observed_worker = worker.clone();
        let observed_root = root.clone();
        let observed_config = pg_config.clone();
        tokio::spawn(async move {
            while !observed_root.join("postgres-ack-pending.json").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let observer_source = PostgresCdcSource::configured_pgoutput(
                ConnectorId(ID),
                observer_schema,
                observed_config,
            )
            .unwrap();
            let mut observer = SourceRuntimeCoordinator::new(
                observer_source,
                ConnectorId(ID),
                PgLsn::ZERO.to_offset_token(),
                SourceCheckpointStore::new(observed_db.clone(), ID as u128, ConnectorId(ID)),
            );
            observer.recover().await.unwrap();
            save(
                &observed_root.join("postgres-checkpoint-at-fault.json"),
                &observe_postgres(&observed_db, &observed_worker, &observer).await,
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
        rockstream_connectors::KafkaSink::connect(
            ConnectorId(ID),
            &bootstrap,
            &std::env::var("ROCKSTREAM_PG_CDC_SINK_TOPIC").unwrap(),
        )
        .unwrap(),
        db.clone(),
        OperatorId(ID),
        2,
    );
    if id == 2 {
        source.acknowledge_recovered(&owner).await.unwrap();
        assert!(sink.deliver_next().await.unwrap());
        save(
            &root.join("postgres-checkpoint-after-recovered-ack.json"),
            &observe_postgres(&db, &worker, &source).await,
        );
        wait_for_child_file(&root.join("continue-transactions")).await;
    }
    let final_epoch = std::env::var("ROCKSTREAM_PG_CDC_FINAL_EPOCH")
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let mut source_ready = false;
    while source.committed_epoch() < final_epoch {
        assert!(Instant::now() < deadline, "source progress deadline");
        let delta = source.poll_delta(64 * 1024, 64).await.unwrap();
        if delta.batches.is_empty() {
            if id == 1 && !source_ready {
                save(
                    &root.join("postgres-source-ready.json"),
                    &json!({"at_ms":recovery::now_ms(),"worker_id":id,"process_id":std::process::id()}),
                );
                source_ready = true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        }
        let epoch = source.next_epoch().unwrap();
        let lease = worker
            .leases()
            .into_iter()
            .find(|lease| lease.shard_id == ShardId(ID))
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
        assert!(worker
            .check_fence_write(ShardId(ID), lease.lease_token)
            .await
            .unwrap());
        source
            .commit_epoch(&owner, epoch, delta.new_offset, writes)
            .await
            .unwrap();
        assert!(sink.deliver_next().await.unwrap());
        save(
            &root.join(format!("postgres-progress-{epoch}.json")),
            &observe_postgres(&db, &worker, &source).await,
        );
    }
    save(
        &root.join("postgres-worker-complete.json"),
        &observe_postgres(&db, &worker, &source).await,
    );
    std::future::pending::<()>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_cdc_real_process_ack_gap_recovers_to_kafka() {
    let docker = Command::new("docker").arg("info").output().unwrap();
    assert!(
        docker.status.success(),
        "Docker required for PostgreSQL CDC chaos test"
    );
    let artifact_root = std::env::var_os("ROCKSTREAM_CHAOS_ARTIFACT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/real-cluster-chaos")
        });
    fs::create_dir_all(&artifact_root).unwrap();
    let manifest: Value = serde_json::from_str(MANIFEST).unwrap();
    let postgres = GenericImage::new("postgres", "18.0")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
        ])
        .start()
        .await
        .unwrap();
    let postgres_host = postgres.get_host().await.unwrap().to_string();
    let postgres_port = postgres.get_host_port_ipv4(5432).await.unwrap();
    let postgres_client = connect_postgres(&postgres_host, postgres_port).await;
    let postgres_version: String = postgres_client
        .query_one("SELECT version()", &[])
        .await
        .unwrap()
        .get(0);
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
    let positive = run_postgres_case(
        &postgres_client,
        &postgres_host,
        postgres_port,
        &postgres_version,
        &bootstrap,
        &manifest,
        &artifact_root,
        "positive",
        false,
    )
    .await;
    assert_eq!(validate_postgres_recovery(&positive, &manifest), Ok(()));
    let mutant = run_postgres_case(
        &postgres_client,
        &postgres_host,
        postgres_port,
        &postgres_version,
        &bootstrap,
        &manifest,
        &artifact_root,
        "mutation",
        true,
    )
    .await;
    assert_eq!(
        validate_unsafe_slot_ack(&mutant),
        Err(QualificationFailure::SourceAcknowledgement)
    );
}

async fn run_postgres_case(
    postgres: &Client,
    postgres_host: &str,
    postgres_port: u16,
    postgres_version: &str,
    bootstrap: &str,
    manifest: &Value,
    artifact_root: &Path,
    suffix: &str,
    unsafe_future_ack: bool,
) -> PostgresCdcEvidence {
    let table = format!("letters_{suffix}");
    let publication = format!("publication_{suffix}");
    let slot = format!("slot_{suffix}");
    let sink_topic = format!("{}-{suffix}", manifest["sink_topic"].as_str().unwrap());
    let bucket = format!("postgres-cdc-kafka-{suffix}");
    postgres
        .batch_execute(&format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL); ALTER TABLE {table} REPLICA IDENTITY FULL; CREATE PUBLICATION {publication} FOR TABLE {table};"
        ))
        .await
        .unwrap();
    postgres
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
        .unwrap();
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    assert_eq!(
        admin
            .create_topics(
                &[NewTopic::new(&sink_topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .unwrap(),
        vec![Ok(sink_topic.clone())]
    );
    let (_minio, minio_port) = start_minio(&bucket).await.expect("MinIO required");
    let root = tempfile::tempdir().unwrap();
    let audit = Arc::new(FileAuditLog::open(root.path().join("audit.jsonl")).unwrap());
    let manager = ShardManager::new();
    let control = ControlService::new(TopologyCatalog::new())
        .with_shard_manager(manager.clone())
        .with_audit(audit)
        .start("127.0.0.1:0")
        .await
        .unwrap();
    let artifact_dir = artifact_root.join(format!("postgres-cdc-{suffix}-{}", recovery::now_ms()));
    fs::create_dir_all(&artifact_dir).unwrap();
    let mut workers = WorkerChildren(Vec::new());
    for id in if unsafe_future_ack {
        vec![1]
    } else {
        vec![1, 2]
    } {
        workers.0.push(spawn_worker(
            id,
            root.path(),
            control.addr,
            bootstrap,
            postgres_host,
            postgres_port,
            &table,
            &publication,
            &slot,
            &sink_topic,
            minio_port,
            &bucket,
            unsafe_future_ack,
            if id == 1 {
                FAULT_EPOCH
            } else {
                manifest["required_frontier"].as_u64().unwrap()
            },
            &artifact_dir,
        ));
    }
    let source_ready = wait_for_json(
        &root.path().join("postgres-source-ready.json"),
        &mut workers.0[0],
    )
    .await;
    assert_eq!(source_ready["worker_id"], 1);
    for (index, transaction) in manifest["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .take(2)
        .enumerate()
    {
        let sql = transaction["sql"]
            .as_str()
            .unwrap()
            .replace("letters", &table);
        postgres.batch_execute(&sql).await.unwrap();
        let epoch = index as u64 + 1;
        let progress = wait_for_json(
            &root.path().join(format!("postgres-progress-{epoch}.json")),
            &mut workers.0[0],
        )
        .await;
        assert_eq!(progress["source_epoch"], epoch);
    }
    let checkpoint_before_fault: PostgresObservation = serde_json::from_value(
        wait_for_json(
            &root.path().join("postgres-progress-2.json"),
            &mut workers.0[0],
        )
        .await,
    )
    .unwrap();
    let slot_lsn_before_fault = pg_slot_lsn(postgres, &slot)
        .await
        .expect("slot LSN before fault");
    let sql = manifest["transactions"][2]["sql"]
        .as_str()
        .unwrap()
        .replace("letters", &table);
    postgres.batch_execute(&sql).await.unwrap();
    let fault_boundary = wait_for_json(
        &root.path().join("postgres-ack-pending.json"),
        &mut workers.0[0],
    )
    .await;
    let checkpoint_at_fault: PostgresObservation = serde_json::from_value(
        wait_for_json(
            &root.path().join("postgres-checkpoint-at-fault.json"),
            &mut workers.0[0],
        )
        .await,
    )
    .unwrap();
    let slot_lsn_at_fault = pg_slot_lsn(postgres, &slot)
        .await
        .expect("slot LSN at fault");
    let sink_records_at_fault = recovery::consume_topic(bootstrap, &sink_topic);
    let (
        checkpoint_after_recovered_ack,
        slot_lsn_after_recovered_ack,
        recovered_ack,
        checkpoint_at_completion,
        slot_lsn_at_completion,
        frontier_at_completion,
        rows_at_completion,
        sink_progress_at_completion,
        sink_records_at_completion,
        unsafe_ack,
    ) = if unsafe_future_ack {
        let sql = manifest["transactions"][3]["sql"]
            .as_str()
            .unwrap()
            .replace("letters", &table);
        postgres.batch_execute(&sql).await.unwrap();
        let target_lsn: String = postgres
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await
            .unwrap()
            .get(0);
        save(
            &root.path().join("unsafe-slot-lsn.json"),
            &json!({"lsn":target_lsn}),
        );
        let unsafe_ack = wait_for_json(
            &root.path().join("postgres-unsafe-ack.json"),
            &mut workers.0[0],
        )
        .await;
        let slot_lsn = pg_slot_lsn(postgres, &slot).await;
        workers.0[0].kill().unwrap();
        workers.0[0].wait().unwrap();
        (
            None,
            None,
            None,
            None,
            slot_lsn,
            None,
            None,
            None,
            None,
            Some(unsafe_ack),
        )
    } else {
        workers.0[0].kill().unwrap();
        workers.0[0].wait().unwrap();
        let (lease, evicted) = manager.force_acquire(ShardId(ID), WorkerId(2));
        assert_eq!(evicted, Some(WorkerId(1)));
        assert!(lease.lease_token.0 > checkpoint_at_fault.lease_token);
        fs::write(root.path().join("request-worker-2"), b"start").unwrap();
        let recovered_ack = wait_for_json(
            &root.path().join("postgres-recovered-ack.json"),
            &mut workers.0[1],
        )
        .await;
        let slot_lsn_after_recovered_ack = pg_slot_lsn(postgres, &slot).await;
        fs::write(root.path().join("continue-recovered-ack"), b"continue").unwrap();
        let checkpoint_after_recovered_ack: PostgresObservation = serde_json::from_value(
            wait_for_json(
                &root
                    .path()
                    .join("postgres-checkpoint-after-recovered-ack.json"),
                &mut workers.0[1],
            )
            .await,
        )
        .unwrap();
        let sql = manifest["transactions"][3]["sql"]
            .as_str()
            .unwrap()
            .replace("letters", &table);
        postgres.batch_execute(&sql).await.unwrap();
        fs::write(root.path().join("continue-transactions"), b"continue").unwrap();
        wait_for_json(
            &root.path().join("postgres-progress-4.json"),
            &mut workers.0[1],
        )
        .await;
        let checkpoint_at_completion: PostgresObservation = serde_json::from_value(
            wait_for_json(
                &root.path().join("postgres-worker-complete.json"),
                &mut workers.0[1],
            )
            .await,
        )
        .unwrap();
        let slot_lsn_at_completion = pg_slot_lsn(postgres, &slot).await;
        let (frontier, rows, sink_progress) = read_minio_view(minio_port, &bucket).await;
        let sink_records = recovery::consume_topic(bootstrap, &sink_topic);
        workers.0[1].kill().unwrap();
        workers.0[1].wait().unwrap();
        (
            Some(checkpoint_after_recovered_ack),
            slot_lsn_after_recovered_ack,
            Some(recovered_ack),
            Some(checkpoint_at_completion),
            slot_lsn_at_completion,
            Some(frontier),
            Some(rows),
            Some(sink_progress),
            Some(sink_records),
            None,
        )
    };
    let source_revision = git_revision();
    let evidence = PostgresCdcEvidence {
        scenario_id: manifest["scenario_id"].as_str().unwrap().into(),
        manifest_sha256: format!("{:x}", Sha256::digest(MANIFEST.as_bytes())),
        source_revision,
        postgres_image: "postgres:18.0".into(),
        postgres_server_version: postgres_version.to_owned(),
        kafka_image: "apache/kafka:3.8.0".into(),
        minio_image: "codemowers/minio:RELEASE.2025-09-07T16-13-09Z".into(),
        run_kind: if unsafe_future_ack {
            "unsafe_future_slot_ack"
        } else {
            "positive"
        }
        .into(),
        table,
        publication,
        slot,
        sink_topic,
        fault_boundary,
        checkpoint_before_fault,
        slot_lsn_before_fault: slot_lsn_before_fault.to_string(),
        checkpoint_at_fault,
        slot_lsn_at_fault: slot_lsn_at_fault.to_string(),
        recovered_ack,
        checkpoint_after_recovered_ack,
        slot_lsn_after_recovered_ack: slot_lsn_after_recovered_ack.map(|lsn| lsn.to_string()),
        checkpoint_at_completion,
        slot_lsn_at_completion: slot_lsn_at_completion.map(|lsn| lsn.to_string()),
        frontier_at_completion,
        rows_at_completion,
        sink_progress_at_completion,
        sink_records_at_fault,
        sink_records_at_completion,
        unsafe_ack,
        oracle: json!({
            "transactions": manifest["transactions"],
            "expected_weighted_output": manifest["expected_weighted_output"],
            "expected_relation": manifest["expected_relation"],
            "expected_sink": manifest["expected_sink"],
        }),
    };
    write_evidence(&artifact_dir, &evidence);
    evidence
}

fn spawn_worker(
    id: u64,
    root: &Path,
    control_addr: std::net::SocketAddr,
    bootstrap: &str,
    postgres_host: &str,
    postgres_port: u16,
    table: &str,
    publication: &str,
    slot: &str,
    sink_topic: &str,
    minio_port: u16,
    bucket: &str,
    unsafe_future_ack: bool,
    final_epoch: u64,
    artifact_dir: &Path,
) -> Child {
    let log = fs::File::create(artifact_dir.join(format!("worker-{id}.log"))).unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "postgres_cdc_worker_process_fixture",
            "--exact",
            "--nocapture",
        ])
        .env("ROCKSTREAM_PG_CDC_ROOT", root)
        .env("ROCKSTREAM_PG_CDC_WORKER_ID", id.to_string())
        .env("ROCKSTREAM_PG_CDC_CONTROL", control_addr.to_string())
        .env("ROCKSTREAM_PG_CDC_KAFKA", bootstrap)
        .env("ROCKSTREAM_PG_CDC_HOST", postgres_host)
        .env("ROCKSTREAM_PG_CDC_PORT", postgres_port.to_string())
        .env("ROCKSTREAM_PG_CDC_DATABASE", "postgres")
        .env("ROCKSTREAM_PG_CDC_USER", "postgres")
        .env("ROCKSTREAM_PG_CDC_PASSWORD", "postgres")
        .env("ROCKSTREAM_PG_CDC_TABLE", table)
        .env("ROCKSTREAM_PG_CDC_PUBLICATION", publication)
        .env("ROCKSTREAM_PG_CDC_SLOT", slot)
        .env("ROCKSTREAM_PG_CDC_SINK_TOPIC", sink_topic)
        .env("ROCKSTREAM_PG_CDC_FINAL_EPOCH", final_epoch.to_string())
        .env(
            "ROCKSTREAM_OBJECT_STORE_ENDPOINT",
            format!("http://127.0.0.1:{minio_port}"),
        )
        .env("ROCKSTREAM_OBJECT_STORE_BUCKET", bucket)
        .env("ROCKSTREAM_OBJECT_STORE_REGION", "us-east-1")
        .env("ROCKSTREAM_OBJECT_STORE_ACCESS_KEY", MINIO_USER)
        .env("ROCKSTREAM_OBJECT_STORE_SECRET_KEY", MINIO_PASS)
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log));
    if unsafe_future_ack {
        command.env("ROCKSTREAM_PG_CDC_UNSAFE_FUTURE_ACK", "1");
    }
    command.spawn().unwrap()
}

async fn observe_postgres<S: SourceConnector>(
    db: &rockstream_storage::ShardDb,
    worker: &WorkerClientHandle,
    source: &SourceRuntimeCoordinator<S>,
) -> PostgresObservation {
    let output = read_view_output(db, OperatorId(ID), 2).await.unwrap();
    let weighted_output = json!(output
        .iter()
        .map(|(epoch, row, values, weight)| json!([epoch, row, recovery::typed(values), weight]))
        .collect::<Vec<_>>());
    let relation = json!(materialize_view_state(output, &[])
        .into_values()
        .map(|(values, weight)| json!([recovery::typed(&values), weight]))
        .collect::<Vec<_>>());
    let metrics = source.metrics().await.unwrap();
    let lease = worker
        .leases()
        .into_iter()
        .find(|lease| lease.shard_id == ShardId(ID))
        .unwrap();
    PostgresObservation {
        observed_at_ms: recovery::now_ms(),
        process_id: std::process::id(),
        worker_id: lease.worker_id.0,
        lease_token: lease.lease_token.0,
        source_epoch: source.committed_epoch(),
        source_checkpoint_lsn: PgLsn::from_offset_token(source.committed_offset())
            .unwrap()
            .to_string(),
        view_frontier: committed_frontier(db).await.unwrap(),
        weighted_output,
        relation,
        in_flight_epochs: metrics.source_runtime_in_flight_epochs,
        checkpoint_history_entries: metrics.source_checkpoint_history_entries,
        sink_progress: serde_json::from_slice(
            &db.get(b"\x06kafka-view/126").await.unwrap().unwrap(),
        )
        .unwrap(),
    }
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
    let sink_progress: Value =
        serde_json::from_slice(&reader.get(b"\x06kafka-view/126").await.unwrap().unwrap()).unwrap();
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

fn validate_postgres_recovery(
    evidence: &PostgresCdcEvidence,
    manifest: &Value,
) -> Result<(), QualificationFailure> {
    if evidence.scenario_id != manifest["scenario_id"]
        || evidence.run_kind != "positive"
        || evidence.manifest_sha256 != format!("{:x}", Sha256::digest(MANIFEST.as_bytes()))
        || evidence.oracle
            != json!({
                "transactions": manifest["transactions"],
                "expected_weighted_output": manifest["expected_weighted_output"],
                "expected_relation": manifest["expected_relation"],
                "expected_sink": manifest["expected_sink"],
            })
    {
        return Err(QualificationFailure::Identity);
    }
    let before_lsn = parse_lsn(&evidence.checkpoint_before_fault.source_checkpoint_lsn);
    let checkpoint_lsn = parse_lsn(&evidence.checkpoint_at_fault.source_checkpoint_lsn);
    let fault_lsn_display = evidence.fault_boundary["checkpoint_lsn"].as_str().unwrap();
    let fault_lsn = parse_lsn(fault_lsn_display);
    let fault_ack_checks = [
        evidence.fault_boundary["epoch"] == FAULT_EPOCH,
        evidence.fault_boundary["worker_id"] == 1,
        evidence.checkpoint_before_fault.source_epoch == FAULT_EPOCH - 1,
        before_lsn == parse_lsn(&evidence.slot_lsn_before_fault),
        evidence.checkpoint_at_fault.source_epoch == FAULT_EPOCH,
        checkpoint_lsn == fault_lsn,
        evidence.slot_lsn_at_fault == evidence.slot_lsn_before_fault,
        parse_lsn(&evidence.slot_lsn_at_fault) < checkpoint_lsn,
    ];
    if fault_ack_checks.contains(&false) {
        eprintln!("fault ACK checks {fault_ack_checks:?}");
        return Err(QualificationFailure::SourceAcknowledgement);
    }
    let Some(recovered_ack) = &evidence.recovered_ack else {
        return Err(QualificationFailure::SourceAcknowledgement);
    };
    let Some(after_ack) = &evidence.checkpoint_after_recovered_ack else {
        return Err(QualificationFailure::SourceProgress);
    };
    let Some(slot_after_ack) = evidence.slot_lsn_after_recovered_ack.as_deref() else {
        return Err(QualificationFailure::SourceAcknowledgement);
    };
    let Some(after) = &evidence.checkpoint_at_completion else {
        return Err(QualificationFailure::SourceProgress);
    };
    let Some(slot_at_completion) = evidence.slot_lsn_at_completion.as_deref() else {
        return Err(QualificationFailure::SourceAcknowledgement);
    };
    let recovered_ack_checks = [
        recovered_ack["epoch"] == FAULT_EPOCH,
        recovered_ack["worker_id"] == 2,
        recovered_ack["checkpoint_lsn"].as_str() == Some(fault_lsn_display),
        recovered_ack["confirmed_flush_lsn"].as_str() == Some(fault_lsn_display),
        parse_lsn(slot_after_ack) == fault_lsn,
        after_ack.source_epoch == FAULT_EPOCH,
        parse_lsn(&after_ack.source_checkpoint_lsn) == fault_lsn,
        after.source_epoch == manifest["required_frontier"].as_u64().unwrap(),
        parse_lsn(slot_at_completion) == parse_lsn(&after.source_checkpoint_lsn),
    ];
    if recovered_ack_checks.contains(&false) {
        eprintln!("recovered ACK checks {recovered_ack_checks:?}");
        return Err(QualificationFailure::SourceAcknowledgement);
    }
    if evidence.fault_boundary["process_id"] != evidence.checkpoint_at_fault.process_id
        || recovered_ack["process_id"] != after_ack.process_id
        || evidence.checkpoint_at_fault.process_id == after_ack.process_id
        || evidence.checkpoint_at_fault.worker_id != 1
        || after_ack.lease_token <= evidence.checkpoint_at_fault.lease_token
        || after_ack.worker_id != 2
        || recovered_ack["at_ms"].as_u64().unwrap()
            < evidence.fault_boundary["at_ms"].as_u64().unwrap()
        || after_ack.observed_at_ms < recovered_ack["at_ms"].as_u64().unwrap()
    {
        return Err(QualificationFailure::Ownership);
    }
    if evidence.checkpoint_at_fault.view_frontier != FAULT_EPOCH
        || after_ack.view_frontier != FAULT_EPOCH
        || after.view_frontier != manifest["required_frontier"].as_u64().unwrap()
        || evidence.frontier_at_completion != Some(after.view_frontier)
        || evidence.rows_at_completion.as_deref() != Some(&[String::from("1\tc")])
    {
        return Err(QualificationFailure::SourceProgress);
    }
    if evidence.checkpoint_at_fault.weighted_output
        != json!(&manifest["expected_weighted_output"].as_array().unwrap()[..4])
        || evidence.checkpoint_at_fault.relation != json!([])
        || after.weighted_output != manifest["expected_weighted_output"]
        || after.relation != manifest["expected_relation"]
    {
        return Err(QualificationFailure::ViewOutput);
    }
    if evidence.sink_records_at_fault != json!(&manifest["expected_sink"].as_array().unwrap()[..2])
        || evidence.sink_records_at_completion.as_ref() != Some(&manifest["expected_sink"])
        || evidence.sink_progress_at_completion
            != Some(json!({
                "view": ID,
                "columns": 2,
                "topic": evidence.sink_topic,
                "snapshot_complete": true,
                "view_epoch": 4,
                "transaction": 4,
                "pending": null,
            }))
    {
        return Err(QualificationFailure::SinkOutput);
    }
    Ok(())
}

fn validate_unsafe_slot_ack(evidence: &PostgresCdcEvidence) -> Result<(), QualificationFailure> {
    if evidence.run_kind != "unsafe_future_slot_ack" {
        return Err(QualificationFailure::Identity);
    }
    let Some(unsafe_ack) = &evidence.unsafe_ack else {
        return Err(QualificationFailure::MutationNotDetected);
    };
    let checkpoint_lsn = parse_lsn(&evidence.checkpoint_at_fault.source_checkpoint_lsn);
    let Some(slot_lsn) = unsafe_ack["confirmed_flush_lsn"].as_str() else {
        return Err(QualificationFailure::MutationNotDetected);
    };
    if parse_lsn(slot_lsn) > checkpoint_lsn {
        Err(QualificationFailure::SourceAcknowledgement)
    } else {
        Err(QualificationFailure::MutationNotDetected)
    }
}

fn parse_lsn(value: &str) -> u64 {
    PgLsn::parse(value).unwrap().0
}

async fn connect_postgres(host: &str, port: u16) -> Client {
    let dsn = format!("host={host} port={port} user=postgres password=postgres dbname=postgres");
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn pg_config_from_env() -> PgOutputConfig {
    PgOutputConfig {
        host: std::env::var("ROCKSTREAM_PG_CDC_HOST").unwrap(),
        port: std::env::var("ROCKSTREAM_PG_CDC_PORT")
            .unwrap()
            .parse()
            .unwrap(),
        database: std::env::var("ROCKSTREAM_PG_CDC_DATABASE").unwrap(),
        user: std::env::var("ROCKSTREAM_PG_CDC_USER").unwrap(),
        password: Some(std::env::var("ROCKSTREAM_PG_CDC_PASSWORD").unwrap()),
        slot: std::env::var("ROCKSTREAM_PG_CDC_SLOT").unwrap(),
        publication: std::env::var("ROCKSTREAM_PG_CDC_PUBLICATION").unwrap(),
        table: std::env::var("ROCKSTREAM_PG_CDC_TABLE").unwrap(),
    }
}

async fn pg_slot_lsn(postgres: &Client, slot: &str) -> Option<PgLsn> {
    let row = postgres
        .query_opt(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap()?;
    row.get::<_, Option<String>>(0)
        .map(|lsn| PgLsn::parse(&lsn).unwrap())
}

async fn wait_for_child_file(path: &Path) {
    let deadline = Instant::now() + WAIT;
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "deadline waiting for {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_child_json(path: &Path) -> Value {
    wait_for_child_file(path).await;
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
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

fn git_revision() -> String {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn write_evidence(artifact_dir: &Path, evidence: &PostgresCdcEvidence) {
    save(&artifact_dir.join("evidence.json"), evidence);
    save(
        &artifact_dir.join("fault-boundary.json"),
        &evidence.fault_boundary,
    );
    save(
        &artifact_dir.join("checkpoint-at-fault.json"),
        &evidence.checkpoint_at_fault,
    );
    if let Some(ack) = &evidence.recovered_ack {
        save(&artifact_dir.join("recovered-ack.json"), ack);
    }
    if let Some(ack) = &evidence.unsafe_ack {
        save(&artifact_dir.join("unsafe-ack.json"), ack);
    }
    let evidence_sha256 = format!(
        "{:x}",
        Sha256::digest(fs::read(artifact_dir.join("evidence.json")).unwrap())
    );
    let worker_logs = (1..=2)
        .filter_map(|id| {
            let path = artifact_dir.join(format!("worker-{id}.log"));
            fs::read(&path).ok().map(|bytes| {
                (
                    path.file_name().unwrap().to_string_lossy().to_string(),
                    format!("{:x}", Sha256::digest(bytes)),
                )
            })
        })
        .collect::<Vec<_>>();
    save(
        &artifact_dir.join("artifact-digests.json"),
        &json!({"evidence_sha256":evidence_sha256,"worker_log_sha256":worker_logs}),
    );
}

struct WorkerChildren(Vec<Child>);

impl Drop for WorkerChildren {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
