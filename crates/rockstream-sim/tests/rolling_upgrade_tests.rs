//! #129: three real workers, pinned release fixtures, exact view/Kafka observations.
#![cfg(feature = "docker_tests")]

use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_gateway::view_reader::{HotOnlyViewReader, ViewReadStrategy, ViewReader};
use rockstream_sim::qualification::rolling_upgrade::{
    manifest, manifest_sha256, operation, preflight, recovery_sha256, reject_downgrade, validate,
};
use rockstream_storage::{keys::ShardKeyEncoder, ShardReader};
use rockstream_test_support::minio::{minio_object_store, start_minio, MINIO_PASS, MINIO_USER};
use rockstream_types::ids::{ShardId, WorkerId};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::kafka::apache::{self, KAFKA_PORT};
use tokio::sync::oneshot;

#[allow(dead_code)]
#[path = "common/recovery.rs"]
mod recovery;
use recovery::{consume, now_ms, save};

fn docker(args: &[&str]) -> std::process::Output {
    Command::new("docker")
        .args(args)
        .output()
        .expect("Docker required")
}

fn checked(args: &[&str]) -> std::process::Output {
    let output = docker(args);
    assert!(
        output.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn image(reference: &str) -> Value {
    let inspected: Value =
        serde_json::from_slice(&checked(&["image", "inspect", reference]).stdout).unwrap();
    let image = &inspected[0];
    let labels = &image["Config"]["Labels"];
    json!({"reference":reference,"id":image["Id"],
        "revision":labels["org.opencontainers.image.revision"],
        "version":labels["org.opencontainers.image.version"],
        "protocol":labels["rockstream.qualification.protocol"]})
}

struct Cluster {
    names: BTreeMap<u64, String>,
    root: PathBuf,
    artifacts: PathBuf,
    control: String,
    kafka: String,
    minio_port: u16,
    old: String,
    candidate: String,
}

impl Cluster {
    fn launch(&mut self, id: u64, reference: &str, fail: bool) -> Result<Value, &'static str> {
        if id > 3 {
            reject_downgrade(&self.old, reference, &self.old, &self.candidate)?;
        }
        let name = format!(
            "rockstream-upgrade-{}-{}-{id}",
            std::process::id(),
            now_ms()
        );
        let mount = format!(
            "type=bind,source={},target=/qualification",
            self.root.display()
        );
        let mut args = vec![
            "run".to_string(),
            "--name".into(),
            name.clone(),
            "--network".into(),
            "host".into(),
            "--mount".into(),
            mount,
        ];
        for (key, value) in [
            ("ROCKSTREAM_RECOVERY_WORKER_ROOT", "/qualification".into()),
            ("ROCKSTREAM_RECOVERY_WORKER_ID", id.to_string()),
            ("ROCKSTREAM_RECOVERY_CONTROL", self.control.clone()),
            ("ROCKSTREAM_RECOVERY_KAFKA", self.kafka.clone()),
            (
                "ROCKSTREAM_OBJECT_STORE_ENDPOINT",
                format!("http://127.0.0.1:{}", self.minio_port),
            ),
            ("ROCKSTREAM_OBJECT_STORE_BUCKET", "worker-upgrade".into()),
            ("ROCKSTREAM_OBJECT_STORE_REGION", "us-east-1".into()),
            ("ROCKSTREAM_OBJECT_STORE_ACCESS_KEY", MINIO_USER.into()),
            ("ROCKSTREAM_OBJECT_STORE_SECRET_KEY", MINIO_PASS.into()),
        ] {
            args.extend(["--env".into(), format!("{key}={value}")]);
        }
        if fail {
            args.extend(["--env".into(), "ROCKSTREAM_UPGRADE_FAIL_START=1".into()]);
        } else {
            args.push("--detach".into());
        }
        args.push(reference.into());
        let output = docker(&args.iter().map(String::as_str).collect::<Vec<_>>());
        if fail {
            let inspected: Value =
                serde_json::from_slice(&checked(&["inspect", &name]).stdout).unwrap();
            let logs = checked(&["logs", &name]);
            fs::write(
                self.artifacts.join("failed-replacement.log"),
                [&logs.stdout[..], &logs.stderr[..]].concat(),
            )
            .unwrap();
            checked(&["rm", &name]);
            assert_eq!(output.status.code(), Some(42));
            assert_eq!(inspected[0]["State"]["ExitCode"], 42);
            return Ok(json!({"worker_id":id,"exit_code":42,"registered":
                self.root.join(format!("worker-{id}-ready.json")).exists(),
                "created_state_directory":self.root.join(format!("worker-{id}")).exists()}));
        }
        assert!(
            output.status.success(),
            "worker launch: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        self.names.insert(id, name);
        Ok(json!(String::from_utf8(output.stdout).unwrap().trim()))
    }

    fn inspect(&self, id: u64) -> Value {
        let value: Value =
            serde_json::from_slice(&checked(&["inspect", &self.names[&id]]).stdout).unwrap();
        assert_eq!(
            value[0]["State"]["Running"], true,
            "worker {id} exited; see artifact logs"
        );
        value[0].clone()
    }

    async fn ready(&self, id: u64, expected_image: &Value) -> Value {
        let deadline = Instant::now() + Duration::from_secs(60);
        let identity = loop {
            self.inspect(id);
            if let Ok(bytes) = fs::read(self.root.join(format!("worker-{id}-ready.json"))) {
                break serde_json::from_slice::<Value>(&bytes).unwrap();
            }
            assert!(
                Instant::now() < deadline,
                "worker {id} registration deadline"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert_eq!(identity["worker_id"], id);
        for key in ["revision", "version", "protocol"] {
            assert_eq!(
                identity[key], expected_image[key],
                "compiled image identity {key}"
            );
        }
        let inspected = self.inspect(id);
        assert_eq!(inspected["Image"], expected_image["id"]);
        let worker = json!({"worker_id":id,"container_id":inspected["Id"],
            "image_id":inspected["Image"],"identity":identity});
        save(
            &self.artifacts.join(format!("worker-{id}-process.json")),
            &worker,
        );
        worker
    }

    fn stop(&mut self, id: u64) -> u64 {
        let at = now_ms();
        checked(&["kill", &self.names[&id]]);
        checked(&["wait", &self.names[&id]]);
        let name = self.names.remove(&id).unwrap();
        let logs = checked(&["logs", &name]);
        fs::write(
            self.artifacts.join(format!("worker-{id}.log")),
            [&logs.stdout[..], &logs.stderr[..]].concat(),
        )
        .unwrap();
        checked(&["rm", &name]);
        at
    }

    async fn checkpoint(
        &self,
        name: &str,
        epoch: u64,
        manager: &ShardManager,
        reopen: bool,
    ) -> Value {
        let deadline = Instant::now() + Duration::from_secs(60);
        let (snapshot, lease) = loop {
            for id in self.names.keys() {
                self.inspect(*id);
            }
            if let Some(lease) = manager.get(ShardId(126)) {
                let id = lease.worker_id.0;
                // A new owner can reopen at this frontier without receiving another delta.
                let files = if reopen {
                    vec![format!("worker-{id}-reopened.json")]
                } else {
                    vec![
                        format!("worker-{id}-epoch-{epoch}.json"),
                        format!("worker-{id}-reopened.json"),
                    ]
                };
                let found = files
                    .iter()
                    .filter_map(|file| fs::read(self.root.join(file)).ok())
                    .map(|bytes| serde_json::from_slice::<Value>(&bytes).unwrap())
                    .find(|s| {
                        s["observation"]["source_epoch"] == epoch
                            && s["observation"]["lease"] == serde_json::to_value(&lease).unwrap()
                    });
                if let Some(snapshot) = found {
                    break (snapshot, lease);
                }
            }
            assert!(
                Instant::now() < deadline,
                "{name}: source/view progress deadline"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        let store = Arc::new(object_store::prefix::PrefixStore::new(
            minio_object_store(self.minio_port, "worker-upgrade"),
            "shards/126",
        ));
        let reader = Arc::new(ShardReader::open("db", store).await.unwrap());
        let frontier = reader
            .get(&ShardKeyEncoder::frontier_key())
            .await
            .unwrap()
            .unwrap();
        let frontier = u64::from_be_bytes(frontier.as_ref().try_into().unwrap());
        let query = HotOnlyViewReader {
            shard_reader: reader,
            frontier_epoch: Some(frontier),
        };
        let rows = query
            .read_view("letters", None, ViewReadStrategy::HotOnly)
            .await
            .unwrap()
            .into_iter()
            .map(|r| String::from_utf8(r).unwrap())
            .collect::<Vec<_>>();
        let query_at_ms = now_ms();
        let records = consume(&self.kafka);
        assert_eq!(
            manager.get(ShardId(126)),
            Some(lease.clone()),
            "ownership changed during checkpoint"
        );
        let checkpoint = json!({"name":name,"observation":snapshot["observation"],
            "schema":snapshot["schema"],"control_lease":lease,"query_frontier":frontier,
            "query_rows":rows,"query_at_ms":query_at_ms,"sink_records":records,
            "active_workers":self.names.keys().collect::<Vec<_>>()});
        save(
            &self.artifacts.join(format!("checkpoint-{name}.json")),
            &checkpoint,
        );
        checkpoint
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for (id, name) in &self.names {
            let logs = docker(&["logs", name]);
            let _ = fs::write(
                self.artifacts.join(format!("worker-{id}.log")),
                [&logs.stdout[..], &logs.stderr[..]].concat(),
            );
            let _ = docker(&["rm", "--force", name]);
        }
    }
}

async fn traffic(
    bootstrap: String,
    start: u64,
    end: u64,
    boundary: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
    history: PathBuf,
) -> Vec<Value> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("acks", "all")
        .create()
        .unwrap();
    let mut acknowledgements = Vec::new();
    let mut boundary = boundary;
    for offset in start..end {
        let row = operation(offset);
        let (partition, actual_offset) = producer
            .send(
                FutureRecord::to("worker-failure-input")
                    .key("")
                    .payload(&row.to_string()),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
        acknowledgements.push(json!({"operation":row,"partition":partition,
            "offset":actual_offset,"ack_at_ms":now_ms()}));
        let mut log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&history)
            .unwrap();
        writeln!(log, "{}", acknowledgements.last().unwrap()).unwrap();
        if offset == start + 1 {
            if let Some((sent, resume)) = boundary.take() {
                sent.send(()).unwrap();
                resume.await.unwrap();
            }
        }
    }
    acknowledgements
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_worker_n_to_n_plus_1_zero_loss_tc() {
    // Required images never fall back to a tag or silently skip.
    let old = std::env::var("ROCKSTREAM_ROLLING_N_IMAGE").expect("pinned old image required");
    let new = std::env::var("ROCKSTREAM_ROLLING_N_PLUS_1_IMAGE")
        .expect("pinned candidate image required");
    preflight(&old, &new).unwrap();
    checked(&["info"]);
    let images = vec![image(&old), image(&new)];
    assert_ne!(
        images[0]["id"], images[1]["id"],
        "distinct refs resolve to the same image"
    );
    assert_ne!(
        images[0]["revision"], images[1]["revision"],
        "distinct source revisions required"
    );
    let artifacts = std::env::var_os("ROCKSTREAM_UPGRADE_ARTIFACT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/rolling-upgrade")
        });
    fs::create_dir_all(&artifacts).unwrap();
    save(&artifacts.join("images.json"), &images);
    let root = tempfile::tempdir().unwrap();
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
    let (_minio, port) = start_minio("worker-upgrade").await.expect("MinIO required");
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
    let manager = ShardManager::new();
    let catalog = TopologyCatalog::new();
    let control = ControlService::new(catalog.clone())
        .with_shard_manager(manager.clone())
        .start("127.0.0.1:0")
        .await
        .unwrap();
    let mut cluster = Cluster {
        names: BTreeMap::new(),
        root: root.path().to_path_buf(),
        artifacts: artifacts.clone(),
        control: control.addr.to_string(),
        kafka: bootstrap.clone(),
        minio_port: port,
        old: old.clone(),
        candidate: new.clone(),
    };
    let mut workers = Vec::new();
    for id in 1..=3 {
        cluster.launch(id, &old, false).unwrap();
        workers.push(cluster.ready(id, &images[0]).await);
    }
    let history = artifacts.join("acknowledged-operations.jsonl");
    fs::write(&history, []).unwrap();
    let mut acks = traffic(bootstrap.clone(), 0, 5, None, history.clone()).await;
    let mut checkpoints = vec![cluster.checkpoint("initial", 5, &manager, false).await];
    let mut replacements = Vec::new();
    let mut failed = Value::Null;
    for id in 1..=3 {
        let start = 5 + (id - 1) * 6;
        let (sent, received) = oneshot::channel();
        let (resume, continued) = oneshot::channel();
        let producer = tokio::spawn(traffic(
            bootstrap.clone(),
            start,
            start + 6,
            Some((sent, continued)),
            history.clone(),
        ));
        received.await.unwrap();
        let stopped_at_ms = cluster.stop(id);
        checkpoints.push(
            cluster
                .checkpoint(&format!("during-replace-{id}"), start + 2, &manager, false)
                .await,
        );
        resume.send(()).unwrap();
        acks.extend(producer.await.unwrap());
        if id == 2 {
            failed = cluster.launch(5, &new, true).unwrap();
            assert!(catalog.get(WorkerId(5)).is_none());
            checkpoints.push(
                cluster
                    .checkpoint("failed-replacement", start + 6, &manager, false)
                    .await,
            );
        }
        cluster.launch(id + 3, &new, false).unwrap();
        workers.push(cluster.ready(id + 3, &images[1]).await);
        replacements.push(json!({"old_worker":id,"new_worker":id+3,"stopped_at_ms":stopped_at_ms}));
        checkpoints.push(
            cluster
                .checkpoint(&format!("replace-{id}"), start + 6, &manager, false)
                .await,
        );
    }
    acks.extend(traffic(bootstrap.clone(), 23, 29, None, history.clone()).await);
    let downgrade = cluster.launch(8, &old, false).unwrap_err();
    assert!(catalog.get(WorkerId(8)).is_none());
    assert!(!root.path().join("worker-8").exists());
    checkpoints.push(
        cluster
            .checkpoint("post-upgrade", 29, &manager, false)
            .await,
    );
    for id in 4..=6 {
        cluster.stop(id);
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    while !catalog.healthy_workers().is_empty() || !manager.is_empty() {
        assert!(
            Instant::now() < deadline,
            "all workers must disconnect before fresh reopen"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let reopen_before_launch = json!({"workers":catalog.healthy_workers(),"leases":manager.leases(),
        "local_state_exists":root.path().join("worker-7").exists()});
    assert_eq!(
        reopen_before_launch,
        json!({"workers":[],"leases":[],"local_state_exists":false})
    );
    cluster.launch(7, &new, false).unwrap();
    workers.push(cluster.ready(7, &images[1]).await);
    checkpoints.push(cluster.checkpoint("reopen", 29, &manager, true).await);
    acks.extend(traffic(bootstrap, 29, 35, None, history).await);
    checkpoints.push(cluster.checkpoint("post-reopen", 35, &manager, false).await);
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(revision.status.success());
    let evidence = json!({"scenario_id":manifest()["scenario_id"],"manifest_sha256":manifest_sha256(),
        "recovery_manifest_sha256":recovery_sha256(),"revision":String::from_utf8(revision.stdout).unwrap().trim(),
        "images":images,"workers":workers,"acknowledged_operations":acks,"replacements":replacements,
        "failed_replacement":failed,"downgrade_rejection":downgrade,"checkpoints":checkpoints,
        "reopen_before_launch":reopen_before_launch});
    save(
        &artifacts.join("rolling-upgrade-kafka-minio-v1.json"),
        &evidence,
    );
    assert_eq!(validate(&evidence), Ok(()));
    let mut controls = Vec::new();
    for (pointer, value, error) in [
        (
            "/checkpoints/1/observation/weighted_output/3/2/1/Utf8",
            json!("corrupted"),
            "view output",
        ),
        (
            "/checkpoints/1/observation/source_epoch",
            json!(5),
            "source/view progress",
        ),
        (
            "/checkpoints/1/schema/0/1",
            json!("Utf8"),
            "schema compatibility",
        ),
    ] {
        let mut corrupted = evidence.clone();
        *corrupted.pointer_mut(pointer).unwrap() = value;
        assert_eq!(validate(&corrupted), Err(error));
        let artifact = format!("negative-control-{}.json", controls.len());
        save(&artifacts.join(&artifact), &corrupted);
        controls.push(json!({"mutation":pointer,"expected_error":error,"actual_error":validate(&corrupted).unwrap_err(),"artifact":artifact}));
    }
    let mut duplicate = evidence.clone();
    duplicate["checkpoints"][1]["sink_records"]
        .as_array_mut()
        .unwrap()
        .push(evidence["checkpoints"][1]["sink_records"][0].clone());
    assert_eq!(validate(&duplicate), Err("sink output"));
    save(
        &artifacts.join("negative-control-duplicate.json"),
        &duplicate,
    );
    controls.push(json!({"mutation":"duplicate consumed sink record","expected_error":"sink output",
        "actual_error":validate(&duplicate).unwrap_err(),"artifact":"negative-control-duplicate.json"}));
    save(
        &artifacts.join("validation.json"),
        &json!({"success":"qualified","negative_controls":controls}),
    );
    drop(cluster);
    control.shutdown();
}
