//! Qualification of automatic skew routing through separate release-capable
//! CLI processes. Every observed result is compared with the batch oracle.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, PlanNode};
use rockstream_runtime::data_plane::DataPlaneClient;
use rockstream_types::config::{JoinStrategy, NodeConfig};
use rockstream_types::data_plane::{
    DeploymentColumn, DeploymentRequest, DeploymentSchema, RuntimeRow, SourceDeltaRequest,
    WorkloadSnapshot, DEPLOYMENT_DESCRIPTOR_VERSION,
};
use rockstream_types::ids::{OperatorId, WorkloadId};

struct ProcessCluster {
    root: tempfile::TempDir,
    control: Child,
    workers: Vec<Child>,
    control_address: String,
    object_store_env: BTreeMap<String, String>,
}

impl Drop for ProcessCluster {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("{}", self.logs());
        }
        for process in &mut self.workers {
            let _ = process.kill();
            let _ = process.wait();
        }
        let _ = self.control.kill();
        let _ = self.control.wait();
    }
}

fn free_address() -> String {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string()
}

impl ProcessCluster {
    async fn start(worker_count: usize) -> Self {
        Self::start_with_elasticity(worker_count, false, BTreeMap::new()).await
    }

    async fn start_with_elasticity(
        worker_count: usize,
        elasticity: bool,
        object_store_env: BTreeMap<String, String>,
    ) -> Self {
        let root = tempfile::tempdir().unwrap();
        let control_address = free_address();
        let control_path = root.path().join("control");
        std::fs::create_dir_all(&control_path).unwrap();
        let mut control_config = NodeConfig::default();
        control_config.control.elasticity.enabled = elasticity;
        control_config.control.elasticity.minimum_residency_ms = 0;
        let control_config_path = control_path.join("node.toml");
        std::fs::write(
            &control_config_path,
            toml::to_string(&control_config).unwrap(),
        )
        .unwrap();
        let control_log = File::create(root.path().join("control.log")).unwrap();
        let control = Command::new(env!("CARGO_BIN_EXE_rockstream"))
            .args([
                "start",
                "--role",
                "control",
                "--allow-insecure-internal-dev",
                "--daemon",
            ])
            .arg("--control-bind")
            .arg(&control_address)
            .arg("--storage")
            .arg(&control_path)
            .env("TOKIO_WORKER_THREADS", "2")
            .env("ROCKSTREAM_CONFIG", control_config_path)
            .envs(&object_store_env)
            .stdout(control_log.try_clone().unwrap())
            .stderr(control_log)
            .spawn()
            .unwrap();
        let mut cluster = Self {
            root,
            control,
            workers: Vec::new(),
            control_address,
            object_store_env,
        };
        let deadline = Instant::now() + Duration::from_secs(15);
        while tokio::net::TcpStream::connect(&cluster.control_address)
            .await
            .is_err()
        {
            assert!(
                cluster.control.try_wait().unwrap().is_none(),
                "control exited: {}",
                cluster.logs()
            );
            assert!(
                Instant::now() < deadline,
                "control did not listen: {}",
                cluster.logs()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        for worker_id in 1..=worker_count {
            cluster.spawn_worker(worker_id);
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let audit =
                std::fs::read_to_string(control_path.join("audit.jsonl")).unwrap_or_default();
            if audit.matches("worker.registered").count() == worker_count {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "workers did not register: {}",
                cluster.logs()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        cluster
    }

    fn spawn_worker(&mut self, worker_id: usize) {
        let storage = self.root.path().join(format!("worker-{worker_id}"));
        std::fs::create_dir_all(&storage).unwrap();
        let exchange = free_address();
        let mut config = NodeConfig::default();
        config.worker.exchange_bind_addr = Some(exchange.clone());
        config.worker.exchange_advertise_addr = Some(exchange);
        let config_path = storage.join("node.toml");
        std::fs::write(&config_path, toml::to_string(&config).unwrap()).unwrap();
        let log = File::create(self.root.path().join(format!("worker-{worker_id}.log"))).unwrap();
        self.workers.push(
            Command::new(env!("CARGO_BIN_EXE_rockstream"))
                .args(["start", "--role", "worker", "--allow-insecure-internal-dev"])
                .arg("--control")
                .arg(&self.control_address)
                .arg("--worker-id")
                .arg(worker_id.to_string())
                .arg("--storage")
                .arg(storage)
                .env("ROCKSTREAM_CONFIG", config_path)
                .env("TOKIO_WORKER_THREADS", "2")
                .envs(&self.object_store_env)
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
    }

    fn client(&self) -> DataPlaneClient {
        DataPlaneClient::new(&self.control_address).with_insecure_internal_dev()
    }

    fn logs(&self) -> String {
        let mut files = vec![
            self.root.path().join("control.log"),
            self.root.path().join("control").join("audit.jsonl"),
        ];
        files.extend(
            (1..=self.workers.len()).map(|id| self.root.path().join(format!("worker-{id}.log"))),
        );
        files
            .into_iter()
            .map(|path| {
                format!(
                    "{}:\n{}",
                    path.display(),
                    std::fs::read_to_string(&path).unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn storage_root(&self) -> PathBuf {
        self.root.path().join("workloads")
    }
}

fn aggregate_deployment(workload_id: WorkloadId, storage_root: PathBuf) -> DeploymentRequest {
    let plan = PlanNode::ViewSink {
        view_name: "automatic_sum".into(),
        pk: vec![0],
        child: Box::new(PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "events".into(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        }),
    };
    DeploymentRequest {
        version: DEPLOYMENT_DESCRIPTOR_VERSION,
        workload_id,
        namespace: "public".into(),
        workload_name: "automatic_sum".into(),
        plan_json: serde_json::to_string(&plan).unwrap(),
        join_strategy: JoinStrategy::Auto,
        schemas: vec![DeploymentSchema {
            relation: "events".into(),
            columns: vec![
                DeploymentColumn {
                    name: "k".into(),
                    data_type: "i64".into(),
                },
                DeploymentColumn {
                    name: "v".into(),
                    data_type: "i64".into(),
                },
            ],
        }],
        frontier: 0,
        storage_root: storage_root.to_string_lossy().into_owned(),
        sink_operator_id: OperatorId(403),
        output_columns: vec!["k".into(), "total".into()],
        primary_key: vec![],
        merge_key_columns: vec![0],
        routing_columns: BTreeMap::from([("events".into(), 0)]),
    }
}

fn source_request(
    workload_id: WorkloadId,
    epoch: u64,
    rows: &[(i64, i64, i64)],
) -> SourceDeltaRequest {
    SourceDeltaRequest {
        version: DEPLOYMENT_DESCRIPTOR_VERSION,
        request_id: format!("skew-qualification-{workload_id}-{epoch}"),
        workload_id,
        epoch,
        source: "events".into(),
        rows: rows
            .iter()
            .map(|(key, value, weight)| RuntimeRow {
                values_tsv: format!("{key}\t{value}"),
                weight: *weight,
            })
            .collect(),
    }
}

fn materialized_rows(snapshot: &WorkloadSnapshot) -> BTreeMap<String, i64> {
    assert!(
        snapshot.complete,
        "incomplete epoch was exposed as a result"
    );
    let mut result = BTreeMap::new();
    for row in snapshot.shards.iter().flat_map(|shard| &shard.rows) {
        *result.entry(row.values_tsv.clone()).or_default() += row.weight;
    }
    result.retain(|_, weight| *weight != 0);
    result
}

async fn assert_aggregate_oracle(
    client: &DataPlaneClient,
    workload: WorkloadId,
    input: &[(i64, i64)],
) -> WorkloadSnapshot {
    let expected = rockstream_oracle::aggregate_oracle::run_datafusion_aggregate(input)
        .await
        .unwrap()
        .into_iter()
        .map(|(key, sum, _, _)| (format!("{key}\t{sum}"), 1))
        .collect::<BTreeMap<_, _>>();
    let snapshot = client.read_workload(workload).await.unwrap();
    assert_eq!(materialized_rows(&snapshot), expected);
    snapshot
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_process_hot_key_epochs_replay_and_retractions_match_batch_oracle() {
    let cluster = ProcessCluster::start(4).await;
    let client = cluster.client();
    let workload = WorkloadId(40301);
    client
        .deploy_with_shards(aggregate_deployment(workload, cluster.storage_root()), 8)
        .await
        .unwrap();
    let mut input = Vec::new();
    for epoch in 1..=5 {
        let mut rows = Vec::new();
        for key in 0..9 {
            let count = if key == 0 { 50 * 8 } else { 8 };
            for index in 0..count {
                let value = epoch as i64 * 10_000 + index;
                rows.push((key, value, 1));
                input.push((key, value));
            }
        }
        let request = source_request(workload, epoch, &rows);
        assert_eq!(client.submit_delta(request.clone()).await.unwrap(), epoch);
        let snapshot = assert_aggregate_oracle(&client, workload, &input).await;
        let observed_processes = snapshot
            .workers
            .iter()
            .map(|worker| worker.process_id)
            .collect::<BTreeSet<_>>();
        let expected_processes = cluster
            .workers
            .iter()
            .map(Child::id)
            .collect::<BTreeSet<_>>();
        assert_eq!(observed_processes, expected_processes);
        if epoch >= 3 {
            let diagnostics = client.routing_diagnostics(workload).await.unwrap();
            let plan = diagnostics.completed.unwrap().plan;
            assert_eq!(plan["heavy_keys"], serde_json::json!([0]));
            let buckets = plan["bucket_input_rows"].as_object().unwrap();
            assert_eq!(
                buckets.values().cloned().collect::<Vec<_>>(),
                vec![serde_json::json!(100); 4]
            );
            let topology = client.get_workload_topology(workload).await.unwrap();
            let owners = buckets
                .keys()
                .map(|id| {
                    let shard_id: u64 = id.parse().unwrap();
                    topology
                        .shards
                        .iter()
                        .find(|shard| shard.shard_id.0 == shard_id)
                        .unwrap()
                        .worker_id
                        .0
                })
                .collect::<BTreeSet<_>>();
            assert_eq!(owners, BTreeSet::from([1, 2, 3, 4]));
        }
        // A fresh routing client must reproduce the same durable epoch plan.
        let restarted_router = cluster.client();
        assert_eq!(restarted_router.submit_delta(request).await.unwrap(), epoch);
        assert_eq!(
            materialized_rows(&restarted_router.read_workload(workload).await.unwrap()),
            materialized_rows(&snapshot)
        );
    }
    let removed = input.drain(..400).collect::<Vec<_>>();
    let retractions = removed
        .into_iter()
        .map(|(key, value)| (key, value, -1))
        .collect::<Vec<_>>();
    client
        .submit_delta(source_request(workload, 6, &retractions))
        .await
        .unwrap();
    assert_aggregate_oracle(&client, workload, &input).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonadmitted_hot_key_arithmetic_uses_exact_single_owner_fallback() {
    let cluster = ProcessCluster::start(4).await;
    let client = cluster.client();
    let workload = WorkloadId(40302);
    client
        .deploy_with_shards(aggregate_deployment(workload, cluster.storage_root()), 8)
        .await
        .unwrap();
    // The final sum is representable, but the absolute regrouping budget is
    // not: this must reach the canonical staged accumulator without splitting.
    let rows = [(7, i64::MAX, 1), (7, -i64::MAX, 1)];
    client
        .submit_delta(source_request(workload, 1, &rows))
        .await
        .unwrap();
    assert_eq!(
        materialized_rows(&client.read_workload(workload).await.unwrap()),
        BTreeMap::from([("7\t0".into(), 1)])
    );
    let before = materialized_rows(&client.read_workload(workload).await.unwrap());
    let error = client
        .submit_delta(source_request(workload, 2, &[(7, i64::MAX, 2)]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("RS-"), "{error}");
    assert_eq!(
        materialized_rows(&client.read_workload(workload).await.unwrap()),
        before
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "release qualification: 100 measured epochs per profile on four processes"]
async fn four_process_skew_p99_is_within_twice_uniform() {
    let cluster = ProcessCluster::start(4).await;
    let client = cluster.client();
    let workloads = [WorkloadId(40311), WorkloadId(40312)];
    for workload in workloads {
        client
            .deploy_with_shards(aggregate_deployment(workload, cluster.storage_root()), 8)
            .await
            .unwrap();
    }
    let mut input = [Vec::new(), Vec::new()];
    let mut latencies = [Vec::new(), Vec::new()];
    for epoch in 1..=105 {
        for (profile, workload) in workloads.into_iter().enumerate() {
            let rows = (0..464)
                .map(|index| {
                    let key = if profile == 0 {
                        index % 9
                    } else if index < 400 {
                        0
                    } else {
                        1 + (index - 400) / 8
                    };
                    (key, epoch as i64 * 1000 + index, 1)
                })
                .collect::<Vec<_>>();
            input[profile].extend(rows.iter().map(|(key, value, _)| (*key, *value)));
            let start = Instant::now();
            client
                .submit_delta(source_request(workload, epoch, &rows))
                .await
                .unwrap();
            let snapshot = client.read_workload(workload).await.unwrap();
            assert!(snapshot.complete);
            if epoch > 5 {
                latencies[profile].push(start.elapsed().as_micros() as u64);
            }
        }
    }
    for (profile, workload) in workloads.into_iter().enumerate() {
        assert_aggregate_oracle(&client, workload, &input[profile]).await;
    }
    let mut p99 = [0u64; 2];
    for (profile, values) in latencies.iter_mut().enumerate() {
        values.sort_unstable();
        p99[profile] = values[98];
    }
    let evidence = serde_json::json!({
        "schema":"rockstream-skew-process-v1", "worker_process_ids":cluster.workers.iter().map(Child::id).collect::<Vec<_>>(),
        "workers":4, "virtual_shards_per_workload":8, "rows_per_epoch":464, "hot_key_ratio":50,
        "warmup_epochs":5, "measured_epochs":100, "latency_boundary":"submit through complete materialized snapshot",
        "uniform_p99_us":p99[0], "skew_p99_us":p99[1], "uniform_samples_us":latencies[0], "skew_samples_us":latencies[1],
        "exact_batch_oracle":true,
    });
    if let Ok(path) = std::env::var("ROCKSTREAM_SKEW_EVIDENCE_PATH") {
        if let Some(parent) = std::path::Path::new(&path).parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    }
    println!("{evidence}");
    assert!(
        p99[1] <= p99[0].saturating_mul(2),
        "observed skew p99 {}us exceeds twice uniform {}us",
        p99[1],
        p99[0]
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_crash_after_durable_prepare_replays_exactly_on_replacement() {
    let mut cluster = ProcessCluster::start(4).await;
    let client = cluster.client();
    let workload = WorkloadId(40331);
    client
        .deploy_with_shards(aggregate_deployment(workload, cluster.storage_root()), 8)
        .await
        .unwrap();
    let mut input = Vec::new();
    for epoch in 1..=3 {
        let rows = (0..464)
            .map(|index| {
                (
                    if index < 400 {
                        0
                    } else {
                        1 + (index - 400) / 8
                    },
                    epoch * 1000 + index,
                    1,
                )
            })
            .collect::<Vec<_>>();
        input.extend(rows.iter().map(|(key, value, _)| (*key, *value)));
        client
            .submit_delta(source_request(workload, epoch as u64, &rows))
            .await
            .unwrap();
    }
    let topology = client.get_workload_topology(workload).await.unwrap();
    let anchor_owner = topology
        .shards
        .iter()
        .min_by_key(|shard| shard.shard_id)
        .unwrap()
        .worker_id
        .0 as usize;
    let victim_index = (0..cluster.workers.len())
        .rev()
        .find(|index| index + 1 != anchor_owner)
        .unwrap();
    let victim_pid = cluster.workers[victim_index].id();
    assert!(Command::new("kill")
        .args(["-STOP", &victim_pid.to_string()])
        .status()
        .unwrap()
        .success());
    let rows = (0..464)
        .map(|index| {
            (
                if index < 400 {
                    0
                } else {
                    1 + (index - 400) / 8
                },
                4000 + index,
                1,
            )
        })
        .collect::<Vec<_>>();
    let request = source_request(workload, 4, &rows);
    let sending = client.clone();
    let in_flight = request.clone();
    // The explicit cached development path lets us pause a recipient before
    // its partial ACK without pausing the metadata authorization handshake.
    let task = tokio::spawn(async move { sending.submit_delta_direct(in_flight).await });
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(snapshot) = client.routing_diagnostics(workload).await {
            if snapshot
                .pending
                .as_ref()
                .is_some_and(|record| record.request_id == request.request_id)
            {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "did not observe a durable in-flight epoch: {}",
            cluster.logs()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        !task.is_finished(),
        "paused participant must withhold epoch completion"
    );
    let mut victim = cluster.workers.remove(victim_index);
    victim.kill().unwrap();
    victim.wait().unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
    let replay_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        client.invalidate_topology(workload);
        if client.submit_delta(request.clone()).await.is_ok() {
            break;
        }
        assert!(
            Instant::now() < replay_deadline,
            "replacement failed to replay: {}",
            cluster.logs()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    input.extend(rows.iter().map(|(key, value, _)| (*key, *value)));
    assert_aggregate_oracle(&client, workload, &input).await;
    let replay = cluster.client();
    replay.submit_delta(request).await.unwrap();
    assert_aggregate_oracle(&replay, workload, &input).await;
    let after = replay.get_workload_topology(workload).await.unwrap();
    assert!(after
        .shards
        .iter()
        .all(|shard| shard.worker_id.0 != victim_index as u64 + 1));
    assert_eq!(
        after
            .shards
            .iter()
            .map(|shard| shard.shard_id)
            .collect::<BTreeSet<_>>(),
        topology
            .shards
            .iter()
            .map(|shard| shard.shard_id)
            .collect::<BTreeSet<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker/MinIO; run by skew qualification CI"]
async fn shared_store_two_to_four_to_two_workers_preserves_exact_output() {
    let bucket = format!("skew-{}", uuid::Uuid::new_v4().simple());
    let (_minio, port) = rockstream_test_support::minio::start_minio(&bucket)
        .await
        .expect("shared-store qualification requires a real Docker/MinIO service");
    let object_store_env = BTreeMap::from([
        (
            "ROCKSTREAM_OBJECT_STORE_ENDPOINT".into(),
            format!("http://127.0.0.1:{port}"),
        ),
        ("ROCKSTREAM_OBJECT_STORE_BUCKET".into(), bucket),
        ("ROCKSTREAM_OBJECT_STORE_REGION".into(), "us-east-1".into()),
        (
            "ROCKSTREAM_OBJECT_STORE_ACCESS_KEY".into(),
            rockstream_test_support::minio::MINIO_USER.into(),
        ),
        (
            "ROCKSTREAM_OBJECT_STORE_SECRET_KEY".into(),
            rockstream_test_support::minio::MINIO_PASS.into(),
        ),
    ]);
    let mut cluster = ProcessCluster::start_with_elasticity(2, true, object_store_env).await;
    let client = cluster.client();
    let workload = WorkloadId(40321);
    client
        .deploy_with_shards(aggregate_deployment(workload, cluster.storage_root()), 8)
        .await
        .unwrap();
    let original = client.get_workload_topology(workload).await.unwrap();
    assert_eq!(
        original
            .shards
            .iter()
            .map(|shard| shard.worker_id.0)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([1, 2])
    );
    cluster.spawn_worker(3);
    cluster.spawn_worker(4);
    let mut input = Vec::new();
    let mut epoch = 0;
    for target in [BTreeSet::from([1, 2, 3, 4]), BTreeSet::from([1, 2])] {
        if target.len() == 2 {
            for worker in [3, 4] {
                let address = cluster.control_address.clone();
                tokio::task::spawn_blocking(move || {
                    rockstream_cli::request_worker_drain(&address, worker)
                })
                .await
                .unwrap()
                .unwrap();
            }
        }
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            epoch += 1;
            let rows = (0..16)
                .map(|key| (key, epoch as i64, 1))
                .collect::<Vec<_>>();
            let request = source_request(workload, epoch, &rows);
            loop {
                match client.submit_delta(request.clone()).await {
                    Ok(_) => break,
                    Err(error) => {
                        assert!(
                            Instant::now() < deadline,
                            "handoff failed to resume epoch: {error}; {}",
                            cluster.logs()
                        );
                        client.invalidate_topology(workload);
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                }
            }
            input.extend(rows.iter().map(|(key, value, _)| (*key, *value)));
            loop {
                match client.read_workload(workload).await {
                    Ok(snapshot) => {
                        let expected =
                            rockstream_oracle::aggregate_oracle::run_datafusion_aggregate(&input)
                                .await
                                .unwrap()
                                .into_iter()
                                .map(|(key, sum, _, _)| (format!("{key}\t{sum}"), 1))
                                .collect::<BTreeMap<_, _>>();
                        assert_eq!(materialized_rows(&snapshot), expected);
                        break;
                    }
                    Err(error) => {
                        assert!(
                            Instant::now() < deadline,
                            "handoff withheld snapshots indefinitely at epoch {epoch}: {error}; {}",
                            cluster.logs()
                        );
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            if let Ok(topology) = client.get_workload_topology(workload).await {
                assert_eq!(
                    topology
                        .shards
                        .iter()
                        .map(|shard| shard.shard_id)
                        .collect::<BTreeSet<_>>(),
                    original
                        .shards
                        .iter()
                        .map(|shard| shard.shard_id)
                        .collect::<BTreeSet<_>>()
                );
                if topology
                    .shards
                    .iter()
                    .map(|shard| shard.worker_id.0)
                    .collect::<BTreeSet<_>>()
                    == target
                {
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "automatic ownership did not reach {target:?}: {}",
                cluster.logs()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    assert_aggregate_oracle(&client, workload, &input).await;
}
