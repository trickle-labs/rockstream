use std::collections::BTreeMap;
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::{local::LocalFileSystem, prefix::PrefixStore, ObjectStore};
use rockstream_cli::output::{
    ManagementClusterStatusInfo, ManagementNodeInfo, ManagementOperationInfo,
};
use rockstream_control::{ShardManager, ShardPersistentStore};
use rockstream_management_proto::v1::{
    management_service_client::ManagementServiceClient, CancelOperationRequest, GetNodeRequest,
    GetOperationRequest, GetShardRequest, ListShardsRequest, MigrateShardRequest,
};
use rockstream_runtime::data_plane::DataPlaneClient;
use rockstream_storage::ShardDb;
use rockstream_test_support::minio::{minio_object_store, start_minio, MINIO_PASS, MINIO_USER};
use rockstream_types::data_plane::WorkerExecutionStatus;
use rockstream_types::ids::{ShardId, WorkerId, WorkloadId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::{Client, NoTls};

static PROCESS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Processes {
    control: Child,
    workers: Vec<Child>,
    worker_metrics: Vec<String>,
    gateway: Child,
}

impl Drop for Processes {
    fn drop(&mut self) {
        let _ = self.gateway.kill();
        let _ = self.gateway.wait();
        for worker in &mut self.workers {
            let _ = worker.kill();
            let _ = worker.wait();
        }
        let _ = self.control.kill();
        let _ = self.control.wait();
    }
}

fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

fn spawn(args: &[&str]) -> Child {
    Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

fn management_status_json(management_addr: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .env("RUST_LOG", "off")
        .args([
            "--output",
            "json",
            "--management",
            management_addr,
            "--control",
            "127.0.0.1:1",
            "status",
        ])
        .output()
        .unwrap()
}

fn allocate_memory_pressure() -> Vec<u8> {
    let mut memory = vec![0_u8; 512 * 1024 * 1024];
    for page in memory.chunks_mut(4096) {
        page[0] = 1;
    }
    memory
}

fn assert_management_status_transcript(
    output: &Output,
    state: &str,
    mut nodes: Vec<ManagementNodeInfo>,
) -> ManagementClusterStatusInfo {
    assert!(
        output.status.success(),
        "management status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stderr, b"");
    let mut status: ManagementClusterStatusInfo =
        serde_json::from_slice(&output.stdout).expect("management status JSON");
    assert_eq!(
        String::from_utf8(output.stdout.clone()).unwrap(),
        format!("{}\n", serde_json::to_string_pretty(&status).unwrap())
    );
    assert!(status.observed_at.ends_with('Z'));
    assert!(status.nodes.iter().all(|node| node.registered_at_ms > 0));
    let source_version = status.source_version.clone();
    for node in &mut nodes {
        let actual = status
            .nodes
            .iter()
            .find(|actual| actual.node_id == node.node_id)
            .expect("expected node is present in status");
        assert!((0.0..=1.0).contains(&actual.capacity_headroom));
        node.capacity_headroom = actual.capacity_headroom;
        node.registered_at_ms = 0;
        node.source_version = source_version.clone();
    }
    status.observed_at = "<timestamp>".to_owned();
    for node in &mut status.nodes {
        node.registered_at_ms = 0;
    }
    assert_eq!(
        status,
        ManagementClusterStatusInfo {
            observed_at: "<timestamp>".to_owned(),
            source_version,
            state: state.to_owned(),
            nodes,
            active_operations: 0,
            retained_operations: 0,
            request_fill: 1,
            request_capacity: 64,
            ack_waiter_fill: 0,
            ack_waiter_capacity: 64,
        }
    );
    status
}

async fn wait_for_management_status_nodes(management_addr: &str, expected_nodes: usize) -> Output {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last_error = String::new();
    while Instant::now() < deadline {
        let output = management_status_json(management_addr);
        if output.status.success() {
            let status: ManagementClusterStatusInfo =
                serde_json::from_slice(&output.stdout).expect("management status JSON");
            if status.nodes.len() == expected_nodes {
                return output;
            }
        } else {
            last_error = String::from_utf8_lossy(&output.stderr).into_owned();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("management status never reported {expected_nodes} nodes: {last_error}");
}

fn topology_revision(status: &ManagementClusterStatusInfo) -> u64 {
    status
        .source_version
        .strip_prefix("topology:")
        .expect("topology source version")
        .parse()
        .expect("numeric topology revision")
}

struct ManagementProcesses {
    control: Child,
    workers: Vec<Child>,
}

impl Drop for ManagementProcesses {
    fn drop(&mut self) {
        for worker in &mut self.workers {
            let _ = worker.kill();
            let _ = worker.wait();
        }
        let _ = self.control.kill();
        let _ = self.control.wait();
    }
}

async fn wait_for_registered_workers(audit_path: &std::path::Path, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let audit = std::fs::read_to_string(audit_path).unwrap_or_default();
        if audit.matches("worker.registered").count() == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("control did not register exactly {expected} workers");
}

async fn connect_gateway(addr: &str) -> Client {
    let addr: std::net::SocketAddr = addr.parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok((client, connection)) = tokio_postgres::connect(
            &format!("host={} port={} user=rockstream", addr.ip(), addr.port()),
            NoTls,
        )
        .await
        {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            return client;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("gateway did not accept pgwire connections at {addr}");
}

async fn read_metrics(addr: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(mut stream) = tokio::net::TcpStream::connect(addr).await {
            stream
                .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            if let Some((_, body)) = response.split_once("\r\n\r\n") {
                return body.to_string();
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("worker metrics endpoint did not respond at {addr}");
}

fn metric_value(body: &str, series: &str) -> u64 {
    body.lines()
        .find_map(|line| {
            line.strip_prefix(series)
                .and_then(|value| value.strip_prefix(' '))
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or_else(|| panic!("missing metric series {series}"))
}

fn metric_sum(body: &str, name: &str) -> u64 {
    body.lines()
        .filter(|line| line.starts_with(name))
        .map(|line| line.rsplit_once(' ').unwrap().1.parse::<u64>().unwrap())
        .sum()
}

fn stable_name_id(namespace: &str, name: &str) -> u64 {
    namespace
        .bytes()
        .chain([0])
        .chain(name.bytes())
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
}

fn stable_route(value: &str, shard_count: usize) -> usize {
    value.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    }) as usize
        % shard_count
}

async fn query_text_rows(client: &Client, sql: &str) -> Vec<Vec<String>> {
    tokio::time::timeout(Duration::from_secs(10), client.query(sql, &[]))
        .await
        .unwrap_or_else(|_| panic!("query timed out: {sql}"))
        .unwrap()
        .into_iter()
        .map(|row| {
            (0..row.len())
                .map(|column| row.get::<_, String>(column))
                .collect()
        })
        .collect()
}

async fn execute(client: &Client, sql: &str) {
    tokio::time::timeout(Duration::from_secs(10), client.batch_execute(sql))
        .await
        .unwrap_or_else(|_| panic!("statement timed out: {sql}"))
        .unwrap();
}

fn aggregate_rows(values: &[i64]) -> Vec<Vec<String>> {
    values
        .iter()
        .enumerate()
        .map(|(group, value)| vec![group.to_string(), value.to_string()])
        .collect()
}

fn join_rows(values: &[i64]) -> Vec<Vec<String>> {
    let mut groups = BTreeMap::<usize, (usize, i64)>::new();
    for (id, value) in values.iter().enumerate() {
        let bucket = id % 16 % 4;
        let entry = groups.entry(bucket).or_default();
        entry.0 += 1;
        entry.1 += value;
    }
    groups
        .into_iter()
        .map(|(bucket, (rows, total))| {
            vec![bucket.to_string(), rows.to_string(), total.to_string()]
        })
        .collect()
}

async fn run_cluster(worker_count: usize, kill_worker: bool) {
    let root = tempfile::tempdir().unwrap();
    let control_addr = free_addr();
    let gateway_addr = free_addr();
    let control_storage = root.path().join("control");
    let gateway_storage = root.path().join("gateway");
    std::fs::create_dir_all(&control_storage).unwrap();
    std::fs::create_dir_all(&gateway_storage).unwrap();

    let control = spawn(&[
        "start",
        "--storage",
        control_storage.to_str().unwrap(),
        "--role",
        "control",
        "--control-bind",
        &control_addr,
        "--daemon",
    ]);
    let mut workers = Vec::new();
    let mut worker_metrics = Vec::new();
    for worker_id in 1..=worker_count {
        let storage = root.path().join(format!("worker-{worker_id}"));
        let metrics_addr = free_addr();
        std::fs::create_dir_all(&storage).unwrap();
        workers.push(spawn(&[
            "start",
            "--storage",
            storage.to_str().unwrap(),
            "--role",
            "worker",
            "--control",
            &control_addr,
            "--worker-id",
            &worker_id.to_string(),
            "--metrics-addr",
            &metrics_addr,
        ]));
        worker_metrics.push(metrics_addr);
    }
    wait_for_registered_workers(&control_storage.join("audit.jsonl"), worker_count).await;
    let gateway = spawn(&[
        "start",
        "--storage",
        gateway_storage.to_str().unwrap(),
        "--role",
        "gateway",
        "--control",
        &control_addr,
        "--listen",
        &gateway_addr,
    ]);
    let mut processes = Processes {
        control,
        workers,
        worker_metrics,
        gateway,
    };
    let client = connect_gateway(&gateway_addr).await;

    execute(
        &client,
        "CREATE TABLE r1_source (id BIGINT PRIMARY KEY, group_id BIGINT NOT NULL, dimension_id BIGINT NOT NULL, value BIGINT NOT NULL, active BOOLEAN NOT NULL)",
    )
    .await;
    execute(
        &client,
        "CREATE TABLE r1_dimension (id BIGINT PRIMARY KEY, bucket BIGINT NOT NULL)",
    )
    .await;
    execute(
        &client,
        "CREATE MATERIALIZED VIEW r1_uniform_scaling AS SELECT group_id, SUM(value) AS total FROM r1_source GROUP BY group_id",
    )
    .await;
    execute(
        &client,
        "CREATE MATERIALIZED VIEW r1_ordinary_join AS SELECT d.bucket, COUNT(*) AS rows, SUM(s.value) AS total FROM r1_source s JOIN r1_dimension d ON s.dimension_id = d.id GROUP BY d.bucket",
    )
    .await;

    let dimensions = (0..16)
        .map(|id| format!("({id},{})", id % 4))
        .collect::<Vec<_>>()
        .join(",");
    execute(
        &client,
        &format!("INSERT INTO r1_dimension VALUES {dimensions}"),
    )
    .await;
    let mut values = (1..=64).map(i64::from).collect::<Vec<_>>();
    let sources = values
        .iter()
        .enumerate()
        .map(|(id, value)| format!("({id},{id},{},{value},TRUE)", id % 16))
        .collect::<Vec<_>>()
        .join(",");
    execute(&client, &format!("INSERT INTO r1_source VALUES {sources}")).await;

    assert_eq!(
        query_text_rows(
            &client,
            "SELECT group_id, total FROM r1_uniform_scaling ORDER BY group_id",
        )
        .await,
        aggregate_rows(&values)
    );
    assert_eq!(
        query_text_rows(
            &client,
            "SELECT bucket, rows, total FROM r1_ordinary_join ORDER BY bucket",
        )
        .await,
        join_rows(&values)
    );

    let workload_id = WorkloadId(stable_name_id("workload", "r1_uniform_scaling"));
    let snapshot = DataPlaneClient::new(&control_addr)
        .read_workload(workload_id)
        .await
        .unwrap();
    let shard_base = workload_id.0.wrapping_mul(16);
    let mut routed = vec![0_u64; worker_count];
    for group in 0..values.len() {
        routed[stable_route(&group.to_string(), worker_count)] += 1;
    }
    let mut assigned_shards = BTreeMap::new();
    for status in &snapshot.workers {
        assert_eq!(status.shard_ids.len(), 1);
        assert_eq!(
            status.process_id,
            processes.workers[status.worker_id.0 as usize - 1].id()
        );
        assert!(assigned_shards
            .insert(status.worker_id, status.shard_ids[0])
            .is_none());
    }
    assert_eq!(
        assigned_shards.keys().copied().collect::<Vec<_>>(),
        (1..=worker_count as u64).map(WorkerId).collect::<Vec<_>>()
    );
    assert_eq!(
        assigned_shards
            .values()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        (0..worker_count)
            .map(|index| ShardId(shard_base.wrapping_add(index as u64)))
            .collect()
    );
    let expected_statuses = (1..=worker_count as u64)
        .map(|worker_id| {
            let worker_id = WorkerId(worker_id);
            let shard_id = assigned_shards[&worker_id];
            let shard_index = shard_id.0.wrapping_sub(shard_base) as usize;
            WorkerExecutionStatus {
                worker_id,
                process_id: processes.workers[worker_id.0 as usize - 1].id(),
                shard_ids: vec![shard_id],
                input_rows: routed[shard_index],
                output_rows: routed[shard_index],
                frontier: 2,
                ready: true,
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(snapshot.workers, expected_statuses);

    let join_workload_id = WorkloadId(stable_name_id("workload", "r1_ordinary_join"));
    let join_snapshot = DataPlaneClient::new(&control_addr)
        .read_workload(join_workload_id)
        .await
        .unwrap();
    for (index, metrics_addr) in processes.worker_metrics.iter().enumerate() {
        let worker_id = WorkerId(index as u64 + 1);
        let body = read_metrics(metrics_addr).await;
        let uniform = snapshot
            .workers
            .iter()
            .find(|status| status.worker_id == worker_id)
            .unwrap();
        let join = join_snapshot
            .workers
            .iter()
            .find(|status| status.worker_id == worker_id)
            .unwrap();
        assert_eq!(
            metric_value(
                &body,
                &format!(
                    "rockstream_r1_worker_shards_owned{{worker_id=\"{}\"}}",
                    worker_id.0
                )
            ),
            2
        );
        assert_eq!(
            metric_value(
                &body,
                &format!(
                    "rockstream_r1_worker_input_rows_total{{worker_id=\"{}\"}}",
                    worker_id.0
                )
            ),
            uniform.input_rows + join.input_rows
        );
        assert_eq!(
            metric_value(
                &body,
                &format!(
                    "rockstream_r1_worker_output_rows_total{{worker_id=\"{}\"}}",
                    worker_id.0
                )
            ),
            uniform.output_rows + join.output_rows
        );
        assert_eq!(
            metric_value(
                &body,
                &format!(
                    "rockstream_r1_worker_state_writes_total{{worker_id=\"{}\"}}",
                    worker_id.0
                )
            ),
            metric_sum(&body, "rockstream_r1_changed_state_writes_total{")
        );
        assert_eq!(
            metric_value(
                &body,
                &format!(
                    "rockstream_r1_worker_exchange_bytes_total{{worker_id=\"{}\"}}",
                    worker_id.0
                )
            ),
            metric_sum(&body, "rockstream_r1_encoded_exchange_bytes_total{")
        );
    }

    if kill_worker {
        let dead_route = (0..16)
            .find(|id| stable_route(&id.to_string(), worker_count) == 0)
            .unwrap();
        processes.workers[0].kill().unwrap();
        processes.workers[0].wait().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let snapshot = DataPlaneClient::new(&control_addr)
                .read_workload(workload_id)
                .await
                .unwrap();
            if snapshot.workers
                == vec![WorkerExecutionStatus {
                    worker_id: WorkerId(2),
                    process_id: processes.workers[1].id(),
                    shard_ids: vec![ShardId(shard_base), ShardId(shard_base.wrapping_add(1))],
                    input_rows: routed[1],
                    output_rows: routed[1],
                    frontier: 2,
                    ready: true,
                }]
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "worker reassignment did not become ready"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let old_value = values[dead_route];
        values[dead_route] += 100;
        execute(
            &client,
            &format!(
                "UPDATE r1_source SET value = {} WHERE id = {dead_route}, group_id = {dead_route}, dimension_id = {}, value = {old_value}, active = TRUE",
                values[dead_route],
                dead_route % 16,
            ),
        )
        .await;
        assert_eq!(
            query_text_rows(
                &client,
                "SELECT group_id, total FROM r1_uniform_scaling ORDER BY group_id",
            )
            .await,
            aggregate_rows(&values)
        );
        assert_eq!(
            query_text_rows(
                &client,
                "SELECT bucket, rows, total FROM r1_ordinary_join ORDER BY bucket",
            )
            .await,
            join_rows(&values)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_workers_execute_aggregate_join_and_fenced_failover() {
    let _test_guard = PROCESS_TEST_LOCK.lock().await;
    run_cluster(1, false).await;
    run_cluster(2, true).await;
    run_cluster(4, false).await;
}

#[tokio::test]
async fn real_multi_process_management_status_is_an_exact_json_worker_lifecycle_transcript() {
    let _test_guard = PROCESS_TEST_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let control_addr = free_addr();
    let mut management_addr = free_addr();
    while management_addr == control_addr {
        management_addr = free_addr();
    }
    let control_storage = root.path().join("management-control");
    let control = spawn(&[
        "start",
        "--storage",
        control_storage.to_str().unwrap(),
        "--role",
        "control",
        "--control-bind",
        &control_addr,
        "--management-addr",
        &management_addr,
        "--daemon",
    ]);
    let mut processes = ManagementProcesses {
        control,
        workers: Vec::new(),
    };

    let empty_output = wait_for_management_status_nodes(&management_addr, 0).await;
    let empty = assert_management_status_transcript(&empty_output, "unknown", vec![]);
    let empty_revision = topology_revision(&empty);

    for (worker_id, host_id, zone) in [
        (501_u64, "multi-host-a", "multi-zone-a"),
        (502_u64, "multi-host-b", "multi-zone-b"),
    ] {
        let storage = root.path().join(format!("management-worker-{worker_id}"));
        let id = worker_id.to_string();
        processes.workers.push(spawn(&[
            "start",
            "--storage",
            storage.to_str().unwrap(),
            "--role",
            "worker",
            "--control",
            &control_addr,
            "--worker-id",
            &id,
            "--host-id",
            host_id,
            "--availability-zone",
            zone,
        ]));
    }
    wait_for_registered_workers(&control_storage.join("audit.jsonl"), 2).await;

    let two_workers_output = wait_for_management_status_nodes(&management_addr, 2).await;
    let two_workers = assert_management_status_transcript(
        &two_workers_output,
        "healthy",
        vec![
            ManagementNodeInfo {
                node_id: 501,
                role: "worker".to_owned(),
                address: "127.0.0.1:0".to_owned(),
                state: "active".to_owned(),
                capacity_headroom: 1.0,
                host_id: "multi-host-a".to_owned(),
                availability_zone: "multi-zone-a".to_owned(),
                healthy: true,
                lifecycle_state: "active".to_owned(),
                registered_at_ms: 0,
                source_version: String::new(),
            },
            ManagementNodeInfo {
                node_id: 502,
                role: "worker".to_owned(),
                address: "127.0.0.1:0".to_owned(),
                state: "active".to_owned(),
                capacity_headroom: 1.0,
                host_id: "multi-host-b".to_owned(),
                availability_zone: "multi-zone-b".to_owned(),
                healthy: true,
                lifecycle_state: "active".to_owned(),
                registered_at_ms: 0,
                source_version: String::new(),
            },
        ],
    );
    assert!(topology_revision(&two_workers) > empty_revision);

    let memory_pressure = allocate_memory_pressure();
    std::hint::black_box(&memory_pressure);
    let pressure_deadline = Instant::now() + Duration::from_secs(10);
    let pressure_output = loop {
        let output = management_status_json(&management_addr);
        if output.status.success() {
            let status: ManagementClusterStatusInfo =
                serde_json::from_slice(&output.stdout).expect("cluster status JSON");
            if status.nodes.len() == 2
                && two_workers.nodes.iter().all(|previous| {
                    status
                        .nodes
                        .iter()
                        .find(|current| current.node_id == previous.node_id)
                        .is_some_and(|current| {
                            current.capacity_headroom < previous.capacity_headroom
                        })
                })
            {
                break output;
            }
        }
        assert!(
            Instant::now() < pressure_deadline,
            "multi-process status headroom did not fall under memory pressure"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let pressure_nodes =
        serde_json::from_slice::<ManagementClusterStatusInfo>(&pressure_output.stdout)
            .expect("memory pressure status JSON")
            .nodes;
    let pressure_status =
        assert_management_status_transcript(&pressure_output, "healthy", pressure_nodes);
    assert!(two_workers.nodes.iter().all(|previous| {
        pressure_status
            .nodes
            .iter()
            .find(|current| current.node_id == previous.node_id)
            .is_some_and(|current| current.capacity_headroom < previous.capacity_headroom)
    }));

    processes.workers[0].kill().unwrap();
    processes.workers[0].wait().unwrap();
    let one_worker_output = wait_for_management_status_nodes(&management_addr, 1).await;
    let one_worker = assert_management_status_transcript(
        &one_worker_output,
        "healthy",
        vec![ManagementNodeInfo {
            node_id: 502,
            role: "worker".to_owned(),
            address: "127.0.0.1:0".to_owned(),
            state: "active".to_owned(),
            capacity_headroom: 1.0,
            host_id: "multi-host-b".to_owned(),
            availability_zone: "multi-zone-b".to_owned(),
            healthy: true,
            lifecycle_state: "active".to_owned(),
            registered_at_ms: 0,
            source_version: String::new(),
        }],
    );
    assert!(topology_revision(&one_worker) > topology_revision(&two_workers));
}

#[tokio::test]
async fn real_multi_process_management_migration_moves_a_shard_and_keeps_its_data() {
    let _test_guard = PROCESS_TEST_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let control_addr = free_addr();
    let management_addr = free_addr();
    let control_storage = root.path().join("migration-control");
    let shared_control_storage = root.path().join("shared-control-state");
    std::fs::create_dir_all(&control_storage).unwrap();
    std::fs::create_dir_all(&shared_control_storage).unwrap();
    let bucket = "v066-migration";
    let (_minio, minio_port) = start_minio(bucket)
        .await
        .expect("release-process migration test requires Docker MinIO");

    let shard_manager = ShardManager::new();
    let old_lease = shard_manager.acquire(ShardId(77), WorkerId(601)).unwrap();
    let control_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&shared_control_storage).unwrap());
    ShardPersistentStore::new(control_store)
        .save(&shard_manager.snapshot())
        .await;
    let worker_store = minio_object_store(minio_port, bucket);
    let shard_store: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(worker_store, "shards/77"));
    let db = ShardDb::builder("db", shard_store)
        .build()
        .await
        .expect("open seeded shard 77");
    db.put(b"v066-migration-key", b"retained-after-migration")
        .await
        .unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    let control = spawn(&[
        "start",
        "--storage",
        control_storage.to_str().unwrap(),
        "--role",
        "control",
        "--control-shared-storage",
        shared_control_storage.to_str().unwrap(),
        "--control-bind",
        &control_addr,
        "--management-addr",
        &management_addr,
        "--daemon",
    ]);
    let mut workers = Vec::new();
    for worker_id in [601_u64, 602] {
        let worker_id = worker_id.to_string();
        let worker_storage = root.path().join(format!("worker-{worker_id}"));
        std::fs::create_dir_all(&worker_storage).unwrap();
        workers.push(
            Command::new(env!("CARGO_BIN_EXE_rockstream"))
                .args([
                    "start",
                    "--storage",
                    worker_storage.to_str().unwrap(),
                    "--role",
                    "worker",
                    "--control",
                    &control_addr,
                    "--worker-id",
                    &worker_id,
                ])
                .env(
                    "ROCKSTREAM_OBJECT_STORE_ENDPOINT",
                    format!("http://127.0.0.1:{minio_port}"),
                )
                .env("ROCKSTREAM_OBJECT_STORE_BUCKET", bucket)
                .env("ROCKSTREAM_OBJECT_STORE_REGION", "us-east-1")
                .env("ROCKSTREAM_OBJECT_STORE_ACCESS_KEY", MINIO_USER)
                .env("ROCKSTREAM_OBJECT_STORE_SECRET_KEY", MINIO_PASS)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
    }
    wait_for_registered_workers(&control_storage.join("audit.jsonl"), 2).await;
    let processes = ManagementProcesses { control, workers };
    wait_for_management_status_nodes(&management_addr, 2).await;

    let endpoint = format!("http://{management_addr}");
    let mut management = ManagementServiceClient::connect(endpoint)
        .await
        .expect("connect to release management process");
    let shard_page = management
        .list_shards(ListShardsRequest {
            protocol_version: 1,
            page_size: 100,
            page_token: String::new(),
        })
        .await
        .expect("list release-process shards")
        .into_inner();
    assert_eq!(shard_page.shards.len(), 1);
    let shard = &shard_page.shards[0];
    assert_eq!(shard.shard_id, "77");
    assert_eq!(shard.owner_node_id, "601");
    assert_eq!(shard.lease_token, old_lease.lease_token.0);
    let shard_id = shard.shard_id.clone();
    let old_lease_token = shard.lease_token;
    let target_node_id = 602_u64;

    unsafe {
        assert_eq!(
            libc::kill(processes.workers[0].id() as libc::pid_t, libc::SIGSTOP),
            0,
            "pause donor worker"
        );
    }
    let cancel_request = MigrateShardRequest {
        protocol_version: 1,
        shard_id: shard_id.clone(),
        target_node_id: target_node_id.to_string(),
        idempotency_key: "v066-release-process-migration-cancel".to_owned(),
    };
    let cancel_accepted = management
        .migrate_shard(cancel_request)
        .await
        .expect("accept cancellable release-process migration")
        .into_inner()
        .operation
        .expect("cancellable migration operation");
    let handoff_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let operation = management
            .get_operation(GetOperationRequest {
                protocol_version: 1,
                operation_id: cancel_accepted.operation_id.clone(),
            })
            .await
            .expect("read cancellable migration phase")
            .into_inner()
            .operation
            .expect("cancellable migration record");
        if operation.phase == "donor_handoff_started" {
            break;
        }
        assert_ne!(operation.state, "failed", "migration failed: {operation:?}");
        assert!(
            Instant::now() < handoff_deadline,
            "migration did not enter donor handoff: {operation:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let cancel_output = Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .env("RUST_LOG", "off")
        .args([
            "--output",
            "json",
            "--management",
            &management_addr,
            "admin",
            "operation",
            "cancel",
            &cancel_accepted.operation_id,
        ])
        .output()
        .expect("cancel migration through release CLI");
    assert!(cancel_output.status.success());
    assert_eq!(cancel_output.stderr, b"");
    let cancelled_cli: ManagementOperationInfo =
        serde_json::from_slice(&cancel_output.stdout).expect("cancelled operation JSON");
    assert_eq!(
        cancel_output.stdout,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&cancelled_cli).unwrap()
        )
        .as_bytes()
    );
    let cancelled = management
        .get_operation(GetOperationRequest {
            protocol_version: 1,
            operation_id: cancel_accepted.operation_id.clone(),
        })
        .await
        .expect("read cancellation record")
        .into_inner()
        .operation
        .expect("cancelled migration record");
    assert_eq!(cancelled.state, "cancelled");
    assert_eq!(cancelled.progress, "10%");
    assert_eq!(cancelled.phase, "cancelled_before_lease_transfer");
    assert_eq!(cancelled.error_code, "");
    assert!(cancelled.next_steps.is_empty());
    assert_eq!(cancelled.source_version, "operation-record:2");
    assert_eq!(
        cancelled_cli,
        ManagementOperationInfo {
            operation_id: cancelled.operation_id.clone(),
            kind: cancelled.kind.clone(),
            state: cancelled.state.clone(),
            started_at: cancelled.started_at.clone(),
            updated_at: cancelled.updated_at.clone(),
            progress: cancelled.progress.clone(),
            phase: cancelled.phase.clone(),
            error_code: cancelled.error_code.clone(),
            next_steps: cancelled.next_steps.clone(),
            source_version: cancelled.source_version.clone(),
        }
    );
    unsafe {
        assert_eq!(
            libc::kill(processes.workers[0].id() as libc::pid_t, libc::SIGCONT),
            0,
            "resume donor worker"
        );
    }
    let reopen_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let status = management
            .get_cluster_status(rockstream_management_proto::v1::GetClusterStatusRequest {
                protocol_version: 1,
            })
            .await
            .expect("read migration cancellation recovery status")
            .into_inner();
        if status.ack_waiter_fill == 0 {
            break;
        }
        assert!(
            Instant::now() < reopen_deadline,
            "cancelled donor handoff did not settle: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let cancelled_shard = management
        .get_shard(GetShardRequest {
            protocol_version: 1,
            shard_id: shard_id.clone(),
        })
        .await
        .expect("read shard after cancellation")
        .into_inner()
        .shard
        .expect("cancelled shard remains leased");
    assert_eq!(cancelled_shard.owner_node_id, "601");
    assert_eq!(cancelled_shard.lease_token, old_lease_token);
    let cancelled_output = Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .env("RUST_LOG", "off")
        .args([
            "--output",
            "json",
            "--management",
            &management_addr,
            "--control",
            "127.0.0.1:1",
            "admin",
            "operation",
            "show",
            &cancelled.operation_id,
        ])
        .output()
        .expect("show cancelled migration through release CLI");
    assert!(cancelled_output.status.success());
    assert_eq!(cancelled_output.stderr, b"");
    let mut cancelled_cli: ManagementOperationInfo =
        serde_json::from_slice(&cancelled_output.stdout).expect("cancelled operation JSON");
    assert_eq!(
        cancelled_output.stdout,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&cancelled_cli).unwrap()
        )
        .as_bytes()
    );
    assert!(!cancelled_cli.started_at.is_empty());
    assert!(!cancelled_cli.updated_at.is_empty());
    cancelled_cli.started_at = "<timestamp>".to_owned();
    cancelled_cli.updated_at = "<timestamp>".to_owned();
    assert_eq!(
        cancelled_cli,
        ManagementOperationInfo {
            operation_id: cancelled.operation_id.clone(),
            kind: "migrate_shard".to_owned(),
            state: "cancelled".to_owned(),
            started_at: "<timestamp>".to_owned(),
            updated_at: "<timestamp>".to_owned(),
            progress: "10%".to_owned(),
            phase: "cancelled_before_lease_transfer".to_owned(),
            error_code: String::new(),
            next_steps: vec![],
            source_version: "operation-record:2".to_owned(),
        }
    );

    let request = MigrateShardRequest {
        protocol_version: 1,
        shard_id: shard_id.clone(),
        target_node_id: target_node_id.to_string(),
        idempotency_key: "v066-release-process-migration".to_owned(),
    };
    unsafe {
        assert_eq!(
            libc::kill(processes.workers[1].id() as libc::pid_t, libc::SIGSTOP),
            0,
            "pause recipient worker"
        );
    }
    let accepted = management
        .migrate_shard(request.clone())
        .await
        .expect("accept release-process migration")
        .into_inner()
        .operation
        .expect("migration operation");
    let retried = management
        .migrate_shard(request)
        .await
        .expect("retry release-process migration")
        .into_inner()
        .operation
        .expect("retried migration operation");
    assert_eq!(retried.operation_id, accepted.operation_id);

    let recipient_deadline = Instant::now() + Duration::from_secs(10);
    let before_late_cancel = loop {
        let operation = management
            .get_operation(GetOperationRequest {
                protocol_version: 1,
                operation_id: accepted.operation_id.clone(),
            })
            .await
            .expect("read post-transfer migration phase")
            .into_inner()
            .operation
            .expect("post-transfer operation record");
        if operation.phase == "recipient_opening" {
            break operation;
        }
        assert_ne!(operation.state, "failed", "migration failed: {operation:?}");
        assert!(
            Instant::now() < recipient_deadline,
            "migration did not reach recipient opening: {operation:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let late_cancel = management
        .cancel_operation(CancelOperationRequest {
            protocol_version: 1,
            operation_id: accepted.operation_id.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(late_cancel.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        late_cancel.message(),
        "operation cancellation is past its safe boundary"
    );
    let after_late_cancel = management
        .get_operation(GetOperationRequest {
            protocol_version: 1,
            operation_id: accepted.operation_id.clone(),
        })
        .await
        .expect("read operation after rejected late cancellation")
        .into_inner()
        .operation
        .expect("unchanged migration record");
    assert_eq!(after_late_cancel, before_late_cancel);
    let transferred_shard = management
        .get_shard(GetShardRequest {
            protocol_version: 1,
            shard_id: shard_id.clone(),
        })
        .await
        .expect("read shard after lease transfer")
        .into_inner()
        .shard
        .expect("transferred shard lease");
    assert_eq!(transferred_shard.owner_node_id, target_node_id.to_string());
    assert!(transferred_shard.lease_token > old_lease_token);
    unsafe {
        assert_eq!(
            libc::kill(processes.workers[1].id() as libc::pid_t, libc::SIGCONT),
            0,
            "resume recipient worker"
        );
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    let terminal = loop {
        let operation = management
            .get_operation(GetOperationRequest {
                protocol_version: 1,
                operation_id: accepted.operation_id.clone(),
            })
            .await
            .expect("read release-process migration operation")
            .into_inner()
            .operation
            .expect("persisted migration operation");
        match operation.state.as_str() {
            "succeeded" => break operation,
            "failed" | "cancelled" => panic!("migration ended unexpectedly: {operation:?}"),
            _ => {}
        }
        assert!(Instant::now() < deadline, "migration did not finish");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(terminal.kind, "migrate_shard");
    assert_eq!(terminal.state, "succeeded");
    assert_eq!(terminal.progress, "100%");
    assert_eq!(terminal.phase, "completed");
    assert_eq!(terminal.error_code, "");
    assert!(terminal.next_steps.is_empty());
    assert_eq!(terminal.source_version, "operation-record:2");

    let current_shard = management
        .get_shard(GetShardRequest {
            protocol_version: 1,
            shard_id,
        })
        .await
        .expect("read migrated shard")
        .into_inner()
        .shard
        .expect("migrated shard");
    assert_eq!(current_shard.owner_node_id, target_node_id.to_string());
    assert!(current_shard.lease_token > old_lease_token);

    let output = Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .env("RUST_LOG", "off")
        .args([
            "--output",
            "json",
            "--management",
            &management_addr,
            "--control",
            "127.0.0.1:1",
            "admin",
            "operation",
            "show",
            &accepted.operation_id,
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(output.stderr, b"");
    let operation: ManagementOperationInfo =
        serde_json::from_slice(&output.stdout).expect("release CLI operation JSON");
    assert_eq!(
        output.stdout,
        format!("{}\n", serde_json::to_string_pretty(&operation).unwrap()).as_bytes()
    );
    assert_eq!(operation.operation_id, accepted.operation_id);
    assert_eq!(operation.kind, "migrate_shard");
    assert_eq!(operation.state, "succeeded");
    assert_eq!(operation.progress, "100%");
    assert_eq!(operation.phase, "completed");
    assert_eq!(operation.error_code, "");
    assert!(operation.next_steps.is_empty());
    assert_eq!(operation.source_version, "operation-record:2");

    unsafe {
        assert_eq!(
            libc::kill(processes.workers[0].id() as libc::pid_t, libc::SIGSTOP),
            0,
            "pause drain recipient worker"
        );
    }
    let drain_accepted_output = Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .env("RUST_LOG", "off")
        .args([
            "--output",
            "json",
            "--management",
            &management_addr,
            "admin",
            "drain",
            "--worker-id",
            "602",
            "--yes",
        ])
        .output()
        .expect("drain worker through release CLI");
    assert!(drain_accepted_output.status.success());
    assert_eq!(drain_accepted_output.stderr, b"");
    let drain_accepted: ManagementOperationInfo =
        serde_json::from_slice(&drain_accepted_output.stdout).expect("drain operation JSON");
    assert_eq!(
        drain_accepted_output.stdout,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&drain_accepted).unwrap()
        )
        .as_bytes()
    );
    assert_eq!(drain_accepted.kind, "drain_worker");
    assert!(matches!(
        drain_accepted.state.as_str(),
        "pending" | "running" | "waiting"
    ));
    let lifecycle_deadline = Instant::now() + Duration::from_secs(10);
    let draining_node = loop {
        let node = management
            .get_node(GetNodeRequest {
                protocol_version: 1,
                node_id: "602".to_owned(),
            })
            .await
            .expect("read draining node")
            .into_inner()
            .node
            .expect("draining worker is retained in topology");
        if node.lifecycle_state.contains("draining") {
            break node;
        }
        assert!(
            Instant::now() < lifecycle_deadline,
            "worker lifecycle did not enter draining: {node:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(draining_node.node_id, "602");
    assert!(draining_node.healthy);
    let draining_status = Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .env("RUST_LOG", "off")
        .args([
            "--output",
            "json",
            "--management",
            &management_addr,
            "status",
        ])
        .output()
        .expect("read draining cluster status through release CLI");
    assert!(draining_status.status.success());
    assert_eq!(draining_status.stderr, b"");
    let draining_transcript = String::from_utf8(draining_status.stdout.clone()).unwrap();
    let status: ManagementClusterStatusInfo =
        serde_json::from_slice(&draining_status.stdout).expect("draining status JSON");
    assert_eq!(
        draining_transcript,
        format!("{}\n", serde_json::to_string_pretty(&status).unwrap())
    );
    assert!(status
        .nodes
        .iter()
        .find(|node| node.node_id == 602)
        .is_some_and(|node| node.lifecycle_state.contains("draining")));
    unsafe {
        assert_eq!(
            libc::kill(processes.workers[0].id() as libc::pid_t, libc::SIGCONT),
            0,
            "resume drain recipient worker"
        );
    }
    let drain_deadline = Instant::now() + Duration::from_secs(30);
    let drain_terminal = loop {
        let shown = Command::new(env!("CARGO_BIN_EXE_rockstream"))
            .env("RUST_LOG", "off")
            .args([
                "--output",
                "json",
                "--management",
                &management_addr,
                "admin",
                "operation",
                "show",
                &drain_accepted.operation_id,
            ])
            .output()
            .expect("show release drain operation");
        assert!(shown.status.success());
        assert_eq!(shown.stderr, b"");
        let transcript = String::from_utf8(shown.stdout.clone()).unwrap();
        let operation: ManagementOperationInfo =
            serde_json::from_slice(&shown.stdout).expect("drain terminal JSON");
        assert_eq!(
            transcript,
            format!("{}\n", serde_json::to_string_pretty(&operation).unwrap())
        );
        match operation.state.as_str() {
            "succeeded" => break operation,
            "failed" | "cancelled" => panic!("drain failed: {transcript}"),
            _ => {}
        }
        assert!(Instant::now() < drain_deadline, "drain did not finish");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(drain_terminal.operation_id, drain_accepted.operation_id);
    assert_eq!(drain_terminal.kind, "drain_worker");
    assert_eq!(drain_terminal.progress, "100%");
    assert_eq!(drain_terminal.phase, "completed");
    assert_eq!(drain_terminal.error_code, "");
    assert!(drain_terminal.next_steps.is_empty());
    assert_eq!(drain_terminal.source_version, "operation-record:2");
    let drained_shard = management
        .get_shard(GetShardRequest {
            protocol_version: 1,
            shard_id: "77".to_owned(),
        })
        .await
        .expect("read drained shard lease")
        .into_inner()
        .shard
        .expect("drained shard remains leased");
    assert_eq!(drained_shard.owner_node_id, "601");
    assert!(drained_shard.lease_token > transferred_shard.lease_token);

    drop(management);
    drop(processes);
    let worker_store = minio_object_store(minio_port, bucket);
    let shard_store: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(worker_store, "shards/77"));
    let db = ShardDb::builder("db", shard_store)
        .build()
        .await
        .expect("reopen migrated shard after worker processes stop");
    assert_eq!(
        db.get(b"v066-migration-key")
            .await
            .unwrap()
            .expect("migrated value")
            .as_ref(),
        b"retained-after-migration"
    );
    db.close().await.unwrap();
}
