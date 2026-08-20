use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use rockstream_runtime::data_plane::DataPlaneClient;
use rockstream_types::data_plane::WorkerExecutionStatus;
use rockstream_types::ids::{ShardId, WorkerId, WorkloadId};
use tokio_postgres::{Client, NoTls};

struct Processes {
    control: Child,
    workers: Vec<Child>,
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
    for worker_id in 1..=worker_count {
        let storage = root.path().join(format!("worker-{worker_id}"));
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
        ]));
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
    let expected_statuses = processes
        .workers
        .iter()
        .enumerate()
        .map(|(index, worker)| WorkerExecutionStatus {
            worker_id: WorkerId(index as u64 + 1),
            process_id: worker.id(),
            shard_ids: vec![ShardId(shard_base.wrapping_add(index as u64))],
            input_rows: routed[index],
            output_rows: routed[index],
            frontier: 2,
            ready: true,
        })
        .collect::<Vec<_>>();
    assert_eq!(snapshot.workers, expected_statuses);

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
    run_cluster(1, false).await;
    run_cluster(2, true).await;
    run_cluster(4, false).await;
}
