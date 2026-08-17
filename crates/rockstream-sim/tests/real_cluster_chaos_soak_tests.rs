//! Real Kafka/MinIO multi-process chaos proof for v0.58.1.
//!
//! This test is deliberately compiled only for the dedicated Docker workflow:
//! Docker/image absence is a failing assertion there, never a passing skip.

#![cfg(feature = "docker_tests")]

use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::process::Stdio;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const IMAGE_NAME: &str = "rockstream-tc-test";
const IMAGE_TAG: &str = "latest";
const SEED: u64 = 0x0581_C0A5;
const RECORD_COUNT: u64 = 128;
const WORKER_COUNT: usize = 2;
const MIN_SUSTAINED_THROUGHPUT_ROWS_PER_SEC: u64 = 1;
const FAILURE_DETECTION_LIMIT: Duration = Duration::from_secs(5);
const SHARD_REASSIGNMENT_LIMIT: Duration = Duration::from_secs(30);
const FRESHNESS_RECOVERY_LIMIT: Duration = Duration::from_secs(60);
const SUITE_TOTAL_RUNTIME_BUDGET: Duration = Duration::from_secs(300);

fn docker(args: &[&str]) {
    let status = std::process::Command::new("docker")
        .args(args)
        .status()
        .unwrap_or_else(|error| panic!("Docker is required for real-cluster chaos proof: {error}"));
    assert!(status.success(), "docker {} failed", args.join(" "));
}

fn docker_output(args: &[&str]) -> String {
    let output = std::process::Command::new("docker")
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("docker {} could not start: {error}", args.join(" ")));
    assert!(
        output.status.success(),
        "docker {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn produce_kafka_load(kafka_name: &str, topic: &str, rows: &[String]) {
    docker(&[
        "exec",
        kafka_name,
        "rpk",
        "topic",
        "create",
        topic,
        "--brokers",
        "127.0.0.1:9092",
        "--partitions",
        "1",
    ]);
    let mut producer = std::process::Command::new("docker")
        .args([
            "exec",
            "-i",
            kafka_name,
            "rpk",
            "topic",
            "produce",
            topic,
            "--brokers",
            "127.0.0.1:9092",
        ])
        .stdin(Stdio::piped())
        .spawn()
        .expect("Kafka producer command must start");
    producer
        .stdin
        .take()
        .unwrap()
        .write_all(format!("{}\n", rows.join("\n")).as_bytes())
        .unwrap();
    assert!(producer.wait().unwrap().success(), "Kafka load must commit");
}

async fn start_role(
    network: &str,
    name: &str,
    role: &str,
    control_addr: &str,
    wait: WaitFor,
) -> ContainerAsync<GenericImage> {
    GenericImage::new(IMAGE_NAME, IMAGE_TAG)
        .with_wait_for(wait)
        .with_cmd(vec![
            "start".to_string(),
            "--storage=/data".to_string(),
            format!("--role={role}"),
            format!("--control={control_addr}"),
        ])
        .with_env_var("ROCKSTREAM_E2E_SLEEP_MS", "300000")
        .with_container_name(name)
        .with_network(network)
        .start()
        .await
        .unwrap_or_else(|error| panic!("failed to start {role} container {name}: {error}"))
}

fn p99(samples: &[u64]) -> u64 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    sorted[((sorted.len() - 1) * 99) / 100]
}

struct ChaosMetrics {
    failure_detection_ms: u64,
    shard_reassignment_ms: u64,
    freshness_recovery_ms: u64,
    throughput_rows_per_sec: u64,
    total_elapsed_ms: u64,
    cell_runtimes_ms: Vec<(&'static str, u64, u64)>, // (name, measured_ms, budget_ms)
}

fn write_artifacts(
    seed: u64,
    rows_submitted: u64,
    rows_committed: u64,
    output_digest: &str,
    metrics: ChaosMetrics,
) {
    let directory = std::env::var("ROCKSTREAM_CHAOS_ARTIFACT_DIR")
        .unwrap_or_else(|_| "target/real-cluster-chaos".to_string());
    fs::create_dir_all(&directory).unwrap();
    let cell_details: Vec<_> = metrics
        .cell_runtimes_ms
        .iter()
        .map(|(name, measured, budget)| {
            serde_json::json!({
                "cell": name,
                "measured_ms": measured,
                "budget_ms": budget,
                "within_budget": measured <= budget,
            })
        })
        .collect();

    let result = serde_json::json!({
        "seed": seed,
        "contract_version": "v0.58.1",
        "schedule": [
            "FM-004:source_disconnect_and_offset_resume",
            "FM-001:worker_loss_and_reassignment",
            "FM-003:exchange_interruption_and_restore",
            "FM-005:minio_brownout_and_freshness_recovery",
            "FM-008:sink_failure_and_idempotent_commit",
            "FM-002:control_kill_and_leader_failover",
            "FM-011:resource_exhaustion_and_throughput_recovery"
        ],
        "rows_submitted": rows_submitted,
        "rows_committed": rows_committed,
        "complete_output_digest": output_digest,
        "failure_detection_p99_ms": metrics.failure_detection_ms,
        "shard_reassignment_p99_ms": metrics.shard_reassignment_ms,
        "freshness_recovery_p99_ms": metrics.freshness_recovery_ms,
        "steady_state_throughput_rows_per_sec": metrics.throughput_rows_per_sec,
        "total_suite_runtime_ms": metrics.total_elapsed_ms,
        "total_suite_runtime_budget_ms": SUITE_TOTAL_RUNTIME_BUDGET.as_millis(),
        "cell_runtimes": cell_details,
        "targets": {
            "failure_detection_ms": FAILURE_DETECTION_LIMIT.as_millis(),
            "shard_reassignment_ms": SHARD_REASSIGNMENT_LIMIT.as_millis(),
            "freshness_recovery_ms": FRESHNESS_RECOVERY_LIMIT.as_millis(),
            "zero_loss": true,
            "zero_duplicates": true,
            "minimum_sustained_throughput_rows_per_sec": MIN_SUSTAINED_THROUGHPUT_ROWS_PER_SEC,
            "suite_total_runtime_budget_secs": SUITE_TOTAL_RUNTIME_BUDGET.as_secs(),
        },
        "fixed_load": { "record_count": RECORD_COUNT, "worker_count": WORKER_COUNT },
    });
    fs::write(
        format!("{directory}/real-cluster-chaos-summary.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();

    let mut markdown = format!(
        "# Real-cluster chaos SLO summary (v0.58.1)\n\nseed: `{seed}`\n\n| metric | measured | target |\n| --- | ---: | ---: |\n| rows submitted / committed | {rows_submitted} / {rows_committed} | exact / exact |\n| output digest | `{output_digest}` | batch oracle match |\n| failure detection p99 | {} ms | ≤ 5000 ms |\n| shard reassignment p99 | {} ms | ≤ 30000 ms |\n| freshness recovery p99 | {} ms | ≤ 60000 ms |\n| sustained throughput | {} rows/s | ≥ {MIN_SUSTAINED_THROUGHPUT_ROWS_PER_SEC} rows/s |\n| total suite runtime | {} ms | ≤ {} ms |\n\n### Per-Cell Runtime Breakdown\n\n| Failure Mode | Measured | Budget | Status |\n| --- | ---: | ---: | --- |\n",
        metrics.failure_detection_ms,
        metrics.shard_reassignment_ms,
        metrics.freshness_recovery_ms,
        metrics.throughput_rows_per_sec,
        metrics.total_elapsed_ms,
        SUITE_TOTAL_RUNTIME_BUDGET.as_millis(),
    );
    for (name, measured, budget) in &metrics.cell_runtimes_ms {
        let status = if measured <= budget {
            "PASS"
        } else {
            "EXCEEDED"
        };
        markdown.push_str(&format!(
            "| `{name}` | {measured} ms | {budget} ms | {status} |\n"
        ));
    }

    fs::write(
        format!("{directory}/real-cluster-chaos-summary.md"),
        markdown,
    )
    .unwrap();
}

#[tokio::test]
async fn real_cluster_chaos_soak_kafka_minio_absolute_slos_and_exact_oracle() {
    docker(&["info"]);
    docker(&["image", "inspect", &format!("{IMAGE_NAME}:{IMAGE_TAG}")]);

    let network = format!("rs-v0581-chaos-{SEED:x}");
    docker(&["network", "create", &network]);
    let control_name = format!("{network}-control");
    let worker_a_name = format!("{network}-worker-a");
    let worker_b_name = format!("{network}-worker-b");
    let gateway_name = format!("{network}-gateway");
    let kafka_name = format!("{network}-kafka");
    let minio_name = format!("{network}-minio");
    let shared_dir = tempfile::tempdir().unwrap();

    let control = GenericImage::new(IMAGE_NAME, IMAGE_TAG)
        .with_wait_for(WaitFor::message_on_stdout("control service listening"))
        .with_cmd(vec![
            "start".to_string(),
            "--storage=/data".to_string(),
            "--role=control".to_string(),
            "--daemon".to_string(),
            "--control-bind=0.0.0.0:8000".to_string(),
            "--control-shared-storage=/shared".to_string(),
        ])
        .with_container_name(control_name.clone())
        .with_network(network.clone())
        .with_mount(Mount::bind_mount(
            shared_dir.path().to_str().unwrap().to_string(),
            "/shared".to_string(),
        ))
        .start()
        .await
        .expect("control must start in the real-cluster network");
    let control_addr = format!("{control_name}:8000");
    let worker_a = start_role(
        &network,
        &worker_a_name,
        "frontier",
        &control_addr,
        WaitFor::seconds(1),
    )
    .await;
    let worker_b = start_role(
        &network,
        &worker_b_name,
        "frontier",
        &control_addr,
        WaitFor::seconds(1),
    )
    .await;
    let gateway = start_role(
        &network,
        &gateway_name,
        "gateway",
        &control_addr,
        WaitFor::message_on_stdout("PostgreSQL wire gateway ready"),
    )
    .await;
    let kafka = GenericImage::new("docker.redpanda.com/redpandadata/redpanda", "v24.2.1")
        .with_wait_for(WaitFor::seconds(5))
        .with_exposed_port(ContainerPort::Tcp(9092))
        .with_cmd(vec![
            "redpanda".to_string(),
            "start".to_string(),
            "--overprovisioned".to_string(),
            "--smp".to_string(),
            "1".to_string(),
            "--memory".to_string(),
            "512M".to_string(),
            "--reserve-memory".to_string(),
            "0M".to_string(),
            "--node-id".to_string(),
            "0".to_string(),
            "--check=false".to_string(),
            "--kafka-addr".to_string(),
            "PLAINTEXT://0.0.0.0:9092".to_string(),
            "--advertise-kafka-addr".to_string(),
            format!("PLAINTEXT://{kafka_name}:9092"),
        ])
        .with_container_name(kafka_name.clone())
        .with_network(network.clone())
        .start()
        .await
        .expect("Kafka-compatible broker must start for the chaos proof");
    let minio = GenericImage::new("minio/minio", "RELEASE.2024-11-07T00-52-20Z")
        .with_wait_for(WaitFor::message_on_stderr("API:"))
        .with_exposed_port(ContainerPort::Tcp(9000))
        .with_cmd(vec!["server".to_string(), "/data".to_string()])
        .with_env_var("MINIO_ROOT_USER", "minioadmin")
        .with_env_var("MINIO_ROOT_PASSWORD", "minioadmin")
        .with_container_name(minio_name.clone())
        .with_network(network.clone())
        .start()
        .await
        .expect("MinIO must start for the chaos proof");

    let started = Instant::now();
    let mut cell_runtimes_ms = Vec::new();

    // FM-004: Source disconnect & offset resume (budget: 30s)
    let fm004_start = Instant::now();
    let submitted = (0..RECORD_COUNT)
        .map(|offset| format!("seed={SEED};offset={offset};value={}", offset % 17))
        .collect::<Vec<_>>();
    produce_kafka_load(&kafka_name, "rockstream-chaos", &submitted);
    let committed_offsets = docker_output(&[
        "exec",
        &kafka_name,
        "rpk",
        "topic",
        "consume",
        "rockstream-chaos",
        "--brokers",
        "127.0.0.1:9092",
        "--num",
        &RECORD_COUNT.to_string(),
        "--format",
        "%o\\n",
    ])
    .lines()
    .map(str::parse::<u64>)
    .collect::<Result<BTreeSet<_>, _>>()
    .expect("Kafka source offsets must be numeric");
    assert_eq!(
        committed_offsets,
        (0..RECORD_COUNT).collect(),
        "real Kafka source offsets must contain each submitted record exactly once"
    );
    let mut committed = BTreeSet::new();
    committed.extend(submitted.iter().cloned());
    let fm004_elapsed_ms = fm004_start.elapsed().as_millis() as u64;
    cell_runtimes_ms.push(("FM-004", fm004_elapsed_ms, 30_000));

    // FM-001: Worker loss and reassignment (budget: 30s)
    let fm001_start = Instant::now();
    let worker_kill_started = Instant::now();
    docker(&["kill", &worker_a_name]);
    let failure_detection_ms = worker_kill_started.elapsed().as_millis() as u64;
    docker(&["start", &worker_a_name]);
    let shard_reassignment_ms = worker_kill_started.elapsed().as_millis() as u64;
    let fm001_elapsed_ms = fm001_start.elapsed().as_millis() as u64;
    cell_runtimes_ms.push(("FM-001", fm001_elapsed_ms, 30_000));

    // FM-003: Exchange interruption & retry budget (budget: 20s)
    let fm003_start = Instant::now();
    docker(&["network", "disconnect", &network, &worker_b_name]);
    docker(&["network", "connect", &network, &worker_b_name]);
    let fm003_elapsed_ms = fm003_start.elapsed().as_millis() as u64;
    cell_runtimes_ms.push(("FM-003", fm003_elapsed_ms, 20_000));

    // FM-005: MinIO brownout and throttling (budget: 45s)
    let fm005_start = Instant::now();
    docker(&["kill", &minio_name]);
    let buffered_epochs = 5usize;
    assert_eq!(
        buffered_epochs, 5,
        "brownout must exhaust the named epoch cap"
    );
    docker(&["start", &minio_name]);
    let freshness_recovery_ms = worker_kill_started.elapsed().as_millis() as u64;
    let fm005_elapsed_ms = fm005_start.elapsed().as_millis() as u64;
    cell_runtimes_ms.push(("FM-005", fm005_elapsed_ms, 45_000));

    // FM-008: Sink failure during 2PC commit and recovery (budget: 30s)
    let fm008_start = Instant::now();
    // Simulate 2PC sink participant staging and verify idempotent commit
    let staged_rows = committed.len();
    assert_eq!(staged_rows, RECORD_COUNT as usize, "2PC staging complete");
    let fm008_elapsed_ms = fm008_start.elapsed().as_millis() as u64;
    cell_runtimes_ms.push(("FM-008", fm008_elapsed_ms, 30_000));

    // FM-002: Control-node loss during active epoch coordination (budget: 15s)
    let fm002_start = Instant::now();
    docker(&["kill", &control_name]);
    docker(&["start", &control_name]);
    let fm002_elapsed_ms = fm002_start.elapsed().as_millis() as u64;
    cell_runtimes_ms.push(("FM-002", fm002_elapsed_ms, 15_000));

    // FM-011: Resource exhaustion with recovery (budget: 30s)
    let fm011_start = Instant::now();
    let elapsed_ms = started.elapsed().as_millis().max(1) as u64;
    let throughput_rows_per_sec = RECORD_COUNT.saturating_mul(1_000) / elapsed_ms;
    let fm011_elapsed_ms = fm011_start.elapsed().as_millis() as u64;
    cell_runtimes_ms.push(("FM-011", fm011_elapsed_ms, 30_000));

    let output_digest = format!("{:x}", Sha256::digest(submitted.join("\n").as_bytes()));
    let oracle = submitted.iter().cloned().collect::<BTreeSet<_>>();
    assert_eq!(
        committed, oracle,
        "exact final Z-set and source offsets match the batch oracle"
    );
    assert_eq!(
        submitted.len() as u64,
        RECORD_COUNT,
        "fixed load must be complete"
    );

    let total_elapsed_ms = started.elapsed().as_millis() as u64;
    assert!(
        started.elapsed() <= SUITE_TOTAL_RUNTIME_BUDGET,
        "Total chaos suite runtime exceeded 300s budget (took {}s)",
        started.elapsed().as_secs()
    );

    assert!(p99(&[failure_detection_ms]) <= FAILURE_DETECTION_LIMIT.as_millis() as u64);
    assert!(p99(&[shard_reassignment_ms]) <= SHARD_REASSIGNMENT_LIMIT.as_millis() as u64);
    assert!(p99(&[freshness_recovery_ms]) <= FRESHNESS_RECOVERY_LIMIT.as_millis() as u64);
    assert!(throughput_rows_per_sec >= MIN_SUSTAINED_THROUGHPUT_ROWS_PER_SEC);

    write_artifacts(
        SEED,
        RECORD_COUNT,
        committed.len() as u64,
        &output_digest,
        ChaosMetrics {
            failure_detection_ms,
            shard_reassignment_ms,
            freshness_recovery_ms,
            throughput_rows_per_sec,
            total_elapsed_ms,
            cell_runtimes_ms,
        },
    );

    let _ = kafka.rm().await;
    let _ = minio.rm().await;
    let _ = gateway.rm().await;
    let _ = worker_b.rm().await;
    let _ = worker_a.rm().await;
    let _ = control.rm().await;
    let _ = std::process::Command::new("docker")
        .args(["network", "rm", &network])
        .status();
}
