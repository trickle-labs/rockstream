//! v0.60 phase 3b: production CLI truth lifecycle acceptance.

use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use rockstream_cli::output::{
    CliErrorEnvelope, ManagementClusterStatusInfo, ManagementConfigSummaryInfo, ManagementNodeInfo,
};
use tempfile::TempDir;

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("read ephemeral port").port()
}

fn run_status(binary: &Path, management_addr: &str) -> Output {
    Command::new(binary)
        .env("RUST_LOG", "off")
        .args([
            "--output",
            "text",
            "--management",
            management_addr,
            "status",
        ])
        .output()
        .expect("run rockstream status")
}

fn run_json_status(binary: &Path, management_addr: &str) -> Output {
    Command::new(binary)
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
        .expect("run JSON rockstream status")
}

fn assert_json_status_transcript(
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

fn wait_for_status(binary: &Path, management_addr: &str, expected_nodes: usize) -> Output {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last_error = String::new();
    while Instant::now() < deadline {
        let output = run_json_status(binary, management_addr);
        if output.status.success() {
            let status: ManagementClusterStatusInfo =
                serde_json::from_slice(&output.stdout).expect("management status JSON");
            if status.nodes.len() == expected_nodes {
                return output;
            }
        } else {
            last_error = String::from_utf8_lossy(&output.stderr).into_owned();
        }
        thread::sleep(Duration::from_millis(50));
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

fn stop(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

struct RunningControl(Child);

impl Drop for RunningControl {
    fn drop(&mut self) {
        stop(&mut self.0);
    }
}

struct RunningWorker(Child);

impl Drop for RunningWorker {
    fn drop(&mut self) {
        stop(&mut self.0);
    }
}

#[test]
fn acceptance_suite_proves_product_truth_lifecycle() {
    let binary = Path::new(env!("CARGO_BIN_EXE_rockstream"));
    let control_addr = format!("127.0.0.1:{}", free_port());
    let management_addr = format!("127.0.0.1:{}", free_port());

    let unavailable = run_status(binary, &management_addr);
    assert!(!unavailable.status.success());
    assert_eq!(unavailable.stdout, b"");
    let unavailable_stderr = String::from_utf8(unavailable.stderr).expect("utf-8 status error");
    assert!(
        unavailable_stderr.contains("RS-0003"),
        "{unavailable_stderr}"
    );
    assert!(
        unavailable_stderr.contains("management connect failed")
            && unavailable_stderr.contains("Check the management endpoint"),
        "{unavailable_stderr}"
    );
    assert!(!unavailable_stderr.contains("Cluster Status:"));
    assert!(!unavailable_stderr.contains("Workers:"));
    assert!(!unavailable_stderr.contains("Shards:"));

    let storage = TempDir::new().expect("create control storage");
    let node = RunningControl(
        Command::new(binary)
            .env("RUST_LOG", "off")
            .args([
                "start",
                "--role",
                "control",
                "--storage",
                storage.path().to_str().expect("storage path is utf-8"),
                "--control-bind",
                &control_addr,
                "--management-addr",
                &management_addr,
                "--daemon",
            ])
            .spawn()
            .expect("start one-node control service"),
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let live = loop {
        let output = run_status(binary, &management_addr);
        if output.status.success() {
            break output;
        }
        if Instant::now() >= deadline {
            panic!(
                "control service never became reachable: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(20));
    };

    assert_eq!(live.stderr, b"");
    let live_stdout = String::from_utf8(live.stdout).expect("utf-8 management status");
    let observed_at = live_stdout
        .lines()
        .nth(1)
        .and_then(|line| line.strip_prefix("Observed At: "))
        .expect("status includes observation timestamp");
    assert_eq!(observed_at.len(), 24);
    assert_eq!(
        live_stdout,
        format!(
            "Cluster State: unknown\nObserved At: {observed_at}\nSource Version: topology:1\nOperations: 0 active, 0 retained\nRequest Fill: 1 / 64\nACK Waiters: 0 / 64\nNodes: 0\n"
        )
    );

    drop(node);
    let after_shutdown = run_status(binary, &management_addr);
    assert!(!after_shutdown.status.success());
    assert_eq!(after_shutdown.stdout, b"");
    let shutdown_stderr = String::from_utf8(after_shutdown.stderr).expect("utf-8 shutdown error");
    assert!(shutdown_stderr.contains("RS-0003"), "{shutdown_stderr}");
    assert!(
        shutdown_stderr.contains("management connect failed")
            && shutdown_stderr.contains("Check the management endpoint"),
        "{shutdown_stderr}"
    );
    assert!(!shutdown_stderr.contains("Cluster Status:"));
    assert!(!shutdown_stderr.contains("Workers:"));
    assert!(!shutdown_stderr.contains("Shards:"));
}

#[test]
fn standalone_worker_management_status_is_an_exact_json_lifecycle_transcript() {
    let binary = Path::new(env!("CARGO_BIN_EXE_rockstream"));
    let root = TempDir::new().expect("create standalone fixture");
    let control_addr = format!("127.0.0.1:{}", free_port());
    let management_addr = format!("127.0.0.1:{}", free_port());
    let control_storage = root.path().join("control");
    let worker_storage = root.path().join("worker");
    let canary = "v066-management-config-canary-93e1";
    std::fs::write(
        root.path().join("rockstream.toml"),
        format!("version = 1\n[auth]\nsecret_path = \"{canary}\"\n"),
    )
    .expect("write canary config");

    let control = RunningControl(
        Command::new(binary)
            .env("RUST_LOG", "off")
            .current_dir(root.path())
            .args([
                "start",
                "--role",
                "control",
                "--storage",
                control_storage.to_str().unwrap(),
                "--control-bind",
                &control_addr,
                "--management-addr",
                &management_addr,
                "--daemon",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start standalone control process"),
    );

    let empty_output = wait_for_status(binary, &management_addr, 0);
    let empty = assert_json_status_transcript(&empty_output, "unknown", vec![]);
    let empty_revision = topology_revision(&empty);

    let mut worker = RunningWorker(
        Command::new(binary)
            .env("RUST_LOG", "off")
            .args([
                "start",
                "--role",
                "worker",
                "--storage",
                worker_storage.to_str().unwrap(),
                "--control",
                &control_addr,
                "--worker-id",
                "77",
                "--host-id",
                "standalone-host",
                "--availability-zone",
                "standalone-zone",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start standalone worker process"),
    );

    let live_output = wait_for_status(binary, &management_addr, 1);
    let live = assert_json_status_transcript(
        &live_output,
        "healthy",
        vec![ManagementNodeInfo {
            node_id: 77,
            role: "worker".to_owned(),
            address: "127.0.0.1:0".to_owned(),
            state: "active".to_owned(),
            capacity_headroom: 1.0,
            host_id: "standalone-host".to_owned(),
            availability_zone: "standalone-zone".to_owned(),
            healthy: true,
            lifecycle_state: "active".to_owned(),
            registered_at_ms: 0,
            source_version: String::new(),
        }],
    );
    assert!(topology_revision(&live) > empty_revision);

    let config = Command::new(binary)
        .env("RUST_LOG", "off")
        .current_dir(root.path())
        .args([
            "--output",
            "json",
            "--management",
            &management_addr,
            "config",
            "summary",
        ])
        .output()
        .expect("run management config summary");
    assert!(config.status.success());
    let full_config_output = format!(
        "{}{}",
        String::from_utf8_lossy(&config.stdout),
        String::from_utf8_lossy(&config.stderr)
    );
    assert!(!full_config_output.contains(canary), "{full_config_output}");
    let config_summary: ManagementConfigSummaryInfo =
        serde_json::from_slice(&config.stdout).expect("management config summary JSON");
    assert_eq!(
        String::from_utf8(config.stdout.clone()).unwrap(),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&config_summary).unwrap()
        )
    );
    let secret = config_summary
        .values
        .iter()
        .find(|value| value.key.ends_with("secret_path"))
        .expect("config summary includes auth secret path");
    assert_eq!(secret.value, "[REDACTED]");
    assert!(secret.redacted);
    assert_eq!(config_summary.source_version, "effective-node-config:v0.62");
    let missing_operation = Command::new(binary)
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
            "missing-v066-operation",
        ])
        .output()
        .expect("query missing management operation");
    assert!(!missing_operation.status.success());
    assert_eq!(missing_operation.stdout, b"");
    let failure: CliErrorEnvelope =
        serde_json::from_slice(&missing_operation.stderr).expect("management error JSON");
    assert_eq!(failure.code, "RS-0003");
    assert!(
        failure
            .message
            .starts_with("management GetOperation failed:"),
        "{}",
        failure.message
    );
    assert!(
        failure
            .message
            .ends_with("operation missing-v066-operation not found"),
        "{}",
        failure.message
    );
    assert_eq!(
        failure.next_steps,
        "Check the management endpoint, protocol compatibility, and server logs."
    );

    stop(&mut worker.0);
    let removed_output = wait_for_status(binary, &management_addr, 0);
    let removed = assert_json_status_transcript(&removed_output, "unknown", vec![]);
    assert!(topology_revision(&removed) > topology_revision(&live));
    drop(control);
}
