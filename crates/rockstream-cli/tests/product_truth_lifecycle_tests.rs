//! v0.60 phase 3b: production CLI truth lifecycle acceptance.

use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Output};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("read ephemeral port").port()
}

fn run_status(binary: &Path, control_addr: &str) -> Output {
    Command::new(binary)
        .env("RUST_LOG", "off")
        .args(["--output", "text", "--control", control_addr, "status"])
        .output()
        .expect("run rockstream status")
}

fn stop(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn acceptance_suite_proves_product_truth_lifecycle() {
    let binary = Path::new(env!("CARGO_BIN_EXE_rockstream"));
    let control_addr = format!("127.0.0.1:{}", free_port());

    let unavailable = run_status(binary, &control_addr);
    assert!(!unavailable.status.success());
    assert_eq!(unavailable.stdout, b"");
    let unavailable_stderr = String::from_utf8(unavailable.stderr).expect("utf-8 status error");
    assert!(
        unavailable_stderr.contains("RS-0004"),
        "{unavailable_stderr}"
    );
    assert!(
        unavailable_stderr.contains("cannot reach RockStream control service")
            && unavailable_stderr.contains("verify `rockstream start` is running")
            && unavailable_stderr.contains("check `rockstream config print-effective`")
            && unavailable_stderr.contains("verify the configured control endpoint"),
        "{unavailable_stderr}"
    );
    assert!(!unavailable_stderr.contains("Cluster Status:"));
    assert!(!unavailable_stderr.contains("Workers:"));
    assert!(!unavailable_stderr.contains("Shards:"));

    let storage = TempDir::new().expect("create control storage");
    let mut node = Command::new(binary)
        .env("RUST_LOG", "off")
        .args([
            "start",
            "--role",
            "control",
            "--storage",
            storage.path().to_str().expect("storage path is utf-8"),
            "--control-bind",
            &control_addr,
            "--daemon",
        ])
        .spawn()
        .expect("start one-node control service");

    let deadline = Instant::now() + Duration::from_secs(5);
    let live = loop {
        let output = run_status(binary, &control_addr);
        if output.status.success() {
            break output;
        }
        if Instant::now() >= deadline {
            stop(&mut node);
            panic!(
                "control service never became reachable: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(20));
    };

    assert_eq!(live.stderr, b"");
    assert_eq!(
        String::from_utf8(live.stdout).expect("utf-8 live status"),
        format!(
            "Cluster Status:\n  Role: control\n  Node ID: none\n  Term: 0\n  Leader ID: none\n  Workers: 0 active, 0 healthy\n  Version: {}\n",
            env!("CARGO_PKG_VERSION")
        )
    );

    stop(&mut node);
    let after_shutdown = run_status(binary, &control_addr);
    assert!(!after_shutdown.status.success());
    assert_eq!(after_shutdown.stdout, b"");
    let shutdown_stderr = String::from_utf8(after_shutdown.stderr).expect("utf-8 shutdown error");
    assert!(shutdown_stderr.contains("RS-0004"), "{shutdown_stderr}");
    assert!(
        shutdown_stderr.contains("cannot reach RockStream control service")
            && shutdown_stderr.contains("verify `rockstream start` is running"),
        "{shutdown_stderr}"
    );
    assert!(!shutdown_stderr.contains("Cluster Status:"));
    assert!(!shutdown_stderr.contains("Workers:"));
    assert!(!shutdown_stderr.contains("Shards:"));
}
