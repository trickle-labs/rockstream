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
            "Cluster State: unknown\nObserved At: {observed_at}\nSource Version: topology:1\nOperations: 0 active, 0 retained\nRequest Fill: 1\nNodes: 0\n"
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
