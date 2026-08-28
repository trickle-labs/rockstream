//! v0.51.3 Slice 2 exit test: `--role all` opens exactly one shared data
//! plane (no second, unreferenced `gateway-shard/` directory).
//!
//! `run_start` blocks on an OS shutdown signal while serving the gateway, so
//! this test launches the CLI as a child and sends it a real SIGTERM once the
//! shard layout has settled.

use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::Duration;

#[test]
fn role_all_creates_exactly_one_shard_directory() {
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().to_path_buf();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let listen_addr = listener.local_addr().unwrap();
    drop(listener);
    let mut child = Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .args([
            "start",
            "--storage",
            storage.to_str().unwrap(),
            "--role",
            "all",
            "--auth",
            "off",
            "--listen",
            &listen_addr.to_string(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start rockstream child");

    // Poll until the gateway has installed its signal handler and finished
    // opening/flushing the shared shard.
    let shard0_dir = storage.join("shards").join("0");
    let audit_path = storage.join("audit.jsonl");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !std::fs::read_to_string(&audit_path)
        .map(|contents| contents.contains("\"gateway.started\""))
        .unwrap_or(false)
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        std::fs::read_to_string(&audit_path)
            .map(|contents| contents.contains("\"gateway.started\""))
            .unwrap_or(false),
        "expected gateway startup event in {}",
        audit_path.display()
    );

    let gateway_shard_dir = storage.join("gateway-shard");
    assert!(
        !gateway_shard_dir.exists(),
        "`--role all` must not create a second, unreferenced gateway-shard/ directory, found {}",
        gateway_shard_dir.display()
    );

    let shards_dir = storage.join("shards");
    let entries: Vec<_> = std::fs::read_dir(&shards_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(
        entries.len(),
        1,
        "expected exactly one shard directory under {}, found {:?}",
        shards_dir.display(),
        entries
    );
    assert_eq!(entries[0].to_string_lossy(), "0");

    // The shard directory must contain a valid (non-empty) manifest —
    // ShardDb::flush()/GatewayServer bind wrote at least one object.
    let has_files = std::fs::read_dir(&shard0_dir).unwrap().next().is_some();
    assert!(
        has_files,
        "expected {} to contain manifest/data files, found none",
        shard0_dir.display()
    );

    // Trigger the same shutdown path an operator's SIGTERM would.
    let pid = child.id().to_string();
    let status = Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("failed to invoke `kill`");
    assert!(status.success(), "kill -TERM failed to signal child");
    assert!(child.wait().unwrap().success());
}
