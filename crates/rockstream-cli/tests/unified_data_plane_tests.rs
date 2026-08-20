//! v0.51.3 Slice 2 exit test: `--role all` opens exactly one shared data
//! plane (no second, unreferenced `gateway-shard/` directory).
//!
//! `run_start` blocks on an OS shutdown signal while serving the gateway, so
//! this test runs it on a background thread and sends a real SIGTERM to the
//! test process (this is the only test in this binary, so it is safe to
//! self-signal) once the shard layout has settled.

use std::path::Path;
use std::time::Duration;

use rockstream_cli::{run_start, StartOptions};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};

fn all_role_opts(storage: &Path, listen_addr: &str) -> StartOptions {
    StartOptions {
        storage: storage.to_path_buf(),
        role: "all".to_string(),
        control: None,
        auth_mode: "off".to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        metrics_addr: None,
        listen_addr: Some(listen_addr.to_string()),
        raft_peers: None,
        raft_node_id: None,
        raft_bind: None,
        raft_bootstrap: false,
        daemon: false,
        worker_id: None,
        control_bind: None,
        control_shared_storage: None,
        query_time_shard_dirs: Vec::new(),
    }
}

#[test]
fn role_all_creates_exactly_one_shard_directory() {
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().to_path_buf();
    let opts = all_role_opts(&storage, "127.0.0.1:0");

    let join = std::thread::spawn(move || run_start(&opts));

    // Poll for the shared shard-0 directory to appear (created once the
    // embedded worker's lease flow completes and the gateway opens it).
    let shard0_dir = storage.join("shards").join("0");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !shard0_dir.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        shard0_dir.exists(),
        "expected {} to be created by `--role all`",
        shard0_dir.display()
    );

    // Give the gateway a moment to finish binding/flushing before we
    // request shutdown, and to prove the directory layout is stable (not a
    // transient partial write).
    std::thread::sleep(Duration::from_millis(200));

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
    let pid = std::process::id().to_string();
    let status = std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("failed to invoke `kill`");
    assert!(status.success(), "kill -TERM failed to signal self");

    let outcome = join
        .join()
        .expect("run_start thread panicked")
        .expect("run_start returned an error");
    assert!(outcome.events_written > 0);
}
