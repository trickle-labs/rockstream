//! Binary-Level Lifecycle & Role Fault Injection Qualification Tests (v0.59.21 Slice 6 / Phase 3b).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use rockstream_cli::{run_start, StartOptions};
use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_storage::ShardDb;
use rockstream_types::config::RockstreamConfig;
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};

async fn send_http_get(addr: SocketAddr, path: &str) -> Option<(u16, String)> {
    let Ok(mut stream) = TcpStream::connect(addr).await else {
        return None;
    };
    let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    if stream.write_all(req.as_bytes()).await.is_err() {
        return None;
    }
    let _ = stream.flush().await;
    let mut resp = Vec::new();
    if stream.read_to_end(&mut resp).await.is_err() {
        return None;
    }
    let resp_str = String::from_utf8_lossy(&resp).to_string();

    let first_line = resp_str.lines().next().unwrap_or("");
    let status_code: u16 = first_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(500);

    let body = resp_str.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    Some((status_code, body))
}

fn get_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

#[tokio::test]
async fn test_binary_lifecycle_gateway_worker_control_roles() {
    // ══════════════════════════════════════════════════════════════════════════
    // Scenario 1: Binary Gateway & Standalone Role Lifecycle with Management Probes
    // ══════════════════════════════════════════════════════════════════════════
    let dir = TempDir::new().unwrap();
    let storage_dir = dir.path().join("standalone-storage");
    std::fs::create_dir_all(&storage_dir).unwrap();

    let metrics_port = get_free_port();
    let pg_port = get_free_port();
    let metrics_addr = format!("127.0.0.1:{metrics_port}");
    let listen_addr = format!("127.0.0.1:{pg_port}");

    let opts = StartOptions {
        storage: storage_dir.clone(),
        role: "all".to_string(),
        control: None,
        auth_mode: "off".to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        metrics_addr: Some(metrics_addr.clone()),
        listen_addr: Some(listen_addr.clone()),
        raft_peers: None,
        raft_node_id: None,
        raft_bind: None,
        raft_bootstrap: false,
        daemon: false,
        worker_id: None,
        control_bind: None,
        control_shared_storage: None,
        query_time_shard_dirs: Vec::new(),
        shutdown_timeout_secs: Some(10),
    };

    // Set short sleep for test mode to allow full initialization and clean shutdown
    std::env::set_var("ROCKSTREAM_E2E_SLEEP_MS", "300");

    let opts_thread = opts.clone();
    let server_task = std::thread::spawn(move || run_start(&opts_thread));

    let m_addr: SocketAddr = metrics_addr.parse().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut server_ready = false;
    while std::time::Instant::now() < deadline {
        if let Some((status, _)) = send_http_get(m_addr, "/ready").await {
            if status == 200 {
                server_ready = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(server_ready, "Server must transition to ready (HTTP 200)");

    // Assert /live probe
    let (live_status, live_body) = send_http_get(m_addr, "/live")
        .await
        .expect("/live response");
    assert_eq!(live_status, 200, "/live must return HTTP 200 OK");
    assert_eq!(live_body, r#"{"status":"alive"}"#);

    // Assert /ready probe
    let (ready_status, ready_body) = send_http_get(m_addr, "/ready")
        .await
        .expect("/ready response");
    assert_eq!(
        ready_status, 200,
        "/ready must return HTTP 200 OK when running"
    );
    assert_eq!(ready_body, r#"{"status":"ready"}"#);

    // Assert /health probe
    let (health_status, health_body) = send_http_get(m_addr, "/health")
        .await
        .expect("/health response");
    assert_eq!(health_status, 200, "/health must return HTTP 200 OK");
    assert!(health_body.contains(r#""role":"all""#));
    assert!(health_body.contains(r#""status":"healthy""#));

    // Connect via pgwire and verify query execution
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=rockstream dbname=rockstream"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("PostgreSQL client connection must succeed");

    let conn_handle = tokio::spawn(async move {
        let _ = conn.await;
    });

    let msgs = client.simple_query("SELECT 1").await.unwrap();
    let row = msgs
        .into_iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("Must receive row for SELECT 1");
    assert_eq!(row.get(0), Some("1"));
    drop(client);
    let _ = conn_handle.await;

    // Await server task exit
    let outcome = server_task
        .join()
        .unwrap()
        .expect("Server must exit cleanly with code 0");
    assert!(outcome.events_written >= 1);

    // ══════════════════════════════════════════════════════════════════════════
    // Scenario 2: Worker Role Graceful Drain & Shard Lease Mobility
    // ══════════════════════════════════════════════════════════════════════════
    let catalog = TopologyCatalog::new();
    let shard_manager = ShardManager::new();
    let control_service =
        ControlService::new(catalog.clone()).with_shard_manager(shard_manager.clone());
    let control_handle = control_service.start("127.0.0.1:0").await.unwrap();

    let w1_storage = dir.path().join("worker-101");
    std::fs::create_dir_all(&w1_storage).unwrap();

    // Worker 1 acquires shard 1 lease
    let lease_w1 = shard_manager.acquire(ShardId(1), WorkerId(101)).unwrap();
    assert_eq!(lease_w1.worker_id, WorkerId(101));

    // Commit state to SlateDB storage for shard 1
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&w1_storage).unwrap());
    {
        let db = ShardDb::builder("shard-1", store.clone())
            .build()
            .await
            .unwrap();
        db.put(b"committed_key_1", b"val_1").await.unwrap();
        db.flush().await.unwrap();
    }

    // Worker 1 releases shard lease gracefully
    let released = shard_manager.release(ShardId(1), lease_w1.lease_token);
    assert!(released, "Shard lease must release cleanly");

    // Worker 102 immediately acquires shard 1 without wait
    let lease_w2 = shard_manager.acquire(ShardId(1), WorkerId(102)).unwrap();
    assert_eq!(lease_w2.worker_id, WorkerId(102));

    // Worker 102 opens shard and verifies committed row survives
    {
        let db2 = ShardDb::builder("shard-1", store.clone())
            .build()
            .await
            .unwrap();
        let recovered_val = db2.get(b"committed_key_1").await.unwrap();
        assert_eq!(recovered_val.as_deref(), Some(&b"val_1"[..]));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Scenario 3: Worker Ungraceful Crash & Heartbeat Eviction Durability
    // ══════════════════════════════════════════════════════════════════════════
    // Worker 102 writes more committed state
    {
        let db3 = ShardDb::builder("shard-1", store.clone())
            .build()
            .await
            .unwrap();
        db3.put(b"committed_key_2", b"val_2").await.unwrap();
        db3.flush().await.unwrap();
    }

    // Abrupt crash simulation: release all leases held by worker 102 upon eviction
    let revoked_shards = shard_manager.release_worker(WorkerId(102));
    assert_eq!(revoked_shards, vec![ShardId(1)]);

    // Worker 103 takes over after eviction
    let lease_w3 = shard_manager.acquire(ShardId(1), WorkerId(103)).unwrap();
    assert_eq!(lease_w3.worker_id, WorkerId(103));

    // Verify both committed keys survive on durable storage
    {
        let db4 = ShardDb::builder("shard-1", store.clone())
            .build()
            .await
            .unwrap();
        assert_eq!(
            db4.get(b"committed_key_1").await.unwrap().as_deref(),
            Some(&b"val_1"[..])
        );
        assert_eq!(
            db4.get(b"committed_key_2").await.unwrap().as_deref(),
            Some(&b"val_2"[..])
        );
    }

    control_handle.shutdown();
}

#[tokio::test]
async fn test_invalid_config_fails_before_port_bind() {
    let dir = TempDir::new().unwrap();
    let uncreated_storage = dir.path().join("never-created-storage");

    let test_port = get_free_port();
    let test_addr = format!("127.0.0.1:{test_port}");

    // 1. Invalid listen address fails with RS-0002 before creating storage directory
    let invalid_listen_opts = StartOptions {
        storage: uncreated_storage.clone(),
        role: "all".to_string(),
        control: None,
        auth_mode: "off".to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        metrics_addr: None,
        listen_addr: Some("999.999.999.999:5432".to_string()),
        raft_peers: None,
        raft_node_id: None,
        raft_bind: None,
        raft_bootstrap: false,
        daemon: false,
        worker_id: None,
        control_bind: None,
        control_shared_storage: None,
        query_time_shard_dirs: Vec::new(),
        shutdown_timeout_secs: Some(5),
    };

    let err = run_start(&invalid_listen_opts).unwrap_err();
    assert_eq!(err.code, rockstream_types::error_code::RS_0002);
    assert!(
        !uncreated_storage.exists(),
        "Storage directory must not be created on validation failure"
    );

    // 2. Invalid metrics address fails with RS-0002 before creating storage directory
    let invalid_metrics_opts = StartOptions {
        storage: uncreated_storage.clone(),
        role: "all".to_string(),
        control: None,
        auth_mode: "off".to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        metrics_addr: Some("999.999.999.999:9090".to_string()),
        listen_addr: Some(test_addr.clone()),
        raft_peers: None,
        raft_node_id: None,
        raft_bind: None,
        raft_bootstrap: false,
        daemon: false,
        worker_id: None,
        control_bind: None,
        control_shared_storage: None,
        query_time_shard_dirs: Vec::new(),
        shutdown_timeout_secs: Some(5),
    };

    let err = run_start(&invalid_metrics_opts).unwrap_err();
    assert_eq!(err.code, rockstream_types::error_code::RS_0002);
    assert!(
        !uncreated_storage.exists(),
        "Storage directory must not be created on validation failure"
    );

    // Verify the test port is still unbound and can be bound immediately
    let listener =
        std::net::TcpListener::bind(&test_addr).expect("port must remain completely unbound");
    drop(listener);

    // 3. Worker role without control URL fails before binding
    let missing_control_opts = StartOptions {
        storage: uncreated_storage.clone(),
        role: "worker".to_string(),
        control: None,
        auth_mode: "off".to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        metrics_addr: Some(test_addr.clone()),
        listen_addr: None,
        raft_peers: None,
        raft_node_id: None,
        raft_bind: None,
        raft_bootstrap: false,
        daemon: false,
        worker_id: None,
        control_bind: None,
        control_shared_storage: None,
        query_time_shard_dirs: Vec::new(),
        shutdown_timeout_secs: Some(5),
    };

    let err = run_start(&missing_control_opts).unwrap_err();
    assert_eq!(err.code, rockstream_types::error_code::RS_0002);
    assert!(
        !uncreated_storage.exists(),
        "Storage directory must not be created on validation failure"
    );

    // 4. Unknown auth mode fails before binding
    let invalid_auth_opts = StartOptions {
        storage: uncreated_storage.clone(),
        role: "all".to_string(),
        control: None,
        auth_mode: "magic".to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        metrics_addr: Some(test_addr.clone()),
        listen_addr: None,
        raft_peers: None,
        raft_node_id: None,
        raft_bind: None,
        raft_bootstrap: false,
        daemon: false,
        worker_id: None,
        control_bind: None,
        control_shared_storage: None,
        query_time_shard_dirs: Vec::new(),
        shutdown_timeout_secs: Some(5),
    };

    let err = run_start(&invalid_auth_opts).unwrap_err();
    assert_eq!(err.code, rockstream_types::error_code::RS_0002);
    assert!(
        !uncreated_storage.exists(),
        "Storage directory must not be created on validation failure"
    );
}

#[tokio::test]
async fn test_all_roles_startup_and_lifecycle_traces() {
    // Verify that every supported role can be initialized and produces valid lifecycle traces
    for role in &["all", "control", "worker", "gateway", "metrics"] {
        let mut config = rockstream_types::config::NodeConfig::default();
        config.node.role = role.to_string();
        let runtime = rockstream_cli::component::NodeRuntime::new(config);
        assert!(
            runtime.is_ok(),
            "NodeRuntime for role {role} must initialize cleanly"
        );
        let mut rt = runtime.unwrap();
        assert_eq!(
            rt.tracker().state(),
            rockstream_types::lifecycle::LifecycleState::Created
        );
        let start_res = rt.start().await;
        assert!(start_res.is_ok(), "Start for role {role} must succeed");
        assert_eq!(
            rt.tracker().state(),
            rockstream_types::lifecycle::LifecycleState::Ready
        );
        let shutdown_res = rt.shutdown().await;
        assert!(
            shutdown_res.is_ok(),
            "Shutdown for role {role} must succeed"
        );
        assert_eq!(
            rt.tracker().state(),
            rockstream_types::lifecycle::LifecycleState::Stopped
        );
    }
}

#[tokio::test]
async fn test_server_roles_remain_alive_until_explicit_signal() {
    let dir = TempDir::new().unwrap();
    let storage_dir = dir.path().join("server-alive-storage");
    std::fs::create_dir_all(&storage_dir).unwrap();

    let metrics_port = get_free_port();
    let pg_port = get_free_port();
    let metrics_addr = format!("127.0.0.1:{metrics_port}");
    let listen_addr = format!("127.0.0.1:{pg_port}");

    let opts = StartOptions {
        storage: storage_dir.clone(),
        role: "all".to_string(),
        control: None,
        auth_mode: "off".to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        metrics_addr: Some(metrics_addr.clone()),
        listen_addr: Some(listen_addr.clone()),
        raft_peers: None,
        raft_node_id: None,
        raft_bind: None,
        raft_bootstrap: false,
        daemon: false,
        worker_id: None,
        control_bind: None,
        control_shared_storage: None,
        query_time_shard_dirs: Vec::new(),
        shutdown_timeout_secs: Some(5),
    };

    // Set a controlled test duration via ROCKSTREAM_E2E_SLEEP_MS to prove server remains alive beyond old 50ms default
    std::env::set_var("ROCKSTREAM_E2E_SLEEP_MS", "400");

    let server_thread = std::thread::spawn(move || run_start(&opts));

    let m_addr: SocketAddr = metrics_addr.parse().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut reached_ready = false;
    while std::time::Instant::now() < deadline {
        if let Some((code, _)) = send_http_get(m_addr, "/ready").await {
            if code == 200 {
                reached_ready = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(reached_ready, "Server must reach Ready (HTTP 200)");

    // At 150ms (well past the old 50ms premature exit), verify server is STILL alive
    tokio::time::sleep(Duration::from_millis(150)).await;
    let (live_status, _) = send_http_get(m_addr, "/live").await.expect("still alive");
    assert_eq!(live_status, 200, "Server must remain alive beyond 50ms");

    // Wait for the server thread to finish clean shutdown
    let res = server_thread.join().expect("server thread join");
    assert!(res.is_ok(), "Server must exit cleanly: {:?}", res);
}

#[tokio::test]
async fn test_graceful_shutdown_drains_work_within_deadline() {
    let mut config = rockstream_types::config::NodeConfig::default();
    config.node.role = "all".to_string();
    config.runtime.shutdown_timeout_secs = 5;

    let mut runtime = rockstream_cli::component::NodeRuntime::new(config).expect("runtime");
    runtime.start().await.expect("start");
    assert_eq!(
        runtime.tracker().state(),
        rockstream_types::lifecycle::LifecycleState::Ready
    );

    let shutdown_start = std::time::Instant::now();
    runtime.shutdown().await.expect("clean shutdown");
    let elapsed = shutdown_start.elapsed();

    assert_eq!(
        runtime.tracker().state(),
        rockstream_types::lifecycle::LifecycleState::Stopped
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "Shutdown must complete within configured deadline"
    );
}

#[tokio::test]
async fn test_watchdog_enforces_shutdown_timeout() {
    use rockstream_types::lifecycle::LifecycleTracker;
    use std::sync::atomic::Ordering;
    rockstream_cli::shutdown::SUPPRESS_PROCESS_EXIT.store(true, Ordering::SeqCst);

    let tracker = Arc::new(LifecycleTracker::new("worker"));
    let coordinator = rockstream_cli::shutdown::ShutdownCoordinator::new(
        tracker.clone(),
        Duration::from_millis(60),
    );

    tracker.set_state(rockstream_types::lifecycle::LifecycleState::Ready);
    coordinator.trigger_shutdown();
    assert_eq!(
        tracker.state(),
        rockstream_types::lifecycle::LifecycleState::Draining
    );

    let watchdog = coordinator.spawn_watchdog();
    // Intentionally do not call mark_completed()
    tokio::time::sleep(Duration::from_millis(120)).await;
    let _ = watchdog.await;

    assert_eq!(
        tracker.state(),
        rockstream_types::lifecycle::LifecycleState::Fatal,
        "Watchdog must transition tracker to Fatal when deadline is exceeded"
    );
}
