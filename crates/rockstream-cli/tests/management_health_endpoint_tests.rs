//! Management HTTP Health, Readiness & Liveness Tests (v0.59.21 Slice 2 / Phase 3a).

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use rockstream_cli::metrics_server::{start_management_server, METRICS_SERVER_TEST_LOCK};
use rockstream_types::error_code::RS_3010;
use rockstream_types::lifecycle::{
    DependencyStatus, HealthReason, LifecycleState, LifecycleTracker,
};

async fn send_get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await.unwrap();
    let resp_str = String::from_utf8_lossy(&resp).to_string();

    let first_line = resp_str.lines().next().unwrap_or("");
    let status_code: u16 = first_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(500);

    let body = resp_str.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status_code, body)
}

#[tokio::test]
async fn test_live_ready_health_endpoints_and_transitions() {
    let _lock = METRICS_SERVER_TEST_LOCK.lock().await;
    let tracker = Arc::new(LifecycleTracker::new("worker"));
    let handle = start_management_server("127.0.0.1:0", tracker.clone())
        .await
        .unwrap();

    // ── Stage 1: Starting State ──
    let (status, body) = send_get(handle.local_addr, "/live").await;
    assert_eq!(status, 200);
    assert_eq!(body, r#"{"status":"alive"}"#);

    let (status, body) = send_get(handle.local_addr, "/ready").await;
    assert_eq!(status, 503);
    assert_eq!(body, r#"{"status":"not_ready","reason":"starting"}"#);

    let (status, body) = send_get(handle.local_addr, "/health").await;
    assert_eq!(status, 503);
    assert!(body.contains(r#""status":"starting""#));
    assert!(body.contains(r#""role":"worker""#));

    // ── Stage 2: Ready & Healthy State ──
    tracker.set_state(LifecycleState::Ready);
    tracker.set_active_shards(8);
    tracker.set_dependency("lfs_storage", DependencyStatus::Ok, None, Some(2));
    tracker.set_dependency("control_plane", DependencyStatus::Ok, None, Some(10));

    let (status, body) = send_get(handle.local_addr, "/ready").await;
    assert_eq!(status, 200);
    assert_eq!(body, r#"{"status":"ready"}"#);

    let (status, body) = send_get(handle.local_addr, "/health").await;
    assert_eq!(status, 200);
    assert!(body.contains(r#""status":"healthy""#));
    assert!(body.contains(r#""active_shards":8"#));
    assert!(body.contains(r#""name":"lfs_storage","status":"ok""#));
    assert!(body.contains(r#""name":"control_plane","status":"ok""#));

    // ── Stage 3: Degraded State (e.g. Replication Lag RS-3010) ──
    tracker.set_state(LifecycleState::Degraded);
    tracker.add_reason(HealthReason::new(
        RS_3010,
        "High consumer replication lag detected on shard-0",
    ));

    let (status, body) = send_get(handle.local_addr, "/ready").await;
    assert_eq!(status, 200);
    assert_eq!(body, r#"{"status":"ready"}"#);

    let (status, body) = send_get(handle.local_addr, "/health").await;
    assert_eq!(status, 200);
    assert!(body.contains(r#""status":"degraded""#));
    assert!(body.contains(r#""code":"RS-3010""#));
    assert!(body.contains("High consumer replication lag"));

    // ── Stage 4: Dependency Loss ──
    tracker.set_state(LifecycleState::DependencyLoss);
    tracker.set_dependency(
        "control_plane",
        DependencyStatus::Unhealthy,
        Some("Heartbeat timeout".to_string()),
        None,
    );

    let (status, body) = send_get(handle.local_addr, "/ready").await;
    assert_eq!(status, 503);
    assert_eq!(body, r#"{"status":"not_ready","reason":"dependency_loss"}"#);

    let (status, body) = send_get(handle.local_addr, "/health").await;
    assert_eq!(status, 503);
    assert!(body.contains(r#""status":"unhealthy""#));

    // ── Stage 5: Draining & Shutdown ──
    tracker.set_state(LifecycleState::Draining);
    let (status, body) = send_get(handle.local_addr, "/ready").await;
    assert_eq!(status, 503);
    assert_eq!(body, r#"{"status":"not_ready","reason":"draining"}"#);

    let (status, body) = send_get(handle.local_addr, "/health").await;
    assert_eq!(status, 503);
    assert!(body.contains(r#""status":"draining""#));

    handle.shutdown();
}
