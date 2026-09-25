//! Management HTTP Health, Readiness & Liveness Tests (v0.59.21 Slice 2 / Phase 3a).

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use rockstream_cli::metrics_server::{start_management_server, METRICS_SERVER_TEST_LOCK};
use rockstream_types::error_code::RS_3010;
use rockstream_types::lifecycle::{
    DependencyStatus, HealthDimensionStatus, HealthReason, HealthReport, LifecycleState,
    LifecycleTracker,
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
#[allow(deprecated)]
async fn test_live_ready_health_endpoints_and_transitions() {
    let _lock = METRICS_SERVER_TEST_LOCK.lock().await;
    let tracker = Arc::new(LifecycleTracker::new("worker"));
    tracker.set_state(LifecycleState::Starting);
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

#[tokio::test]
async fn test_seven_health_dimensions_evaluation_and_stale_policy() {
    let _lock = METRICS_SERVER_TEST_LOCK.lock().await;
    let tracker = Arc::new(LifecycleTracker::new("worker"));
    tracker.set_state(LifecycleState::Ready);
    let handle = start_management_server("127.0.0.1:0", tracker.clone())
        .await
        .unwrap();

    // 1. Healthy state: all dimensions PASS
    let (status, body) = send_get(handle.local_addr, "/health").await;
    assert_eq!(status, 200);
    let report: HealthReport = serde_json::from_str(&body).expect("valid health report JSON");
    let dims = report.dimensions.expect("dimensions must be present");

    assert_eq!(dims.liveness.status, HealthDimensionStatus::Pass);
    assert_eq!(dims.readiness.status, HealthDimensionStatus::Pass);
    assert_eq!(dims.availability.status, HealthDimensionStatus::Pass);
    assert_eq!(dims.freshness.status, HealthDimensionStatus::Pass);
    assert_eq!(dims.durability.status, HealthDimensionStatus::Pass);
    assert_eq!(dims.capacity.status, HealthDimensionStatus::Pass);
    assert_eq!(dims.degradation.status, HealthDimensionStatus::Pass);
    assert!(!dims.liveness.is_stale);
    assert!(dims.is_overall_healthy());

    // 2. Storage failure decoupling:
    // Grounding Rule: A live process with unavailable storage MUST remain live while reporting durability: FAIL and availability: FAIL.
    // Liveness alone NEVER produces workload health.
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    tracker.set_durability_status(
        HealthDimensionStatus::Fail,
        Some("SlateDB LFS write failed: I/O error".to_string()),
        now_secs,
    );
    tracker.set_availability_status(
        HealthDimensionStatus::Fail,
        Some("Query path unavailable due to storage write failure".to_string()),
        now_secs,
    );

    // Liveness probe must STILL succeed!
    let (live_status, live_body) = send_get(handle.local_addr, "/live").await;
    assert_eq!(
        live_status, 200,
        "Process must remain live even when storage is unavailable"
    );
    assert_eq!(live_body, r#"{"status":"alive"}"#);

    // Overall /health must fail (503)
    let (health_status, health_body) = send_get(handle.local_addr, "/health").await;
    assert_eq!(
        health_status, 503,
        "Health must fail when durability/availability is FAIL"
    );
    let fail_report: HealthReport =
        serde_json::from_str(&health_body).expect("valid health report JSON");
    let fail_dims = fail_report.dimensions.expect("dimensions must be present");
    assert_eq!(fail_dims.liveness.status, HealthDimensionStatus::Pass);
    assert_eq!(fail_dims.durability.status, HealthDimensionStatus::Fail);
    assert_eq!(fail_dims.availability.status, HealthDimensionStatus::Fail);
    assert!(!fail_dims.is_overall_healthy());

    // 3. Stale observation policy:
    // Observations older than 15s are marked is_stale: true and downgraded to UNKNOWN.
    tracker.set_durability_status(HealthDimensionStatus::Pass, None, now_secs);
    tracker.set_availability_status(HealthDimensionStatus::Pass, None, now_secs);
    // Set freshness observation from 20 seconds ago
    let stale_time = now_secs.saturating_sub(20);
    tracker.set_freshness_status(HealthDimensionStatus::Pass, None, stale_time);

    let (_stale_status, stale_body) = send_get(handle.local_addr, "/health").await;
    let stale_report: HealthReport =
        serde_json::from_str(&stale_body).expect("valid health report JSON");
    let stale_dims = stale_report.dimensions.expect("dimensions must be present");
    assert!(
        stale_dims.freshness.is_stale,
        "Freshness observation older than 15s must be marked stale"
    );
    assert_eq!(
        stale_dims.freshness.status,
        HealthDimensionStatus::Unknown,
        "Stale freshness observation must be downgraded to UNKNOWN rather than false healthy 0s"
    );
    // Liveness remains instantaneous / never stale
    assert!(!stale_dims.liveness.is_stale);
    assert_eq!(stale_dims.liveness.status, HealthDimensionStatus::Pass);

    handle.shutdown();
}

#[tokio::test]
async fn test_health_dimension_liveness_evaluation() {
    let tracker = LifecycleTracker::new("worker");
    assert!(tracker.is_alive());
    let (_, report) = tracker.generate_health_report();
    let dims = report.dimensions.unwrap();
    assert_eq!(dims.liveness.status, HealthDimensionStatus::Pass);
    assert!(!dims.liveness.is_stale);

    tracker.set_state(LifecycleState::Fatal);
    assert!(!tracker.is_alive());
    let (_, fatal_report) = tracker.generate_health_report();
    assert_eq!(
        fatal_report.dimensions.unwrap().liveness.status,
        HealthDimensionStatus::Fail
    );
}

#[tokio::test]
async fn test_health_dimension_readiness_evaluation() {
    let tracker = LifecycleTracker::new("worker");
    tracker.set_state(LifecycleState::Starting);
    let (_, report) = tracker.generate_health_report();
    assert_eq!(
        report.dimensions.unwrap().readiness.status,
        HealthDimensionStatus::Fail
    );

    tracker.set_state(LifecycleState::Ready);
    let (_, ready_report) = tracker.generate_health_report();
    assert_eq!(
        ready_report.dimensions.unwrap().readiness.status,
        HealthDimensionStatus::Pass
    );
}

#[tokio::test]
async fn test_health_dimension_availability_evaluation() {
    let tracker = LifecycleTracker::new("worker");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    tracker.set_availability_status(HealthDimensionStatus::Pass, None, now);
    let (_, report) = tracker.generate_health_report();
    assert_eq!(
        report.dimensions.unwrap().availability.status,
        HealthDimensionStatus::Pass
    );

    // After 15s timeout, marked UNKNOWN
    tracker.set_availability_status(HealthDimensionStatus::Pass, None, now.saturating_sub(16));
    let (_, stale_report) = tracker.generate_health_report();
    let dims = stale_report.dimensions.unwrap();
    assert!(dims.availability.is_stale);
    assert_eq!(dims.availability.status, HealthDimensionStatus::Unknown);
}

#[tokio::test]
async fn test_health_dimension_freshness_evaluation() {
    let tracker = LifecycleTracker::new("worker");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    tracker.set_freshness_status(
        HealthDimensionStatus::Fail,
        Some("SLO breached".to_string()),
        now,
    );
    let (_, report) = tracker.generate_health_report();
    assert_eq!(
        report.dimensions.unwrap().freshness.status,
        HealthDimensionStatus::Fail
    );
}

#[tokio::test]
async fn test_health_dimension_durability_evaluation() {
    let tracker = LifecycleTracker::new("worker");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    tracker.set_durability_status(
        HealthDimensionStatus::Fail,
        Some("Disk I/O error".to_string()),
        now,
    );
    let (code, report) = tracker.generate_health_report();
    assert_eq!(code, 503);
    assert_eq!(
        report.dimensions.unwrap().durability.status,
        HealthDimensionStatus::Fail
    );
}

#[tokio::test]
async fn test_health_dimension_capacity_evaluation() {
    let tracker = LifecycleTracker::new("worker");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    tracker.set_capacity_status(
        HealthDimensionStatus::Warn,
        Some("Memory at 85%".to_string()),
        now,
    );
    let (_, report) = tracker.generate_health_report();
    assert_eq!(
        report.dimensions.unwrap().capacity.status,
        HealthDimensionStatus::Warn
    );
}

#[tokio::test]
async fn test_health_dimension_degradation_evaluation() {
    let tracker = LifecycleTracker::new("worker");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    tracker.set_degradation_status(
        HealthDimensionStatus::Warn,
        Some("Shard migration active".to_string()),
        now,
    );
    let (_, report) = tracker.generate_health_report();
    assert_eq!(
        report.dimensions.unwrap().degradation.status,
        HealthDimensionStatus::Warn
    );
}
