//! Component Lifecycle State Machine & Readiness Gating Tests (v0.62 Slice 5 / Phase 3b).

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use rockstream_cli::component::{Component, ControlComponent, NodeRuntime, WorkerComponent};
use rockstream_cli::metrics_server::{start_management_server, METRICS_SERVER_TEST_LOCK};
use rockstream_types::config::NodeConfig;
use rockstream_types::lifecycle::{LifecycleState, LifecycleTracker};

async fn send_probe(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect to management server");
    let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush request");
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await.expect("read response");
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
async fn test_lifecycle_state_machine_legal_transitions() {
    // Canonical transitions:
    // Created -> Starting -> Recovering -> Ready -> Draining -> Stopping -> Stopped
    let states = [
        LifecycleState::Created,
        LifecycleState::Starting,
        LifecycleState::Recovering,
        LifecycleState::Ready,
        LifecycleState::Draining,
        LifecycleState::Stopping,
        LifecycleState::Stopped,
    ];

    for i in 0..states.len() - 1 {
        let current = states[i];
        let next = states[i + 1];
        assert!(
            current.can_transition_to(next),
            "Expected {:?} -> {:?} to be legal",
            current,
            next
        );
        assert!(
            current.transition_to(next).is_ok(),
            "Expected transition {:?} -> {:?} to succeed",
            current,
            next
        );
    }

    // Any non-terminal state can transition to Fatal
    for s in &states[..states.len() - 1] {
        assert!(
            s.can_transition_to(LifecycleState::Fatal),
            "Expected {:?} -> Fatal to be legal",
            s
        );
    }

    // Terminal states cannot transition to Fatal or any other state
    assert!(!LifecycleState::Stopped.can_transition_to(LifecycleState::Fatal));
    assert!(!LifecycleState::Stopped.can_transition_to(LifecycleState::Starting));
    assert!(!LifecycleState::Fatal.can_transition_to(LifecycleState::Ready));

    // Illegal backward transitions
    assert!(!LifecycleState::Ready.can_transition_to(LifecycleState::Starting));
    assert!(!LifecycleState::Stopping.can_transition_to(LifecycleState::Recovering));
    assert!(!LifecycleState::Created.can_transition_to(LifecycleState::Ready));
}

#[tokio::test]
async fn test_readiness_probe_blocked_during_recovery() {
    let _lock = METRICS_SERVER_TEST_LOCK.lock().await;
    let tracker = Arc::new(LifecycleTracker::new("worker"));
    let handle = start_management_server("127.0.0.1:0", tracker.clone())
        .await
        .expect("start management server");
    let addr = handle.local_addr;

    // 1. Created state: /live is 200, /ready and /readyz are 503
    assert_eq!(tracker.state(), LifecycleState::Created);
    let (status, body) = send_probe(addr, "/live").await;
    assert_eq!(status, 200);
    assert_eq!(body, r#"{"status":"alive"}"#);

    let (status, body) = send_probe(addr, "/ready").await;
    assert_eq!(status, 503);
    assert_eq!(body, r#"{"status":"not_ready","reason":"created"}"#);

    let (status, body) = send_probe(addr, "/readyz").await;
    assert_eq!(status, 503);
    assert_eq!(body, r#"{"status":"not_ready","reason":"created"}"#);

    // 2. Starting state: /live is 200, /ready is 503
    tracker.transition_to(LifecycleState::Starting).unwrap();
    let (status, body) = send_probe(addr, "/ready").await;
    assert_eq!(status, 503);
    assert_eq!(body, r#"{"status":"not_ready","reason":"starting"}"#);

    // 3. Recovering state: /live is 200, /ready is 503
    tracker.transition_to(LifecycleState::Recovering).unwrap();
    let (status, body) = send_probe(addr, "/ready").await;
    assert_eq!(status, 503);
    assert_eq!(body, r#"{"status":"not_ready","reason":"recovering"}"#);

    let (status, body) = send_probe(addr, "/readyz").await;
    assert_eq!(status, 503);
    assert_eq!(body, r#"{"status":"not_ready","reason":"recovering"}"#);

    // 4. Ready state: /ready transitions to 200
    tracker.transition_to(LifecycleState::Ready).unwrap();
    let (status, body) = send_probe(addr, "/ready").await;
    assert_eq!(status, 200);
    assert_eq!(body, r#"{"status":"ready"}"#);

    let (status, body) = send_probe(addr, "/readyz").await;
    assert_eq!(status, 200);
    assert_eq!(body, r#"{"status":"ready"}"#);

    // 5. Draining state: /ready transitions back to 503
    tracker.transition_to(LifecycleState::Draining).unwrap();
    let (status, body) = send_probe(addr, "/ready").await;
    assert_eq!(status, 503);
    assert_eq!(body, r#"{"status":"not_ready","reason":"draining"}"#);

    handle.shutdown();
}

#[tokio::test]
async fn test_recovery_failure_triggers_fatal_state() {
    let _lock = METRICS_SERVER_TEST_LOCK.lock().await;

    // Build a NodeRuntime with a failing recovery component
    let mut config = NodeConfig::default();
    config.node.role = "all".to_string();

    let components: Vec<Box<dyn Component>> = vec![
        Box::new(ControlComponent::new()),
        Box::new(WorkerComponent::new().with_recovery_failure(true)),
    ];

    let mut runtime = NodeRuntime::with_components(config, components);
    let handle = start_management_server("127.0.0.1:0", runtime.tracker().clone())
        .await
        .expect("start management server");
    let addr = handle.local_addr;

    // Start runtime — must fail during recovery and transition tracker to Fatal
    let result = runtime.start().await;
    assert!(
        result.is_err(),
        "Runtime start must fail on recovery failure"
    );

    assert_eq!(runtime.tracker().state(), LifecycleState::Fatal);

    // /live and /healthz must report HTTP 500 in Fatal state
    let (status, body) = send_probe(addr, "/live").await;
    assert_eq!(status, 500, "/live must return 500 when Fatal");
    assert_eq!(body, r#"{"status":"fatal"}"#);

    let (status, body) = send_probe(addr, "/healthz").await;
    assert_eq!(status, 500, "/healthz must return 500 when Fatal");
    assert_eq!(body, r#"{"status":"fatal"}"#);

    // /ready and /readyz must report 503
    let (status, body) = send_probe(addr, "/ready").await;
    assert_eq!(status, 503);
    assert_eq!(body, r#"{"status":"not_ready","reason":"fatal"}"#);

    handle.shutdown();
}

#[tokio::test]
async fn test_lifecycle_all_states_invariants() {
    // Assert all 8 states invariant definitions
    let tracker = LifecycleTracker::new("worker");
    assert_eq!(tracker.state(), LifecycleState::Created);
    assert!(tracker.is_alive());
    assert!(!tracker.is_ready());

    tracker.set_state(LifecycleState::Starting);
    assert!(tracker.is_alive());
    assert!(!tracker.is_ready());

    tracker.set_state(LifecycleState::Recovering);
    assert!(tracker.is_alive());
    assert!(!tracker.is_ready());

    tracker.set_state(LifecycleState::Ready);
    assert!(tracker.is_alive());
    assert!(tracker.is_ready());

    tracker.set_state(LifecycleState::Draining);
    assert!(tracker.is_alive());
    assert!(!tracker.is_ready());

    tracker.set_state(LifecycleState::Stopping);
    assert!(tracker.is_alive());
    assert!(!tracker.is_ready());

    tracker.set_state(LifecycleState::Stopped);
    assert!(!tracker.is_alive());
    assert!(!tracker.is_ready());

    tracker.set_state(LifecycleState::Fatal);
    assert!(!tracker.is_alive());
    assert!(!tracker.is_ready());
}
