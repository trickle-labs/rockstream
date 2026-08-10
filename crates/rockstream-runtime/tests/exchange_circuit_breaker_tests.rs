//! v0.51.25 Slice S6 — Peer Circuit Breaker & Retry Budget Tests (Proof P4)

use parking_lot::RwLock;
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_types::config::ExchangeConfig;
use rockstream_types::ids::WorkerId;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_circuit_breaker_on_unreachable_peer() {
    let peers = Arc::new(RwLock::new(HashMap::new()));
    let worker_id = WorkerId(999);
    peers.write().insert(worker_id, "127.0.0.1:1".to_string()); // unreachable port

    let config = ExchangeConfig {
        connect_timeout_ms: 10,
        max_retries: 3,
        backoff_jitter_ms: 5,
        ..ExchangeConfig::default()
    };

    let pool = ShuffleClientPool::new(peers).with_config(config);

    // Initial state: not circuit broken
    assert!(!pool.is_circuit_broken(worker_id));

    // First attempt fails after max_retries attempts
    let res = pool.get_client(worker_id).await;
    assert!(res.is_err());
    let err_msg = res.unwrap_err();
    assert!(
        err_msg.contains("RS-5003"),
        "expected RS-5003 error, got: {}",
        err_msg
    );

    // Circuit breaker is now open
    assert!(pool.is_circuit_broken(worker_id));

    // Subsequent call immediately returns RS-5003 without network attempt
    let res2 = pool.get_client(worker_id).await;
    assert!(res2.is_err());
    let err_msg2 = res2.unwrap_err();
    assert!(
        err_msg2.contains("circuit breaker tripped"),
        "expected circuit breaker message, got: {}",
        err_msg2
    );

    // Resetting circuit breaker allows retrying
    pool.reset_circuit_breaker(worker_id);
    assert!(!pool.is_circuit_broken(worker_id));
}
