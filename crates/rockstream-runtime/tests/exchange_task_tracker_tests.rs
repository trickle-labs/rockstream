use rockstream_runtime::exchange::service::{ExchangeRegistry, ExchangeService};
use std::time::Instant;

#[tokio::test]
async fn test_exchange_service_graceful_shutdown_joins_all_tasks() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let registry = ExchangeRegistry::new();
    let service = ExchangeService::new(registry);

    service.start(addr).await.unwrap();

    let start_time = Instant::now();
    service.shutdown().await;

    let elapsed = start_time.elapsed();
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "shutdown took longer than 5 seconds: {:?}",
        elapsed
    );
}
