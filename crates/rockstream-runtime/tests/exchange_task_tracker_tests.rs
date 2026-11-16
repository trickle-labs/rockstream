use rockstream_runtime::exchange::service::{ExchangeRegistry, ExchangeService};
use rockstream_runtime::{start_worker_client_with_exchange_endpoint, WorkerExchangeEndpoint};
use rockstream_types::config::ExchangeConfig;
use rockstream_types::identity::InternalTlsConfig;
use rockstream_types::topology::{
    ControlMessage, WorkerCapabilities, WorkerLocation, WorkerMessage,
};
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::test]
async fn test_exchange_service_graceful_shutdown_joins_all_tasks() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let registry = ExchangeRegistry::new();
    let service = ExchangeService::new(registry).with_insecure_internal_dev();

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

#[tokio::test]
async fn test_exchange_service_reports_listener_bind_failure() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = ExchangeService::new(ExchangeRegistry::new()).with_insecure_internal_dev();

    let error = service.start(addr).await.err().expect("bind must fail");
    assert!(error.starts_with(&format!(
        "RS-0001: failed to bind worker shuffle listener {addr}:"
    )));
}

#[tokio::test]
async fn worker_exchange_listener_binds_before_registration_advertises_it() {
    let exchange_reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let exchange_addr = exchange_reservation.local_addr().unwrap();
    drop(exchange_reservation);
    let control_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let control_addr = control_listener.local_addr().unwrap();

    let control_task = tokio::spawn(async move {
        let (stream, _) = control_listener.accept().await.unwrap();
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let WorkerMessage::Register(registration) = serde_json::from_str(&line).unwrap() else {
            panic!("worker must register before control messages");
        };
        assert_eq!(registration.address, exchange_addr.to_string());
        let reply = serde_json::to_string(&ControlMessage::Registered {
            worker_id: registration.worker_id,
        })
        .unwrap()
            + "\n";
        let stream = reader.get_mut();
        stream.write_all(reply.as_bytes()).await.unwrap();
        tokio::net::TcpStream::connect(exchange_addr)
            .await
            .expect("shuffle listener must already accept connections at registration");
    });

    let storage = tempfile::tempdir().unwrap();
    let (_client, worker_task) = start_worker_client_with_exchange_endpoint(
        1,
        &control_addr.to_string(),
        storage.path(),
        WorkerLocation::default(),
        WorkerCapabilities::default(),
        InternalTlsConfig::default(),
        ExchangeConfig::default(),
        WorkerExchangeEndpoint {
            bind_addr: exchange_addr,
            advertise_addr: exchange_addr.to_string(),
        },
        true,
    )
    .await
    .unwrap();

    control_task.await.unwrap();
    worker_task.abort();
    let _ = worker_task.await;
}
