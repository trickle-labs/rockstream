use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_control::{
    ManagementOperationStore, ManagementService, ShardManager, TopologyCatalog,
};
use rockstream_management_proto::v1::{
    management_service_client::ManagementServiceClient, DrainWorkerRequest, GetHealthRequest,
    GetNodeRequest, GetOperationRequest, ListNodesRequest,
};
use rockstream_types::config::NodeConfig;
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerCapabilities, WorkerMessage,
    WorkerRegistration,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

async fn register_worker(addr: std::net::SocketAddr, worker_id: u64) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let registration = WorkerRegistration::new(
        WorkerId(worker_id),
        NodeRole::Worker,
        format!("127.0.0.1:{}", 7000 + worker_id),
        CapacityHeadroom::FULL,
    )
    .with_capabilities(WorkerCapabilities {
        shared_shard_store_id: Some([9; 32]),
        ..Default::default()
    });
    let request = serde_json::to_string(&WorkerMessage::Register(registration)).unwrap() + "\n";
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(&mut stream);
    let mut response = String::new();
    reader.read_line(&mut response).await.unwrap();
    assert!(matches!(
        serde_json::from_str::<ControlMessage>(response.trim()).unwrap(),
        ControlMessage::Registered { worker_id: registered } if registered == WorkerId(worker_id)
    ));
    drop(reader);
    stream
}

async fn read_until(
    stream: &mut TcpStream,
    matches: impl Fn(&ControlMessage) -> bool,
) -> ControlMessage {
    let mut reader = BufReader::new(stream);
    loop {
        let mut response = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            reader.read_line(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        let message = serde_json::from_str(response.trim()).unwrap();
        if matches(&message) {
            return message;
        }
    }
}

#[tokio::test]
async fn management_reads_return_exact_authoritative_empty_state() {
    let catalog = TopologyCatalog::new();
    let service = ManagementService::new(
        catalog,
        ShardManager::new(),
        ManagementOperationStore::new(Arc::new(InMemory::new())),
        NodeConfig::default(),
    );
    let server = service.start("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", server.addr);
    let mut client = ManagementServiceClient::connect(endpoint).await.unwrap();

    let nodes = client
        .list_nodes(ListNodesRequest {
            protocol_version: 1,
            page_size: 0,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(nodes.protocol_version, 1);
    assert_eq!(nodes.source_version, "topology:0");
    assert!(nodes
        .observed_at
        .parse::<chrono::DateTime<chrono::FixedOffset>>()
        .is_ok());
    assert!(nodes.nodes.is_empty());
    assert!(nodes.next_page_token.is_empty());

    let health = client
        .get_health(GetHealthRequest {
            protocol_version: 1,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(health.protocol_version, 1);
    assert_eq!(health.source_version, "topology:0");
    assert!(health
        .observed_at
        .parse::<chrono::DateTime<chrono::FixedOffset>>()
        .is_ok());
    assert_eq!(health.state, "unknown");
    assert_eq!(
        health.reason,
        "authoritative process health telemetry is not registered"
    );

    let missing = client
        .get_node(GetNodeRequest {
            protocol_version: 1,
            node_id: "404".to_owned(),
        })
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);
    assert_eq!(missing.message(), "node 404 not found");
    server.shutdown();
}

#[tokio::test]
async fn management_rejects_incompatible_versions_before_reading_state() {
    let service = ManagementService::new(
        TopologyCatalog::new(),
        ShardManager::new(),
        ManagementOperationStore::new(Arc::new(InMemory::new())),
        NodeConfig::default(),
    );
    let server = service.start("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", server.addr);
    let mut client = ManagementServiceClient::connect(endpoint).await.unwrap();
    let error = client
        .list_nodes(ListNodesRequest {
            protocol_version: 2,
            page_size: 0,
            page_token: String::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        error.message(),
        "unsupported protocol version 2; supported range is 1..=1"
    );
    server.shutdown();
}

#[tokio::test]
async fn management_drain_is_idempotent_and_transfers_the_owned_shard() {
    let catalog = TopologyCatalog::new();
    let shard_manager = ShardManager::new();
    let operations = Arc::new(InMemory::new());
    let management_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let management_addr = management_listener.local_addr().unwrap();
    drop(management_listener);
    let service = rockstream_control::ControlService::new(catalog.clone())
        .with_shard_manager(shard_manager.clone())
        .with_management(
            management_addr.to_string(),
            operations,
            NodeConfig::default(),
        );
    let control = service.start("127.0.0.1:0").await.unwrap();
    let mut donor = register_worker(control.addr, 1).await;
    let mut recipient = register_worker(control.addr, 2).await;
    shard_manager.acquire(ShardId(77), WorkerId(1)).unwrap();
    let mut client = ManagementServiceClient::connect(format!("http://{management_addr}"))
        .await
        .unwrap();
    let request = DrainWorkerRequest {
        protocol_version: 1,
        worker_id: "1".to_owned(),
        idempotency_key: "drain-worker-1".to_owned(),
    };
    let accepted = client
        .drain_worker(request.clone())
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    let repeated = client
        .drain_worker(request)
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(repeated.operation_id, accepted.operation_id);
    assert_eq!(accepted.kind, "drain_worker");
    assert_eq!(accepted.state, "pending");
    assert!(accepted.phase.is_empty());
    assert!(accepted.error_code.is_empty());
    assert!(accepted.next_steps.is_empty());
    assert_eq!(
        shard_manager.get(ShardId(77)).unwrap().worker_id,
        WorkerId(1)
    );

    let _ = read_until(&mut donor, |message| {
        matches!(message, ControlMessage::BeginDrain(_))
    })
    .await;
    let acknowledgement = serde_json::to_string(&WorkerMessage::DrainAck {
        worker_id: WorkerId(1),
        shards_remaining: 0,
    })
    .unwrap()
        + "\n";
    donor.write_all(acknowledgement.as_bytes()).await.unwrap();
    let assignment = read_until(&mut recipient, |message| {
        matches!(message, ControlMessage::ShardAssigned { lease } if lease.shard_id == ShardId(77))
    })
    .await;
    assert!(matches!(
        assignment,
        ControlMessage::ShardAssigned { lease }
            if lease.worker_id == WorkerId(2) && lease.shard_id == ShardId(77)
    ));

    let mut completed = None;
    for _ in 0..50 {
        let response = client
            .get_operation(GetOperationRequest {
                protocol_version: 1,
                operation_id: accepted.operation_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .operation
            .unwrap();
        if response.state == "succeeded" {
            completed = Some(response);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let completed = completed.expect("drain operation did not reach its durable terminal state");
    assert_eq!(completed.operation_id, accepted.operation_id);
    assert_eq!(completed.kind, "drain_worker");
    assert_eq!(completed.state, "succeeded");
    assert_eq!(completed.progress, "100%");
    assert_eq!(completed.phase, "completed");
    assert!(completed.error_code.is_empty());
    assert!(completed.next_steps.is_empty());
    assert_eq!(completed.source_version, "operation-record:1");
    assert_eq!(
        shard_manager.get(ShardId(77)).unwrap().worker_id,
        WorkerId(2)
    );
    control.shutdown();
}
