use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_management_proto::v1::{
    management_service_client::ManagementServiceClient, DrainWorkerRequest, GetOperationRequest,
};
use rockstream_types::config::NodeConfig;
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerCapabilities, WorkerLifecycleState,
    WorkerMessage, WorkerRegistration,
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
        shared_shard_store_id: Some([6; 32]),
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
        tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut response))
            .await
            .unwrap()
            .unwrap();
        let message = serde_json::from_str(response.trim()).unwrap();
        if matches(&message) {
            return message;
        }
    }
}

async fn send_on(stream: &mut TcpStream, message: &WorkerMessage) {
    let request = serde_json::to_string(message).unwrap() + "\n";
    stream.write_all(request.as_bytes()).await.unwrap();
}

async fn operation(
    client: &mut ManagementServiceClient<tonic::transport::Channel>,
    operation_id: &str,
) -> rockstream_management_proto::v1::Operation {
    client
        .get_operation(GetOperationRequest {
            protocol_version: 1,
            operation_id: operation_id.to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap()
}

#[tokio::test]
async fn management_drain_retries_after_mismatched_recipient_ack() {
    let catalog = TopologyCatalog::new();
    let shard_manager = ShardManager::new();
    let management_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let management_addr = management_listener.local_addr().unwrap();
    drop(management_listener);
    let service = ControlService::new(catalog.clone())
        .with_shard_manager(shard_manager.clone())
        .with_management(
            management_addr.to_string(),
            Arc::new(InMemory::new()),
            NodeConfig::default(),
        );
    let control = service.start("127.0.0.1:0").await.unwrap();
    let mut donor = register_worker(control.addr, 31).await;
    let mut recipient = register_worker(control.addr, 32).await;
    shard_manager.acquire(ShardId(301), WorkerId(31)).unwrap();
    let mut client = ManagementServiceClient::connect(format!("http://{management_addr}"))
        .await
        .unwrap();
    let accepted = client
        .drain_worker(DrainWorkerRequest {
            protocol_version: 1,
            worker_id: "31".to_owned(),
            idempotency_key: "drain-ack-retry-301".to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();

    let _ = read_until(&mut donor, |message| {
        matches!(message, ControlMessage::BeginDrain(_))
    })
    .await;
    send_on(
        &mut donor,
        &WorkerMessage::DrainAck {
            worker_id: WorkerId(31),
            shards_remaining: 0,
        },
    )
    .await;
    let first_assignment = read_until(&mut recipient, |message| {
        matches!(message, ControlMessage::ShardAssigned { lease, operation_id: Some(_) }
            if lease.shard_id == ShardId(301))
    })
    .await;
    let (lease, transfer_id) = match first_assignment {
        ControlMessage::ShardAssigned {
            lease,
            operation_id: Some(transfer_id),
        } => (lease, transfer_id),
        other => panic!("expected recipient shard assignment, got {other:?}"),
    };
    assert_eq!(lease.worker_id, WorkerId(32));
    assert_eq!(transfer_id, format!("{}:shard:301", accepted.operation_id));
    assert_eq!(shard_manager.get(ShardId(301)), Some(lease.clone()));

    send_on(
        &mut recipient,
        &WorkerMessage::ShardTransferAck {
            operation_id: transfer_id.clone(),
            stage: "recipient".to_owned(),
            worker_id: WorkerId(32),
            shard_id: ShardId(301),
            lease_token: rockstream_types::ids::LeaseToken(lease.lease_token.0 + 1),
            success: true,
            error: None,
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    let awaiting_ack = operation(&mut client, &accepted.operation_id).await;
    assert_eq!(awaiting_ack.state, "running");
    assert!(matches!(
        catalog.get(WorkerId(31)).unwrap().lifecycle,
        WorkerLifecycleState::Draining { .. }
    ));

    send_on(
        &mut donor,
        &WorkerMessage::DrainAck {
            worker_id: WorkerId(31),
            shards_remaining: 0,
        },
    )
    .await;
    let retry_assignment = read_until(&mut recipient, |message| {
        matches!(message, ControlMessage::ShardAssigned { lease, operation_id: Some(_)}
            if lease.shard_id == ShardId(301))
    })
    .await;
    let (retry_lease, retry_transfer_id) = match retry_assignment {
        ControlMessage::ShardAssigned {
            lease,
            operation_id: Some(transfer_id),
        } => (lease, transfer_id),
        other => panic!("expected recipient shard retry, got {other:?}"),
    };
    assert_eq!(retry_lease, lease);
    assert_eq!(retry_transfer_id, transfer_id);
    send_on(
        &mut recipient,
        &WorkerMessage::ShardTransferAck {
            operation_id: transfer_id,
            stage: "recipient".to_owned(),
            worker_id: lease.worker_id,
            shard_id: lease.shard_id,
            lease_token: lease.lease_token,
            success: true,
            error: None,
        },
    )
    .await;

    let mut completed = None;
    for _ in 0..50 {
        let current = operation(&mut client, &accepted.operation_id).await;
        if current.state == "succeeded" {
            completed = Some(current);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let completed = completed.expect("drain did not complete after the matching recipient ACK");
    assert_eq!(completed.state, "succeeded");
    assert_eq!(completed.phase, "completed");
    assert_eq!(completed.progress, "100%");
    assert_eq!(completed.error_code, "");
    assert_eq!(completed.next_steps, Vec::<String>::new());
    assert!(matches!(
        catalog.get(WorkerId(31)).unwrap().lifecycle,
        WorkerLifecycleState::Decommissioned { .. }
    ));
    assert_eq!(shard_manager.get(ShardId(301)), Some(lease));
    control.shutdown();
}
