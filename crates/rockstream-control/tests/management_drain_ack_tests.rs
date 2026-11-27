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

#[path = "common/management_auth.rs"]
mod management_auth;
use management_auth::ManagementAuth;

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
        ControlMessage::Registered { worker_id: registered, .. } if registered == WorkerId(worker_id)
    ));
    drop(reader);
    stream
}

async fn read_until(
    stream: &mut TcpStream,
    matches: impl Fn(&ControlMessage) -> bool,
) -> ControlMessage {
    // Do not discard a subsequent control frame when this reader is dropped.
    let mut reader = BufReader::with_capacity(1, stream);
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
    let auth = ManagementAuth::new(&[]).await;
    let catalog = TopologyCatalog::new();
    let shard_manager = ShardManager::new();
    let management_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let management_addr = management_listener.local_addr().unwrap();
    drop(management_listener);
    let service = auth.protect_control(
        ControlService::new(catalog.clone())
            .with_shard_manager(shard_manager.clone())
            .with_management(
                management_addr.to_string(),
                Arc::new(InMemory::new()),
                NodeConfig::default(),
            ),
    );
    let control = service
        .with_insecure_dev_mode()
        .start("127.0.0.1:0")
        .await
        .unwrap();
    let mut donor = register_worker(control.addr, 31).await;
    let mut recipient = register_worker(control.addr, 32).await;
    let original = shard_manager.acquire(ShardId(301), WorkerId(31)).unwrap();
    let mut client = auth.client(management_addr).await;
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

    let prepare = read_until(&mut donor, |message| {
        matches!(message, ControlMessage::PrepareShardTransfer { .. })
    })
    .await;
    let ControlMessage::PrepareShardTransfer {
        operation_id: child_id,
        lease: prepared,
    } = prepare
    else {
        unreachable!();
    };
    assert_eq!(prepared, original);
    assert_ne!(child_id, accepted.operation_id);
    send_on(
        &mut donor,
        &WorkerMessage::ShardTransferAck {
            operation_id: child_id.clone(),
            stage: "donor".to_owned(),
            worker_id: original.worker_id,
            shard_id: original.shard_id,
            lease_token: original.lease_token,
            success: true,
            error: None,
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
    assert_eq!(transfer_id, child_id);
    assert!(lease.lease_token.0 > original.lease_token.0);
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
    // A mismatched recipient ACK must roll ownership back to the donor
    // with a new fence before the parent drain may attempt another move.
    let rollback = read_until(&mut donor, |message| {
        matches!(
            message,
            ControlMessage::ShardAssigned {
                operation_id: Some(_),
                ..
            }
        )
    })
    .await;
    let ControlMessage::ShardAssigned {
        lease: restored,
        operation_id: Some(rollback_id),
    } = rollback
    else {
        unreachable!();
    };
    assert_eq!(rollback_id, transfer_id);
    assert_eq!(restored.worker_id, WorkerId(31));
    assert_eq!(restored.shard_id, original.shard_id);
    assert!(restored.lease_token.0 > lease.lease_token.0);
    assert_eq!(shard_manager.get(ShardId(301)), Some(restored.clone()));
    let awaiting_ack = operation(&mut client, &accepted.operation_id).await;
    assert_eq!(awaiting_ack.state, "running");
    assert!(matches!(
        catalog.get(WorkerId(31)).unwrap().lifecycle,
        WorkerLifecycleState::Draining { .. }
    ));

    send_on(
        &mut donor,
        &WorkerMessage::ShardTransferAck {
            operation_id: rollback_id,
            stage: "recipient".to_owned(),
            worker_id: restored.worker_id,
            shard_id: restored.shard_id,
            lease_token: restored.lease_token,
            success: true,
            error: None,
        },
    )
    .await;
    let retry_prepare = read_until(&mut donor, |message| {
        matches!(message, ControlMessage::PrepareShardTransfer { .. })
    })
    .await;
    let ControlMessage::PrepareShardTransfer {
        operation_id: retry_child_id,
        lease: retry_prepared,
    } = retry_prepare
    else {
        unreachable!();
    };
    assert_ne!(retry_child_id, transfer_id);
    assert_eq!(retry_prepared, restored);
    let failed_child = operation(&mut client, &transfer_id).await;
    assert_eq!(failed_child.state, "failed");
    assert_eq!(failed_child.error_code, "RS-3610");
    send_on(
        &mut donor,
        &WorkerMessage::ShardTransferAck {
            operation_id: retry_child_id.clone(),
            stage: "donor".to_owned(),
            worker_id: restored.worker_id,
            shard_id: restored.shard_id,
            lease_token: restored.lease_token,
            success: true,
            error: None,
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
    assert_eq!(retry_lease.worker_id, lease.worker_id);
    assert_eq!(retry_lease.shard_id, lease.shard_id);
    assert!(retry_lease.lease_token.0 > restored.lease_token.0);
    assert_eq!(retry_transfer_id, retry_child_id);
    assert_eq!(shard_manager.get(ShardId(301)), Some(retry_lease.clone()));
    send_on(
        &mut recipient,
        &WorkerMessage::ShardTransferAck {
            operation_id: retry_transfer_id,
            stage: "recipient".to_owned(),
            worker_id: retry_lease.worker_id,
            shard_id: retry_lease.shard_id,
            lease_token: retry_lease.lease_token,
            success: true,
            error: None,
        },
    )
    .await;

    let mut completed = None;
    // The parent completion is reconciled on a one-second scheduler tick.
    for _ in 0..300 {
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
    assert_eq!(shard_manager.get(ShardId(301)), Some(retry_lease));
    control.shutdown();
}
