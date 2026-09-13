use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_control::{
    ManagementOperationStore, ManagementService, OperationKind, OperationStatus, OperationUpdate,
    ShardManager, TopologyCatalog,
};
use rockstream_management_proto::v1::{
    management_service_client::ManagementServiceClient, CancelOperationRequest, DrainWorkerRequest,
    GetHealthRequest, GetNodeRequest, GetOperationRequest, ListNodesRequest, MigrateShardRequest,
    Operation,
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
async fn management_start_recovers_running_operation_with_exact_phase() {
    let store = Arc::new(InMemory::new());
    let operations = ManagementOperationStore::new(store);
    operations
        .accept_idempotent(
            "drain-recovery-key",
            1,
            &serde_json::json!({"worker_id": 9}),
            "op_recovered",
            OperationKind::DrainWorker,
            1_000,
        )
        .await
        .unwrap();
    operations
        .transition(
            "op_recovered",
            OperationUpdate {
                status: OperationStatus::Running,
                updated_at_ms: 2_000,
                progress: Some(5),
                phase: Some("worker_handoff_started".to_owned()),
                error_code: None,
                next_steps: Vec::new(),
            },
        )
        .await
        .unwrap();
    let service = ManagementService::new(
        TopologyCatalog::new(),
        ShardManager::new(),
        operations,
        NodeConfig::default(),
    );
    let server = service.start("127.0.0.1:0").await.unwrap();
    let mut client = ManagementServiceClient::connect(format!("http://{}", server.addr))
        .await
        .unwrap();
    let operation = client
        .get_operation(GetOperationRequest {
            protocol_version: 1,
            operation_id: "op_recovered".to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(
        operation,
        Operation {
            operation_id: "op_recovered".to_owned(),
            kind: "drain_worker".to_owned(),
            state: "waiting".to_owned(),
            started_at: "1970-01-01T00:00:01.000Z".to_owned(),
            updated_at: operation.updated_at.clone(),
            progress: "5%".to_owned(),
            phase: "worker_handoff_started".to_owned(),
            error_code: String::new(),
            next_steps: vec![
                "Management restarted; reconcile this operation before retrying.".to_owned(),
            ],
            source_version: "operation-record:2".to_owned(),
        }
    );
    assert!(operation
        .updated_at
        .parse::<chrono::DateTime<chrono::FixedOffset>>()
        .is_ok());
    server.shutdown();
}

#[tokio::test]
async fn management_cancellation_obeys_the_safe_boundary() {
    let operations = ManagementOperationStore::new(Arc::new(InMemory::new()));
    operations
        .accept_idempotent(
            "cancel-pending-key",
            1,
            &serde_json::json!({"worker_id": 11}),
            "op_cancel_pending",
            OperationKind::DrainWorker,
            1_000,
        )
        .await
        .unwrap();
    operations
        .accept_idempotent(
            "cancel-late-key",
            1,
            &serde_json::json!({"worker_id": 12}),
            "op_cancel_late",
            OperationKind::DrainWorker,
            1_000,
        )
        .await
        .unwrap();
    operations
        .transition(
            "op_cancel_late",
            OperationUpdate {
                status: OperationStatus::Waiting,
                updated_at_ms: 2_000,
                progress: Some(5),
                phase: Some("worker_handoff_started".to_owned()),
                error_code: None,
                next_steps: Vec::new(),
            },
        )
        .await
        .unwrap();
    let service = ManagementService::new(
        TopologyCatalog::new(),
        ShardManager::new(),
        operations.clone(),
        NodeConfig::default(),
    );
    let server = service.start("127.0.0.1:0").await.unwrap();
    let mut client = ManagementServiceClient::connect(format!("http://{}", server.addr))
        .await
        .unwrap();

    let cancelled = client
        .cancel_operation(CancelOperationRequest {
            protocol_version: 1,
            operation_id: "op_cancel_pending".to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    let updated_at = cancelled.updated_at.clone();
    assert_eq!(
        cancelled,
        Operation {
            operation_id: "op_cancel_pending".to_owned(),
            kind: "drain_worker".to_owned(),
            state: "cancelled".to_owned(),
            started_at: "1970-01-01T00:00:01.000Z".to_owned(),
            updated_at,
            progress: String::new(),
            phase: "cancelled_before_start".to_owned(),
            error_code: String::new(),
            next_steps: Vec::new(),
            source_version: "operation-record:2".to_owned(),
        }
    );

    let rejected = client
        .cancel_operation(CancelOperationRequest {
            protocol_version: 1,
            operation_id: "op_cancel_late".to_owned(),
        })
        .await
        .unwrap_err();
    assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        rejected.message(),
        "operation cancellation is past its safe boundary"
    );
    let still_running = client
        .get_operation(GetOperationRequest {
            protocol_version: 1,
            operation_id: "op_cancel_late".to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(
        still_running,
        Operation {
            operation_id: "op_cancel_late".to_owned(),
            kind: "drain_worker".to_owned(),
            state: "waiting".to_owned(),
            started_at: "1970-01-01T00:00:01.000Z".to_owned(),
            updated_at: "1970-01-01T00:00:02.000Z".to_owned(),
            progress: "5%".to_owned(),
            phase: "worker_handoff_started".to_owned(),
            error_code: String::new(),
            next_steps: Vec::new(),
            source_version: "operation-record:2".to_owned(),
        }
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
        matches!(message, ControlMessage::ShardAssigned { lease, .. } if lease.shard_id == ShardId(77))
    })
    .await;
    let (lease, transfer_id) = match assignment {
        ControlMessage::ShardAssigned {
            lease,
            operation_id: Some(transfer_id),
        } => (lease, transfer_id),
        other => panic!("expected recipient shard assignment, got {other:?}"),
    };
    assert_eq!(lease.worker_id, WorkerId(2));
    assert_eq!(lease.shard_id, ShardId(77));
    assert!(transfer_id.starts_with(&format!("{}:shard:", accepted.operation_id)));

    let awaiting_recipient = client
        .get_operation(GetOperationRequest {
            protocol_version: 1,
            operation_id: accepted.operation_id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert!(matches!(
        awaiting_recipient.state.as_str(),
        "pending" | "running" | "waiting"
    ));
    assert!(matches!(
        catalog.get(WorkerId(1)).unwrap().lifecycle,
        rockstream_types::topology::WorkerLifecycleState::Draining { .. }
    ));

    recipient
        .write_all(
            (serde_json::to_string(&WorkerMessage::ShardTransferAck {
                operation_id: transfer_id,
                stage: "recipient".to_owned(),
                worker_id: lease.worker_id,
                shard_id: lease.shard_id,
                lease_token: lease.lease_token,
                success: true,
                error: None,
            })
            .unwrap()
                + "\n")
                .as_bytes(),
        )
        .await
        .unwrap();

    let mut completed = None;
    let mut last_state = None;
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
        last_state = Some((
            response.state.clone(),
            response.phase.clone(),
            response.error_code.clone(),
        ));
        if response.state == "succeeded" {
            completed = Some(response);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let completed = completed.unwrap_or_else(|| {
        panic!("drain operation did not reach its durable terminal state: {last_state:?}")
    });
    assert_eq!(completed.operation_id, accepted.operation_id);
    assert_eq!(completed.kind, "drain_worker");
    assert_eq!(completed.state, "succeeded");
    assert_eq!(completed.progress, "100%");
    assert_eq!(completed.phase, "completed");
    assert!(completed.error_code.is_empty());
    assert!(completed.next_steps.is_empty());
    assert_eq!(completed.source_version, "operation-record:2");
    assert_eq!(
        shard_manager.get(ShardId(77)).unwrap().worker_id,
        WorkerId(2)
    );
    control.shutdown();
}

#[tokio::test]
async fn management_migrate_moves_a_real_shard_once() {
    let catalog = TopologyCatalog::new();
    let shard_manager = ShardManager::new();
    let operations = Arc::new(InMemory::new());
    let management_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let management_addr = management_listener.local_addr().unwrap();
    drop(management_listener);
    let service = rockstream_control::ControlService::new(catalog)
        .with_shard_manager(shard_manager.clone())
        .with_management(
            management_addr.to_string(),
            operations,
            NodeConfig::default(),
        );
    let control = service.start("127.0.0.1:0").await.unwrap();
    let mut donor = register_worker(control.addr, 11).await;
    let mut recipient = register_worker(control.addr, 12).await;
    let old_lease = shard_manager.acquire(ShardId(77), WorkerId(11)).unwrap();
    let mut client = ManagementServiceClient::connect(format!("http://{management_addr}"))
        .await
        .unwrap();
    let request = MigrateShardRequest {
        protocol_version: 1,
        shard_id: "77".to_owned(),
        target_node_id: "12".to_owned(),
        idempotency_key: "migrate-shard-77-to-12".to_owned(),
    };
    let accepted = client
        .migrate_shard(request.clone())
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    let retried = client
        .migrate_shard(request)
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(retried.operation_id, accepted.operation_id);
    assert_eq!(accepted.kind, "migrate_shard");
    assert!(matches!(accepted.state.as_str(), "pending" | "running"));

    let prepare = read_until(&mut donor, |message| {
        matches!(message, ControlMessage::PrepareShardTransfer { operation_id, lease }
            if operation_id == &accepted.operation_id && lease.shard_id == ShardId(77))
    })
    .await;
    let lease = match prepare {
        ControlMessage::PrepareShardTransfer { lease, .. } => lease,
        other => panic!("expected donor prepare, got {other:?}"),
    };
    assert_eq!(lease, old_lease);
    donor
        .write_all(
            (serde_json::to_string(&WorkerMessage::ShardTransferAck {
                operation_id: accepted.operation_id.clone(),
                stage: "donor".to_owned(),
                worker_id: WorkerId(11),
                shard_id: ShardId(77),
                lease_token: lease.lease_token,
                success: true,
                error: None,
            })
            .unwrap()
                + "\n")
                .as_bytes(),
        )
        .await
        .unwrap();

    let assignment = read_until(&mut recipient, |message| {
        matches!(message, ControlMessage::ShardAssigned { lease, operation_id: Some(id) }
            if id == &accepted.operation_id && lease.shard_id == ShardId(77))
    })
    .await;
    let new_lease = match assignment {
        ControlMessage::ShardAssigned { lease, .. } => lease,
        other => panic!("expected recipient lease, got {other:?}"),
    };
    assert_eq!(new_lease.worker_id, WorkerId(12));
    assert!(new_lease.lease_token.0 > old_lease.lease_token.0);
    recipient
        .write_all(
            (serde_json::to_string(&WorkerMessage::ShardTransferAck {
                operation_id: accepted.operation_id.clone(),
                stage: "recipient".to_owned(),
                worker_id: WorkerId(12),
                shard_id: ShardId(77),
                lease_token: new_lease.lease_token,
                success: true,
                error: None,
            })
            .unwrap()
                + "\n")
                .as_bytes(),
        )
        .await
        .unwrap();

    let mut completed = None;
    for _ in 0..100 {
        let operation = client
            .get_operation(GetOperationRequest {
                protocol_version: 1,
                operation_id: accepted.operation_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .operation
            .unwrap();
        if operation.state == "succeeded" {
            completed = Some(operation);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let completed = completed.expect("migration did not reach its durable terminal state");
    assert_eq!(completed.operation_id, accepted.operation_id);
    assert_eq!(completed.kind, "migrate_shard");
    assert_eq!(completed.state, "succeeded");
    assert_eq!(completed.progress, "100%");
    assert_eq!(completed.phase, "completed");
    assert_eq!(completed.error_code, "");
    assert_eq!(completed.next_steps, Vec::<String>::new());
    assert_eq!(completed.source_version, "operation-record:2");
    assert_eq!(shard_manager.get(ShardId(77)), Some(new_lease));
    control.shutdown();
}
