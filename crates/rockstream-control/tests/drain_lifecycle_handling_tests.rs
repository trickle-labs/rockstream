use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use rockstream_control::{
    ControlService, MigrationPersistentStore, ShardManager, TopologyCatalog,
    TopologyPersistentStore,
};
use rockstream_runtime::exchange::flow_control::FlowController;
use rockstream_runtime::exchange::multiplexer::WorkerStreamMultiplexer;
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerCapabilities, WorkerLifecycleState,
    WorkerMessage, WorkerRegistration,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

async fn send(addr: std::net::SocketAddr, msg: &WorkerMessage) -> Vec<ControlMessage> {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let line = serde_json::to_string(msg).unwrap() + "\n";
    stream.write_all(line.as_bytes()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut reader = BufReader::new(stream);
    let mut responses = Vec::new();
    loop {
        let mut line = String::new();
        let Ok(read) =
            tokio::time::timeout(Duration::from_millis(50), reader.read_line(&mut line)).await
        else {
            break;
        };
        let Ok(read) = read else { break };
        if read == 0 || line.trim().is_empty() {
            break;
        }
        responses.push(serde_json::from_str(line.trim()).unwrap());
    }
    responses
}

async fn register(addr: std::net::SocketAddr, worker_id: u64) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let reg = WorkerRegistration::new(
        WorkerId(worker_id),
        NodeRole::Worker,
        format!("127.0.0.1:{}", 7000 + worker_id),
        CapacityHeadroom::FULL,
    )
    .with_capabilities(WorkerCapabilities {
        shared_shard_store_id: Some([1; 32]),
        ..Default::default()
    });
    let line = serde_json::to_string(&WorkerMessage::Register(reg)).unwrap() + "\n";
    stream.write_all(line.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(&mut stream);
    let mut response = String::new();
    reader.read_line(&mut response).await.unwrap();
    match serde_json::from_str::<ControlMessage>(response.trim()).unwrap() {
        ControlMessage::Registered {
            worker_id: registered,
        } => assert_eq!(registered, WorkerId(worker_id)),
        other => panic!("expected exact registration response, got {other:?}"),
    }
    drop(reader);
    stream
}

async fn send_on(stream: &mut TcpStream, msg: &WorkerMessage) -> Vec<ControlMessage> {
    let line = serde_json::to_string(msg).unwrap() + "\n";
    stream.write_all(line.as_bytes()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut reader = BufReader::new(stream);
    let mut responses = Vec::new();
    loop {
        let mut line = String::new();
        let Ok(Ok(read)) =
            tokio::time::timeout(Duration::from_millis(50), reader.read_line(&mut line)).await
        else {
            break;
        };
        if read == 0 || line.trim().is_empty() {
            break;
        }
        responses.push(serde_json::from_str(line.trim()).unwrap());
    }
    responses
}

fn worker_lease_count(manager: &ShardManager, worker_id: WorkerId) -> usize {
    manager
        .leases()
        .into_iter()
        .filter(|l| l.worker_id == worker_id)
        .count()
}

#[tokio::test]
async fn test_drain_ack_completes_drain_and_evicts_worker() {
    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let store = Arc::new(InMemory::new());
    let service = ControlService::new(catalog.clone())
        .with_shard_manager(manager.clone())
        .with_topology_store(Arc::new(TopologyPersistentStore::new(store.clone())))
        .with_migration_store(Arc::new(MigrationPersistentStore::new(store)));
    let handle = service.start("127.0.0.1:0").await.unwrap();

    let mut worker_1 = register(handle.addr, 1).await;
    let _worker_2 = register(handle.addr, 2).await;
    manager.acquire(ShardId(101), WorkerId(1)).unwrap();

    let pool = ShuffleClientPool::default();
    let controller = FlowController::new();
    let multiplexer = WorkerStreamMultiplexer::new(pool.clone(), controller);

    multiplexer.evict_worker(WorkerId(1));
    pool.evict_worker(WorkerId(1));

    // Request drain for worker 1
    let drain_replies = send_on(
        &mut worker_1,
        &WorkerMessage::RequestDrain {
            worker_id: WorkerId(1),
        },
    )
    .await;
    assert!(drain_replies
        .iter()
        .any(|message| matches!(message, ControlMessage::DrainStatus { .. })));

    // Send DrainAck with shards_remaining = 0
    let _ack_replies = send_on(
        &mut worker_1,
        &WorkerMessage::DrainAck {
            worker_id: WorkerId(1),
            shards_remaining: 0,
        },
    )
    .await;

    // Lifecycle should advance to Decommissioned
    let w1 = catalog.get(WorkerId(1)).unwrap();
    assert!(matches!(
        w1.lifecycle,
        WorkerLifecycleState::Decommissioned { .. }
    ));

    // The lease must move to the acknowledged recipient, not become unowned.
    assert_eq!(worker_lease_count(&manager, WorkerId(1)), 0);
    assert_eq!(manager.get(ShardId(101)).unwrap().worker_id, WorkerId(2));

    handle.shutdown();
}

#[tokio::test]
async fn test_lifecycle_state_update_triggers_release_and_eviction() {
    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let service = ControlService::new(catalog.clone()).with_shard_manager(manager.clone());
    let handle = service.start("127.0.0.1:0").await.unwrap();

    let mut worker = register(handle.addr, 10).await;
    manager.acquire(ShardId(50), WorkerId(10)).unwrap();
    assert_eq!(worker_lease_count(&manager, WorkerId(10)), 1);

    // Send LifecycleState decommissioned
    let replies = send_on(
        &mut worker,
        &WorkerMessage::LifecycleState {
            worker_id: WorkerId(10),
            state: WorkerLifecycleState::Decommissioned {
                completed_at_ms: 1000,
            },
        },
    )
    .await;

    assert!(replies.iter().any(|reply| matches!(
        reply,
        ControlMessage::OperationFailed { code, .. } if code == "RS-3604"
    )));
    assert_eq!(worker_lease_count(&manager, WorkerId(10)), 1);

    let pool = ShuffleClientPool::default();
    let controller = FlowController::new();
    let multiplexer = WorkerStreamMultiplexer::new(pool.clone(), controller);
    multiplexer.evict_worker(WorkerId(10));
    pool.evict_worker(WorkerId(10));

    handle.shutdown();
}
