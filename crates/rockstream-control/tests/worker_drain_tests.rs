use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use rockstream_control::{
    ControlService, MigrationPersistentStore, ShardManager, TopologyCatalog,
    TopologyPersistentStore,
};
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerLifecycleState, WorkerMessage,
    WorkerRegistration,
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
    );
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

async fn start_service(
    auto_drain: bool,
) -> (
    rockstream_control::ControlServiceHandle,
    TopologyCatalog,
    ShardManager,
) {
    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let store = Arc::new(InMemory::new());
    let service = ControlService::new(catalog.clone())
        .with_shard_manager(manager.clone())
        .with_topology_store(Arc::new(TopologyPersistentStore::new(store.clone())))
        .with_migration_store(Arc::new(MigrationPersistentStore::new(store)))
        .with_auto_drain(auto_drain);
    let handle = service.start("127.0.0.1:0").await.unwrap();
    (handle, catalog, manager)
}

#[tokio::test]
async fn draining_worker_receives_no_new_shards() {
    let (handle, catalog, _manager) = start_service(false).await;
    let _worker_1 = register(handle.addr, 1).await;
    let _worker_2 = register(handle.addr, 2).await;

    let replies = send(
        handle.addr,
        &WorkerMessage::RequestDrain {
            worker_id: WorkerId(1),
        },
    )
    .await;
    assert!(matches!(
        replies.last(),
        Some(ControlMessage::DrainStatus { .. })
    ));
    assert!(matches!(
        catalog.get(WorkerId(1)).unwrap().lifecycle,
        WorkerLifecycleState::Draining {
            shards_remaining: 0,
            ..
        }
    ));

    let denied = send(
        handle.addr,
        &WorkerMessage::RequestShard {
            worker_id: WorkerId(1),
            shard_id: ShardId(99),
        },
    )
    .await;
    assert!(matches!(
        denied.first(),
        Some(ControlMessage::OperationFailed { code, .. }) if code == "RS-3604"
    ));
    handle.shutdown();
}

#[tokio::test]
async fn drain_completes_after_all_shards_migrate() {
    let (handle, catalog, manager) = start_service(true).await;
    let _worker_1 = register(handle.addr, 1).await;
    let _worker_2 = register(handle.addr, 2).await;
    manager.acquire(ShardId(7), WorkerId(1)).unwrap();
    manager.acquire(ShardId(8), WorkerId(1)).unwrap();

    let replies = send(
        handle.addr,
        &WorkerMessage::RequestDrain {
            worker_id: WorkerId(1),
        },
    )
    .await;
    assert!(matches!(
        replies.last(),
        Some(ControlMessage::DrainStatus { .. })
    ));
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert!(matches!(
        catalog.get(WorkerId(1)).unwrap().lifecycle,
        WorkerLifecycleState::Decommissioned { .. }
    ));
    assert_eq!(manager.get(ShardId(7)).unwrap().worker_id, WorkerId(2));
    assert_eq!(manager.get(ShardId(8)).unwrap().worker_id, WorkerId(2));
    handle.shutdown();
}

#[tokio::test]
async fn decommissioned_worker_removed_from_topology_after_grace_period() {
    let (handle, catalog, manager) = start_service(true).await;
    let _worker_1 = register(handle.addr, 1).await;
    let _worker_2 = register(handle.addr, 2).await;
    manager.acquire(ShardId(10), WorkerId(1)).unwrap();

    let _ = send(
        handle.addr,
        &WorkerMessage::RequestDrain {
            worker_id: WorkerId(1),
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    tokio::time::sleep(Duration::from_millis(5_100)).await;
    let _ = send(handle.addr, &WorkerMessage::ClusterStatusQuery).await;

    assert!(catalog.get(WorkerId(1)).is_none());
    handle.shutdown();
}

#[tokio::test]
async fn test_worker_drain_progress_monotonic() {
    let mut state = WorkerLifecycleState::draining(4, 1_000_000);
    assert_eq!(state.progress_phase(), "draining");
    assert_eq!(state.shards_remaining(), Some(4));
    assert!(state.estimated_remaining_ms().is_some());

    // Advance drain with successive shard migrations
    state.advance_drain_progress(3, Some(30_000_000), Some(150_000));
    assert_eq!(state.progress_phase(), "draining");
    assert_eq!(state.shards_remaining(), Some(3));
    assert_eq!(state.bytes_remaining(), Some(30_000_000));
    assert_eq!(state.rows_remaining(), Some(150_000));
    assert!(state.estimated_remaining_ms().unwrap() > 0);

    state.advance_drain_progress(2, Some(20_000_000), Some(100_000));
    assert_eq!(state.progress_phase(), "draining");
    assert_eq!(state.shards_remaining(), Some(2));
    assert_eq!(state.bytes_remaining(), Some(20_000_000));
    assert_eq!(state.rows_remaining(), Some(100_000));

    state.advance_drain_progress(1, Some(10_000_000), Some(50_000));
    assert_eq!(state.progress_phase(), "draining");
    assert_eq!(state.shards_remaining(), Some(1));
    assert_eq!(state.bytes_remaining(), Some(10_000_000));
    assert_eq!(state.rows_remaining(), Some(50_000));

    // Terminal completion to decommissioned
    state.advance_drain_progress(0, Some(0), Some(0));
    assert_eq!(state.progress_phase(), "decommissioned");
    assert_eq!(state.shards_remaining(), Some(0));
    assert_eq!(state.bytes_remaining(), Some(0));
    assert_eq!(state.rows_remaining(), Some(0));
    assert_eq!(state.estimated_remaining_ms(), Some(0));
}
