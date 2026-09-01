//! Control Plane Graceful Shutdown, Raft Stepdown & Store Durability Tests (v0.59.21 Slice 5 / Phase 3b).

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpStream;

use object_store::memory::InMemory;
use object_store::ObjectStore;
use rockstream_control::audit::FileAuditLog;
use rockstream_control::raft::{spawn_raft_node, RaftConfig};
use rockstream_control::service::ControlService;
use rockstream_control::shard::{ShardManager, ShardPersistentStore};
use rockstream_control::topology::{TopologyCatalog, TopologyPersistentStore};
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::topology::{CapacityHeadroom, NodeRole, WorkerInfo, WorkerRegistration};

#[tokio::test]
async fn test_control_leader_stepdown_and_store_flush() {
    let temp_dir = TempDir::new().unwrap();
    let audit_file = temp_dir.path().join("audit.jsonl");
    let audit = Arc::new(FileAuditLog::open(&audit_file).unwrap());

    let shared_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard_store = Arc::new(ShardPersistentStore::new(shared_store.clone()));
    let topology_store = Arc::new(TopologyPersistentStore::new(shared_store.clone()));

    // 1. Start Raft leader node
    let raft_node = spawn_raft_node(
        "127.0.0.1:0",
        RaftConfig::new(0, Vec::new(), true),
        shared_store.clone(),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(raft_node.handle.is_leader(), "Node 0 must be leader");

    // 2. Start ControlService with attached stores and audit log
    let catalog = TopologyCatalog::new();
    let reg = WorkerRegistration::new(
        WorkerId(1),
        NodeRole::Worker,
        "127.0.0.1:9001".to_string(),
        CapacityHeadroom::FULL,
    );
    catalog.register(&reg);
    let worker_info = WorkerInfo::from_registration(&reg);
    topology_store.save_worker(&worker_info).await.unwrap();

    let shard_manager = ShardManager::new();
    let lease = shard_manager.acquire(ShardId(1), WorkerId(1)).unwrap();
    assert_eq!(lease.shard_id, ShardId(1));
    assert_eq!(lease.worker_id, WorkerId(1));

    let service = ControlService::new(catalog.clone())
        .with_shard_manager(shard_manager.clone())
        .with_raft(raft_node.handle.clone())
        .with_shard_store(shard_store.clone())
        .with_topology_store(topology_store.clone())
        .with_audit(audit.clone());

    let handle = service.start("127.0.0.1:0").await.unwrap();
    let bound_addr = handle.addr;

    // Verify service accepts connections
    let stream = TcpStream::connect(bound_addr).await;
    assert!(
        stream.is_ok(),
        "Control service must accept connections when running"
    );
    drop(stream);

    // 3. Trigger Graceful Shutdown
    handle.shutdown();
    raft_node.shutdown();
    raft_node.step_down();

    // Await shutdown processing
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 4. Assert Raft leadership stepped down
    assert!(
        !raft_node.handle.is_leader(),
        "Raft node must step down leadership upon graceful shutdown"
    );

    // 5. Assert persistent shard store received flushed snapshot
    let loaded_snapshot = shard_store.load().await;
    assert!(
        loaded_snapshot.leases.contains_key(&ShardId(1)),
        "Flushed snapshot must contain active lease for Shard 1"
    );
    assert_eq!(
        loaded_snapshot.leases.get(&ShardId(1)).unwrap().worker_id,
        WorkerId(1)
    );

    // 6. Assert persistent topology store contains registered worker
    let loaded_workers = topology_store.load_all().await.unwrap();
    assert_eq!(loaded_workers.len(), 1);
    assert_eq!(loaded_workers[0].worker_id, WorkerId(1));

    // 7. Assert audit records for server.stopping and server.stopped
    let events = audit.read_all().unwrap();
    let stopping_events: Vec<_> = events
        .iter()
        .filter(|e| e.action == "server.stopping")
        .collect();
    let stopped_events: Vec<_> = events
        .iter()
        .filter(|e| e.action == "server.stopped")
        .collect();
    assert!(
        !stopping_events.is_empty(),
        "Must record server.stopping audit event"
    );
    assert!(
        !stopped_events.is_empty(),
        "Must record server.stopped audit event"
    );

    // 8. Assert new connections are rejected after shutdown
    tokio::time::sleep(Duration::from_millis(50)).await;
    let post_shutdown_conn = TcpStream::connect(bound_addr).await;
    assert!(
        post_shutdown_conn.is_err(),
        "Control service listener must reject new connections after shutdown"
    );
}
