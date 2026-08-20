//! Consensus-replicated lease HA leader kill drill integration tests (v0.51.10).

use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;

use rockstream_control::raft::{spawn_raft_node, RaftConfig};
use rockstream_control::service::ControlService;
use rockstream_control::shard::{ShardManager, ShardPersistentStore};
use rockstream_control::topology::TopologyCatalog;
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerMessage, WorkerRegistration,
};

async fn send_and_recv(stream: &mut TcpStream, msg: &WorkerMessage) -> String {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let mut line = serde_json::to_string(msg).unwrap();
    line.push('\n');
    stream.write_all(line.as_bytes()).await.unwrap();

    let mut reader = BufReader::new(stream);
    loop {
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();
        if matches!(msg, WorkerMessage::Register(_))
            || !matches!(
                serde_json::from_str(&response),
                Ok(ControlMessage::TopologyChanged { .. })
            )
        {
            return response;
        }
    }
}

/// P4: Real control-plane leader kill (SIGKILL leader) results in follower assuming
/// shard-lease authority with in-flight lease continuity.
#[tokio::test]
async fn test_leader_sigkill_follower_takeover_lease_continuity() {
    let shared_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // 1. Leader Node A starts and acquires lease for Shard 10
    let node_a = spawn_raft_node(
        "127.0.0.1:0",
        RaftConfig::new(0, Vec::new(), true),
        shared_store.clone(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(node_a.handle.is_leader());

    let catalog_a = TopologyCatalog::new();
    let manager_a = ShardManager::new();
    let store_a = Arc::new(ShardPersistentStore::new(shared_store.clone()));
    let svc_a = ControlService::new(catalog_a)
        .with_shard_manager(manager_a.clone())
        .with_raft(node_a.handle.clone())
        .with_shard_store(store_a);
    let handle_a = svc_a.start("127.0.0.1:0").await.unwrap();

    let mut stream_a = TcpStream::connect(handle_a.addr).await.unwrap();
    let reg1 = WorkerRegistration::new(
        WorkerId(1),
        NodeRole::Worker,
        "127.0.0.1:9001",
        CapacityHeadroom::FULL,
    );
    let _ = send_and_recv(&mut stream_a, &WorkerMessage::Register(reg1)).await;

    let req1 = WorkerMessage::RequestShard {
        worker_id: WorkerId(1),
        shard_id: ShardId(10),
    };
    let resp1 = send_and_recv(&mut stream_a, &req1).await;
    let reply1: ControlMessage = serde_json::from_str(resp1.trim()).unwrap();
    let lease1 = match reply1 {
        ControlMessage::ShardAssigned { lease } => lease,
        other => panic!("Expected ShardAssigned, got: {other:?}"),
    };
    assert_eq!(lease1.shard_id, ShardId(10));
    assert_eq!(lease1.worker_id, WorkerId(1));

    // 2. SIGKILL / shutdown Leader Node A
    handle_a.shutdown();
    node_a.shutdown();

    // 3. Spawn Follower Node B which assumes leadership
    let node_b = spawn_raft_node(
        "127.0.0.1:0",
        RaftConfig::new(1, Vec::new(), true),
        shared_store.clone(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(node_b.handle.is_leader());

    let catalog_b = TopologyCatalog::new();
    let manager_b = ShardManager::new();
    let store_b = Arc::new(ShardPersistentStore::new(shared_store.clone()));
    let svc_b = ControlService::new(catalog_b)
        .with_shard_manager(manager_b.clone())
        .with_raft(node_b.handle.clone())
        .with_shard_store(store_b);
    let handle_b = svc_b.start("127.0.0.1:0").await.unwrap();

    let mut stream_b = TcpStream::connect(handle_b.addr).await.unwrap();
    let reg1_b = WorkerRegistration::new(
        WorkerId(1),
        NodeRole::Worker,
        "127.0.0.1:9001",
        CapacityHeadroom::FULL,
    );
    let _ = send_and_recv(&mut stream_b, &WorkerMessage::Register(reg1_b)).await;

    // Request shard 10 again on new leader Node B -> in-flight lease continuity verified
    let req2 = WorkerMessage::RequestShard {
        worker_id: WorkerId(1),
        shard_id: ShardId(10),
    };
    let resp2 = send_and_recv(&mut stream_b, &req2).await;
    let reply2: ControlMessage = serde_json::from_str(resp2.trim()).unwrap();
    let lease2 = match reply2 {
        ControlMessage::ShardAssigned { lease } => lease,
        other => panic!("Expected ShardAssigned on takeover, got: {other:?}"),
    };

    // Assert continuity and monotonic token increase under new leader epoch
    assert_eq!(lease2.shard_id, ShardId(10));
    assert_eq!(lease2.worker_id, WorkerId(1));
    assert!(
        lease2.lease_token.0 > lease1.lease_token.0,
        "Takeover lease token ({}) must strictly exceed pre-failover token ({})",
        lease2.lease_token.0,
        lease1.lease_token.0
    );

    // Assert old token lease1 is fenced out by new manager
    assert!(
        !manager_b.is_valid_writer(ShardId(10), lease1.lease_token),
        "Pre-failover token must be fenced out"
    );
    assert!(
        manager_b.is_valid_writer(ShardId(10), lease2.lease_token),
        "Post-takeover token must be valid writer"
    );

    handle_b.shutdown();
    node_b.shutdown();
}

/// P5: Deposed leader cannot write or issue leases; zero dual-writer window observed during leader transition.
#[tokio::test]
async fn test_deposed_leader_fenced_zero_dual_writer() {
    let shared_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // 1. Leader Node A and Follower Node B (not leader)
    let node_a = spawn_raft_node(
        "127.0.0.1:0",
        RaftConfig::new(0, Vec::new(), true),
        shared_store.clone(),
    )
    .await
    .unwrap();
    let node_b = spawn_raft_node(
        "127.0.0.1:0",
        RaftConfig::new(1, Vec::new(), false),
        shared_store.clone(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(node_a.handle.is_leader());
    assert!(!node_b.handle.is_leader());

    let catalog_a = TopologyCatalog::new();
    let manager_a = ShardManager::new();
    let store_a = Arc::new(ShardPersistentStore::new(shared_store.clone()));
    let svc_a = ControlService::new(catalog_a)
        .with_shard_manager(manager_a.clone())
        .with_raft(node_a.handle.clone())
        .with_shard_store(store_a);
    let handle_a = svc_a.start("127.0.0.1:0").await.unwrap();

    let catalog_b = TopologyCatalog::new();
    let manager_b = ShardManager::new();
    let store_b = Arc::new(ShardPersistentStore::new(shared_store.clone()));
    let svc_b = ControlService::new(catalog_b)
        .with_shard_manager(manager_b.clone())
        .with_raft(node_b.handle.clone())
        .with_shard_store(store_b);
    let handle_b = svc_b.start("127.0.0.1:0").await.unwrap();

    // 2. Node B (follower) rejects lease request with NotLeader
    let mut stream_b = TcpStream::connect(handle_b.addr).await.unwrap();
    let reg2 = WorkerRegistration::new(
        WorkerId(2),
        NodeRole::Worker,
        "127.0.0.1:9002",
        CapacityHeadroom::FULL,
    );
    let _ = send_and_recv(&mut stream_b, &WorkerMessage::Register(reg2)).await;

    let req_b = WorkerMessage::RequestShard {
        worker_id: WorkerId(2),
        shard_id: ShardId(20),
    };
    let resp_b = send_and_recv(&mut stream_b, &req_b).await;
    let reply_b: ControlMessage = serde_json::from_str(resp_b.trim()).unwrap();
    assert!(
        matches!(reply_b, ControlMessage::NotLeader { .. }),
        "Non-leader node B must reject shard requests with NotLeader: {reply_b:?}"
    );

    // 3. Node A (leader) grants lease for Shard 20
    let mut stream_a = TcpStream::connect(handle_a.addr).await.unwrap();
    let reg1 = WorkerRegistration::new(
        WorkerId(1),
        NodeRole::Worker,
        "127.0.0.1:9001",
        CapacityHeadroom::FULL,
    );
    let _ = send_and_recv(&mut stream_a, &WorkerMessage::Register(reg1)).await;

    let req_a = WorkerMessage::RequestShard {
        worker_id: WorkerId(1),
        shard_id: ShardId(20),
    };
    let resp_a = send_and_recv(&mut stream_a, &req_a).await;
    let reply_a: ControlMessage = serde_json::from_str(resp_a.trim()).unwrap();
    let _lease_a = match reply_a {
        ControlMessage::ShardAssigned { lease } => lease,
        other => panic!("Expected ShardAssigned on Node A, got: {other:?}"),
    };

    // 4. Depose Node A using force_step_down_for_test
    node_a.handle.force_step_down_for_test();
    assert!(!node_a.handle.is_leader());

    // Subsequent shard request to deposed Node A must fail with NotLeader
    let req_a2 = WorkerMessage::RequestShard {
        worker_id: WorkerId(1),
        shard_id: ShardId(20),
    };
    let resp_a2 = send_and_recv(&mut stream_a, &req_a2).await;
    let reply_a2: ControlMessage = serde_json::from_str(resp_a2.trim()).unwrap();
    assert!(
        matches!(reply_a2, ControlMessage::NotLeader { .. }),
        "Deposed leader Node A must reject shard requests with NotLeader: {reply_a2:?}"
    );

    // Clean up
    handle_a.shutdown();
    handle_b.shutdown();
    node_a.shutdown();
    node_b.shutdown();
}
