//! Worker Graceful Drain & Shard Lease Mobility Tests (v0.59.21 Slice 4 / Phase 3a).

use object_store::local::LocalFileSystem;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_runtime::exchange::flow_control::FlowController;
use rockstream_runtime::exchange::multiplexer::WorkerStreamMultiplexer;
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_storage::ShardDb;
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerLifecycleState, WorkerMessage,
    WorkerRegistration,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

async fn send_worker_msg(addr: std::net::SocketAddr, msg: &WorkerMessage) -> Vec<ControlMessage> {
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

async fn register_worker(addr: std::net::SocketAddr, worker_id: u64) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let reg = WorkerRegistration::new(
        WorkerId(worker_id),
        NodeRole::Worker,
        format!("127.0.0.1:{}", 8000 + worker_id),
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
        } => {
            assert_eq!(registered, WorkerId(worker_id));
        }
        other => panic!("expected exact registration response, got {other:?}"),
    }
    stream
}

#[tokio::test]
async fn test_worker_epoch_flush_lease_release_durability() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

    // 1. Worker 1 writes committed data to durable SlateDB storage
    {
        let db = ShardDb::builder("shard-42", store.clone())
            .build()
            .await
            .unwrap();
        db.put(b"k1", b"v1").await.unwrap();
        db.put(b"k2", b"v2").await.unwrap();
        db.flush().await.unwrap();
    }

    // 2. Start Control Plane and register Worker 1 & Worker 2
    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let service = ControlService::new(catalog.clone()).with_shard_manager(manager.clone());
    let handle = service.start("127.0.0.1:0").await.unwrap();

    let _w1_conn = register_worker(handle.addr, 1).await;
    let _w2_conn = register_worker(handle.addr, 2).await;

    // Worker 1 acquires Shard 42
    manager.acquire(ShardId(42), WorkerId(1)).unwrap();
    assert_eq!(
        manager.get(ShardId(42)).map(|l| l.worker_id),
        Some(WorkerId(1))
    );

    // Set up Exchange pool and multiplexer
    let pool = ShuffleClientPool::default();
    let controller = FlowController::new();
    let multiplexer = WorkerStreamMultiplexer::new(pool.clone(), controller);

    // 3. Initiate Graceful Drain on Worker 1
    // Request drain
    let drain_replies = send_worker_msg(
        handle.addr,
        &WorkerMessage::RequestDrain {
            worker_id: WorkerId(1),
        },
    )
    .await;
    assert!(matches!(
        drain_replies.last(),
        Some(ControlMessage::DrainStatus { .. })
    ));

    // Worker 1 purges exchange connections and sends DrainAck with 0 shards remaining
    multiplexer.evict_worker(WorkerId(1));
    pool.evict_worker(WorkerId(1));

    let _ack_replies = send_worker_msg(
        handle.addr,
        &WorkerMessage::DrainAck {
            worker_id: WorkerId(1),
            shards_remaining: 0,
        },
    )
    .await;

    // 4. Verify lease is immediately released to control plane
    let w1 = catalog.get(WorkerId(1)).unwrap();
    assert!(matches!(
        w1.lifecycle,
        WorkerLifecycleState::Decommissioned { .. }
    ));
    assert_eq!(manager.get(ShardId(42)).map(|l| l.worker_id), None);

    // 5. Worker 2 immediately acquires the released Shard 42 without timeout delay
    manager.acquire(ShardId(42), WorkerId(2)).unwrap();
    assert_eq!(
        manager.get(ShardId(42)).map(|l| l.worker_id),
        Some(WorkerId(2))
    );

    // 6. Worker 2 opens shard database and verifies all committed rows survived
    {
        let db = ShardDb::builder("shard-42", store.clone())
            .build()
            .await
            .unwrap();
        assert_eq!(
            db.get(b"k1").await.unwrap(),
            Some(bytes::Bytes::from_static(b"v1"))
        );
        assert_eq!(
            db.get(b"k2").await.unwrap(),
            Some(bytes::Bytes::from_static(b"v2"))
        );
    }

    handle.shutdown();
}
