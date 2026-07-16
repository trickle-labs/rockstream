#![cfg(feature = "simulation")]

use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use rockstream_control::{
    ControlService, MigrationPersistentStore, ShardManager, TopologyCatalog,
    TopologyPersistentStore,
};
use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_init};
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::topology::{CapacityHeadroom, NodeRole, WorkerMessage, WorkerRegistration};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

async fn send(addr: std::net::SocketAddr, msg: &WorkerMessage) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let line = serde_json::to_string(msg).unwrap() + "\n";
    stream.write_all(line.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(stream);
    let mut buf = String::new();
    let _ = tokio::time::timeout(Duration::from_millis(50), reader.read_line(&mut buf)).await;
}

#[tokio::test]
async fn drain_converges_under_buggify_seed() {
    buggify_init(4603);
    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let store = Arc::new(InMemory::new());
    let handle = ControlService::new(catalog.clone())
        .with_shard_manager(manager.clone())
        .with_topology_store(Arc::new(TopologyPersistentStore::new(store.clone())))
        .with_migration_store(Arc::new(MigrationPersistentStore::new(store)))
        .with_auto_drain(true)
        .start("127.0.0.1:0")
        .await
        .unwrap();
    for worker_id in [1u64, 2u64] {
        let reg = WorkerRegistration::new(
            WorkerId(worker_id),
            NodeRole::Worker,
            format!("127.0.0.1:{}", 7000 + worker_id),
            CapacityHeadroom::FULL,
        );
        send(handle.addr, &WorkerMessage::Register(reg)).await;
    }
    manager.acquire(ShardId(77), WorkerId(1)).unwrap();
    if buggify!("worker_drain.delay", 1.0) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    send(
        handle.addr,
        &WorkerMessage::RequestDrain {
            worker_id: WorkerId(1),
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert!(catalog.get(WorkerId(1)).is_some());
    assert_eq!(manager.get(ShardId(77)).unwrap().worker_id, WorkerId(2));
    handle.shutdown();
    buggify_disable();
}
