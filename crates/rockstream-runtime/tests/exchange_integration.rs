use std::sync::Arc;
use tokio::sync::mpsc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_ops::zset::ArrowZSet;
use rockstream_runtime::exchange::flow_control::FlowController;
use rockstream_runtime::exchange::multiplexer::WorkerStreamMultiplexer;
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_runtime::exchange::proto::ShuffleFrame;
use rockstream_runtime::exchange::service::{ExchangeRegistry, ShuffleServer};
use rockstream_types::ids::WorkerId;

#[tokio::test]
async fn test_distributed_tpch_connections_bounded() {
    // Spin up gRPC servers for Worker 1 and Worker 2
    let addr1 = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };
    let addr2 = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };

    let registry1 = ExchangeRegistry::new();
    let server1 = ShuffleServer::new(registry1.clone());

    let (tx_close1, rx_close1) = tokio::sync::oneshot::channel::<()>();
    let server_handle1 = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(rockstream_runtime::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(server1))
            .serve_with_shutdown(addr1, async {
                let _ = rx_close1.await;
            })
            .await;
    });

    let registry2 = ExchangeRegistry::new();
    let server2 = ShuffleServer::new(registry2.clone());

    let (tx_close2, rx_close2) = tokio::sync::oneshot::channel::<()>();
    let server_handle2 = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(rockstream_runtime::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(server2))
            .serve_with_shutdown(addr2, async {
                let _ = rx_close2.await;
            })
            .await;
    });

    // Give servers a moment to start listening
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Set up peer address mapping
    let peers = Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new()));
    peers.write().insert(WorkerId(1), addr1.to_string());
    peers.write().insert(WorkerId(2), addr2.to_string());

    // Register 16 local inlets on Worker 2 (target shards 0..16)
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nationkey",
        DataType::Int32,
        false,
    )]));

    let mut receivers = Vec::new();
    for target_shard in 0..16u32 {
        let (tx, rx) = mpsc::channel(10);
        registry2.register(100, target_shard, tx, schema.clone());
        receivers.push(rx);
    }

    // Set up multiplexer on Worker 1
    let pool = ShuffleClientPool::new(peers.clone());
    let flow_controller = FlowController::new();
    let multiplexer = WorkerStreamMultiplexer::new(pool, flow_controller);

    // Simulate sending partitioned TPC-H subset (16 shards) from Worker 1 to Worker 2
    for target_shard in 0..16u32 {
        let zset = ArrowZSet::from_ab_rows(&[(target_shard as i64, 100)], 1);
        let payload = rockstream_runtime::exchange::serialization::serialize_zset(&zset).unwrap();

        let frame = ShuffleFrame {
            exchange_id: 100,
            src_shard: 0,
            target_shard,
            epoch: 1,
            seq: target_shard as u64 + 1,
            payload: payload.into(),
        };

        // Send to Worker 2
        multiplexer.send_frame(WorkerId(2), frame).await.unwrap();
    }

    // Assert that Worker 2 receives all 16 frames
    for target_shard in 0..16u32 {
        let rx = &mut receivers[target_shard as usize];
        let received = rx.recv().await.unwrap();
        assert_eq!(received.num_rows(), 1);
    }

    // CONNECTION BOUND INVARIANT ASSERTION:
    // Assert that multiplexer cached at most 1 connection/stream to Worker 2,
    // not 16 connections (which would be shard-to-shard).
    assert_eq!(multiplexer.connection_count(), 1);

    // Clean up
    let _ = tx_close1.send(());
    let _ = tx_close2.send(());
    server_handle1.abort();
    server_handle2.abort();
}
