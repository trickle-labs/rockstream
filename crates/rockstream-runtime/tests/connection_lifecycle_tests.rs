//! v0.67 Slice 4 exit tests: Persistent Multiplexed Connections & Obsolete Generation Fencing.
//!
//! Verifies:
//! 1. Generation ID advances on reconnect/failure and obsolete generation responses are fenced (RS-3004).
//! 2. Permit safety under saturation (RS-3006) and permit reclamation on disconnect/error.
//! 3. Multiplexed streams across long-lived connection bounds.
//! 4. Peer heartbeat tracking and deadline-triggered reconnection.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parking_lot::RwLock;
use rockstream_ops::ArrowZSet;
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_runtime::exchange::proto::shuffle_service_server::ShuffleServiceServer;
use rockstream_runtime::exchange::serialization::build_exchange_frame;
use rockstream_runtime::exchange::service::{ExchangeRegistry, ShuffleServer};
use rockstream_types::ids::WorkerId;
use tonic::transport::Server;

/// Test 1 (Slice 4): Generation fencing and permit safety bounds.
#[tokio::test]
async fn test_shuffle_client_pool_generation_fencing_and_permit_safety() {
    let peers = Arc::new(RwLock::new(HashMap::new()));
    let worker_id = WorkerId(101);
    peers.write().insert(worker_id, "127.0.0.1:50051".into());

    let pool = ShuffleClientPool::new(peers).with_max_pending_requests(3);

    let initial_gen = pool.current_generation(worker_id);
    assert_eq!(initial_gen, 1, "initial generation must be 1");

    // Acquire permits up to max_pending_requests
    assert!(pool.acquire_permit(worker_id, 1).is_ok());
    assert!(pool.acquire_permit(worker_id, 1).is_ok());
    assert!(pool.acquire_permit(worker_id, 1).is_ok());
    assert_eq!(pool.active_permits(worker_id), 3);

    // Exceeding max_pending_requests fails with RS-3006 (RESOURCE_EXHAUSTED)
    let overflow_err = pool.acquire_permit(worker_id, 1).unwrap_err();
    assert!(
        overflow_err.contains("RS-3006"),
        "expected RS-3006, got: {overflow_err}"
    );
    assert!(overflow_err.contains("RESOURCE_EXHAUSTED"));

    // Stale generation request fails with RS-3004
    let stale_err = pool.acquire_permit(worker_id, 0).unwrap_err();
    assert!(
        stale_err.contains("RS-3004"),
        "expected RS-3004, got: {stale_err}"
    );

    // Release one permit
    pool.release_permit(worker_id);
    assert_eq!(pool.active_permits(worker_id), 2);

    // Simulate reconnect / failure: generation advances
    let next_gen = pool.advance_generation(worker_id);
    assert_eq!(next_gen, 2);

    // Fence response from obsolete generation 1
    let fence_err = pool.fence_response(worker_id, 1).unwrap_err();
    assert!(
        fence_err.contains("RS-3004"),
        "expected RS-3004, got: {fence_err}"
    );
    assert!(fence_err.contains("obsolete generation response fenced"));

    // Valid response from generation 2 succeeds
    assert!(pool.fence_response(worker_id, 2).is_ok());
}

/// Test 2 (Slice 4): Persistent connection multiplexing across multiple concurrent streams.
#[tokio::test]
async fn test_persistent_connection_multiplexing_bounds() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(ShuffleServiceServer::new(ShuffleServer::new(
                ExchangeRegistry::new(),
            )))
            .serve(addr)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let worker_id = WorkerId(201);
    let peers = Arc::new(RwLock::new(HashMap::new()));
    peers.write().insert(worker_id, addr.to_string());
    let pool = ShuffleClientPool::new(peers);

    // Multiple get_client calls return cached multiplexed connection
    let client1 = pool.get_client(worker_id).await.expect("get client 1");
    let client2 = pool.get_client(worker_id).await.expect("get client 2");

    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let array = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
    let zset = ArrowZSet::new(batch, vec![1, 1]);

    // Launch 16 concurrent multiplexed streams over the connection
    let mut handles = Vec::new();
    for stream_idx in 0..16 {
        let mut client = if stream_idx % 2 == 0 {
            client1.clone()
        } else {
            client2.clone()
        };
        let frame = build_exchange_frame(500, 1, stream_idx as u64, 1, 10, &schema, &zset).unwrap();

        handles.push(tokio::spawn(async move {
            let mut request = tonic::Request::new(futures::stream::iter(vec![frame]));
            request
                .metadata_mut()
                .insert("protocol_version", "1".parse().unwrap());
            let mut response_stream = client.exchange_stream(request).await.unwrap().into_inner();
            let ack = response_stream.next().await.unwrap().unwrap();
            assert!(ack.success);
            assert_eq!(ack.operator_id, stream_idx as u64);
        }));
    }

    for h in handles {
        h.await.expect("stream task panicked");
    }

    server_handle.abort();
}

/// Test 3 (Slice 4): Mid-frame worker disconnect reclaims all active permits.
#[tokio::test]
async fn test_worker_disconnect_mid_frame_reclaims_permits() {
    let worker_id = WorkerId(301);
    let peers = Arc::new(RwLock::new(HashMap::new()));
    peers.write().insert(worker_id, "127.0.0.1:50052".into());
    let pool = ShuffleClientPool::new(peers).with_max_pending_requests(10);

    // Acquire 5 in-flight permits
    for _ in 0..5 {
        pool.acquire_permit(worker_id, 1).unwrap();
    }
    assert_eq!(pool.active_permits(worker_id), 5);

    // Simulate worker disconnect mid-frame
    let reclaimed = pool.reclaim_permits_on_disconnect(worker_id);
    assert_eq!(reclaimed, 5, "must reclaim all 5 active permits");
    assert_eq!(
        pool.active_permits(worker_id),
        0,
        "active permits must be 0 after reclaim"
    );

    // Failure automatically increments generation and reclaims
    pool.acquire_permit(worker_id, 1).unwrap();
    pool.record_failure(worker_id);
    assert_eq!(pool.active_permits(worker_id), 0);
    assert_eq!(pool.current_generation(worker_id), 2);
}

/// Test 4 (Slice 4): Gateway reconnect fences obsolete generation responses.
#[tokio::test]
async fn test_gateway_reconnect_fences_obsolete_generation_responses() {
    let worker_id = WorkerId(401);
    let peers = Arc::new(RwLock::new(HashMap::new()));
    peers.write().insert(worker_id, "127.0.0.1:50053".into());
    let pool = ShuffleClientPool::new(peers);

    // Generation 1: request sent
    assert_eq!(pool.current_generation(worker_id), 1);
    pool.acquire_permit(worker_id, 1).unwrap();

    // Reconnect triggered: generation advances to 2
    let gen2 = pool.advance_generation(worker_id);
    assert_eq!(gen2, 2);

    // Late response arriving with generation 1 must be fenced
    let err = pool.fence_response(worker_id, 1).unwrap_err();
    assert!(err.contains("RS-3004"));
    assert!(err.contains("obsolete generation response fenced"));

    // Request on current generation 2
    pool.acquire_permit(worker_id, 2).unwrap();
    assert!(pool.fence_response(worker_id, 2).is_ok());
}

/// Test 5 (Slice 4): Peer heartbeat loss triggers deadline and reconnect.
#[tokio::test]
async fn test_peer_heartbeat_loss_triggers_deadline_and_reconnect() {
    let worker_id = WorkerId(501);
    let peers = Arc::new(RwLock::new(HashMap::new()));
    peers.write().insert(worker_id, "127.0.0.1:50054".into());
    let pool = ShuffleClientPool::new(peers);

    // Record initial heartbeat
    pool.record_heartbeat(worker_id);
    assert!(pool.is_peer_alive(worker_id, Duration::from_secs(10)));
    assert!(pool
        .check_heartbeat_deadline(worker_id, Duration::from_secs(10))
        .is_ok());

    // With 0ns deadline, peer is silent > deadline -> triggers RS-5003
    tokio::time::sleep(Duration::from_millis(10)).await;
    let deadline_err = pool
        .check_heartbeat_deadline(worker_id, Duration::from_millis(1))
        .unwrap_err();
    assert!(deadline_err.contains("RS-5003"));
    assert!(deadline_err.contains("channel marked unhealthy"));

    // Generation advanced and failure recorded
    assert_eq!(pool.current_generation(worker_id), 2);
}
