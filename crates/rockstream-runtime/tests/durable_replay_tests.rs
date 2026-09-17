//! v0.67 Slice 6 tests: Durable Request Identity, Monotonic Frontier Advancement & Safe Replay.
//!
//! Verifies:
//! 1. Replay post-ACK returns cached outcome with 0 duplicate logical writes (V067-04).
//! 2. Replay after kill pre-ACK recognizes committed epoch and returns ACK without re-executing.
//! 3. Crash before commit replays cleanly without data loss.
//! 4. Conflicting payload digest with reused request ID is deterministically rejected with RS-3008.
//! 5. Out-of-order epoch is rejected with RS-3011.
//! 6. Stale lease token is rejected with RS-3004.
//! 7. Monotonic frontier advancement across restarts.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_runtime::exchange::persistence::{
    committed_frontier, execute_durable_request, RequestIdentity,
};
use rockstream_storage::shard_db::ShardDbBuilder;

#[tokio::test]
async fn test_durable_request_identity_exact_replay_across_restart() {
    let store = Arc::new(InMemory::new());
    let identity = RequestIdentity::new(100, 1, 10, 1, 42);
    let payload = b"arrow-record-batch-1";
    let active_lease = 10;

    let op_executions = Arc::new(AtomicUsize::new(0));

    // First process: execute and commit epoch 1
    {
        let db = ShardDbBuilder::new("shard-1", store.clone())
            .build()
            .await
            .unwrap();

        let ops = op_executions.clone();
        let (res, opt) = execute_durable_request(
            &db,
            &identity,
            payload,
            active_lease,
            active_lease,
            || async {
                ops.fetch_add(1, Ordering::SeqCst);
                Ok("exec-result-1")
            },
        )
        .await
        .unwrap();

        assert!(!res.is_replayed());
        assert_eq!(opt, Some("exec-result-1"));
        assert_eq!(op_executions.load(Ordering::SeqCst), 1);
        assert_eq!(committed_frontier(&db).await.unwrap(), 1);
    }

    // Second process (restart): reopen ShardDb from durable store, replay identical request
    {
        let db = ShardDbBuilder::new("shard-1", store.clone())
            .build()
            .await
            .unwrap();

        assert_eq!(committed_frontier(&db).await.unwrap(), 1);

        let ops = op_executions.clone();
        let (res, opt) = execute_durable_request(
            &db,
            &identity,
            payload,
            active_lease,
            active_lease,
            || async {
                ops.fetch_add(1, Ordering::SeqCst);
                Ok("should-not-execute")
            },
        )
        .await
        .unwrap();

        // Must recognize committed identity, return cached outcome, 0 duplicate logical writes
        assert!(res.is_replayed());
        assert_eq!(opt, None);
        assert_eq!(
            op_executions.load(Ordering::SeqCst),
            1,
            "must not re-execute op"
        );
        assert_eq!(res.outcome().committed_epoch, 1);
    }
}

#[tokio::test]
async fn test_replay_post_ack_returns_cached_outcome() {
    let store = Arc::new(InMemory::new());
    let db = ShardDbBuilder::new("shard-ack", store.clone())
        .build()
        .await
        .unwrap();

    let identity = RequestIdentity::new(200, 2, 20, 5, 999);
    let payload = b"batch-data-5";
    let executions = Arc::new(AtomicUsize::new(0));

    // First delivery and ACK
    let execs = executions.clone();
    let (res1, _) = execute_durable_request(&db, &identity, payload, 1, 1, || async {
        execs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .await
    .unwrap();
    assert!(!res1.is_replayed());
    assert_eq!(executions.load(Ordering::SeqCst), 1);

    // Replay post-ACK (e.g. client retries)
    let execs2 = executions.clone();
    let (res2, _) = execute_durable_request(&db, &identity, payload, 1, 1, || async {
        execs2.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .await
    .unwrap();

    assert!(res2.is_replayed());
    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "0 duplicate logical writes"
    );
    assert_eq!(res1.outcome().payload_digest, res2.outcome().payload_digest);
}

#[tokio::test]
async fn test_replay_after_kill_pre_ack_recognizes_committed_epoch() {
    let store = Arc::new(InMemory::new());
    let identity = RequestIdentity::new(300, 3, 30, 2, 77);
    let payload = b"batch-killed-before-ack";
    let op_count = Arc::new(AtomicUsize::new(0));

    // Process commits to durable storage, but dies right before sending ACK back
    {
        let db = ShardDbBuilder::new("shard-kill", store.clone())
            .build()
            .await
            .unwrap();

        let ops = op_count.clone();
        let _ = execute_durable_request(&db, &identity, payload, 5, 5, || async {
            ops.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();
        // Drop db abruptly simulating process crash
    }

    // On restart: client replays the unacknowledged frame
    {
        let db = ShardDbBuilder::new("shard-kill", store.clone())
            .build()
            .await
            .unwrap();

        let ops = op_count.clone();
        let (res, _) = execute_durable_request(&db, &identity, payload, 5, 5, || async {
            ops.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();

        assert!(res.is_replayed());
        assert_eq!(op_count.load(Ordering::SeqCst), 1, "no duplicate write");
        assert_eq!(res.outcome().committed_epoch, 2);
    }
}

#[tokio::test]
async fn test_crash_before_commit_replays_cleanly() {
    let store = Arc::new(InMemory::new());
    let identity = RequestIdentity::new(400, 4, 40, 1, 123);
    let payload = b"batch-crashed-during-compute";
    let op_count = Arc::new(AtomicUsize::new(0));

    // First attempt: operation fails or crashes before commit
    {
        let db = ShardDbBuilder::new("shard-crash", store.clone())
            .build()
            .await
            .unwrap();

        let ops = op_count.clone();
        let res = execute_durable_request(&db, &identity, payload, 1, 1, || async {
            ops.fetch_add(1, Ordering::SeqCst);
            Err::<(), String>("simulated crash before commit".to_string())
        })
        .await;

        assert!(res.is_err());
        assert_eq!(committed_frontier(&db).await.unwrap(), 0);
    }

    // On restart: frame is replayed, now completes normally
    {
        let db = ShardDbBuilder::new("shard-crash", store.clone())
            .build()
            .await
            .unwrap();

        let ops = op_count.clone();
        let (res, opt) = execute_durable_request(&db, &identity, payload, 1, 1, || async {
            ops.fetch_add(1, Ordering::SeqCst);
            Ok("recovered-ok")
        })
        .await
        .unwrap();

        assert!(!res.is_replayed());
        assert_eq!(opt, Some("recovered-ok"));
        assert_eq!(op_count.load(Ordering::SeqCst), 2);
        assert_eq!(committed_frontier(&db).await.unwrap(), 1);
    }
}

#[tokio::test]
async fn test_conflicting_payload_digest_rejected_deterministically() {
    let store = Arc::new(InMemory::new());
    let db = ShardDbBuilder::new("shard-conflict", store.clone())
        .build()
        .await
        .unwrap();

    let identity = RequestIdentity::new(500, 5, 50, 1, 888);
    let payload1 = b"original-payload";
    let payload2 = b"different-conflicting-payload";

    // First commit
    let (res, _) = execute_durable_request(&db, &identity, payload1, 1, 1, || async { Ok(()) })
        .await
        .unwrap();
    assert!(!res.is_replayed());

    // Second request: SAME request_id and epoch, but DIFFERENT payload digest -> RS-3008
    let err = execute_durable_request(&db, &identity, payload2, 1, 1, || async { Ok(()) })
        .await
        .unwrap_err();

    assert!(err.contains("RS-3008"), "expected RS-3008, got: {err}");
    assert!(err.contains("conflicting payload digest"));
}

#[tokio::test]
async fn test_out_of_order_epoch_rejected_deterministically() {
    let store = Arc::new(InMemory::new());
    let db = ShardDbBuilder::new("shard-ooo", store.clone())
        .build()
        .await
        .unwrap();

    // Commit epoch 10
    let id10 = RequestIdentity::new(600, 6, 60, 10, 1);
    execute_durable_request(&db, &id10, b"epoch-10", 1, 1, || async { Ok(()) })
        .await
        .unwrap();
    assert_eq!(committed_frontier(&db).await.unwrap(), 10);

    // Frame with epoch 5 < 10 arrives out-of-order -> RS-3011
    let id5 = RequestIdentity::new(600, 6, 60, 5, 2);
    let err = execute_durable_request(&db, &id5, b"epoch-5", 1, 1, || async { Ok(()) })
        .await
        .unwrap_err();

    assert!(err.contains("RS-3011"), "expected RS-3011, got: {err}");
    assert!(err.contains("out-of-order epoch"));
}

#[tokio::test]
async fn test_stale_lease_token_rejected_deterministically() {
    let store = Arc::new(InMemory::new());
    let db = ShardDbBuilder::new("shard-lease", store.clone())
        .build()
        .await
        .unwrap();

    let id = RequestIdentity::new(700, 7, 70, 1, 1);
    let active_lease = 10;
    let stale_lease = 9;

    let err = execute_durable_request(&db, &id, b"payload", stale_lease, active_lease, || async {
        Ok(())
    })
    .await
    .unwrap_err();

    assert!(err.contains("RS-3004"), "expected RS-3004, got: {err}");
    assert!(err.contains("stale lease token"));
}
