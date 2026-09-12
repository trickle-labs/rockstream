//! v0.45.6 S7 — durability tests for the *new* `frontier/leader` fencing-
//! token CAS record and synchronously-flushed published-frontier value
//! introduced by S3–S6 (`FrontierLeaseStore`, `rockstream_control::frontier`).
//!
//! `.claude/v0.45.6-plan.md` §4 "Durability Slices": this is new durable
//! state that did not exist before this version (the pre-v0.45.6
//! `FrontierAggregator` was purely in-memory, per-process, with no
//! persisted lease) and therefore needs both required tests per the Phase 2
//! rule — a LocalFileSystem (embedded) test and a MinIO (S3, TestContainers)
//! test — plus a focused sync-flush regression pair, per the plan.
//!
//! This is also the runtime witness for **M2-L1** (`PublicationProgress`:
//! the store's `published_frontier` reaches its target value across a
//! restart) and **M2-L2** (`FailoverProgress`: a second aggregator recovers
//! and continues publishing after the first crashes) from
//! `formal/m2_frontier_agg.fizz`.

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_control::frontier::{FrontierLeaseError, FrontierLeaseStore};
use rockstream_test_support::docker_available;
use rockstream_test_support::minio::{minio_object_store, start_minio};
use rockstream_types::ids::AggregatorId;

/// **S7 durability slice** — LocalFileSystem (embedded) backend: a
/// publisher acquires the lease, publishes, "crashes" (the `FrontierLeaseStore`
/// handle is dropped without closing anything explicitly — SlateDB durability
/// is not process-lifetime-dependent); a second aggregator recovers against
/// the same on-disk directory, acquires with a strictly higher token, and
/// its first read observes the crashed publisher's last (synchronously-
/// flushed) write — never a stale value.
///
/// M2-L1/M2-L2 witness: `published_frontier` survives the restart
/// (`PublicationProgress`) and a fresh aggregator continues publishing after
/// the first's "crash" (`FailoverProgress`).
#[tokio::test]
async fn frontier_leader_lease_cas_survives_restart_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

    // First "boot": aggregator 1 acquires the lease and publishes frontier 42.
    let lease_a = FrontierLeaseStore::open("frontier-lease", store.clone())
        .await
        .unwrap();
    assert_eq!(lease_a.current_fence_token().await, 0);
    let token_a = lease_a
        .acquire_publisher_lease(AggregatorId(1), 0)
        .await
        .unwrap();
    lease_a.publish_frontier(token_a, 42).await.unwrap();
    assert_eq!(
        lease_a.read_published_frontier_after_handoff().await,
        Some(42)
    );

    // "Crash": drop the first handle without any explicit close/flush call
    // (SlateDB's own durability, not process cleanup, is what's under test).
    drop(lease_a);

    // "Restart": a brand-new `FrontierLeaseStore` (as a fresh aggregator
    // process would construct), backed by the SAME on-disk directory.
    let lease_b = FrontierLeaseStore::open(
        "frontier-lease",
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
    )
    .await
    .unwrap();

    // M2-L1: published_frontier survived the restart, and the recovered
    // handoff read observes the prior synchronously-flushed write — never
    // a stale value (S5's assert_flush_before_lease_handoff_read must not
    // panic here).
    assert!(
        lease_b.read_published_frontier_after_handoff().await == Some(42),
        "M2-L1: published_frontier must survive the restart"
    );

    // M2-L2: the recovering aggregator (2) acquires with a strictly higher
    // token than the crashed publisher's last token, and can continue
    // publishing — failover progress.
    assert_eq!(lease_b.current_fence_token().await, token_a.0);
    let token_b = lease_b
        .acquire_publisher_lease(AggregatorId(2), token_a.0)
        .await
        .unwrap();
    assert!(token_b.0 > token_a.0);
    lease_b.publish_frontier(token_b, 99).await.unwrap();
    assert_eq!(
        lease_b.read_published_frontier_after_handoff().await,
        Some(99)
    );

    // The superseded aggregator's stale token must now be rejected — never
    // resurrecting a lower/older frontier value.
    let err = lease_b
        .acquire_publisher_lease(AggregatorId(1), token_a.0)
        .await
        .unwrap_err();
    assert!(matches!(err, FrontierLeaseError::StaleFenceToken { .. }));
}

/// **S7 focused regression**: directly exercises S5's
/// `assert_flush_before_lease_handoff_read` panic path by forcing an
/// unflushed write ahead of a lease handoff, mirroring the existing
/// `assert_valid_publisher` panic-path pattern in `frontier.rs`.
///
/// `FrontierLeaseStore` itself always writes with `await_durable: true`
/// (there is no code path that produces an unflushed publish), so this
/// drives the paired assertion function directly, the same way
/// `assert_valid_publisher`'s panic path is unit-tested in `frontier.rs`.
#[tokio::test]
async fn sync_flush_before_lease_handoff_read_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let lease = FrontierLeaseStore::open("frontier-lease-sync", store)
        .await
        .unwrap();

    // The real store never produces an unflushed published value — confirm
    // that directly (S5's panic path is unreachable via the public API).
    let token = lease
        .acquire_publisher_lease(AggregatorId(1), 0)
        .await
        .unwrap();
    lease.publish_frontier(token, 3).await.unwrap();
    assert_eq!(lease.read_published_frontier_after_handoff().await, Some(3));

    // Directly drive the paired assertion with a forced "unflushed write"
    // scenario (`has_published_value=true`, `last_write_synced=false`) —
    // this is the RS-8003 panic path itself.
    let result = std::panic::catch_unwind(|| {
        rockstream_control::frontier::assert_flush_before_lease_handoff_read(true, false);
    });
    assert!(
        result.is_err(),
        "RS-8003: expected panic on unflushed publish ahead of lease-handoff read"
    );
}

// ─── MinIO (S3, TestContainers) durability test ────────────────────────────

const MINIO_BUCKET: &str = "rockstream-frontier-lease-durability-test";

/// **S7 durability slice** — MinIO (S3, TestContainers) backend: same
/// acquire → publish → "crash" → recover → re-acquire-with-higher-token
/// round trip as the LFS test above, but against a real S3-compatible
/// object store.
///
/// Skips gracefully (rather than failing) when Docker is unavailable,
/// following this repo's existing MinIO TestContainers convention.
#[tokio::test]
async fn frontier_leader_lease_cas_survives_restart_minio_tc() {
    if !docker_available() {
        eprintln!("SKIP frontier_leader_lease_cas_survives_restart_minio_tc: Docker not available");
        return;
    }

    let (_container, port) = match start_minio(MINIO_BUCKET).await {
        Some(res) => res,
        None => return,
    };

    let store_a = minio_object_store(port, MINIO_BUCKET);
    let lease_a = FrontierLeaseStore::open("frontier-lease-minio", store_a)
        .await
        .unwrap();
    let token_a = lease_a
        .acquire_publisher_lease(AggregatorId(1), 0)
        .await
        .unwrap();
    lease_a.publish_frontier(token_a, 42).await.unwrap();
    assert_eq!(
        lease_a.read_published_frontier_after_handoff().await,
        Some(42)
    );
    drop(lease_a);

    let store_b = minio_object_store(port, MINIO_BUCKET);
    let lease_b = FrontierLeaseStore::open("frontier-lease-minio", store_b)
        .await
        .unwrap();
    assert_eq!(
        lease_b.read_published_frontier_after_handoff().await,
        Some(42)
    );

    let token_b = lease_b
        .acquire_publisher_lease(AggregatorId(2), token_a.0)
        .await
        .unwrap();
    assert!(token_b.0 > token_a.0);
    lease_b.publish_frontier(token_b, 99).await.unwrap();
    assert_eq!(
        lease_b.read_published_frontier_after_handoff().await,
        Some(99)
    );

    let err = lease_b
        .acquire_publisher_lease(AggregatorId(1), token_a.0)
        .await
        .unwrap_err();
    assert!(matches!(err, FrontierLeaseError::StaleFenceToken { .. }));
}

/// **S7 focused regression** — MinIO backend: same
/// `assert_flush_before_lease_handoff_read` direct-drive as the LFS variant
/// above, confirming the real S3-backed store also never produces an
/// unflushed publish via its public API.
#[tokio::test]
async fn sync_flush_before_lease_handoff_read_minio_tc() {
    if !docker_available() {
        eprintln!("SKIP sync_flush_before_lease_handoff_read_minio_tc: Docker not available");
        return;
    }

    let (_container, port) = match start_minio(MINIO_BUCKET).await {
        Some(res) => res,
        None => return,
    };

    let store = minio_object_store(port, MINIO_BUCKET);
    let lease = FrontierLeaseStore::open("frontier-lease-minio-sync", store)
        .await
        .unwrap();
    let token = lease
        .acquire_publisher_lease(AggregatorId(1), 0)
        .await
        .unwrap();
    lease.publish_frontier(token, 3).await.unwrap();
    assert_eq!(lease.read_published_frontier_after_handoff().await, Some(3));

    let result = std::panic::catch_unwind(|| {
        rockstream_control::frontier::assert_flush_before_lease_handoff_read(true, false);
    });
    assert!(
        result.is_err(),
        "RS-8003: expected panic on unflushed publish ahead of lease-handoff read"
    );
}
