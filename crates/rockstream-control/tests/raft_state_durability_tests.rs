//! v0.45.2 M7 S5 — durability tests for the *new* Raft term/vote persistence
//! path introduced by S1 (`RaftPersistentStore`, `control/raft/state.json`).
//!
//! `.claude/v0.45.2-plan.md` §4 "Durability Slices": this is new durable
//! state that did not exist before v0.45.2 and therefore needs both required
//! tests per the Phase 2 rule — a LocalFileSystem (embedded) test and a
//! MinIO (S3, TestContainers) test.
//!
//! Scope note: the persisted state is `current_term`/`voted_for` only
//! (`RaftPersistentState`, see `rockstream_control::raft`) — this
//! implementation's Raft group does not replicate an application log
//! through this store (only leader-election state), so there is no
//! separate "log entries" round trip to test here; the plan's prose
//! mentions log entries as an aspirational superset, but the actual S1
//! implementation (already complete, not modified by this test) only ever
//! writes term/vote. Both tests below assert what is actually persisted.

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_control::raft::{RaftPersistentState, RaftPersistentStore};
use rockstream_test_support::docker_available;
use rockstream_test_support::minio::{minio_object_store, start_minio};

/// **S5 durability slice** — LocalFileSystem (embedded) backend: a control
/// node persists `current_term`/`voted_for`, "restarts" (a fresh
/// `RaftPersistentStore` pointed at the same on-disk directory, modeling a
/// process restart against the same `--storage` path), and recovers the
/// identical state.
#[tokio::test]
async fn raft_term_vote_log_survive_restart_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

    // First "boot": no state persisted yet, node votes for itself at term 3.
    let persistent_a = RaftPersistentStore::new(store.clone());
    let before_boot = persistent_a.load().await;
    assert_eq!(
        before_boot,
        RaftPersistentState::default(),
        "a fresh control node with no prior state must see the zero-value default"
    );
    let state = RaftPersistentState {
        current_term: 3,
        voted_for: Some(7),
    };
    persistent_a.save(&state).await;

    // "Restart": a brand-new `RaftPersistentStore` instance (as a fresh
    // process boot would construct), backed by the SAME on-disk directory.
    let persistent_b = RaftPersistentStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    let recovered = persistent_b.load().await;
    assert_eq!(
        recovered, state,
        "current_term/voted_for must survive an LFS-backed control node restart identically"
    );

    // A second term/vote update after "restart" also persists correctly —
    // proves the recovered store is fully live, not just readable once.
    let state2 = RaftPersistentState {
        current_term: 4,
        voted_for: Some(9),
    };
    persistent_b.save(&state2).await;
    let persistent_c = RaftPersistentStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    assert_eq!(persistent_c.load().await, state2);
}

// ─── MinIO (S3, TestContainers) durability test ────────────────────────────

const MINIO_BUCKET: &str = "rockstream-raft-durability-test";

/// **S5 durability slice** — MinIO (S3, TestContainers) backend: same
/// persist → "restart" → recover round trip as the LFS test above, but
/// against a real S3-compatible object store, proving the Tier-3 deployment
/// profile's durability path for the new Raft term/vote state.
///
/// Skips gracefully (rather than failing) when Docker is unavailable,
/// following this repo's existing MinIO TestContainers convention (see
/// `rockstream-storage/tests/minio_backend.rs`).
#[tokio::test]
async fn raft_term_vote_log_survive_restart_minio_tc() {
    if !docker_available() {
        eprintln!("SKIP raft_term_vote_log_survive_restart_minio_tc: Docker not available");
        return;
    }

    let (_container, port) = match start_minio(MINIO_BUCKET).await {
        Some(res) => res,
        None => return,
    };

    let store_a = minio_object_store(port, MINIO_BUCKET);
    let persistent_a = RaftPersistentStore::new(store_a);
    assert_eq!(
        persistent_a.load().await,
        RaftPersistentState::default(),
        "a fresh bucket must see the zero-value default"
    );
    let state = RaftPersistentState {
        current_term: 5,
        voted_for: Some(2),
    };
    persistent_a.save(&state).await;

    // "Restart": brand-new `RaftPersistentStore` + brand-new S3 client
    // pointed at the same MinIO bucket (as a fresh process boot would
    // construct against the same durable backing store).
    let store_b = minio_object_store(port, MINIO_BUCKET);
    let persistent_b = RaftPersistentStore::new(store_b);
    let recovered = persistent_b.load().await;
    assert_eq!(
        recovered, state,
        "current_term/voted_for must survive a MinIO-backed control node restart identically"
    );
}
