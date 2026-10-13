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
    let recovered = persistent_b.load().await.unwrap();
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
    assert_eq!(persistent_c.load().await.unwrap(), state2);
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
        persistent_a.load().await.unwrap(),
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
    let recovered = persistent_b.load().await.unwrap();
    assert_eq!(
        recovered, state,
        "current_term/voted_for must survive a MinIO-backed control node restart identically"
    );
}

// ─── Issue #104: fail closed on unreadable Raft authority ─────────────────

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use rockstream_control::raft::{spawn_raft_node, RaftConfig, RaftRole};
use std::fmt;
use std::io;
use std::sync::Mutex;
use std::time::Duration;

const RAFT_STATE_PATH: &str = "control/raft/state.json";
const AUTHORITY: RaftPersistentState = RaftPersistentState {
    current_term: 7,
    voted_for: Some(1), // Candidate A.
};

#[derive(Clone, Copy, Debug)]
enum ReadFault {
    None,
    Permission,
    TransientGet,
    Body,
}

#[derive(Debug)]
struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    fault: ReadFault,
    transcript: Arc<Mutex<Vec<String>>>,
}

impl fmt::Display for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "raft startup fault store")
    }
}

impl FaultStore {
    fn error(&self, message: &str, kind: io::ErrorKind) -> object_store::Error {
        object_store::Error::Generic {
            store: "raft startup fault store",
            source: Box::new(io::Error::new(kind, message.to_string())),
        }
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.transcript.lock().unwrap().push("persistent-write".into());
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        match self.fault {
            ReadFault::Permission => Err(self.error("permission denied", io::ErrorKind::PermissionDenied)),
            ReadFault::TransientGet => Err(self.error("GET temporarily unavailable", io::ErrorKind::TimedOut)),
            ReadFault::Body => {
                let mut result = self.inner.get_opts(location, opts).await?;
                result.payload = GetResultPayload::Stream(
                    stream::once(async {
                        Err(object_store::Error::Generic {
                            store: "raft startup fault store",
                            source: Box::new(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "body read failed",
                            )),
                        })
                    })
                    .boxed(),
                );
                Ok(result)
            }
            ReadFault::None => self.inner.get_opts(location, opts).await,
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

async fn stored_bytes(store: &Arc<dyn ObjectStore>) -> Vec<u8> {
    store
        .get(&Path::from(RAFT_STATE_PATH))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec()
}

async fn assert_failed_startup(
    store: Arc<dyn ObjectStore>,
    fault: ReadFault,
    expected_kind: io::ErrorKind,
    expected_message: &str,
) {
    let original = stored_bytes(&store).await;
    let transcript = Arc::new(Mutex::new(Vec::<String>::new()));
    let injected: Arc<dyn ObjectStore> = Arc::new(FaultStore {
        inner: store.clone(),
        fault,
        transcript: transcript.clone(),
    });

    let reserved = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = reserved.local_addr().unwrap();
    drop(reserved);

    let error = match spawn_raft_node(
        &addr.to_string(),
        RaftConfig::new(0, vec![], true),
        injected,
    )
    .await
    {
        Ok(node) => {
            node.shutdown();
            panic!("corrupt/unreadable authority unexpectedly started Raft");
        }
        Err(error) => error,
    };
    assert_eq!(error.kind(), expected_kind);
    assert!(
        error.to_string().contains(expected_message),
        "unexpected startup error: {error}"
    );
    assert_eq!(stored_bytes(&store).await, original);

    // Startup must fail before binding the listener or spawning election /
    // peer-RPC tasks. No votes or leadership events can be emitted.
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    drop(listener);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(*transcript.lock().unwrap(), Vec::<String>::new());
}

#[tokio::test]
async fn raft_startup_restores_term_seven_and_vote_a_exactly() {
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    RaftPersistentStore::new(backing.clone()).save(&AUTHORITY).await;

    // A non-bootstrap restart must not initiate an election on startup.
    let node = spawn_raft_node(
        "127.0.0.1:0",
        RaftConfig::new(0, vec![], false),
        backing.clone(),
    )
    .await
    .unwrap();
    assert_eq!(node.handle.current_term(), 7);
    assert_eq!(node.handle.role(), RaftRole::Follower);
    assert_eq!(node.handle.current_leader(), None);
    assert_eq!(RaftPersistentStore::new(backing.clone()).load().await.unwrap(), AUTHORITY);
    node.shutdown();
}

#[tokio::test]
async fn raft_startup_rejects_malformed_authority_without_side_effects() {
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    backing
        .put(&Path::from(RAFT_STATE_PATH), b"{malformed".to_vec().into())
        .await
        .unwrap();
    assert_failed_startup(backing, ReadFault::None, io::ErrorKind::InvalidData, "malformed Raft term/vote state").await;
}

#[tokio::test]
async fn raft_startup_rejects_permission_denied_without_side_effects() {
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    RaftPersistentStore::new(backing.clone()).save(&AUTHORITY).await;
    assert_failed_startup(backing, ReadFault::Permission, io::ErrorKind::PermissionDenied, "permission denied").await;
}

#[tokio::test]
async fn raft_startup_rejects_transient_get_without_side_effects() {
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    RaftPersistentStore::new(backing.clone()).save(&AUTHORITY).await;
    assert_failed_startup(backing, ReadFault::TransientGet, io::ErrorKind::TimedOut, "GET temporarily unavailable").await;
}

#[tokio::test]
async fn raft_startup_rejects_body_read_failure_without_side_effects() {
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    RaftPersistentStore::new(backing.clone()).save(&AUTHORITY).await;
    assert_failed_startup(backing, ReadFault::Body, io::ErrorKind::Other, "body read failed").await;
}

#[tokio::test]
async fn raft_startup_bootstraps_only_when_authority_is_truly_absent() {
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let persistent = RaftPersistentStore::new(backing.clone());
    assert_eq!(persistent.load().await.unwrap(), RaftPersistentState::default());

    let node = spawn_raft_node(
        "127.0.0.1:0",
        RaftConfig::new(0, vec![], true),
        backing.clone(),
    )
    .await
    .unwrap();
    for _ in 0..30 {
        if node.handle.is_leader() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(node.handle.role(), RaftRole::Leader);
    assert_eq!(node.handle.current_term(), 1);
    assert_eq!(
        persistent.load().await.unwrap(),
        RaftPersistentState {
            current_term: 1,
            voted_for: Some(0),
        }
    );
    node.shutdown();
}
