//! Deterministic issue #105 regressions for Raft authority write ordering.
//! Poll actual Raft handlers to a stalled ObjectStore PUT, then poll a
//! competing handler *before* releasing it. No timer or scheduling sleep
//! determines the interleaving.

use super::*;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex as StdMutex;
use std::task::Poll;
use tokio::sync::oneshot;

const A: RaftNodeId = 1;
const B: RaftNodeId = 2;
const C: RaftNodeId = 3;

fn authority(term: u64, vote: Option<RaftNodeId>) -> RaftPersistentState {
    RaftPersistentState {
        current_term: term,
        voted_for: vote,
    }
}

/// Pauses precisely one matching write at the actual ObjectStore boundary.
/// The existing bytes remain unchanged until the test releases the barrier.
struct DelayedPutStore {
    backing: Arc<dyn ObjectStore>,
    held_state: RaftPersistentState,
    blocked: AtomicBool,
    entered: StdMutex<Option<oneshot::Sender<()>>>,
    release: StdMutex<Option<oneshot::Receiver<()>>>,
    committed: StdMutex<Vec<RaftPersistentState>>,
}

impl fmt::Debug for DelayedPutStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DelayedPutStore")
            .field("held_state", &self.held_state)
            .finish()
    }
}

impl fmt::Display for DelayedPutStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "delayed Raft persistence")
    }
}

impl DelayedPutStore {
    fn committed(&self) -> Vec<RaftPersistentState> {
        self.committed.lock().unwrap().clone()
    }
}

#[async_trait]
impl ObjectStore for DelayedPutStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        assert_eq!(location, &Path::from("control/raft/state.json"));
        let bytes: bytes::Bytes = payload.clone().into();
        let snapshot: RaftPersistentState = serde_json::from_slice(&bytes).unwrap();
        if snapshot == self.held_state && !self.blocked.swap(true, Ordering::SeqCst) {
            self.entered.lock().unwrap().take().unwrap().send(()).unwrap();
            let release = self.release.lock().unwrap().take().unwrap();
            release.await.expect("test must release held Raft PUT");
        }
        let result = self.backing.put_opts(location, payload, opts).await?;
        self.committed.lock().unwrap().push(snapshot);
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.backing.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        self.backing.get_opts(location, opts).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.backing.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.backing.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.backing.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.backing.copy_opts(from, to, opts).await
    }
}

struct Fixture {
    inner: Arc<RwLock<RaftInner>>,
    store: Arc<RaftPersistentStore>,
    delayed: Arc<DelayedPutStore>,
    backing: Arc<dyn ObjectStore>,
}

async fn fixture(
    held_state: RaftPersistentState,
) -> (Fixture, oneshot::Receiver<()>, oneshot::Sender<()>) {
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let initial = authority(6, None);
    RaftPersistentStore::new(backing.clone()).save(&initial).await;

    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let delayed = Arc::new(DelayedPutStore {
        backing: backing.clone(),
        held_state,
        blocked: AtomicBool::new(false),
        entered: StdMutex::new(Some(entered_tx)),
        release: StdMutex::new(Some(release_rx)),
        committed: StdMutex::new(Vec::new()),
    });
    let store = Arc::new(RaftPersistentStore::new(delayed.clone()));
    let inner = Arc::new(RwLock::new(RaftInner {
        node_id: A,
        persistent: initial,
        role: RaftRole::Follower,
        current_leader: None,
        last_contact: Instant::now(),
    }));
    (
        Fixture {
            inner,
            store,
            delayed,
            backing,
        },
        entered_rx,
        release_tx,
    )
}

impl Fixture {
    async fn assert_restart(
        &self,
        expected: RaftPersistentState,
        expected_writes: Vec<RaftPersistentState>,
    ) {
        // Compare every completed write, not only a count or the final term.
        assert_eq!(self.delayed.committed(), expected_writes);
        assert_eq!(
            RaftPersistentStore::new(self.backing.clone())
                .load()
                .await
                .unwrap(),
            expected
        );

        let mut config = RaftConfig::new(A, vec![], false);
        config.election_timeout_min = Duration::from_secs(3600);
        config.election_timeout_max = Duration::from_secs(3601);
        let restarted = spawn_raft_node("127.0.0.1:0", config, self.backing.clone())
            .await
            .unwrap();
        assert_eq!(restarted.handle.current_term(), expected.current_term);
        assert_eq!(restarted.handle.role(), RaftRole::Follower);
        assert_eq!(restarted.handle.current_leader(), None);
        assert_eq!(
            RaftPersistentStore::new(self.backing.clone())
                .load()
                .await
                .unwrap(),
            expected
        );

        if expected.voted_for == Some(B) {
            // Exercise the *real wire RPC listener* on the restarted node,
            // not a mock vote oracle: a second candidate in term 8 loses.
            let reply = send_request_vote(
                &restarted.listen_addr.to_string(),
                8,
                C,
                Duration::from_secs(10),
            )
            .await
            .unwrap();
            assert_eq!(
                reply,
                RequestVoteResponse {
                    term: 8,
                    vote_granted: false,
                }
            );
            assert_eq!(
                RaftPersistentStore::new(self.backing.clone())
                    .load()
                    .await
                    .unwrap(),
                expected
            );
        }
        restarted.shutdown();
    }
}

#[tokio::test]
async fn held_term_seven_vote_cannot_overwrite_durable_term_eight_vote() {
    let held = authority(7, Some(A));
    let latest = authority(8, Some(B));
    let (fixture, entered, release) = fixture(held).await;

    let first = handle_request_vote(
        RequestVoteRequest {
            term: 7,
            candidate_id: A,
        },
        &fixture.inner,
        &fixture.store,
    );
    tokio::pin!(first);
    assert!(matches!(futures::poll!(first.as_mut()), Poll::Pending));
    entered.await.unwrap();
    assert_eq!(fixture.delayed.committed(), Vec::<RaftPersistentState>::new());

    let second = handle_request_vote(
        RequestVoteRequest {
            term: 8,
            candidate_id: B,
        },
        &fixture.inner,
        &fixture.store,
    );
    tokio::pin!(second);
    // The newer vote cannot be acknowledged before the older PUT finishes:
    // unconditional ObjectStore PUTs are not safe to commit out of order.
    assert!(matches!(futures::poll!(second.as_mut()), Poll::Pending));
    assert_eq!(fixture.inner.read().persistent, held);

    release.send(()).unwrap();
    assert_eq!(
        first.await,
        RequestVoteResponse {
            term: 7,
            vote_granted: true,
        }
    );
    assert_eq!(
        second.await,
        RequestVoteResponse {
            term: 8,
            vote_granted: true,
        }
    );
    fixture.assert_restart(latest, vec![held, latest]).await;
}

#[tokio::test]
async fn held_heartbeat_then_higher_vote_preserves_term_eight_vote() {
    let held = authority(7, None);
    let latest = authority(8, Some(B));
    let (fixture, entered, release) = fixture(held).await;

    let first = handle_heartbeat(
        HeartbeatRequest {
            term: 7,
            leader_id: A,
        },
        &fixture.inner,
        &fixture.store,
    );
    tokio::pin!(first);
    assert!(matches!(futures::poll!(first.as_mut()), Poll::Pending));
    entered.await.unwrap();

    let second = handle_request_vote(
        RequestVoteRequest {
            term: 8,
            candidate_id: B,
        },
        &fixture.inner,
        &fixture.store,
    );
    tokio::pin!(second);
    assert!(matches!(futures::poll!(second.as_mut()), Poll::Pending));
    assert_eq!(fixture.delayed.committed(), Vec::<RaftPersistentState>::new());

    release.send(()).unwrap();
    assert_eq!(first.await, HeartbeatResponse { term: 7 });
    assert_eq!(
        second.await,
        RequestVoteResponse {
            term: 8,
            vote_granted: true,
        }
    );
    fixture.assert_restart(latest, vec![held, latest]).await;
}

#[tokio::test]
async fn held_vote_then_higher_heartbeat_preserves_term_eight_empty_vote() {
    let held = authority(7, Some(A));
    let latest = authority(8, None);
    let (fixture, entered, release) = fixture(held).await;

    let first = handle_request_vote(
        RequestVoteRequest {
            term: 7,
            candidate_id: A,
        },
        &fixture.inner,
        &fixture.store,
    );
    tokio::pin!(first);
    assert!(matches!(futures::poll!(first.as_mut()), Poll::Pending));
    entered.await.unwrap();

    let second = handle_heartbeat(
        HeartbeatRequest {
            term: 8,
            leader_id: B,
        },
        &fixture.inner,
        &fixture.store,
    );
    tokio::pin!(second);
    assert!(matches!(futures::poll!(second.as_mut()), Poll::Pending));
    assert_eq!(fixture.delayed.committed(), Vec::<RaftPersistentState>::new());

    release.send(()).unwrap();
    assert_eq!(
        first.await,
        RequestVoteResponse {
            term: 7,
            vote_granted: true,
        }
    );
    assert_eq!(second.await, HeartbeatResponse { term: 8 });
    fixture.assert_restart(latest, vec![held, latest]).await;
}

#[tokio::test]
async fn held_self_election_then_higher_vote_cannot_regress_authority() {
    let held = authority(7, Some(A));
    let latest = authority(8, Some(B));
    let (fixture, entered, release) = fixture(held).await;

    let config = RaftConfig::new(A, vec![], true);
    let first = start_election(&config, &fixture.inner, &fixture.store);
    tokio::pin!(first);
    assert!(matches!(futures::poll!(first.as_mut()), Poll::Pending));
    entered.await.unwrap();
    assert_eq!(fixture.inner.read().role, RaftRole::Candidate);

    let second = handle_request_vote(
        RequestVoteRequest {
            term: 8,
            candidate_id: B,
        },
        &fixture.inner,
        &fixture.store,
    );
    tokio::pin!(second);
    assert!(matches!(futures::poll!(second.as_mut()), Poll::Pending));
    // No leadership event while the self-vote is not yet durable.
    assert_eq!(fixture.inner.read().role, RaftRole::Candidate);
    assert_eq!(fixture.delayed.committed(), Vec::<RaftPersistentState>::new());

    release.send(()).unwrap();
    first.await;
    assert_eq!(fixture.inner.read().role, RaftRole::Leader);
    assert_eq!(
        second.await,
        RequestVoteResponse {
            term: 8,
            vote_granted: true,
        }
    );
    assert_eq!(fixture.inner.read().role, RaftRole::Follower);
    fixture.assert_restart(latest, vec![held, latest]).await;
}
