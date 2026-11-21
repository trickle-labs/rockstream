//! Issue #351: stale quorum evidence must never extend a leadership lease.
//! Real Raft socket handlers, response gates and a paused Tokio clock establish
//! that a replacement is elected BEFORE the old leader is probed.

use super::*;
use crate::service::ControlService;
use crate::shard::{ShardManager, ShardManagerSnapshot, ShardPersistentStore};
use crate::topology::TopologyCatalog;
use object_store::memory::InMemory;
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerMessage, WorkerRegistration,
};
use std::net::SocketAddr;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const A: RaftNodeId = 1;
const B: RaftNodeId = 2;
const C: RaftNodeId = 3;
const SHARD: ShardId = ShardId(91);

fn authority(term: u64, vote: RaftNodeId) -> RaftPersistentState {
    RaftPersistentState {
        current_term: term,
        voted_for: Some(vote),
    }
}

struct Node {
    inner: Arc<RwLock<RaftInner>>,
    handle: RaftHandle,
    store: Arc<RaftPersistentStore>,
    backing: Arc<dyn ObjectStore>,
    addr: SocketAddr,
    shutdown: broadcast::Sender<()>,
}

impl Node {
    async fn new(id: RaftNodeId) -> Self {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(RaftPersistentStore::new(backing.clone()));
        let now = Instant::now();
        let config = RaftConfig::new(id, vec![], false);
        let inner = Arc::new(RwLock::new(RaftInner {
            node_id: id,
            persistent: RaftPersistentState::default(),
            role: RaftRole::Follower,
            current_leader: None,
            last_contact: now,
            last_quorum_contact: now,
            leader_quorum_timeout: config.leader_quorum_timeout(),
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown, _) = broadcast::channel(4);
        tokio::spawn(accept_loop(
            listener,
            inner.clone(),
            store.clone(),
            shutdown.subscribe(),
        ));
        Self {
            handle: RaftHandle {
                inner: inner.clone(),
            },
            inner,
            store,
            backing,
            addr,
            shutdown,
        }
    }

    fn stop(&self) {
        let _ = self.shutdown.send(());
    }

    async fn persisted(&self) -> RaftPersistentState {
        RaftPersistentStore::new(self.backing.clone())
            .load()
            .await
            .unwrap()
    }
}

// Avoid Tokio's automatic paused-clock advance while a real loopback socket
// progresses. This also fails rather than silently waiting out an RPC timeout.
async fn completed<T>(task: JoinHandle<T>) -> T {
    for _ in 0..100_000 {
        if task.is_finished() {
            return task.await.unwrap();
        }
        tokio::task::yield_now().await;
    }
    panic!("socket task failed to complete without advancing the Raft clock");
}

async fn signaled<T>(receiver: &mut oneshot::Receiver<T>) -> T {
    for _ in 0..100_000 {
        match receiver.try_recv() {
            Ok(value) => return value,
            Err(oneshot::error::TryRecvError::Empty) => tokio::task::yield_now().await,
            Err(oneshot::error::TryRecvError::Closed) => panic!("socket gate closed early"),
        }
    }
    panic!("socket gate was not reached");
}

async fn refused_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

#[derive(Clone, Copy)]
enum RpcKind {
    Vote,
    Heartbeat,
}

fn assert_request(kind: RpcKind, line: &str) {
    match (kind, serde_json::from_str::<RaftRpcRequest>(line).unwrap()) {
        (RpcKind::Vote, RaftRpcRequest::RequestVote(req)) => assert_eq!(
            req,
            RequestVoteRequest {
                term: 1,
                candidate_id: A,
            }
        ),
        (RpcKind::Heartbeat, RaftRpcRequest::Heartbeat(req)) => assert_eq!(
            req,
            HeartbeatRequest {
                term: 1,
                leader_id: A,
            }
        ),
        (_, other) => panic!("unexpected gated RPC: {other:?}"),
    }
}

// Relay the request to B's REAL production RPC listener and read its response.
// The gate may hold the already-produced response before forwarding it to A.
async fn relay_b(
    listener: TcpListener,
    b_addr: SocketAddr,
    kind: RpcKind,
    responded: oneshot::Sender<()>,
    release: Option<oneshot::Receiver<()>>,
) {
    let (incoming, _) = listener.accept().await.unwrap();
    let mut from_a = BufReader::new(incoming);
    let mut request_line = String::new();
    from_a.read_line(&mut request_line).await.unwrap();
    assert_request(kind, request_line.trim());
    let mut to_b = TcpStream::connect(b_addr).await.unwrap();
    to_b.write_all(request_line.as_bytes()).await.unwrap();
    let mut from_b = BufReader::new(to_b);
    let mut response_line = String::new();
    assert!(from_b.read_line(&mut response_line).await.unwrap() > 0);
    match (
        kind,
        serde_json::from_str::<RaftRpcResponse>(response_line.trim()).unwrap(),
    ) {
        (RpcKind::Vote, RaftRpcResponse::RequestVote(response)) => {
            assert_eq!(
                response,
                RequestVoteResponse {
                    term: 1,
                    vote_granted: true,
                }
            );
        }
        (RpcKind::Heartbeat, RaftRpcResponse::Heartbeat(response)) => {
            assert_eq!(response, HeartbeatResponse { term: 1 });
        }
        (_, other) => panic!("unexpected gated response: {other:?}"),
    }
    responded.send(()).unwrap();
    if let Some(release) = release {
        release.await.unwrap();
    }
    from_a
        .into_inner()
        .write_all(response_line.as_bytes())
        .await
        .unwrap();
}

// Peer C deliberately does NOT process this request. Its TCP connection is
// held open so join_all waits even when B's majority response was prompt.
async fn slow_peer(
    listener: TcpListener,
    kind: RpcKind,
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
) {
    let (incoming, _) = listener.accept().await.unwrap();
    let mut reader = BufReader::new(incoming);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).await.unwrap();
    assert_request(kind, request_line.trim());
    entered.send(()).unwrap();
    release.await.unwrap();
    // Closing without a reply makes the outstanding RPC fail promptly.
}

// Capture every node's role, term, follower contact age, quorum-evidence age,
// authority, write gate and epoch, in one exact transcript (milliseconds are
// virtual and the tests never let the runtime auto-advance the clock).
fn transcript(nodes: &[(&str, &Node)]) -> Vec<String> {
    nodes
        .iter()
        .map(|(name, node)| {
            let (term, role, last_contact, last_quorum) = {
                let state = node.inner.read();
                (
                    state.persistent.current_term,
                    state.role,
                    state.last_contact,
                    state.last_quorum_contact,
                )
            };
            format!(
                "{name}: term={term} role={role:?} contact={}ms quorum={}ms live={} gate={:?} epoch={:?}",
                last_contact.elapsed().as_millis(),
                last_quorum.elapsed().as_millis(),
                node.handle.is_leader(),
                node.handle.require_leader(),
                node.handle.leader_epoch(),
            )
        })
        .collect()
}

async fn send_message(reader: &mut BufReader<TcpStream>, message: &WorkerMessage) {
    let encoded = format!("{}\n", serde_json::to_string(message).unwrap());
    reader
        .get_mut()
        .write_all(encoded.as_bytes())
        .await
        .unwrap();
}

async fn worker_write(
    addr: SocketAddr,
    worker_id: WorkerId,
) -> (BufReader<TcpStream>, ControlMessage) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut reader = BufReader::new(stream);
    send_message(
        &mut reader,
        &WorkerMessage::Register(WorkerRegistration::new(
            worker_id,
            NodeRole::Worker,
            "127.0.0.1:9090",
            CapacityHeadroom::FULL,
        )),
    )
    .await;
    let mut line = String::new();
    assert!(reader.read_line(&mut line).await.unwrap() > 0);
    match serde_json::from_str::<ControlMessage>(line.trim()).unwrap() {
        ControlMessage::Registered { worker_id: id, .. } => assert_eq!(id, worker_id),
        other => panic!("registration was not acknowledged: {other:?}"),
    }
    send_message(
        &mut reader,
        &WorkerMessage::RequestShard {
            worker_id,
            shard_id: SHARD,
        },
    )
    .await;
    for _ in 0..10 {
        line.clear();
        assert!(reader.read_line(&mut line).await.unwrap() > 0);
        let response: ControlMessage = serde_json::from_str(line.trim()).unwrap();
        if !matches!(response, ControlMessage::TopologyChanged { .. }) {
            // Keep the connection open so an accepted B lease is not
            // automatically released on worker disconnect.
            return (reader, response);
        }
    }
    panic!("worker received no shard request result");
}

async fn service(node: &Node) -> crate::service::ControlServiceHandle {
    ControlService::new(TopologyCatalog::new())
        .with_shard_manager(ShardManager::new())
        .with_raft(node.handle.clone())
        .with_shard_store(Arc::new(ShardPersistentStore::new(node.backing.clone())))
        .with_insecure_dev_mode()
        .start("127.0.0.1:0")
        .await
        .unwrap()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Delay {
    QuorumAck,
    UnrelatedPeer,
}

async fn heartbeat_replacement_schedule(delay: Delay) {
    let origin = Instant::now();
    let a = Node::new(A).await;
    let b = Node::new(B).await;
    let c = Node::new(C).await;

    // The first leader wins a REAL vote from both healthy followers.
    let initial_config = RaftConfig::new(
        A,
        vec![(B, b.addr.to_string()), (C, c.addr.to_string())],
        false,
    );
    let a_inner = a.inner.clone();
    let a_store = a.store.clone();
    completed(tokio::spawn(async move {
        start_election(&initial_config, &a_inner, &a_store).await;
    }))
    .await;
    assert_eq!(Instant::now(), origin);
    assert_eq!(a.handle.require_leader(), Ok(1));
    assert_eq!(b.persisted().await, authority(1, A));
    assert_eq!(c.persisted().await, authority(1, A));

    let b_proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b_proxy_addr = b_proxy.local_addr().unwrap();
    let (b_done_tx, mut b_done_rx) = oneshot::channel();
    let (b_release_tx, b_release_rx) = oneshot::channel();
    let mut b_task = Some(tokio::spawn(relay_b(
        b_proxy,
        b.addr,
        RpcKind::Heartbeat,
        b_done_tx,
        (delay == Delay::QuorumAck).then_some(b_release_rx),
    )));
    let (c_addr, c_task, mut c_entered, c_release) = if delay == Delay::UnrelatedPeer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        (
            addr,
            Some(tokio::spawn(slow_peer(
                listener,
                RpcKind::Heartbeat,
                entered_tx,
                release_rx,
            ))),
            Some(entered_rx),
            Some(release_tx),
        )
    } else {
        (refused_addr().await, None, None, None)
    };
    let round_config = RaftConfig::new(
        A,
        vec![(B, b_proxy_addr.to_string()), (C, c_addr.to_string())],
        false,
    );
    let a_inner = a.inner.clone();
    let a_store = a.store.clone();
    let heartbeat = tokio::spawn(async move {
        send_heartbeats(&round_config, &a_inner, &a_store).await;
    });

    // Follower B receives and answers the heartbeat at virtual time 0.
    signaled(&mut b_done_rx).await;
    if let Some(entered) = &mut c_entered {
        signaled(entered).await;
    }
    assert_eq!(Instant::now(), origin);
    assert_eq!(b.inner.read().last_contact, origin);
    assert_eq!(a.inner.read().last_quorum_contact, origin);
    if delay == Delay::UnrelatedPeer {
        // Prove B's response was forwarded at time 0; only C holds join_all.
        completed(b_task.take().unwrap()).await;
        assert_eq!(Instant::now(), origin);
    }
    assert!(!heartbeat.is_finished());

    // Deliver B's OLD ACK at 74ms, or hold its prompt ACK in join_all
    // until C's unrelated connection closes at 74ms.
    tokio::time::advance(Duration::from_millis(74)).await;
    if delay == Delay::QuorumAck {
        b_release_tx.send(()).unwrap();
    } else {
        c_release.unwrap().send(()).unwrap();
    }
    completed(heartbeat).await;
    if let Some(task) = b_task {
        completed(task).await;
    }
    if let Some(task) = c_task {
        completed(task).await;
    }
    assert_eq!(
        Instant::now().duration_since(origin),
        Duration::from_millis(74)
    );
    assert_eq!(a.inner.read().last_quorum_contact, origin);
    assert_eq!(a.handle.require_leader(), Ok(1));

    // The vote RPC to A fails immediately (closed socket), not at the
    // 75ms RPC timeout. C grants a term-2 vote to B via the real handler.
    tokio::time::advance(Duration::from_millis(77)).await;
    assert_eq!(
        b.inner.read().last_contact.elapsed(),
        Duration::from_millis(151)
    );
    let b_config = RaftConfig::new(
        B,
        vec![
            (A, refused_addr().await.to_string()),
            (C, c.addr.to_string()),
        ],
        false,
    );
    let b_inner = b.inner.clone();
    let b_store = b.store.clone();
    completed(tokio::spawn(async move {
        start_election(&b_config, &b_inner, &b_store).await;
    }))
    .await;
    assert_eq!(
        Instant::now().duration_since(origin),
        Duration::from_millis(151)
    );
    assert_eq!(b.handle.require_leader(), Ok(2));
    assert_eq!(c.handle.current_term(), 2);
    // A was never force-demoted: it still BELIEVES its role is Leader.
    assert_eq!(a.handle.role(), RaftRole::Leader);

    tokio::time::advance(Duration::from_millis(19)).await;
    assert_eq!(
        transcript(&[("A", &a), ("B", &b), ("C", &c)]),
        vec![
            "A: term=1 role=Leader contact=170ms quorum=170ms live=false gate=Err(NotLeader) epoch=None",
            "B: term=2 role=Leader contact=19ms quorum=19ms live=true gate=Ok(2) epoch=Some(131074)",
            "C: term=2 role=Follower contact=19ms quorum=170ms live=false gate=Err(NotLeader) epoch=None",
        ]
    );

    // The actual protected shard-write TCP service must deny A and C and
    // allow B. Verify the ENTIRE persisted lease snapshot and Raft votes.
    let svc_a = service(&a).await;
    let svc_b = service(&b).await;
    let svc_c = service(&c).await;
    let (worker_a, reply_a) = completed(tokio::spawn(worker_write(svc_a.addr, WorkerId(11)))).await;
    let (worker_b, reply_b) = completed(tokio::spawn(worker_write(svc_b.addr, WorkerId(12)))).await;
    let (worker_c, reply_c) = completed(tokio::spawn(worker_write(svc_c.addr, WorkerId(13)))).await;
    assert!(matches!(
        reply_a,
        ControlMessage::NotLeader {
            current_leader: Some(A)
        }
    ));
    // Voting in a higher term does not necessarily refresh C's optional
    // cached leader hint until B's first heartbeat, but it must still
    // reject all protected writes while it is only a follower.
    assert!(matches!(reply_c, ControlMessage::NotLeader { .. }));
    let lease = match reply_b {
        ControlMessage::ShardAssigned {
            lease,
            operation_id: None,
        } => lease,
        other => panic!("B's protected write was rejected: {other:?}"),
    };
    assert_eq!(lease.shard_id, SHARD);
    assert_eq!(lease.worker_id, WorkerId(12));

    assert_eq!(a.persisted().await, authority(1, A));
    assert_eq!(b.persisted().await, authority(2, B));
    assert_eq!(c.persisted().await, authority(2, B));
    let old = ShardPersistentStore::new(a.backing.clone()).load().await;
    let replacement = ShardPersistentStore::new(b.backing.clone()).load().await;
    let other = ShardPersistentStore::new(c.backing.clone()).load().await;
    let mut expected = ShardManagerSnapshot::default();
    expected.leases.insert(SHARD, lease);
    expected.next_token = 2;
    expected.leader_epoch = control_leader_epoch(2, B);
    assert_eq!(
        (old, replacement, other),
        (
            ShardManagerSnapshot::default(),
            expected,
            ShardManagerSnapshot::default()
        )
    );
    assert_eq!(
        Instant::now().duration_since(origin),
        Duration::from_millis(170)
    );

    drop((worker_a, worker_b, worker_c));
    svc_a.shutdown();
    svc_b.shutdown();
    svc_c.shutdown();
    a.stop();
    b.stop();
    c.stop();
}

#[tokio::test(start_paused = true)]
async fn delayed_quorum_ack_cannot_extend_lease_past_replacement_election() {
    heartbeat_replacement_schedule(Delay::QuorumAck).await;
}

#[tokio::test(start_paused = true)]
async fn prompt_quorum_ack_cannot_be_aged_by_unrelated_slow_peer() {
    heartbeat_replacement_schedule(Delay::UnrelatedPeer).await;
}

#[tokio::test(start_paused = true)]
async fn election_promotion_uses_request_age_not_join_completion_time() {
    let origin = Instant::now();
    let a = Node::new(A).await;
    let b = Node::new(B).await;
    let c = Node::new(C).await;

    let b_proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b_addr = b_proxy.local_addr().unwrap();
    let (b_done_tx, mut b_done_rx) = oneshot::channel();
    let relay = tokio::spawn(relay_b(b_proxy, b.addr, RpcKind::Vote, b_done_tx, None));
    let c_slow = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c_addr = c_slow.local_addr().unwrap();
    let (entered_tx, mut entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let held = tokio::spawn(slow_peer(c_slow, RpcKind::Vote, entered_tx, release_rx));
    let config = RaftConfig::new(
        A,
        vec![(B, b_addr.to_string()), (C, c_addr.to_string())],
        false,
    );
    let a_inner = a.inner.clone();
    let a_store = a.store.clone();
    let election = tokio::spawn(async move {
        start_election(&config, &a_inner, &a_store).await;
    });
    signaled(&mut b_done_rx).await;
    signaled(&mut entered_rx).await;
    // The majority response is actually delivered before the unrelated C
    // connection is released, so only join_all can age the observation.
    completed(relay).await;
    assert_eq!(Instant::now(), origin);
    assert_eq!(b.inner.read().last_contact, origin);
    assert_eq!(a.handle.role(), RaftRole::Candidate);

    tokio::time::advance(Duration::from_millis(74)).await;
    release_tx.send(()).unwrap();
    completed(election).await;
    completed(held).await;
    assert_eq!(a.handle.require_leader(), Ok(1));
    assert_eq!(a.inner.read().last_quorum_contact, origin);

    tokio::time::advance(Duration::from_millis(77)).await;
    let config = RaftConfig::new(
        B,
        vec![
            (A, refused_addr().await.to_string()),
            (C, c.addr.to_string()),
        ],
        false,
    );
    let b_inner = b.inner.clone();
    let b_store = b.store.clone();
    completed(tokio::spawn(async move {
        start_election(&config, &b_inner, &b_store).await;
    }))
    .await;
    assert_eq!(
        Instant::now().duration_since(origin),
        Duration::from_millis(151)
    );
    assert_eq!(b.handle.require_leader(), Ok(2));

    tokio::time::advance(Duration::from_millis(19)).await;
    assert_eq!(
        transcript(&[("A", &a), ("B", &b), ("C", &c)]),
        vec![
            "A: term=1 role=Leader contact=170ms quorum=170ms live=false gate=Err(NotLeader) epoch=None",
            "B: term=2 role=Leader contact=19ms quorum=19ms live=true gate=Ok(2) epoch=Some(131074)",
            "C: term=2 role=Follower contact=19ms quorum=170ms live=false gate=Err(NotLeader) epoch=None",
        ]
    );
    assert_eq!(a.persisted().await, authority(1, A));
    assert_eq!(b.persisted().await, authority(2, B));
    assert_eq!(c.persisted().await, authority(2, B));
    a.stop();
    b.stop();
    c.stop();
}

#[test]
fn reject_timing_configs_that_could_outlive_follower_election() {
    let good = RaftConfig::new(A, vec![], false);
    good.validate().unwrap();
    assert_eq!(good.leader_quorum_timeout(), Duration::from_millis(125));

    let mut min_equals_max = good.clone();
    min_equals_max.election_timeout_max = min_equals_max.election_timeout_min;
    let mut heartbeat_too_slow = good.clone();
    heartbeat_too_slow.heartbeat_interval = heartbeat_too_slow.election_timeout_min;
    let mut rpc_too_slow = good.clone();
    rpc_too_slow.rpc_timeout = rpc_too_slow.election_timeout_min;
    let mut heartbeat_outlives_lease = good.clone();
    heartbeat_outlives_lease.heartbeat_interval = Duration::from_millis(100);
    let mut rpc_outlives_lease = good.clone();
    rpc_outlives_lease.rpc_timeout = Duration::from_millis(126);
    let mut zero_rpc = good.clone();
    zero_rpc.rpc_timeout = Duration::ZERO;
    let mut zero_heartbeat = good.clone();
    zero_heartbeat.heartbeat_interval = Duration::ZERO;
    for bad in [
        min_equals_max,
        heartbeat_too_slow,
        rpc_too_slow,
        heartbeat_outlives_lease,
        rpc_outlives_lease,
        zero_rpc,
        zero_heartbeat,
    ] {
        let err = bad.validate().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().starts_with("RS-1731:"));
    }

    let mut nanosecond = good;
    nanosecond.election_timeout_min = Duration::from_nanos(3);
    nanosecond.election_timeout_max = Duration::from_nanos(4);
    nanosecond.heartbeat_interval = Duration::from_nanos(1);
    nanosecond.rpc_timeout = Duration::from_nanos(1);
    nanosecond.validate().unwrap();
    assert_eq!(nanosecond.leader_quorum_timeout(), Duration::from_nanos(2));
}

#[tokio::test(start_paused = true)]
async fn expired_leader_cannot_revive_authority_with_another_round() {
    let node = Node::new(A).await;
    {
        let mut guard = node.inner.write();
        guard.role = RaftRole::Leader;
        guard.current_leader = Some(A);
        guard.persistent = authority(1, A);
        guard.last_quorum_contact = Instant::now() - Duration::from_millis(126);
    }
    let config = RaftConfig::new(A, vec![], false);
    send_heartbeats(&config, &node.inner, &node.store).await;
    assert_eq!(node.handle.role(), RaftRole::Follower);
    assert_eq!(node.handle.current_leader(), None);
    assert_eq!(node.handle.require_leader(), Err(NotLeader));
    assert_eq!(node.handle.leader_epoch(), None);
    node.stop();
}

#[tokio::test(start_paused = true)]
async fn healthy_quorum_survives_repeated_unrelated_peer_rpc_timeouts() {
    let a = Node::new(A).await;
    let b = Node::new(B).await;
    let c = Node::new(C).await;
    let initial = RaftConfig::new(
        A,
        vec![(B, b.addr.to_string()), (C, c.addr.to_string())],
        false,
    );
    let inner = a.inner.clone();
    let store = a.store.clone();
    completed(tokio::spawn(async move {
        start_election(&initial, &inner, &store).await;
    }))
    .await;
    assert_eq!(a.handle.require_leader(), Ok(1));

    // C's proxy reads but never ACKs. The real peer-RPC timeout is 75ms,
    // longer than the 50ms heartbeat interval, so join_all always waits.
    let blackhole = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = blackhole.local_addr().unwrap();
    let (first_tx, mut first_rx) = oneshot::channel();
    let slow = tokio::spawn(async move {
        let mut first_tx = Some(first_tx);
        loop {
            let (stream, _) = blackhole.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            assert!(reader.read_line(&mut line).await.unwrap() > 0);
            assert_request(RpcKind::Heartbeat, line.trim());
            if let Some(first) = first_tx.take() {
                first.send(()).unwrap();
            }
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(76)).await;
                drop(reader);
            });
        }
    });

    let config = RaftConfig::new(
        A,
        vec![(B, b.addr.to_string()), (C, addr.to_string())],
        false,
    );
    config.validate().unwrap();
    let (shutdown, _) = broadcast::channel(4);
    let driver = tokio::spawn(driver_loop(
        config,
        a.inner.clone(),
        a.store.clone(),
        shutdown.subscribe(),
    ));
    signaled(&mut first_rx).await;
    // Prove B got the first real heartbeat, rather than relying on elapsed
    // wall-clock time or scheduling luck.
    for _ in 0..100_000 {
        if b.handle.current_leader() == Some(A) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(b.handle.current_leader(), Some(A));

    for step in 1..=21 {
        tokio::time::advance(Duration::from_millis(20)).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        // A fast majority ACK must refresh the safely anchored lease BEFORE
        // the slow minority RPC finishes, with no temporary admission gap.
        assert_eq!(
            a.handle.require_leader(),
            Ok(1),
            "healthy quorum lost write authority at {}ms",
            step * 20
        );
        assert!(
            a.inner.read().last_quorum_contact.elapsed() < Duration::from_millis(125),
            "healthy quorum evidence aged out at {}ms",
            step * 20
        );
    }
    // Reaching 420ms continuously live also confirms the driver starts each
    // round relative to its previous start, not after an extra 50ms sleep.
    assert_eq!(a.handle.current_term(), 1);
    assert_eq!(a.handle.require_leader(), Ok(1));
    assert!(a.inner.read().last_quorum_contact.elapsed() < Duration::from_millis(125));
    assert!(b.inner.read().last_contact.elapsed() < Duration::from_millis(150));
    assert_eq!(b.handle.current_term(), 1);
    assert_eq!(c.handle.current_term(), 1);

    driver.abort();
    slow.abort();
    let _ = shutdown.send(());
    a.stop();
    b.stop();
    c.stop();
}
