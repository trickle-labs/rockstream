//! Control-plane Raft leader election (v0.45.2, M7 slices S1–S3).
//!
//! Implements the protocol verified by `formal/m7_control_plane_ha.fizz`:
//! `RequestVote`-based leader election among a small (3–5 node) group of
//! `ControlNode`s, with the durable/ephemeral split the spec documents
//! (`current_term`/`voted_for` persisted through an `ObjectStore`-backed
//! store — DESIGN.md §3's "control SlateDB" `control: raft/term` /
//! `control: raft/vote` keys, here as one combined JSON object rather than
//! two separate SlateDB keys/a full WAL instance, a deliberate
//! proportionate simplification for a single small durable value; `role`
//! is ephemeral and resets to `Follower` on crash-restart, exactly as
//! `CrashLeader` models in the spec).
//!
//! Unlike the FizzBee model — which serializes elections cluster-wide via
//! `any_other_candidate`/`any_leader_exists` as an explicit stand-in for
//! Raft's real randomized-timeout desynchronization (see the spec's header
//! comments) — this real implementation uses genuine randomized per-node
//! election timeouts (`DEFAULT_ELECTION_TIMEOUT_MIN`/`_MAX`), which is the
//! real mechanism the model's abstraction stands in for.
//!
//! ## Paired assertions (FIZZBEE_TEST_PLAN.md §3.7)
//!
//! | FizzBee invariant | This module |
//! |---|---|
//! | M7-S1 | [`assert_single_control_leader`] |
//! | M7-S2 | [`assert_write_requires_leadership`], backing [`RaftHandle::require_leader`] |
//! | M7-S3 | [`control_leader_epoch`] (derivation); validated by `rockstream_runtime::fence::assert_valid_control_leader_epoch` |

use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use object_store::path::Path;
use object_store::ObjectStore;
use parking_lot::RwLock;
use rand::Rng;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

use rockstream_types::raft::{
    HeartbeatRequest, HeartbeatResponse, RaftNodeId, RaftRpcRequest, RaftRpcResponse,
    RequestVoteRequest, RequestVoteResponse,
};

/// Default randomized election timeout range (mirrors typical real Raft
/// deployments — wide enough relative to `DEFAULT_HEARTBEAT_INTERVAL` that a
/// live leader's heartbeats reliably reset every follower's timer).
pub const DEFAULT_ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(150);
pub const DEFAULT_ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(300);

/// Default leader heartbeat interval — well under the election timeout
/// floor so healthy followers never spuriously time out.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

/// Default bounded timeout for a single outbound peer RPC — see
/// [`RaftConfig::rpc_timeout`]'s doc comment for why this bound exists.
/// Kept comfortably under `DEFAULT_ELECTION_TIMEOUT_MIN` (150ms).
pub const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_millis(75);

/// Raft role — mirrors `node_role` in `formal/m7_control_plane_ha.fizz`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

/// Durable Raft state (`current_term`/`voted_for`) — the exact fields the
/// FizzBee spec documents as durable/persisted through crash-restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct RaftPersistentState {
    pub current_term: u64,
    pub voted_for: Option<RaftNodeId>,
}

/// Durable store for [`RaftPersistentState`], backed by any [`ObjectStore`]
/// (`LocalFileSystem` for the embedded/LFS profile, S3/MinIO for Tier 3) —
/// this is the new durability path introduced by v0.45.2 S1.
pub struct RaftPersistentStore {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl RaftPersistentStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            path: Path::from("control/raft/state.json"),
        }
    }

    /// Load the persisted state, or the zero-value default if nothing has
    /// been persisted yet (first-ever boot of this control node).
    pub async fn load(&self) -> RaftPersistentState {
        match self.store.get(&self.path).await {
            Ok(result) => match result.bytes().await {
                Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
                Err(_) => RaftPersistentState::default(),
            },
            Err(_) => RaftPersistentState::default(),
        }
    }

    /// Persist state. Term/vote MUST be durable before the caller acts on
    /// the mutation it guards (granting a vote, starting an election) — a
    /// silently-dropped write here could let a restarted node forget it
    /// already voted this term, exactly the bug M7-S1 depends on not
    /// happening. Panics (rather than silently swallowing the error) on
    /// failure so that hazard can never pass unnoticed.
    pub async fn save(&self, state: &RaftPersistentState) {
        let bytes = serde_json::to_vec(state).expect("serialize RaftPersistentState");
        self.store
            .put(&self.path, bytes.into())
            .await
            .expect("RS-0003: failed to persist Raft term/vote state");
    }
}

struct RaftInner {
    node_id: RaftNodeId,
    persistent: RaftPersistentState,
    role: RaftRole,
    current_leader: Option<RaftNodeId>,
    last_contact: Instant,
}

/// Handle to a running Raft node's election state. Cheap to clone; safe to
/// share across the `ControlService`'s request-handling tasks.
#[derive(Clone)]
pub struct RaftHandle {
    inner: Arc<RwLock<RaftInner>>,
}

/// Marker error returned by [`RaftHandle::require_leader`] when this node is
/// not currently the elected leader. Carries no payload (the caller already
/// has everything it needs — `RaftHandle::current_leader()` — to build an
/// actionable `RS-1731` message); a dedicated type instead of `()` satisfies
/// `clippy::result_unit_err` and gives call sites a self-documenting name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotLeader;

impl std::fmt::Display for NotLeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RS-1731: control.not_leader — this control node is not the current Raft leader"
        )
    }
}

impl std::error::Error for NotLeader {}

impl RaftHandle {
    pub fn node_id(&self) -> RaftNodeId {
        self.inner.read().node_id
    }

    pub fn role(&self) -> RaftRole {
        self.inner.read().role
    }

    pub fn is_leader(&self) -> bool {
        self.inner.read().role == RaftRole::Leader
    }

    pub fn current_term(&self) -> u64 {
        self.inner.read().persistent.current_term
    }

    pub fn current_leader(&self) -> Option<RaftNodeId> {
        self.inner.read().current_leader
    }

    /// M7-S2 leader-only write gate (`require_leader()` in the plan).
    /// Returns the current term (to stamp the write with) if this node is
    /// the elected leader, or [`NotLeader`] if not — callers must translate
    /// the `Err` into `RS_1731` at the API boundary and MUST NOT proceed
    /// with the write.
    pub fn require_leader(&self) -> Result<u64, NotLeader> {
        let guard = self.inner.read();
        if guard.role == RaftRole::Leader {
            assert_write_requires_leadership(guard.role, true);
            Ok(guard.persistent.current_term)
        } else {
            Err(NotLeader)
        }
    }

    /// M7-S3: the control-leader epoch the shard-fence token must be
    /// derived from, or `None` if this node is not currently leader.
    pub fn leader_epoch(&self) -> Option<u64> {
        let guard = self.inner.read();
        if guard.role == RaftRole::Leader {
            Some(control_leader_epoch(
                guard.persistent.current_term,
                guard.node_id,
            ))
        } else {
            None
        }
    }

    /// Test/ops-only: force this node to step down to `Follower` without
    /// altering its persistent term/vote state. Models `CrashLeader` in
    /// `formal/m7_control_plane_ha.fizz` — ephemeral `node_role` resets on
    /// crash-restart; durable `current_term`/`voted_for` survive.
    pub fn force_step_down_for_test(&self) {
        let mut guard = self.inner.write();
        guard.role = RaftRole::Follower;
        guard.current_leader = None;
    }
}

/// Derive the combined control-leader epoch from `(raft_term,
/// control_leader_id)` (M7-S3's "the shard-fence token is derived from the
/// control-leader epoch"). Packs `term` into the high 48 bits and the node
/// id (assumed < 2^16 — enforced by the assert, always true at the
/// roadmap's 3–5-node control-group scale) into the low 16 bits, so the
/// epoch strictly increases across any term change and two different
/// leaders can never collide on the same epoch value even in the same
/// term (defense in depth; M7-S1 already forbids that at the source).
pub fn control_leader_epoch(term: u64, leader_id: RaftNodeId) -> u64 {
    assert!(
        leader_id < (1 << 16),
        "control node id must fit in 16 bits for leader-epoch packing (got {leader_id})"
    );
    (term << 16) | (leader_id & 0xFFFF)
}

// ─── Paired assertions (FIZZBEE_TEST_PLAN.md §3.7) ───────────────────────────

/// M7-S1 paired assertion: at most one node in `nodes` may be `Leader` for
/// any given term. A single node cannot verify this about its peers by
/// itself (unlike the FizzBee model, which has global visibility) — this is
/// a test/ops-tooling assertion, called with every control node's handle.
pub fn assert_single_control_leader(nodes: &[RaftHandle]) {
    use std::collections::HashMap;
    let mut leaders_per_term: HashMap<u64, Vec<RaftNodeId>> = HashMap::new();
    for n in nodes {
        let guard = n.inner.read();
        if guard.role == RaftRole::Leader {
            leaders_per_term
                .entry(guard.persistent.current_term)
                .or_default()
                .push(guard.node_id);
        }
    }
    for (term, leaders) in &leaders_per_term {
        assert!(
            leaders.len() <= 1,
            "RS-1731: M7-S1 violation — dual leader at term {term}: {leaders:?}"
        );
    }
}

/// M7-S2 paired assertion: a leader-gated write must only be attempted while
/// `role == Leader`. Called from [`RaftHandle::require_leader`]'s `Ok` path
/// as a defensive redundant check (TigerBeetle assertion discipline,
/// DESIGN.md §17.3 — the same style as `rockstream-runtime`'s
/// `assert_valid_writer`): the normal path is for `require_leader()` to
/// return `Err` and the caller to reject the request with `RS_1731` before
/// ever reaching a write, so this should never actually fire.
pub fn assert_write_requires_leadership(role: RaftRole, attempted_write: bool) {
    if attempted_write {
        assert!(
            role == RaftRole::Leader,
            "RS-1731: M7-S2 violation — write attempted while role={role:?}, not Leader"
        );
    }
}

// M7-S3 paired assertion: a shard-fence (or any leader-gated) write's
// captured epoch must never be staler than the current control-leader
// epoch it is being validated against. The canonical implementation lives
// in `rockstream_runtime::fence::assert_valid_control_leader_epoch`
// (alongside M4's `assert_valid_writer`, per the plan) — `control_leader_epoch`
// above is what callers use to *derive* the epoch value that assertion
// validates against.

// ─── Configuration & bootstrap ─────────────────────────────────────────────
/// Configuration for a single control node's Raft participation.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// This node's id.
    pub node_id: RaftNodeId,
    /// All *other* peers in the control group: `(node_id, raft_addr)`.
    pub peers: Vec<(RaftNodeId, String)>,
    /// If `true`, this node starts an election immediately on boot rather
    /// than waiting out a randomized timeout — deterministically biases the
    /// very first election towards this node ("the first node starts with
    /// `--bootstrap`"), while every subsequent election (e.g. after this
    /// node crashes) is decided the normal way, by whichever follower's
    /// randomized timeout elapses first.
    pub bootstrap: bool,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
    /// Bounded timeout for a single outbound peer RPC (connect + write +
    /// read-reply), covering `RequestVote` and `Heartbeat` alike. Without
    /// this bound, an RPC to a peer whose container/process has been
    /// killed (rather than gracefully closed) can block on the OS's own
    /// TCP-connect retry/timeout — tens of seconds on a typical Docker
    /// bridge network — which would silently blow through every §11.5
    /// recovery-time budget (`DESIGN.md` §11.5) by stalling the *survivors'*
    /// own election/heartbeat round while it waits on the dead peer.
    /// Kept well under `election_timeout_min` so a single unreachable peer
    /// can never itself prevent a candidate from concluding its own
    /// election within one timeout window.
    pub rpc_timeout: Duration,
}

impl RaftConfig {
    pub fn new(node_id: RaftNodeId, peers: Vec<(RaftNodeId, String)>, bootstrap: bool) -> Self {
        Self {
            node_id,
            peers,
            bootstrap,
            election_timeout_min: DEFAULT_ELECTION_TIMEOUT_MIN,
            election_timeout_max: DEFAULT_ELECTION_TIMEOUT_MAX,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
        }
    }
}

/// Parse a `--peers` CLI argument of the form `"1@host:port,2@host:port"`
/// into `(node_id, addr)` pairs.
pub fn parse_peers(spec: &str) -> Result<Vec<(RaftNodeId, String)>, String> {
    if spec.trim().is_empty() {
        return Ok(Vec::new());
    }
    spec.split(',')
        .map(|entry| {
            let entry = entry.trim();
            let (id_str, addr) = entry
                .split_once('@')
                .ok_or_else(|| format!("invalid peer spec `{entry}`, expected `id@host:port`"))?;
            let id: RaftNodeId = id_str
                .parse()
                .map_err(|_| format!("invalid peer node id `{id_str}` in `{entry}`"))?;
            Ok((id, addr.to_string()))
        })
        .collect()
}

/// Handle to a spawned Raft node's background tasks (peer-RPC listener +
/// election driver).
pub struct RaftNodeHandleFull {
    pub handle: RaftHandle,
    pub listen_addr: std::net::SocketAddr,
    shutdown_tx: broadcast::Sender<()>,
}

impl RaftNodeHandleFull {
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// Spawn a Raft node: binds a peer-RPC listener at `bind_addr`, loads
/// persisted term/vote state from `object_store`, and starts the election
/// driver loop.
pub async fn spawn_raft_node(
    bind_addr: &str,
    config: RaftConfig,
    object_store: Arc<dyn ObjectStore>,
) -> io::Result<RaftNodeHandleFull> {
    let store = Arc::new(RaftPersistentStore::new(object_store));
    let initial = store.load().await;
    let inner = Arc::new(RwLock::new(RaftInner {
        node_id: config.node_id,
        persistent: initial,
        role: RaftRole::Follower,
        current_leader: None,
        last_contact: Instant::now(),
    }));

    let listener = TcpListener::bind(bind_addr).await?;
    let listen_addr = listener.local_addr()?;
    let (shutdown_tx, _) = broadcast::channel(4);

    tokio::spawn(accept_loop(
        listener,
        inner.clone(),
        store.clone(),
        shutdown_tx.subscribe(),
    ));
    tokio::spawn(driver_loop(
        config,
        inner.clone(),
        store,
        shutdown_tx.subscribe(),
    ));

    Ok(RaftNodeHandleFull {
        handle: RaftHandle { inner },
        listen_addr,
        shutdown_tx,
    })
}

// ─── Peer RPC server ──────────────────────────────────────────────────────────

async fn accept_loop(
    listener: TcpListener,
    inner: Arc<RwLock<RaftInner>>,
    store: Arc<RaftPersistentStore>,
    mut shutdown: broadcast::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            accept_result = listener.accept() => {
                if let Ok((stream, _addr)) = accept_result {
                    let inner = inner.clone();
                    let store = store.clone();
                    tokio::spawn(async move {
                        let _ = handle_peer_connection(stream, inner, store).await;
                    });
                }
            }
        }
    }
}

async fn handle_peer_connection(
    stream: TcpStream,
    inner: Arc<RwLock<RaftInner>>,
    store: Arc<RaftPersistentStore>,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    if line.trim().is_empty() {
        return Ok(());
    }
    let request: RaftRpcRequest = match serde_json::from_str(line.trim()) {
        Ok(r) => r,
        Err(_) => return Ok(()),
    };
    let response = match request {
        RaftRpcRequest::RequestVote(r) => {
            RaftRpcResponse::RequestVote(handle_request_vote(r, &inner, &store).await)
        }
        RaftRpcRequest::Heartbeat(r) => {
            RaftRpcResponse::Heartbeat(handle_heartbeat(r, &inner, &store).await)
        }
    };
    let mut out = serde_json::to_string(&response).expect("serialize RaftRpcResponse");
    out.push('\n');
    let mut stream = reader.into_inner();
    stream.write_all(out.as_bytes()).await?;
    Ok(())
}

async fn handle_request_vote(
    req: RequestVoteRequest,
    inner: &Arc<RwLock<RaftInner>>,
    store: &RaftPersistentStore,
) -> RequestVoteResponse {
    let (grant, response_term, snapshot) = {
        let mut guard = inner.write();
        if req.term > guard.persistent.current_term {
            guard.persistent.current_term = req.term;
            guard.persistent.voted_for = None;
            guard.role = RaftRole::Follower;
        }
        let can_vote = req.term == guard.persistent.current_term
            && (guard.persistent.voted_for.is_none()
                || guard.persistent.voted_for == Some(req.candidate_id));
        if can_vote {
            guard.persistent.voted_for = Some(req.candidate_id);
            guard.last_contact = Instant::now();
        }
        (can_vote, guard.persistent.current_term, guard.persistent)
    };
    store.save(&snapshot).await;
    RequestVoteResponse {
        term: response_term,
        vote_granted: grant,
    }
}

async fn handle_heartbeat(
    req: HeartbeatRequest,
    inner: &Arc<RwLock<RaftInner>>,
    store: &RaftPersistentStore,
) -> HeartbeatResponse {
    let mut to_persist = None;
    let response_term = {
        let mut guard = inner.write();
        if req.term >= guard.persistent.current_term {
            let term_advanced = req.term > guard.persistent.current_term;
            guard.persistent.current_term = req.term;
            if term_advanced {
                guard.persistent.voted_for = None;
                to_persist = Some(guard.persistent);
            }
            guard.role = RaftRole::Follower;
            guard.current_leader = Some(req.leader_id);
            guard.last_contact = Instant::now();
        }
        guard.persistent.current_term
    };
    if let Some(snapshot) = to_persist {
        store.save(&snapshot).await;
    }
    HeartbeatResponse {
        term: response_term,
    }
}

// ─── Peer RPC client ──────────────────────────────────────────────────────────

async fn send_request_vote(
    addr: &str,
    term: u64,
    candidate_id: RaftNodeId,
    rpc_timeout: Duration,
) -> io::Result<RequestVoteResponse> {
    let request = RaftRpcRequest::RequestVote(RequestVoteRequest { term, candidate_id });
    match send_rpc(addr, request, rpc_timeout).await? {
        RaftRpcResponse::RequestVote(r) => Ok(r),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected response variant",
        )),
    }
}

async fn send_heartbeat(
    addr: &str,
    term: u64,
    leader_id: RaftNodeId,
    rpc_timeout: Duration,
) -> io::Result<HeartbeatResponse> {
    let request = RaftRpcRequest::Heartbeat(HeartbeatRequest { term, leader_id });
    match send_rpc(addr, request, rpc_timeout).await? {
        RaftRpcResponse::Heartbeat(r) => Ok(r),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected response variant",
        )),
    }
}

/// Send one Raft peer RPC, bounded end-to-end (connect + write + read-reply)
/// by `rpc_timeout` — see [`RaftConfig::rpc_timeout`]. A timeout is surfaced
/// as an ordinary `io::Error` (`TimedOut`), handled by every caller exactly
/// like any other unreachable-peer error: the RPC is simply dropped from
/// this round's vote/heartbeat tally (`join_all(...).into_iter().flatten()`),
/// never allowed to block the round itself.
async fn send_rpc(
    addr: &str,
    request: RaftRpcRequest,
    rpc_timeout: Duration,
) -> io::Result<RaftRpcResponse> {
    tokio::time::timeout(rpc_timeout, send_rpc_inner(addr, request))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "raft peer RPC timed out"))?
}

async fn send_rpc_inner(addr: &str, request: RaftRpcRequest) -> io::Result<RaftRpcResponse> {
    let mut stream = TcpStream::connect(addr).await?;
    let mut line = serde_json::to_string(&request).expect("serialize RaftRpcRequest");
    line.push('\n');
    stream.write_all(line.as_bytes()).await?;
    let mut reader = BufReader::new(stream);
    let mut resp_line = String::new();
    reader.read_line(&mut resp_line).await?;
    serde_json::from_str(resp_line.trim())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

// ─── Election driver ──────────────────────────────────────────────────────────

async fn driver_loop(
    config: RaftConfig,
    inner: Arc<RwLock<RaftInner>>,
    store: Arc<RaftPersistentStore>,
    mut shutdown: broadcast::Receiver<()>,
) {
    if config.bootstrap {
        start_election(&config, &inner, &store).await;
    }
    loop {
        if shutdown.try_recv().is_ok() {
            break;
        }
        let role = inner.read().role;
        match role {
            RaftRole::Leader => {
                send_heartbeats(&config, &inner, &store).await;
                tokio::select! {
                    _ = tokio::time::sleep(config.heartbeat_interval) => {},
                    _ = shutdown.recv() => break,
                }
            }
            RaftRole::Follower | RaftRole::Candidate => {
                let timeout = {
                    let mut rng = rand::thread_rng();
                    rng.gen_range(config.election_timeout_min..config.election_timeout_max)
                };
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {},
                    _ = shutdown.recv() => break,
                }
                let elapsed = inner.read().last_contact.elapsed();
                if elapsed >= timeout {
                    start_election(&config, &inner, &store).await;
                }
            }
        }
    }
}

async fn start_election(
    config: &RaftConfig,
    inner: &Arc<RwLock<RaftInner>>,
    store: &RaftPersistentStore,
) {
    let (term, node_id, snapshot) = {
        let mut guard = inner.write();
        guard.role = RaftRole::Candidate;
        guard.persistent.current_term += 1;
        guard.persistent.voted_for = Some(config.node_id);
        guard.last_contact = Instant::now();
        (
            guard.persistent.current_term,
            config.node_id,
            guard.persistent,
        )
    };
    store.save(&snapshot).await;

    let mut votes = 1usize; // vote for self
    let majority = config.peers.len().div_ceil(2) + 1;
    let mut highest_term_seen = term;

    let vote_futures = config
        .peers
        .iter()
        .map(|(_, addr)| send_request_vote(addr, term, node_id, config.rpc_timeout));
    for resp in join_all(vote_futures).await.into_iter().flatten() {
        if resp.term > highest_term_seen {
            highest_term_seen = resp.term;
        }
        if resp.vote_granted && resp.term == term {
            votes += 1;
        }
    }

    // Compute the outcome inside a scoped block so the write guard's
    // lifetime unconditionally ends at the closing brace, *before* any
    // `.await` below — required for the containing future to remain `Send`
    // (a lock guard held live across an `.await` point is not `Send`, even
    // if it is logically dropped on every path before reaching that point).
    let step_down_snapshot: Option<RaftPersistentState> = {
        let mut guard = inner.write();
        // Drop this outcome if a concurrent RPC already moved us past the
        // election we started (e.g. a heartbeat/vote-request from a higher
        // term arrived while our votes were in flight).
        if guard.role != RaftRole::Candidate || guard.persistent.current_term != term {
            None
        } else if highest_term_seen > term {
            guard.persistent.current_term = highest_term_seen;
            guard.persistent.voted_for = None;
            guard.role = RaftRole::Follower;
            Some(guard.persistent)
        } else {
            if votes >= majority {
                guard.role = RaftRole::Leader;
                guard.current_leader = Some(node_id);
                // node_role is ephemeral (not persisted) — matches the
                // FizzBee spec's `@state(ephemeral=["node_role", ...])`
                // annotation.
            }
            // Else: remain Candidate; the driver loop's next randomized
            // timeout will retry (mirrors real Raft's
            // candidate-retry-on-timeout behavior).
            None
        }
    };
    if let Some(snapshot) = step_down_snapshot {
        store.save(&snapshot).await;
    }
}

async fn send_heartbeats(
    config: &RaftConfig,
    inner: &Arc<RwLock<RaftInner>>,
    store: &RaftPersistentStore,
) {
    let (term, node_id) = {
        let guard = inner.read();
        (guard.persistent.current_term, config.node_id)
    };
    let futures = config
        .peers
        .iter()
        .map(|(_, addr)| send_heartbeat(addr, term, node_id, config.rpc_timeout));
    let mut highest = term;
    for resp in join_all(futures).await.into_iter().flatten() {
        if resp.term > highest {
            highest = resp.term;
        }
    }
    if highest > term {
        // See `start_election`'s comment: the guard's scope must end at
        // this block's closing brace, before the `.await` below.
        let step_down_snapshot: Option<RaftPersistentState> = {
            let mut guard = inner.write();
            if guard.role == RaftRole::Leader && highest > guard.persistent.current_term {
                guard.persistent.current_term = highest;
                guard.persistent.voted_for = None;
                guard.role = RaftRole::Follower;
                guard.current_leader = None;
                Some(guard.persistent)
            } else {
                None
            }
        };
        if let Some(snapshot) = step_down_snapshot {
            store.save(&snapshot).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn mem_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    #[test]
    fn control_leader_epoch_increases_with_term() {
        let e1 = control_leader_epoch(1, 0);
        let e2 = control_leader_epoch(2, 0);
        assert!(e2 > e1);
    }

    #[test]
    fn control_leader_epoch_differs_by_leader_at_same_term() {
        let e_a = control_leader_epoch(1, 0);
        let e_b = control_leader_epoch(1, 1);
        assert_ne!(e_a, e_b);
    }

    #[test]
    #[should_panic(expected = "control node id must fit in 16 bits")]
    fn control_leader_epoch_rejects_oversized_node_id() {
        control_leader_epoch(1, 1 << 16);
    }

    #[test]
    fn parse_peers_parses_valid_spec() {
        let peers = parse_peers("1@127.0.0.1:9001,2@127.0.0.1:9002").unwrap();
        assert_eq!(
            peers,
            vec![
                (1, "127.0.0.1:9001".to_string()),
                (2, "127.0.0.1:9002".to_string())
            ]
        );
    }

    #[test]
    fn parse_peers_empty_spec_is_empty_list() {
        assert_eq!(parse_peers("").unwrap(), Vec::new());
    }

    #[test]
    fn parse_peers_rejects_malformed_entry() {
        assert!(parse_peers("not-a-valid-entry").is_err());
    }

    #[test]
    #[should_panic(expected = "RS-1731")]
    fn assert_write_requires_leadership_panics_when_not_leader() {
        assert_write_requires_leadership(RaftRole::Follower, true);
    }

    #[test]
    fn assert_write_requires_leadership_passes_when_leader() {
        assert_write_requires_leadership(RaftRole::Leader, true);
    }

    #[tokio::test]
    async fn persistent_store_roundtrips_through_object_store() {
        let store = RaftPersistentStore::new(mem_store());
        let loaded = store.load().await;
        assert_eq!(loaded, RaftPersistentState::default());

        let state = RaftPersistentState {
            current_term: 7,
            voted_for: Some(2),
        };
        store.save(&state).await;
        let reloaded = store.load().await;
        assert_eq!(reloaded, state);
    }

    #[tokio::test]
    async fn single_bootstrap_node_becomes_leader() {
        let config = RaftConfig::new(0, Vec::new(), true);
        let node = spawn_raft_node("127.0.0.1:0", config, mem_store())
            .await
            .unwrap();
        // A 1-node group has a majority of 1 — the bootstrap node should
        // become leader almost immediately.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(node.handle.is_leader());
        assert_eq!(node.handle.current_term(), 1);
        node.shutdown();
    }

    #[tokio::test]
    async fn three_node_group_elects_exactly_one_leader() {
        // Bind all three listeners up front so peer addresses are known
        // before any node starts sending RPCs.
        let l0 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let l2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a0 = l0.local_addr().unwrap();
        let a1 = l1.local_addr().unwrap();
        let a2 = l2.local_addr().unwrap();
        drop(l0);
        drop(l1);
        drop(l2);

        let cfg0 = RaftConfig::new(0, vec![(1, a1.to_string()), (2, a2.to_string())], true);
        let cfg1 = RaftConfig::new(1, vec![(0, a0.to_string()), (2, a2.to_string())], false);
        let cfg2 = RaftConfig::new(2, vec![(0, a0.to_string()), (1, a1.to_string())], false);

        let n0 = spawn_raft_node(&a0.to_string(), cfg0, mem_store())
            .await
            .unwrap();
        let n1 = spawn_raft_node(&a1.to_string(), cfg1, mem_store())
            .await
            .unwrap();
        let n2 = spawn_raft_node(&a2.to_string(), cfg2, mem_store())
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(400)).await;

        let handles = [n0.handle.clone(), n1.handle.clone(), n2.handle.clone()];
        let leader_count = handles.iter().filter(|h| h.is_leader()).count();
        assert_eq!(
            leader_count,
            1,
            "expected exactly one leader, roles={:?}",
            handles.iter().map(|h| h.role()).collect::<Vec<_>>()
        );

        // M7-S1 paired assertion over the real running nodes.
        assert_single_control_leader(&handles);

        n0.shutdown();
        n1.shutdown();
        n2.shutdown();
    }
}
