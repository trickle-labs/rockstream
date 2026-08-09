//! The `rockstream` CLI library.
//!
//! One binary serves every node role via the `--role` flag:
//!
//! - **`gateway`** / **`all`** — starts the PostgreSQL wire gateway on
//!   `--listen` and blocks until SIGTERM / Ctrl-C.  Use `psql -h <host>
//!   -p 5432 -U rockstream` to connect.
//! - **`control`**, **`worker`**, **`frontier`** — run the respective
//!   distributed role (requires `--control=<url>` for worker/frontier).
//!
//! All user/operator-visible failures carry an `RS-XXXX` error code with
//! actionable `next_steps` text (see [`CliError`]).

use rockstream_types::audit::AuditEvent;
use rockstream_types::config::RockstreamConfig;
use rockstream_types::error_code::{ErrorCode, RS_0002, RS_0003};
use rockstream_types::topology::{
    ControlMessage, WorkerCapabilities, WorkerLocation, WorkerMessage,
};
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub mod metrics_server;

/// Node roles recognised by the single binary. v0.1 ships only the embedded
/// `all` profile; the other roles are accepted as valid names so that scripts
/// written against later versions parse, but they run the same embedded node.
pub const KNOWN_ROLES: &[&str] = &["all", "control", "worker", "gateway", "frontier"];

/// The actor recorded for actions taken by the node itself.
const SYSTEM_ACTOR: &str = "system";

/// A CLI error carrying an `RS-XXXX` code and actionable next steps.
#[derive(Debug, Clone)]
pub struct CliError {
    /// The registered `RS-XXXX` error code.
    pub code: ErrorCode,
    /// Human-readable description of what went wrong.
    pub message: String,
    /// Actionable guidance for resolving the error.
    pub next_steps: String,
}

impl CliError {
    /// Construct a new CLI error.
    pub fn new(code: ErrorCode, message: impl Into<String>, next_steps: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            next_steps: next_steps.into(),
        }
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}\n  next steps: {}",
            self.code, self.message, self.next_steps
        )
    }
}

impl std::error::Error for CliError {}

/// Options for `rockstream start`.
#[derive(Debug, Clone)]
pub struct StartOptions {
    /// Local storage directory for node state and artifacts.
    pub storage: PathBuf,
    /// Requested node role.
    pub role: String,
    /// Control service URL.
    pub control: Option<String>,
    /// Authentication mode: "off", "oidc", or "mtls".
    pub auth_mode: String,
    /// Worker locality metadata.
    pub worker_location: WorkerLocation,
    /// Worker exchange/checkpoint capability advertisement.
    pub worker_capabilities: WorkerCapabilities,
    /// Effective Rockstream configuration for this process.
    pub config: RockstreamConfig,
    /// Optional metrics server listen address.
    pub metrics_addr: Option<String>,
    /// PostgreSQL wire gateway listen address.
    ///
    /// For the `gateway` role this defaults to `127.0.0.1:5432`.  When
    /// `None` the gateway is not started (no-op / test mode).
    pub listen_addr: Option<String>,
    /// v0.45.2 M7 (control-plane Raft leader election): the other control
    /// nodes in this node's Raft group, `"id@host:port,id@host:port"`.
    /// When `None` (the default), the `control` role runs exactly as it
    /// did before v0.45.2 — a single embedded control node with no Raft
    /// gating attached, preserving full backward compatibility. When
    /// `Some`, this node joins a real multi-node Raft group and every
    /// leader-gated write (shard lease grants) is rejected with `RS-1731`
    /// unless this node is the current elected leader.
    pub raft_peers: Option<String>,
    /// This node's id within its Raft group. Required when `raft_peers` is
    /// `Some`.
    pub raft_node_id: Option<u64>,
    /// The address this node's Raft peer-RPC listener binds to (distinct
    /// from the worker-facing `ControlService` port). Required when
    /// `raft_peers` is `Some`, since peers must know it ahead of time.
    pub raft_bind: Option<String>,
    /// If `true`, this node starts an election immediately on boot rather
    /// than waiting out a randomized timeout. Exactly one node in a
    /// freshly-bootstrapped group should set this.
    pub raft_bootstrap: bool,
    /// v0.45.2 M7 S4: when `true`, the `control` role blocks on SIGTERM /
    /// Ctrl-C like the `gateway`/`all` roles' live wire server, instead of
    /// running the short embedded no-op sleep. Defaults to `false`,
    /// preserving exact pre-v0.45.2 behavior. Only meaningful for
    /// `--role=control`; required for a real multi-process control-plane
    /// cluster, where every node's process must stay alive to serve peers
    /// and workers.
    pub daemon: bool,
    /// v0.45.2 M7 S4: override the address the `control` role's
    /// worker-facing `ControlService` binds to. Defaults to
    /// `127.0.0.1:8000` when `None` (the pre-v0.45.2 convention).
    pub control_bind: Option<String>,
    /// v0.45.2 M7 S4/S5: directory for state shared across every control
    /// node in this node's Raft group — specifically the shard-lease-
    /// manager snapshot (`ShardPersistentStore`), the one piece of state
    /// DESIGN.md §3's "one writer (leader), many readers" architecture
    /// requires every control node to actually share so a newly-elected
    /// leader (a different real process) can adopt the outgoing leader's
    /// lease state. When `None` (the default), each control node's state
    /// lives under its own private `--storage` directory exactly as before
    /// v0.45.2, and lease continuity across a real process crash/restart on
    /// a *different* node is not available.
    ///
    /// Raft's own `current_term`/`voted_for` persistence is **never** routed
    /// through this shared directory, even when it is set — that state is,
    /// by definition, per-replica-local durable state (each Raft node must
    /// independently remember its own vote to avoid double-voting in a
    /// term), and routing it through one cross-node-shared object-store key
    /// would let concurrent writes from different node processes corrupt
    /// each other's persisted term/vote on restart. It always lives under
    /// this node's own private `--storage`/`raft` directory.
    pub control_shared_storage: Option<PathBuf>,
    /// Root directories for every non-local shard that owns query-time
    /// relations. Each directory must contain the same logical SlateDB shard
    /// path as the local gateway shard (normally `db`). The gateway refreshes
    /// all of them and accepts a query only at a common durable frontier.
    pub query_time_shard_dirs: Vec<PathBuf>,
}

/// The result of a successful `rockstream start` no-op run.
#[derive(Debug, Clone)]
pub struct StartOutcome {
    /// Path to the audit log that was written.
    pub audit_path: PathBuf,
    /// Path to the support bundle that was written.
    pub bundle_path: PathBuf,
    /// Number of audit events emitted.
    pub events_written: usize,
}

/// Minimal system information captured in the support bundle.
#[derive(Debug, Clone, Serialize)]
struct SystemInfo {
    version: String,
    os: String,
    arch: String,
    role: String,
}

/// A minimal hot-path metrics snapshot included in the support bundle. The
/// metrics emitter is wired in from day one; at v0.1 the no-op node reports its
/// run duration and the number of audit events emitted.
#[derive(Debug, Clone, Serialize)]
struct MetricsSnapshot {
    uptime_ms: u64,
    audit_events_emitted: usize,
}

/// The on-disk support bundle.
#[derive(Debug, Clone, Serialize)]
struct SupportBundle {
    generated_at_ms: u64,
    system_info: SystemInfo,
    metrics: MetricsSnapshot,
    audit_events: Vec<AuditEvent>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Validate a requested role at the system boundary.
fn validate_role(role: &str) -> Result<(), CliError> {
    if KNOWN_ROLES.contains(&role) {
        Ok(())
    } else {
        Err(CliError::new(
            RS_0002,
            format!("unknown node role `{role}`"),
            format!("Pass --role with one of: {}.", KNOWN_ROLES.join(", ")),
        ))
    }
}

/// Start the PostgreSQL wire gateway and return the bound local address and a
/// task handle that keeps the server alive.
///
/// The caller must be inside a tokio runtime (`#[tokio::test]` or
/// `rt.block_on`). The gateway serves until the handle is dropped or aborted.
///
/// This is the **standalone** gateway entry point (`--role gateway`, no
/// worker in this process): it opens its own `<opts.storage>/gateway-shard/`
/// directory and `ShardDb`, since there is no worker in this process to
/// share a shard with. `--role all` instead opens a single shared
/// `<opts.storage>/shards/0/` shard (via the embedded worker's normal lease
/// flow) and calls [`start_gateway_with_shard`] directly with it, so no
/// second, unreferenced `gateway-shard/` directory is ever created for that
/// role.
///
/// # Errors
///
/// Returns a [`CliError`] (RS-0003) if the storage cannot be initialised or
/// the listen port is already in use, or RS-0002 for an unparseable address.
pub async fn start_gateway(
    opts: &StartOptions,
) -> Result<(std::net::SocketAddr, tokio::task::JoinHandle<()>), CliError> {
    let gateway_shard_dir = opts.storage.join("gateway-shard");
    std::fs::create_dir_all(&gateway_shard_dir).map_err(|e| {
        CliError::new(
            RS_0003,
            format!("cannot create gateway-shard dir: {e}"),
            "Check storage directory permissions.",
        )
    })?;

    let store = rockstream_storage::build_runtime_object_store(&gateway_shard_dir, "gateway-shard")
        .map_err(|error| {
            CliError::new(
                RS_0003,
                format!("gateway storage init failed: {error}"),
                "Check that the storage path or object-store credentials are valid.",
            )
        })?;

    let shard_db = rockstream_storage::ShardDb::builder("gateway", store.clone())
        .build()
        .await
        .map_err(|e| {
            CliError::new(
                RS_0003,
                format!("gateway ShardDb failed to open: {e}"),
                "Check that the storage directory is accessible.",
            )
        })?;
    let shard_db = Arc::new(shard_db);

    start_gateway_with_shard(opts, shard_db, store, "gateway").await
}

/// Start the PostgreSQL wire gateway against an **already-open** shard
/// (`shard_db`, backed by `store` rooted at whatever directory `shard_path`
/// was opened under). Used by `--role all` to serve the exact same shard the
/// embedded worker's data-plane DAG operates on — one shared `ShardDb`, no
/// second gateway-local shard directory. [`start_gateway`] (standalone
/// `--role gateway`) delegates to this after opening its own shard.
///
/// # Errors
///
/// Returns a [`CliError`] (RS-0003) if the shard cannot be flushed/read or
/// the listen port is already in use, or RS-0002 for an unparseable address.
pub async fn start_gateway_with_shard(
    opts: &StartOptions,
    shard_db: Arc<rockstream_storage::ShardDb>,
    store: Arc<dyn object_store::ObjectStore>,
    shard_path: &str,
) -> Result<(std::net::SocketAddr, tokio::task::JoinHandle<()>), CliError> {
    // Flush to create the initial manifest so ShardReader can open on a fresh node.
    shard_db.flush().await.map_err(|e| {
        CliError::new(
            RS_0003,
            format!("gateway shard initial flush failed: {e}"),
            "",
        )
    })?;

    let reader = rockstream_storage::ShardReader::open(shard_path, store.clone())
        .await
        .map_err(|e| {
            CliError::new(
                RS_0003,
                format!("gateway ShardReader failed to open: {e}"),
                "",
            )
        })?;

    let view_reader: Arc<dyn rockstream_gateway::ViewReader> =
        Arc::new(rockstream_gateway::HotOnlyViewReader {
            shard_reader: Arc::new(reader),
            frontier_epoch: None,
        });

    let catalog = Arc::new(rockstream_gateway::catalog_stubs::CatalogStubs::new());

    let mut topology_readers = vec![rockstream_gateway::QueryTimeShardReaderSpec::new(
        shard_path, store,
    )];
    for shard_dir in &opts.query_time_shard_dirs {
        let shard_store: Arc<dyn object_store::ObjectStore> = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(shard_dir).map_err(|e| {
                CliError::new(
                    RS_0003,
                    format!(
                        "query-time shard storage init failed for {}: {e}",
                        shard_dir.display()
                    ),
                    "Pass --query-time-shard-dir for every readable owning shard directory.",
                )
            })?,
        );
        topology_readers.push(rockstream_gateway::QueryTimeShardReaderSpec::new(
            shard_path,
            shard_store,
        ));
    }
    let topology_provider =
        rockstream_gateway::QueryTimeShardTopologyProvider::new(topology_readers);

    let listen = opts.listen_addr.as_deref().unwrap_or("127.0.0.1:5432");
    let addr: std::net::SocketAddr = listen.parse().map_err(|e| {
        CliError::new(
            RS_0002,
            format!("invalid listen address `{listen}`: {e}"),
            "Pass a valid socket address such as 127.0.0.1:5432.",
        )
    })?;

    let auth_mode_str = opts.auth_mode.trim().to_lowercase();
    tracing::info!("Gateway server starting with auth mode: {}", auth_mode_str);

    let server = match auth_mode_str.as_str() {
        "off" | "" => {
            rockstream_gateway::GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db)
        }
        "scram" => {
            let role_catalog = Arc::new(rockstream_gateway::RoleCatalog::new());
            rockstream_gateway::GatewayServer::with_shard_db_and_scram_auth(
                addr,
                catalog,
                view_reader,
                shard_db,
                role_catalog,
            )
        }
        "md5" => {
            let role_catalog = Arc::new(rockstream_gateway::RoleCatalog::new());
            rockstream_gateway::GatewayServer::with_shard_db_and_md5_auth(
                addr,
                catalog,
                view_reader,
                shard_db,
                role_catalog,
            )
        }
        "oidc" => {
            let secret = b"rockstream-default-jwt-secret";
            rockstream_gateway::GatewayServer::with_shard_db_and_auth(
                addr,
                catalog,
                view_reader,
                shard_db,
                secret,
            )
        }
        "mtls" => rockstream_gateway::GatewayServer::with_shard_db_and_mtls_auth(
            addr,
            catalog,
            view_reader,
            shard_db,
        ),
        _ => {
            return Err(CliError::new(
                RS_0002,
                format!("unknown auth mode `{}`", opts.auth_mode),
                "Pass --auth with one of: off, scram, md5, oidc, mtls.",
            ));
        }
    };

    let server = server.with_query_time_shard_topology_provider(topology_provider);
    if let Some(webhook_listen) = &opts.config.gateway.webhook_listen_addr {
        let webhook_addr = webhook_listen.parse().map_err(|e| {
            CliError::new(
                RS_0002,
                format!("invalid webhook listen address `{webhook_listen}`: {e}"),
                "Pass a valid --webhook-listen address such as 127.0.0.1:8080.",
            )
        })?;
        let (pgwire_addr, _, pgwire_handle, _) = server
            .serve_background_with_webhook(webhook_addr)
            .await
            .map_err(|e| {
                CliError::new(
                    RS_0003,
                    format!("failed to bind gateway or webhook listener: {e}"),
                    "Check that both --listen and --webhook-listen ports are available.",
                )
            })?;
        Ok((pgwire_addr, pgwire_handle))
    } else {
        server.serve_background().await.map_err(|e| {
            CliError::new(
                RS_0003,
                format!("failed to bind gateway on {listen}: {e}"),
                "Check that the port is not already in use.",
            )
        })
    }
}

/// Run `rockstream start`.
///
/// For the `gateway` and `all` roles with a configured listen address this
/// starts the live PostgreSQL wire server and blocks until SIGTERM / Ctrl-C.
/// For all other roles (and for gateway/all without a listen address) it runs
/// the embedded no-op node, writes an audit log and support bundle, and exits.
pub fn run_start(opts: &StartOptions) -> Result<StartOutcome, CliError> {
    let started_ms = now_ms();
    validate_role(&opts.role)?;

    let auth_mode_norm = opts.auth_mode.trim().to_lowercase();
    let valid_auth_modes = ["off", "scram", "md5", "oidc", "mtls", ""];
    if !valid_auth_modes.contains(&auth_mode_norm.as_str()) {
        return Err(CliError::new(
            RS_0002,
            format!("unknown auth mode `{}`", opts.auth_mode),
            "Pass --auth with one of: off, scram, md5, oidc, mtls.",
        ));
    }

    // Worker requires a control URL to register with the control plane.
    // Gateway can run in standalone mode without a control URL.
    if opts.role == "worker" && opts.control.is_none() {
        return Err(CliError::new(
            rockstream_types::error_code::RS_0002,
            "role `worker` requires --control=<url>",
            "Provide the control plane URL via the --control argument.",
        ));
    }

    // `frontier` role also requires a control URL so it can subscribe to shard reports.
    if opts.role == "frontier" && opts.control.is_none() {
        return Err(CliError::new(
            rockstream_types::error_code::RS_0002,
            "role `frontier` requires --control=<url>",
            "Provide the control plane URL via the --control argument.",
        ));
    }

    fs::create_dir_all(&opts.storage).map_err(|e| {
        CliError::new(
            RS_0003,
            format!(
                "could not create storage directory {}: {e}",
                opts.storage.display()
            ),
            "Check that the parent path exists and is writable.",
        )
    })?;

    let audit_path = opts.storage.join("audit.jsonl");
    let audit_log = rockstream_control::audit::FileAuditLog::open(&audit_path).map_err(|e| {
        CliError::new(
            RS_0003,
            format!("could not open audit log: {e}"),
            "Check storage directory permissions.",
        )
    })?;

    // Log baseline startup events
    let _ = audit_log.append(
        &AuditEvent::now(SYSTEM_ACTOR, "server.started", "rockstream")
            .with_detail(format!("role={}", opts.role)),
    );
    let _ = audit_log.append(
        &AuditEvent::now(SYSTEM_ACTOR, "pipeline.created", "noop-pipeline")
            .with_detail("embedded no-op pipeline"),
    );
    let _ = audit_log.append(&AuditEvent::now(
        SYSTEM_ACTOR,
        "pipeline.started",
        "noop-pipeline",
    ));

    // Determine whether to serve a live gateway for this run.
    // A listen address must be explicitly provided; absence means no-op/test mode.
    let serve_gateway =
        opts.listen_addr.is_some() && (opts.role == "gateway" || opts.role == "all");

    // Pre-validate the listen address before starting the runtime.
    if serve_gateway {
        let listen = opts.listen_addr.as_deref().unwrap_or("127.0.0.1:5432");
        listen.parse::<std::net::SocketAddr>().map_err(|e| {
            CliError::new(
                RS_0002,
                format!("invalid --listen address `{listen}`: {e}"),
                "Pass a valid socket address such as 127.0.0.1:5432.",
            )
        })?;
    }

    // Start services in a tokio runtime
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| CliError::new(RS_0003, format!("failed to start tokio runtime: {e}"), ""))?;

    let serve_result: Result<(), CliError> = rt.block_on(async {
        let mut metrics_handle = None;
        if let Some(metrics_addr) = &opts.metrics_addr {
            let mh = metrics_server::start_metrics_server(metrics_addr)
                .await
                .unwrap();
            tracing::info!(metrics_addr = %mh.local_addr, "metrics server started");
            metrics_handle = Some(mh);
        }

        let mut control_handle = None;
        let mut worker_handle = None;
        let mut control_url = opts.control.clone();
        let mut raft_node_guard: Option<rockstream_control::raft::RaftNodeHandleFull> = None;
        // `--role all` opens exactly one shared shard (id 0) via the
        // embedded worker's normal lease flow and hands it to the gateway
        // below, instead of the gateway opening its own `gateway-shard/`
        // directory — see `start_gateway_with_shard`.
        let mut shared_gateway_shard: Option<(
            Arc<rockstream_storage::ShardDb>,
            Arc<dyn object_store::ObjectStore>,
        )> = None;

        if opts.role == "all" {
            let catalog = rockstream_control::TopologyCatalog::new();
            let manager = rockstream_control::ShardManager::new();
            let service = rockstream_control::ControlService::new(catalog)
                .with_shard_manager(manager)
                .with_audit(Arc::new(
                    rockstream_control::audit::FileAuditLog::open(&audit_path).unwrap(),
                ));
            let handle = service.start("127.0.0.1:0").await.unwrap();
            control_url = Some(handle.addr.to_string());
            control_handle = Some(handle);
        } else if opts.role == "control" {
            let catalog = rockstream_control::TopologyCatalog::new();
            let manager = rockstream_control::ShardManager::new();
            let frontier = Arc::new(rockstream_control::FrontierAggregator::new());
            let mut service = rockstream_control::ControlService::new(catalog)
                .with_shard_manager(manager.clone())
                .with_frontier(frontier)
                .with_audit(Arc::new(
                    rockstream_control::audit::FileAuditLog::open(&audit_path).unwrap(),
                ));

            // v0.45.2 M7 S4/S5: state that must be visible to whichever
            // control node in the group is currently elected leader (the
            // shard-lease-manager snapshot) lives under
            // `--control-shared-storage` when provided, so a newly-elected
            // leader running as a *different real process* loads the
            // last-persisted lease state before granting new leases —
            // closing the split-brain gap a purely private, per-node
            // directory would leave open. When omitted, this falls back to
            // the node's own private `--storage` dir, preserving exact
            // pre-v0.45.2 single-node behavior (no cross-process lease
            // continuity, which is fine because there is only ever one
            // process).
            let shared_store_dir = opts
                .control_shared_storage
                .clone()
                .unwrap_or_else(|| opts.storage.join("raft"));
            fs::create_dir_all(&shared_store_dir).map_err(|e| {
                CliError::new(
                    RS_0003,
                    format!("failed to create control shared-storage dir: {e}"),
                    "Check filesystem permissions for --storage/--control-shared-storage.",
                )
            })?;
            let shared_object_store: Arc<dyn object_store::ObjectStore> = Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(&shared_store_dir).map_err(
                    |e| {
                        CliError::new(
                            RS_0003,
                            format!("failed to open control shared-storage dir: {e}"),
                            "Check filesystem permissions for --storage/--control-shared-storage.",
                        )
                    },
                )?,
            );
            service = service.with_shard_store(Arc::new(
                rockstream_control::ShardPersistentStore::new(shared_object_store.clone()),
            ));
            service = service
                .with_topology_store(Arc::new(rockstream_control::TopologyPersistentStore::new(
                    shared_object_store.clone(),
                )))
                .with_migration_store(Arc::new(rockstream_control::MigrationPersistentStore::new(
                    shared_object_store.clone(),
                )))
                .with_auto_drain(true);

            // v0.45.2 M7: join a real multi-node Raft group when
            // `--raft-peers` is provided; otherwise run exactly as before
            // v0.45.2 (no Raft gating attached).
            if let Some(peers_spec) = &opts.raft_peers {
                let peers = rockstream_control::raft::parse_peers(peers_spec).map_err(|e| {
                    CliError::new(
                        RS_0002,
                        format!("invalid --raft-peers: {e}"),
                        "Use the format id@host:port,id@host:port for every OTHER control node in the group.",
                    )
                })?;
                let node_id = opts.raft_node_id.ok_or_else(|| {
                    CliError::new(
                        RS_0002,
                        "control role with --raft-peers requires --raft-node-id",
                        "Pass --raft-node-id=<u64>, this node's id within the group.",
                    )
                })?;
                let raft_bind = opts.raft_bind.as_deref().ok_or_else(|| {
                    CliError::new(
                        RS_0002,
                        "control role with --raft-peers requires --raft-bind",
                        "Pass --raft-bind=<host:port>; every peer's --raft-peers list must reference this address.",
                    )
                })?;
                let raft_config = rockstream_control::raft::RaftConfig::new(
                    node_id,
                    peers,
                    opts.raft_bootstrap,
                );
                // Raft's own term/vote durability is per-node-private —
                // see `StartOptions::control_shared_storage`'s doc comment
                // for why this must never be routed through the
                // cross-node-shared directory even when one is configured.
                let raft_store_dir = opts.storage.join("raft");
                fs::create_dir_all(&raft_store_dir).map_err(|e| {
                    CliError::new(
                        RS_0003,
                        format!("failed to create private raft-state dir: {e}"),
                        "Check filesystem permissions for --storage.",
                    )
                })?;
                let raft_object_store: Arc<dyn object_store::ObjectStore> = Arc::new(
                    object_store::local::LocalFileSystem::new_with_prefix(&raft_store_dir)
                        .map_err(|e| {
                            CliError::new(
                                RS_0003,
                                format!("failed to open private raft-state dir: {e}"),
                                "Check filesystem permissions for --storage.",
                            )
                        })?,
                );
                let raft_node = rockstream_control::raft::spawn_raft_node(
                    raft_bind,
                    raft_config,
                    raft_object_store,
                )
                .await
                .map_err(|e| {
                    CliError::new(
                        RS_0003,
                        format!("failed to start raft peer listener on {raft_bind}: {e}"),
                        "Check that --raft-bind is not already in use.",
                    )
                })?;
                tracing::info!(
                    node_id,
                    addr = %raft_node.listen_addr,
                    "control: raft peer listener started"
                );
                service = service.with_raft(raft_node.handle.clone());
                raft_node_guard = Some(raft_node);
            }

            let control_bind = opts.control_bind.as_deref().unwrap_or("127.0.0.1:8000");
            let handle = service.start(control_bind).await.unwrap();
            control_handle = Some(handle);
        }

        if opts.role == "worker" || opts.role == "all" {
            let url = control_url.as_deref().unwrap_or("127.0.0.1:8000");
            let mut worker = None;
            let mut last_error = None;
            for _ in 0..20 {
                match rockstream_runtime::start_worker_client_with_metadata(
                    1,
                    url,
                    &opts.storage,
                    opts.worker_location.clone(),
                    opts.worker_capabilities,
                )
                .await
                {
                    Ok(connected) => {
                        worker = Some(connected);
                        break;
                    }
                    Err(error) => {
                        last_error = Some(error);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
            let (client, handle) = worker.ok_or_else(|| CliError::new(
                RS_0003,
                format!("worker failed to connect to control at {url}: {}", last_error.expect("connection attempts ran")),
                "Verify the control address and that the control service is healthy before retrying.",
            ))?;

            if opts.role == "all" {
                // Wait for worker registration handshake
                tokio::time::sleep(Duration::from_millis(50)).await;
                // Acquire the real shard-0 lease through the normal
                // control-plane lease flow (no demo/bypass lease): this is
                // the single shard both the worker's data-plane DAG and the
                // gateway's pgwire reads serve from in `--role all`.
                let _ = client
                    .request_shard(rockstream_types::ids::ShardId(0))
                    .await;
                // Poll briefly for the ShardAssigned response to be
                // processed (client.rs opens the ShardDb asynchronously
                // when it arrives).
                let mut shard_db = None;
                for _ in 0..50 {
                    if let Some(db) = client.get_shard_db(rockstream_types::ids::ShardId(0)) {
                        shard_db = Some(db);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                if let Some(db) = shard_db {
                    let shard_path = opts.storage.join("shards").join("0");
                    let store = rockstream_storage::build_runtime_object_store(
                        &shard_path,
                        "shards/0",
                    )
                    .map_err(|error| {
                        CliError::new(
                            RS_0003,
                            format!("failed to open shared shard-0 store: {error}"),
                            "Check storage directory or object-store credentials.",
                        )
                    })?;
                    shared_gateway_shard = Some((Arc::new(db), store));
                }
            }
            worker_handle = Some(handle);
        }

        if opts.role == "frontier" {
            // Start an in-process FrontierAggregator and emit audit events.
            let aggregator = rockstream_control::FrontierAggregator::new();
            // Emit audit event for this control-plane action.
            let event = AuditEvent::now("system", "frontier.aggregator.started", "frontier")
                .with_detail(format!("control={}", opts.control.as_deref().unwrap_or("")));
            let _ = audit_log.append(&event);
            // Fill-level metric snapshot at startup.
            let fill = aggregator.fill_level();
            tracing::info!(
                registered = fill.registered,
                capacity = fill.capacity,
                "frontier aggregator started"
            );
        }

        if serve_gateway {
            // ── Live gateway serve mode ────────────────────────────────────
            // Build the gateway and wait for an OS shutdown signal.
            // `--role all` serves the single shared shard-0 opened above (no
            // second `gateway-shard/` directory); standalone `--role
            // gateway` opens its own shard, since there's no worker sharing
            // this process with it.
            let gateway_result = if let Some((shard_db, store)) = shared_gateway_shard.take() {
                start_gateway_with_shard(opts, shard_db, store, "db").await
            } else {
                start_gateway(opts).await
            };
            match gateway_result {
                Ok((local_addr, gw_handle)) => {
                    let _ = audit_log.append(
                        &AuditEvent::now(SYSTEM_ACTOR, "gateway.started", local_addr.to_string())
                            .with_detail(format!("role={}", opts.role)),
                    );
                    tracing::info!(
                        addr = %local_addr,
                        "PostgreSQL wire gateway ready — connect with: psql -h {} -p {} -U rockstream",
                        local_addr.ip(),
                        local_addr.port(),
                    );

                    // Block until Ctrl-C (SIGINT) or SIGTERM.
                    #[cfg(unix)]
                    {
                        use tokio::signal::unix::{signal, SignalKind};
                        let mut sigterm =
                            signal(SignalKind::terminate()).unwrap_or_else(|_| {
                                // Fallback: if SIGTERM handler fails, just wait for Ctrl-C.
                                panic!("failed to install SIGTERM handler")
                            });
                        tokio::select! {
                            _ = tokio::signal::ctrl_c() => {}
                            _ = sigterm.recv() => {}
                        }
                    }
                    #[cfg(not(unix))]
                    {
                        let _ = tokio::signal::ctrl_c().await;
                    }
                    tracing::info!("shutdown signal received — stopping gateway");
                    gw_handle.abort();

                    let _ = audit_log.append(
                        &AuditEvent::now(SYSTEM_ACTOR, "gateway.stopped", local_addr.to_string()),
                    );
                }
                Err(e) => {
                    // Clean up already-started services before surfacing the error.
                    if let Some(wh) = worker_handle.take() {
                        wh.abort();
                    }
                    if let Some(ch) = control_handle.take() {
                        ch.shutdown();
                    }
                    if let Some(rn) = raft_node_guard.take() {
                        rn.shutdown();
                    }
                    if let Some(mh) = metrics_handle.take() {
                        mh.shutdown();
                    }
                    return Err(e);
                }
            }
        } else {
            // ── No-op / test mode ─────────────────────────────────────────
            // v0.45.2 M7 S4: `--role=control --daemon` blocks on SIGTERM /
            // Ctrl-C exactly like the live gateway server, instead of
            // running the short embedded no-op sleep. This is what lets a
            // real multi-process control-plane cluster stay up long enough
            // for peers/workers to reach it. Every other combination keeps
            // the pre-v0.45.2 short-sleep-then-exit behavior unchanged.
            let daemon_mode = opts.daemon && opts.role == "control";
            if daemon_mode {
                tracing::info!(
                    role = %opts.role,
                    "control node running in daemon mode — blocking until shutdown signal"
                );
                #[cfg(unix)]
                {
                    use tokio::signal::unix::{signal, SignalKind};
                    let mut sigterm = signal(SignalKind::terminate())
                        .unwrap_or_else(|_| panic!("failed to install SIGTERM handler"));
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => {}
                        _ = sigterm.recv() => {}
                    }
                }
                #[cfg(not(unix))]
                {
                    let _ = tokio::signal::ctrl_c().await;
                }
                tracing::info!("shutdown signal received — stopping control daemon");
            } else {
                // Allow live interactions to complete, then exit cleanly.
                let sleep_ms = std::env::var("ROCKSTREAM_E2E_SLEEP_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(50);
                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            }
        }

        if let Some(wh) = worker_handle {
            wh.abort();
        }
        if let Some(ch) = control_handle {
            ch.shutdown();
        }
        if let Some(rn) = raft_node_guard {
            rn.shutdown();
        }
        if let Some(mh) = metrics_handle {
            mh.shutdown();
        }

        Ok(())
    });

    serve_result?;

    let _ = audit_log.append(&AuditEvent::now(
        SYSTEM_ACTOR,
        "pipeline.stopped",
        "noop-pipeline",
    ));
    let _ = audit_log.append(&AuditEvent::now(
        SYSTEM_ACTOR,
        "server.stopped",
        "rockstream",
    ));

    let events = audit_log.read_all().map_err(|e| {
        CliError::new(
            RS_0003,
            format!("could not read audit events: {e}"),
            "Check audit log file readability.",
        )
    })?;

    let bundle_path = write_support_bundle(&opts.storage, &opts.role, started_ms, &events)?;

    Ok(StartOutcome {
        audit_path,
        bundle_path,
        events_written: events.len(),
    })
}

/// Thin v0.46 admin-CLI stub: request a worker drain over the control wire API.
pub fn request_worker_drain(control: &str, worker_id: u64) -> Result<(), CliError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| CliError::new(RS_0003, format!("failed to start tokio runtime: {e}"), ""))?;
    rt.block_on(async move {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(control).await.map_err(|e| {
            CliError::new(
                RS_0003,
                format!("failed to connect to control service at {control}: {e}"),
                "Check that the control node is running and --control points at its worker-facing address.",
            )
        })?;
        let request = serde_json::to_string(&WorkerMessage::RequestDrain {
            worker_id: rockstream_types::ids::WorkerId(worker_id),
        })
        .map_err(|e| CliError::new(RS_0003, format!("failed to encode drain request: {e}"), ""))?
            + "\n";
        stream.write_all(request.as_bytes()).await.map_err(|e| {
            CliError::new(
                RS_0003,
                format!("failed to send drain request: {e}"),
                "Retry the request after verifying network connectivity to the control node.",
            )
        })?;
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        loop {
            line.clear();
            let read = reader.read_line(&mut line).await.map_err(|e| {
                CliError::new(
                    RS_0003,
                    format!("failed reading control response: {e}"),
                    "Retry the request after checking the control-plane logs.",
                )
            })?;
            if read == 0 {
                return Err(CliError::new(
                    RS_0003,
                    "control service closed the drain request without a reply",
                    "Retry against the current control leader and inspect the control-plane audit log.",
                ));
            }
            match serde_json::from_str::<ControlMessage>(line.trim()).map_err(|e| {
                CliError::new(
                    RS_0003,
                    format!("failed to decode control response: {e}"),
                    "Upgrade the CLI and control plane together so they agree on the wire format.",
                )
            })? {
                ControlMessage::BeginDrain(_) => continue,
                ControlMessage::DrainStatus { state, .. } => {
                    println!("{state:?}");
                    return Ok(());
                }
                ControlMessage::OperationFailed {
                    code,
                    message,
                    next_steps,
                } => {
                    return Err(CliError::new(
                        ErrorCode::new(code.trim_start_matches("RS-").parse().unwrap_or(3)),
                        message,
                        next_steps,
                    ));
                }
                other => {
                    return Err(CliError::new(
                        RS_0003,
                        format!("unexpected control response to drain request: {other:?}"),
                        "Retry against the current control leader; if the problem persists, inspect the control-plane logs.",
                    ));
                }
            }
        }
    })
}

fn write_support_bundle(
    storage: &Path,
    role: &str,
    started_ms: u64,
    events: &[AuditEvent],
) -> Result<PathBuf, CliError> {
    let generated_at_ms = now_ms();
    let bundle = SupportBundle {
        generated_at_ms,
        system_info: SystemInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            role: role.to_string(),
        },
        metrics: MetricsSnapshot {
            uptime_ms: generated_at_ms.saturating_sub(started_ms),
            audit_events_emitted: events.len(),
        },
        audit_events: events.to_vec(),
    };

    let bundle_path = storage.join(format!("support-bundle-{generated_at_ms}.json"));
    let json = serde_json::to_string_pretty(&bundle).map_err(|e| {
        CliError::new(
            RS_0003,
            format!("could not serialize support bundle: {e}"),
            "This is an internal error; please report it with the audit log.",
        )
    })?;
    fs::write(&bundle_path, json).map_err(|e| {
        CliError::new(
            RS_0003,
            format!(
                "could not write support bundle {}: {e}",
                bundle_path.display()
            ),
            "Check that the storage directory is writable and the disk is not full.",
        )
    })?;
    Ok(bundle_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_role_is_rejected_with_rs_0002() {
        let err = validate_role("bogus").unwrap_err();
        assert_eq!(
            err.to_string(),
            "RS-0002 unknown node role `bogus`\n  next steps: Pass --role with one of: all, control, worker, gateway, frontier."
        );
    }

    #[test]
    fn known_roles_are_accepted() {
        for role in KNOWN_ROLES {
            assert!(validate_role(role).is_ok());
        }
    }

    #[test]
    fn invalid_auth_mode_is_rejected_with_rs_0002() {
        let dir = tempfile::tempdir().unwrap();
        let opts = StartOptions {
            storage: dir.path().to_path_buf(),
            role: "all".to_string(),
            control: None,
            auth_mode: "invalid".to_string(),
            worker_location: WorkerLocation::default(),
            worker_capabilities: WorkerCapabilities::default(),
            config: RockstreamConfig::default(),
            metrics_addr: None,
            listen_addr: None,
            raft_peers: None,
            raft_node_id: None,
            raft_bind: None,
            raft_bootstrap: false,
            daemon: false,
            control_bind: None,
            control_shared_storage: None,
            query_time_shard_dirs: Vec::new(),
        };
        let err = run_start(&opts).unwrap_err();
        assert_eq!(
            err.to_string(),
            "RS-0002 unknown auth mode `invalid`\n  next steps: Pass --auth with one of: off, scram, md5, oidc, mtls."
        );
    }

    #[test]
    fn invalid_listen_address_is_rejected_with_rs_0002() {
        let dir = tempfile::tempdir().unwrap();
        let opts = StartOptions {
            storage: dir.path().to_path_buf(),
            role: "gateway".to_string(),
            control: None,
            auth_mode: "off".to_string(),
            worker_location: WorkerLocation::default(),
            worker_capabilities: WorkerCapabilities::default(),
            config: RockstreamConfig::default(),
            metrics_addr: None,
            listen_addr: Some("not-an-address".to_string()),
            raft_peers: None,
            raft_node_id: None,
            raft_bind: None,
            raft_bootstrap: false,
            daemon: false,
            control_bind: None,
            control_shared_storage: None,
            query_time_shard_dirs: Vec::new(),
        };
        let err = run_start(&opts).unwrap_err();
        assert_eq!(
            err.to_string(),
            "RS-0002 invalid --listen address `not-an-address`: invalid socket address syntax\n  next steps: Pass a valid socket address such as 127.0.0.1:5432."
        );
    }

    #[test]
    fn worker_drain_closed_connection_returns_exact_rs_0003() {
        use std::io::{BufRead, BufReader};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut request = String::new();
            BufReader::new(stream).read_line(&mut request).unwrap();
            assert_eq!(request, "{\"type\":\"request_drain\",\"worker_id\":7}\n");
        });

        let err = request_worker_drain(&address, 7).unwrap_err();
        server.join().unwrap();
        assert_eq!(
            err.to_string(),
            "RS-0003 control service closed the drain request without a reply\n  next steps: Retry against the current control leader and inspect the control-plane audit log."
        );
    }

    #[test]
    fn worker_drain_operation_failure_preserves_exact_error() {
        use std::io::{BufRead, BufReader, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = String::new();
            BufReader::new(stream.try_clone().unwrap())
                .read_line(&mut request)
                .unwrap();
            assert_eq!(request, "{\"type\":\"request_drain\",\"worker_id\":7}\n");
            stream
                .write_all(b"{\"type\":\"operation_failed\",\"code\":\"RS-3604\",\"message\":\"worker is draining\",\"next_steps\":\"Wait for completion.\"}\n")
                .unwrap();
        });

        let err = request_worker_drain(&address, 7).unwrap_err();
        server.join().unwrap();
        assert_eq!(
            err.to_string(),
            "RS-3604 worker is draining\n  next steps: Wait for completion."
        );
    }

    #[test]
    fn run_start_writes_audit_log_and_support_bundle() {
        let dir = tempfile::tempdir().unwrap();
        let opts = StartOptions {
            storage: dir.path().to_path_buf(),
            role: "all".to_string(),
            control: None,
            auth_mode: "off".to_string(),
            worker_location: WorkerLocation::default(),
            worker_capabilities: WorkerCapabilities::default(),
            config: RockstreamConfig::default(),
            metrics_addr: None,
            listen_addr: None,
            raft_peers: None,
            raft_node_id: None,
            raft_bind: None,
            raft_bootstrap: false,
            daemon: false,
            control_bind: None,
            control_shared_storage: None,
            query_time_shard_dirs: Vec::new(),
        };
        let outcome = run_start(&opts).unwrap();

        assert!(outcome.events_written >= 5);

        let audit = fs::read_to_string(&outcome.audit_path).unwrap();
        for expected in [
            "server.started",
            "pipeline.created",
            "pipeline.started",
            "pipeline.stopped",
            "server.stopped",
            "worker.registered",
            "shard.lease_granted",
        ] {
            assert!(audit.contains(expected), "audit log missing {expected}");
        }
        // Every audit line must be valid JSON.
        for line in audit.lines() {
            let _: AuditEvent = serde_json::from_str(line).unwrap();
        }

        let bundle = fs::read_to_string(&outcome.bundle_path).unwrap();
        assert!(bundle.contains("system_info"));
        assert!(bundle.contains("audit_events"));
        assert!(bundle.contains("metrics"));
    }

    #[test]
    fn run_start_creates_missing_storage_directory() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("a").join("b");
        let opts = StartOptions {
            storage: nested.clone(),
            role: "all".to_string(),
            control: None,
            auth_mode: "off".to_string(),
            worker_location: WorkerLocation::default(),
            worker_capabilities: WorkerCapabilities::default(),
            config: RockstreamConfig::default(),
            metrics_addr: None,
            listen_addr: None,
            raft_peers: None,
            raft_node_id: None,
            raft_bind: None,
            raft_bootstrap: false,
            daemon: false,
            control_bind: None,
            control_shared_storage: None,
            query_time_shard_dirs: Vec::new(),
        };
        run_start(&opts).unwrap();
        assert!(nested.join("audit.jsonl").exists());
    }

    #[test]
    fn worker_role_without_control_fails_with_rs_0002() {
        let dir = tempfile::tempdir().unwrap();
        let opts = StartOptions {
            storage: dir.path().to_path_buf(),
            role: "worker".to_string(),
            control: None,
            auth_mode: "off".to_string(),
            worker_location: WorkerLocation::default(),
            worker_capabilities: WorkerCapabilities::default(),
            config: RockstreamConfig::default(),
            metrics_addr: None,
            listen_addr: None,
            raft_peers: None,
            raft_node_id: None,
            raft_bind: None,
            raft_bootstrap: false,
            daemon: false,
            control_bind: None,
            control_shared_storage: None,
            query_time_shard_dirs: Vec::new(),
        };
        let err = run_start(&opts).unwrap_err();
        assert_eq!(err.code.to_string(), "RS-0002");
    }

    /// gateway role without --control succeeds in standalone mode.
    #[test]
    fn gateway_role_without_control_runs_standalone() {
        let dir = tempfile::tempdir().unwrap();
        // listen_addr: None → no-op mode (no blocking gateway serve)
        let opts = StartOptions {
            storage: dir.path().to_path_buf(),
            role: "gateway".to_string(),
            control: None,
            auth_mode: "off".to_string(),
            worker_location: WorkerLocation::default(),
            worker_capabilities: WorkerCapabilities::default(),
            config: RockstreamConfig::default(),
            metrics_addr: None,
            listen_addr: None,
            raft_peers: None,
            raft_node_id: None,
            raft_bind: None,
            raft_bootstrap: false,
            daemon: false,
            control_bind: None,
            control_shared_storage: None,
            query_time_shard_dirs: Vec::new(),
        };
        // Should succeed: gateway + no listen_addr → no-op path
        let outcome = run_start(&opts).unwrap();
        assert!(outcome.events_written >= 3);
    }

    /// Slice 6: `--role=frontier` without `--control` must fail with RS-0002.
    #[test]
    fn frontier_role_without_control_fails_with_rs_0002() {
        let dir = tempfile::tempdir().unwrap();
        let opts = StartOptions {
            storage: dir.path().to_path_buf(),
            role: "frontier".to_string(),
            control: None,
            auth_mode: "off".to_string(),
            worker_location: WorkerLocation::default(),
            worker_capabilities: WorkerCapabilities::default(),
            config: RockstreamConfig::default(),
            metrics_addr: None,
            listen_addr: None,
            raft_peers: None,
            raft_node_id: None,
            raft_bind: None,
            raft_bootstrap: false,
            daemon: false,
            control_bind: None,
            control_shared_storage: None,
            query_time_shard_dirs: Vec::new(),
        };
        let err = run_start(&opts).unwrap_err();
        assert_eq!(err.code.to_string(), "RS-0002");
        assert!(err.message.contains("frontier"));
    }

    /// Slice 6: `frontier` is a valid (known) role.
    #[test]
    fn frontier_role_is_known() {
        assert!(KNOWN_ROLES.contains(&"frontier"));
        assert!(validate_role("frontier").is_ok());
    }

    // -----------------------------------------------------------------------
    // v0.45.2 M7: control-plane Raft CLI wiring
    // -----------------------------------------------------------------------

    /// `--role=control`, with and without `--raft-peers`, both start
    /// successfully end-to-end through the CLI. Combined into one test
    /// (run sequentially, not in parallel) because the `control` role binds
    /// its control-service listener on the conventional default port 8000
    /// (matching pre-v0.45.2 behavior, unchanged by this phase) — two
    /// separate `#[test]` functions doing a real bind would race for that
    /// port under cargo's default parallel test execution.
    #[test]
    fn control_role_start_end_to_end_with_and_without_raft() {
        // Without `--raft-peers`: runs exactly as before v0.45.2 — no Raft
        // gating attached, shard leases granted immediately.
        {
            let dir = tempfile::tempdir().unwrap();
            let opts = StartOptions {
                storage: dir.path().to_path_buf(),
                role: "control".to_string(),
                control: None,
                auth_mode: "off".to_string(),
                worker_location: WorkerLocation::default(),
                worker_capabilities: WorkerCapabilities::default(),
                config: RockstreamConfig::default(),
                metrics_addr: None,
                listen_addr: None,
                raft_peers: None,
                raft_node_id: None,
                raft_bind: None,
                raft_bootstrap: false,
                daemon: false,
                control_bind: None,
                control_shared_storage: None,
                query_time_shard_dirs: Vec::new(),
            };
            let outcome = run_start(&opts).unwrap();
            assert!(outcome.events_written >= 2);
        }

        // With `--raft-peers` (single-node bootstrap group): becomes its own
        // leader and starts up successfully, creating the raft storage dir.
        {
            let dir = tempfile::tempdir().unwrap();
            let opts = StartOptions {
                storage: dir.path().to_path_buf(),
                role: "control".to_string(),
                control: None,
                auth_mode: "off".to_string(),
                worker_location: WorkerLocation::default(),
                worker_capabilities: WorkerCapabilities::default(),
                config: RockstreamConfig::default(),
                metrics_addr: None,
                listen_addr: None,
                raft_peers: Some(String::new()),
                raft_node_id: Some(0),
                raft_bind: Some("127.0.0.1:0".to_string()),
                raft_bootstrap: true,
                daemon: false,
                control_bind: None,
                control_shared_storage: None,
                query_time_shard_dirs: Vec::new(),
            };
            let outcome = run_start(&opts).unwrap();
            assert!(outcome.events_written >= 2);
            assert!(dir.path().join("raft").exists());
        }
    }

    /// `control` role with `--raft-peers` but no `--raft-node-id` fails with
    /// an actionable `RS-0002`.
    #[test]
    fn control_role_with_raft_peers_requires_node_id() {
        let dir = tempfile::tempdir().unwrap();
        let opts = StartOptions {
            storage: dir.path().to_path_buf(),
            role: "control".to_string(),
            control: None,
            auth_mode: "off".to_string(),
            worker_location: WorkerLocation::default(),
            worker_capabilities: WorkerCapabilities::default(),
            config: RockstreamConfig::default(),
            metrics_addr: None,
            listen_addr: None,
            raft_peers: Some(String::new()),
            raft_node_id: None,
            raft_bind: Some("127.0.0.1:0".to_string()),
            raft_bootstrap: true,
            daemon: false,
            control_bind: None,
            control_shared_storage: None,
            query_time_shard_dirs: Vec::new(),
        };
        let err = run_start(&opts).unwrap_err();
        assert_eq!(err.code.to_string(), "RS-0002");
        assert!(err.message.contains("raft-node-id"));
    }

    /// `control` role with `--raft-peers` but no `--raft-bind` fails with an
    /// actionable `RS-0002`.
    #[test]
    fn control_role_with_raft_peers_requires_raft_bind() {
        let dir = tempfile::tempdir().unwrap();
        let opts = StartOptions {
            storage: dir.path().to_path_buf(),
            role: "control".to_string(),
            control: None,
            auth_mode: "off".to_string(),
            worker_location: WorkerLocation::default(),
            worker_capabilities: WorkerCapabilities::default(),
            config: RockstreamConfig::default(),
            metrics_addr: None,
            listen_addr: None,
            raft_peers: Some(String::new()),
            raft_node_id: Some(0),
            raft_bind: None,
            raft_bootstrap: true,
            daemon: false,
            control_bind: None,
            control_shared_storage: None,
            query_time_shard_dirs: Vec::new(),
        };
        let err = run_start(&opts).unwrap_err();
        assert_eq!(err.code.to_string(), "RS-0002");
        assert!(err.message.contains("raft-bind"));
    }
}
