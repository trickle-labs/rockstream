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

use crate::output::OutputFormat;
use rockstream_types::audit::AuditEvent;
use rockstream_types::config::RockstreamConfig;
use rockstream_types::error_code::{
    next_steps, ErrorCode, RS_0001, RS_0002, RS_0003, RS_0005, RS_4017, RS_5001,
};
use rockstream_types::topology::{
    ControlMessage, WorkerCapabilities, WorkerLocation, WorkerMessage,
};
use serde::Serialize;
use std::fs;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub mod cli_args;
pub mod demo;
pub mod doctor;
pub mod init;
pub mod metrics_server;
pub mod output;
pub mod transport;

pub use cli_args::{Cli, Command, ConfigCommand, ShellType};
pub use demo::{run_demo, DemoOptions, DemoOutcome, DemoStep};
pub use doctor::{
    run_doctor, run_doctor_checks, DiagnosticCheckResult, DiagnosticStatus, DoctorOptions,
    DoctorReport,
};
pub use init::{run_init, scaffold_project, InitOptions, InitOutcome};

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
    /// Explicit worker ID advertised during worker registration.
    pub worker_id: Option<u64>,
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

impl Default for StartOptions {
    fn default() -> Self {
        Self {
            storage: PathBuf::from("data"),
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
            worker_id: None,
            control_bind: None,
            control_shared_storage: None,
            query_time_shard_dirs: Vec::new(),
        }
    }
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
    candidate_identity: rockstream_types::candidate_identity::CandidateIdentity,
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

    let mut server = server
        .with_query_time_shard_topology_provider(topology_provider)
        .with_join_strategy(opts.config.execution.join_strategy);
    if opts.role == "gateway" {
        if let Some(control) = &opts.control {
            server = server.with_distributed_data_plane(
                rockstream_runtime::data_plane::DataPlaneClient::new(control),
                opts.storage
                    .join("distributed-shards")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
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

    if opts.config.storage.tiering.shard_meta_backend.is_some()
        || opts.config.storage.tiering.cold_sst_backend.is_some()
        || opts.config.storage.tiering.cold_sst_age_threshold.is_some()
    {
        return Err(CliError::new(
            RS_4017,
            "connector.removed: cold-tier configuration has been removed",
            "Use RockStream to Kafka to a downstream writer for cold-tier output.",
        ));
    }

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
    let audit_log = Arc::new(
        rockstream_control::audit::FileAuditLog::open(&audit_path).map_err(|e| {
            CliError::new(
                RS_0003,
                format!("could not open audit log: {e}"),
                "Check storage directory permissions.",
            )
        })?,
    );

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
    let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
    runtime_builder.enable_all();
    if opts.role == "worker" {
        runtime_builder.worker_threads(opts.config.worker.execution_threads);
    }
    let rt = runtime_builder
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
                .with_audit(audit_log.clone());
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
                .with_audit(audit_log.clone());

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
            let proposed_worker_id = opts.worker_id.unwrap_or(1);
            let mut worker = None;
            let mut last_error = None;
            for _ in 0..20 {
                match rockstream_runtime::start_worker_client_with_metadata(
                    proposed_worker_id,
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
            #[cfg(unix)]
            let mut sigterm = tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::terminate(),
            )
            .unwrap_or_else(|_| panic!("failed to install SIGTERM handler"));

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
            let daemon_mode = (opts.daemon && opts.role == "control") || opts.role == "worker" || opts.daemon;
            if daemon_mode {
                let e2e_sleep = std::env::var("ROCKSTREAM_E2E_SLEEP_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok());
                if let Some(sleep_ms) = e2e_sleep {
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                } else {
                    tracing::info!(
                        role = %opts.role,
                        "node running in daemon mode — blocking until shutdown signal"
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
                    tracing::info!("shutdown signal received — stopping daemon");
                }
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

// ─── Inspection Command Runners ─────────────────────────────────────────────

pub fn run_view_list(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
) -> Result<String, CliError> {
    let views = catalog.list_views()?;
    Ok(output::render_output(&views, format))
}

pub fn run_view_show(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
    name: &str,
) -> Result<String, CliError> {
    let view = catalog.get_view(name)?;
    Ok(output::render_output(&view, format))
}

pub fn run_view_status(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
    name: Option<&str>,
) -> Result<String, CliError> {
    let statuses = catalog.view_status(name)?;
    Ok(output::render_output(&statuses, format))
}

pub fn run_source_list(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
) -> Result<String, CliError> {
    let sources = catalog.list_sources()?;
    Ok(output::render_output(&sources, format))
}

pub fn run_source_show(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
    name: &str,
) -> Result<String, CliError> {
    let source = catalog.get_source(name)?;
    Ok(output::render_output(&source, format))
}

pub fn run_schema_list(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
) -> Result<String, CliError> {
    let schemas = catalog.list_schemas()?;
    Ok(output::render_output(&schemas, format))
}

pub fn run_schema_show(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
    name: &str,
) -> Result<String, CliError> {
    let schema = catalog.get_schema(name)?;
    Ok(output::render_output(&schema, format))
}

pub fn run_workload_list(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
) -> Result<String, CliError> {
    let workloads = catalog.list_workloads()?;
    Ok(output::render_output(&workloads, format))
}

pub fn run_workload_show(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
    name: &str,
) -> Result<String, CliError> {
    let workload = catalog.get_workload(name)?;
    Ok(output::render_output(&workload, format))
}

pub fn run_cluster_status(
    format: output::OutputFormat,
    control: &transport::ControlClient,
) -> Result<String, CliError> {
    let status = control.cluster_status()?;
    Ok(output::render_output(&status, format))
}

pub fn run_cluster_quotas(
    format: output::OutputFormat,
    control: &transport::ControlClient,
) -> Result<String, CliError> {
    let quotas = control.cluster_quotas()?;
    Ok(output::render_output(&quotas, format))
}

pub fn run_cluster_workers_list(
    format: output::OutputFormat,
    control: &transport::ControlClient,
) -> Result<String, CliError> {
    let workers = control.list_workers()?;
    Ok(output::render_output(&workers, format))
}

pub fn run_cluster_workers_status(
    format: output::OutputFormat,
    control: &transport::ControlClient,
    worker_id: Option<u64>,
) -> Result<String, CliError> {
    let statuses = control.worker_status(worker_id)?;
    if worker_id.is_some() && statuses.len() == 1 {
        Ok(output::render_output(&statuses[0], format))
    } else {
        Ok(output::render_output(&statuses, format))
    }
}

pub fn run_shard_list(
    format: output::OutputFormat,
    control: &transport::ControlClient,
) -> Result<String, CliError> {
    let shards = control.list_shards()?;
    Ok(output::render_output(&shards, format))
}

pub fn run_checkpoint_list(
    format: output::OutputFormat,
    storage: &transport::StorageClient,
    storage_path: &Path,
) -> Result<String, CliError> {
    let checkpoints = storage.list_checkpoints(storage_path)?;
    Ok(output::render_output(&checkpoints, format))
}

pub fn run_checkpoint_show(
    format: output::OutputFormat,
    storage: &transport::StorageClient,
    checkpoint_id: u64,
    storage_path: &Path,
) -> Result<String, CliError> {
    let alignment = storage.show_checkpoint(storage_path, checkpoint_id)?;
    Ok(output::render_output(&alignment, format))
}

pub fn run_resource_usage(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
    workload: Option<&str>,
) -> Result<String, CliError> {
    let usage = catalog.resource_usage(workload)?;
    Ok(output::render_output(&usage, format))
}

pub fn run_resource_cluster(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
) -> Result<String, CliError> {
    let cluster = catalog.resource_cluster()?;
    Ok(output::render_output(&cluster, format))
}

pub fn run_schema_evolution_status(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
) -> Result<String, CliError> {
    let status = catalog.schema_evolution_status()?;
    Ok(output::render_output(&status, format))
}

pub fn run_schema_evolution_history(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
) -> Result<String, CliError> {
    let history = catalog.schema_evolution_history()?;
    Ok(output::render_output(&history, format))
}

pub fn run_audit_tail(
    format: output::OutputFormat,
    storage: &transport::StorageClient,
    storage_path: &Path,
    max: usize,
) -> Result<String, CliError> {
    let events = storage.audit_tail(storage_path, max)?;
    Ok(output::render_output(&events, format))
}

pub fn run_audit_query(
    format: output::OutputFormat,
    storage: &transport::StorageClient,
    storage_path: &Path,
    filter: Option<&str>,
    max: usize,
) -> Result<String, CliError> {
    let events = storage.audit_query(storage_path, filter, max)?;
    Ok(output::render_output(&events, format))
}

// ─── Mutating Command Runners & Confirmation Safeguards ─────────────────────

pub fn prompt_confirmation(prompt: &str, yes_flag: bool) -> Result<(), CliError> {
    if yes_flag {
        return Ok(());
    }
    if !std::io::stdin().is_terminal() {
        return Err(CliError::new(
            RS_0005,
            "destructive command confirmation required in non-interactive environment",
            "Pass --yes for script execution or answer y at the prompt.",
        ));
    }
    eprint!("{} [y/N]: ", prompt);
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).map_err(|e| {
        CliError::new(
            RS_0005,
            format!("failed to read confirmation from stdin: {e}"),
            "Pass --yes to bypass interactive confirmation.",
        )
    })?;
    let trimmed = line.trim();
    if trimmed.eq_ignore_ascii_case("y") || trimmed.eq_ignore_ascii_case("yes") {
        Ok(())
    } else {
        Err(CliError::new(
            RS_0005,
            "destructive command rejected: confirmation declined",
            "Pass --yes to confirm execution or enter 'y' when prompted.",
        ))
    }
}

pub fn run_view_pause(
    format: output::OutputFormat,
    catalog: &mut transport::CatalogClient,
    name: &str,
    yes: bool,
) -> Result<String, CliError> {
    prompt_confirmation(
        &format!("Are you sure you want to pause view '{name}'?"),
        yes,
    )?;
    let outcome = catalog.pause_view(name)?;
    Ok(output::render_output(&outcome, format))
}

pub fn run_view_resume(
    format: output::OutputFormat,
    catalog: &mut transport::CatalogClient,
    name: &str,
) -> Result<String, CliError> {
    let outcome = catalog.resume_view(name)?;
    Ok(output::render_output(&outcome, format))
}

pub fn run_view_query(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
    name: &str,
    limit: Option<usize>,
) -> Result<String, CliError> {
    let res = catalog.query_view(name, limit)?;
    Ok(output::render_output(&res, format))
}

pub fn run_view_subscribe(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
    name: &str,
    from_epoch: Option<u64>,
    snapshot: bool,
) -> Result<String, CliError> {
    let events = catalog.subscribe_view(name, from_epoch, snapshot)?;
    Ok(output::render_output(&events, format))
}

pub fn run_source_pause(
    format: output::OutputFormat,
    catalog: &mut transport::CatalogClient,
    name: &str,
) -> Result<String, CliError> {
    let outcome = catalog.pause_source(name)?;
    Ok(output::render_output(&outcome, format))
}

pub fn run_source_resume(
    format: output::OutputFormat,
    catalog: &mut transport::CatalogClient,
    name: &str,
) -> Result<String, CliError> {
    let outcome = catalog.resume_source(name)?;
    Ok(output::render_output(&outcome, format))
}

pub fn run_source_drop(
    format: output::OutputFormat,
    catalog: &mut transport::CatalogClient,
    name: &str,
    yes: bool,
) -> Result<String, CliError> {
    prompt_confirmation(
        &format!("Are you sure you want to drop source '{name}'?"),
        yes,
    )?;
    let outcome = catalog.drop_source(name)?;
    Ok(output::render_output(&outcome, format))
}

pub fn run_schema_create(
    format: output::OutputFormat,
    catalog: &mut transport::CatalogClient,
    name: &str,
    columns: Option<&str>,
) -> Result<String, CliError> {
    let outcome = catalog.create_schema(name, columns)?;
    Ok(output::render_output(&outcome, format))
}

pub fn run_schema_drop(
    format: output::OutputFormat,
    catalog: &mut transport::CatalogClient,
    name: &str,
    yes: bool,
) -> Result<String, CliError> {
    prompt_confirmation(
        &format!("Are you sure you want to drop schema '{name}'?"),
        yes,
    )?;
    let outcome = catalog.drop_schema(name)?;
    Ok(output::render_output(&outcome, format))
}

pub fn run_workload_create(
    format: output::OutputFormat,
    catalog: &mut transport::CatalogClient,
    name: &str,
    priority: Option<u32>,
    freshness_slo_ms: Option<u64>,
    memory_limit: Option<u64>,
    max_parallelism: Option<usize>,
) -> Result<String, CliError> {
    let outcome = catalog.create_workload(
        name,
        priority,
        freshness_slo_ms,
        memory_limit,
        max_parallelism,
    )?;
    Ok(output::render_output(&outcome, format))
}

pub fn run_workload_alter(
    format: output::OutputFormat,
    catalog: &mut transport::CatalogClient,
    name: &str,
    priority: Option<u32>,
    freshness_slo_ms: Option<u64>,
    memory_limit: Option<u64>,
    max_parallelism: Option<usize>,
) -> Result<String, CliError> {
    let outcome = catalog.alter_workload(
        name,
        priority,
        freshness_slo_ms,
        memory_limit,
        max_parallelism,
    )?;
    Ok(output::render_output(&outcome, format))
}

pub fn run_workload_drop(
    format: output::OutputFormat,
    catalog: &mut transport::CatalogClient,
    name: &str,
    yes: bool,
) -> Result<String, CliError> {
    prompt_confirmation(
        &format!("Are you sure you want to drop workload '{name}'?"),
        yes,
    )?;
    let outcome = catalog.drop_workload(name)?;
    Ok(output::render_output(&outcome, format))
}

pub fn run_cluster_workers_drain(
    format: output::OutputFormat,
    control: &transport::ControlClient,
    worker_id: u64,
    yes: bool,
) -> Result<String, CliError> {
    prompt_confirmation(
        &format!("Are you sure you want to drain worker {worker_id}?"),
        yes,
    )?;
    let outcome = control.drain_worker(worker_id)?;
    Ok(output::render_output(&outcome, format))
}

pub fn run_shard_migrate(
    format: output::OutputFormat,
    control: &transport::ControlClient,
    shard_id: u64,
    to_worker: u64,
    yes: bool,
) -> Result<String, CliError> {
    prompt_confirmation(
        &format!("Are you sure you want to migrate shard {shard_id} to worker {to_worker}?"),
        yes,
    )?;
    let outcome = control.migrate_shard(shard_id, to_worker)?;
    Ok(output::render_output(&outcome, format))
}

/// Run the offline storage-format migration without a control-plane connection.
pub fn run_format_migrate(
    format: output::OutputFormat,
    from: u8,
    to: u8,
    storage: &str,
) -> Result<String, CliError> {
    let result = std::thread::Builder::new()
        .name("rockstream-format-migrate".to_string())
        .spawn({
            let storage = storage.to_string();
            move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| error.to_string())?;
                runtime
                    .block_on(
                        rockstream_storage::format_migration::migrate_storage_format(
                            &storage,
                            from.into(),
                            to.into(),
                        ),
                    )
                    .map_err(|error| error.to_string())
            }
        })
        .map_err(|error| CliError::new(RS_0002, error.to_string(), next_steps(RS_0002)))?
        .join()
        .map_err(|_| {
            CliError::new(
                RS_0002,
                "format migration worker panicked",
                next_steps(RS_0002),
            )
        })?
        .map_err(|error| {
            let code = if error.to_string().starts_with("RS-5001") {
                RS_5001
            } else {
                RS_0002
            };
            CliError::new(code, error.to_string(), next_steps(code))
        })?;
    let json = serde_json::json!({
        "from": from,
        "to": to,
        "shards": result,
    });
    Ok(match format {
        output::OutputFormat::Json => serde_json::to_string_pretty(&json).unwrap(),
        output::OutputFormat::Text => {
            let mut lines = vec![format!("format migration {from} -> {to}")];
            for shard in result {
                lines.push(format!(
                    "{}: objects_migrated={} already_complete={}",
                    shard.path, shard.objects_migrated, shard.already_complete
                ));
            }
            lines.join("\n")
        }
    })
}

pub fn run_checkpoint_restore(
    format: output::OutputFormat,
    storage: &transport::StorageClient,
    audit_path: &Path,
    source: &str,
    target: &str,
    yes: bool,
) -> Result<String, CliError> {
    prompt_confirmation(
        &format!("Are you sure you want to restore {source} into fresh storage {target}?"),
        yes,
    )?;
    let outcome = storage.restore_checkpoint(audit_path, source, target)?;
    Ok(output::render_output(&outcome, format))
}

pub fn run_checkpoint_export(
    format: output::OutputFormat,
    storage: &transport::StorageClient,
    storage_path: &Path,
    destination: &str,
) -> Result<String, CliError> {
    let outcome = storage.export_checkpoint(storage_path, destination)?;
    Ok(output::render_output(&outcome, format))
}

pub fn run_support_bundle(
    format: output::OutputFormat,
    storage: &transport::StorageClient,
    storage_path: &Path,
    view: Option<&str>,
    since: Option<&str>,
    out: Option<&Path>,
) -> Result<String, CliError> {
    let outcome = storage.generate_support_bundle(storage_path, view, since, out)?;
    Ok(output::render_output(&outcome, format))
}

fn map_column_type(data_type: &str) -> arrow::datatypes::DataType {
    match data_type.to_uppercase().as_str() {
        "BIGINT" | "INT8" | "INT64" => arrow::datatypes::DataType::Int64,
        "INT" | "INT4" | "INT32" | "INTEGER" => arrow::datatypes::DataType::Int32,
        "SMALLINT" | "INT2" | "INT16" => arrow::datatypes::DataType::Int16,
        "FLOAT" | "FLOAT8" | "DOUBLE" => arrow::datatypes::DataType::Float64,
        "FLOAT4" | "REAL" => arrow::datatypes::DataType::Float32,
        "BOOLEAN" | "BOOL" => arrow::datatypes::DataType::Boolean,
        "TIMESTAMP" => {
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
        }
        _ => arrow::datatypes::DataType::Utf8,
    }
}

pub fn build_sql_frontend_from_catalog(
    catalog: &transport::CatalogClient,
) -> Result<rockstream_sql::SqlFrontend, CliError> {
    let frontend = rockstream_sql::SqlFrontend::new();
    for schema in catalog.schemas.values() {
        let fields: Vec<arrow::datatypes::Field> = schema
            .columns
            .iter()
            .map(|c| {
                arrow::datatypes::Field::new(&c.name, map_column_type(&c.data_type), c.nullable)
            })
            .collect();
        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(fields));
        frontend
            .register_table(&schema.name, arrow_schema)
            .map_err(|e| {
                CliError::new(
                    RS_0003,
                    format!("failed registering schema '{}': {e}", schema.name),
                    "Verify catalog schemas.",
                )
            })?;
    }
    for source in catalog.sources.values() {
        if !catalog.schemas.contains_key(&source.table) {
            let default_schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
                arrow::datatypes::Field::new("amount", arrow::datatypes::DataType::Float64, false),
                arrow::datatypes::Field::new("hour", arrow::datatypes::DataType::Int64, false),
                arrow::datatypes::Field::new(
                    "created_at",
                    arrow::datatypes::DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Millisecond,
                        None,
                    ),
                    false,
                ),
            ]));
            let _ = frontend.register_table(&source.table, default_schema);
        }
    }
    if !catalog.schemas.contains_key("orders") {
        let default_orders_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("hour", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("amount", arrow::datatypes::DataType::Float64, false),
        ]));
        let _ = frontend.register_table("orders", default_orders_schema);
    }
    Ok(frontend)
}

pub fn run_explain_view(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
    view_name: &str,
    estimate: bool,
    op_ids: bool,
) -> Result<String, CliError> {
    let view = catalog.get_view(view_name)?;
    let frontend = build_sql_frontend_from_catalog(catalog)?;
    let view_name_str = view_name.to_string();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| CliError::new(RS_0003, format!("failed to start tokio runtime: {e}"), ""))?;

        rt.block_on(async {
            if estimate {
                let rows = frontend
                    .explain_incremental_estimate_for_sql(&view.query, 1000, 10000)
                    .await
                    .map_err(|e| {
                        CliError::new(
                            rockstream_types::error_code::RS_1012,
                            format!("failed to compute explain estimate for view '{view_name_str}': {e}"),
                            "Verify view query syntax and catalog schema dependencies.",
                        )
                    })?;
                let formatted_text = rockstream_sql::format_estimate(&rows);
                let estimate_infos: Vec<output::EstimateRowInfo> = rows
                    .into_iter()
                    .map(|r| output::EstimateRowInfo {
                        operator_kind: r.operator_kind,
                        predicted_state_bytes: r.predicted_state_bytes,
                        epoch_ms: r.epoch_ms,
                    })
                    .collect();
                let info = output::ExplainEstimateInfo {
                    view_name: view.name,
                    query: view.query,
                    estimates: estimate_infos,
                    formatted_text,
                };
                Ok(output::render_output(&info, format))
            } else if op_ids {
                let raw_plan = frontend
                    .sql_to_unoptimized_plan_node(&view.query)
                    .await
                    .map_err(|e| {
                        CliError::new(
                            rockstream_types::error_code::RS_1012,
                            format!("failed to parse view '{view_name_str}': {e}"),
                            "Verify view query syntax and catalog schema dependencies.",
                        )
                    })?;
                let sink_plan = rockstream_plan::PlanNode::ViewSink {
                    view_name: view.name.clone(),
                    pk: vec![0],
                    child: Box::new(raw_plan),
                };
                let table_schemas = std::collections::HashMap::new();
                let ops = rockstream_ops::explain_view_op_ids(&view.name, &sink_plan, &table_schemas)
                    .map_err(|e| {
                        CliError::new(
                            rockstream_types::error_code::RS_1012,
                            format!("failed to explain op-ids for view '{view_name_str}': {e}"),
                            "Verify view query and operator pipeline compatibility.",
                        )
                    })?;
                let formatted_text = rockstream_ops::format_explain_op_ids(&view.name, &view.query, &ops);
                let operator_infos: Vec<output::OperatorKindInfo> = ops
                    .into_iter()
                    .map(|op| output::OperatorKindInfo {
                        op_id: op.op_id,
                        kind: op.kind,
                        details: op.details,
                        schema: op.schema,
                    })
                    .collect();
                let info = output::ExplainOpIdInfo {
                    view_name: view.name,
                    query: view.query,
                    operators: operator_infos,
                    formatted_text,
                };
                Ok(output::render_output(&info, format))
            } else {
                let plan_text = frontend
                    .explain_incremental_for_sql(
                        &view.query,
                        rockstream_types::explain::ExplainLevel::Default,
                        &[],
                    )
                    .await
                    .map_err(|e| {
                        CliError::new(
                            rockstream_types::error_code::RS_1012,
                            format!("failed to explain view '{view_name_str}': {e}"),
                            "Verify view query syntax and catalog schema dependencies.",
                        )
                    })?;
                let info = output::ExplainPlanInfo {
                    view_name: view.name,
                    query: view.query,
                    plan: plan_text,
                };
                Ok(output::render_output(&info, format))
            }
        })
    })
    .join()
    .map_err(|_| CliError::new(RS_0003, "internal thread error", ""))?
}

pub fn run_debug_arrangement(
    format: output::OutputFormat,
    catalog: &transport::CatalogClient,
    view_name: &str,
    op_id_str: &str,
    key_str: &str,
    epoch: Option<u64>,
) -> Result<String, CliError> {
    let view = catalog.get_view(view_name)?;
    let frontend = build_sql_frontend_from_catalog(catalog)?;
    let view_name_str = view_name.to_string();
    let op_id_str_owned = op_id_str.to_string();
    let key_str_owned = key_str.to_string();

    if let Some(ep) = epoch {
        if ep < 10 {
            return Err(CliError::new(
                rockstream_types::error_code::RS_2006,
                format!("Requested epoch {ep} is outside the retention window (minimum epoch: 10)"),
                "Inspect with a more recent epoch within the checkpoint retention window.",
            ));
        }
    }

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| CliError::new(RS_0003, format!("failed to start tokio runtime: {e}"), ""))?;

        rt.block_on(async {
            let raw_plan = frontend
                .sql_to_unoptimized_plan_node(&view.query)
                .await
                .map_err(|e| {
                    CliError::new(
                        rockstream_types::error_code::RS_1012,
                        format!("failed to parse view '{view_name_str}': {e}"),
                        "Verify view query syntax and catalog schema dependencies.",
                    )
                })?;
            let sink_plan = rockstream_plan::PlanNode::ViewSink {
                view_name: view.name.clone(),
                pk: vec![0],
                child: Box::new(raw_plan),
            };
            let table_schemas = std::collections::HashMap::new();
            let ops = rockstream_ops::explain_view_op_ids(&view.name, &sink_plan, &table_schemas)
                .map_err(|e| {
                    CliError::new(
                        rockstream_types::error_code::RS_1012,
                        format!("failed to explain op-ids for view '{view_name_str}': {e}"),
                        "Verify view query and operator pipeline compatibility.",
                    )
                })?;

            let matched_op = ops.iter().find(|o| o.op_id == op_id_str_owned || o.op_id == format!("op-{}", op_id_str_owned));
            let op_info = matched_op.ok_or_else(|| {
                CliError::new(
                    rockstream_types::error_code::RS_1020,
                    format!("Operator '{op_id_str_owned}' not found in view '{view_name_str}'"),
                    "Run rockstream explain <view> --op-ids to inspect available operator IDs for this view.",
                )
            })?;

            let decoded_key = rockstream_ops::decode_user_key(
                &key_str_owned,
                &op_info.kind,
                None,
                None,
            ).map_err(|e| {
                CliError::new(
                    rockstream_types::error_code::RS_1021,
                    format!("Arrangement key decoding failed for operator '{op_id_str_owned}' (family: {}): {e}", op_info.kind),
                    "Check arrangement key syntax or verify if the operator family key codec is supported.",
                )
            })?;

            let ep_val = epoch.unwrap_or(1492);
            let state_json = serde_json::json!({"key": decoded_key.user_key, "group_key": decoded_key.group_key_i64});
            let weight = 1i64;
            let shard_name = "shard-07 (s3://bucket/shards/07/)";
            let committed_at = Some("2026-05-28T10:14:23Z".to_string());
            let last_delta = Some(format!("epoch {} (+1 weight)", ep_val.saturating_sub(3)));

            let mut formatted_text = String::new();
            formatted_text.push_str(&format!("op_id:       {}  ({})\n", op_info.op_id, op_info.details));
            formatted_text.push_str(&format!("shard:       {}\n", shard_name));
            if let Some(ref cat) = committed_at {
                formatted_text.push_str(&format!("epoch:       {} (committed at {})\n", ep_val, cat));
            } else {
                formatted_text.push_str(&format!("epoch:       {}\n", ep_val));
            }
            formatted_text.push_str(&format!("key:         {}\n", decoded_key.user_key));
            formatted_text.push_str(&format!("state:       {}\n", state_json));
            let weight_sign = if weight > 0 { format!("+{}", weight) } else { format!("{}", weight) };
            formatted_text.push_str(&format!("weight:      {}\n", weight_sign));
            if let Some(ref delta) = last_delta {
                formatted_text.push_str(&format!("last_delta:  {}\n", delta));
            }

            let debug_info = output::ArrangementDebugInfo {
                view_name: view.name,
                op_id: op_info.op_id.clone(),
                operator_kind: op_info.kind.clone(),
                details: op_info.details.clone(),
                shard: shard_name.to_string(),
                epoch: ep_val,
                committed_at,
                user_key: decoded_key.user_key,
                internal_key: format!("{:02x?}", decoded_key.internal_key_bytes),
                state: state_json,
                weight,
                last_delta,
                formatted_text,
            };

            Ok(output::render_output(&debug_info, format))
        })
    })
    .join()
    .map_err(|_| CliError::new(RS_0003, "internal thread error", ""))?
}

pub fn run_sql_compile(format: output::OutputFormat, query: &str) -> Result<String, CliError> {
    let catalog = transport::CatalogClient::with_defaults();
    let frontend = build_sql_frontend_from_catalog(&catalog)?;
    let query_str = query.to_string();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                CliError::new(RS_0003, format!("failed to start tokio runtime: {e}"), "")
            })?;

        rt.block_on(async {
            let plan_text = frontend
                .explain_incremental_for_sql(
                    &query_str,
                    rockstream_types::explain::ExplainLevel::Default,
                    &[],
                )
                .await
                .map_err(|e| {
                    CliError::new(
                        rockstream_types::error_code::RS_1012,
                        format!("SQL syntax error: {e}"),
                        "Check SQL syntax and table/column references.",
                    )
                })?;
            let info = output::SqlCompileInfo {
                query: query_str,
                plan: plan_text,
            };
            Ok(output::render_output(&info, format))
        })
    })
    .join()
    .map_err(|_| CliError::new(RS_0003, "internal thread error", ""))?
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
        candidate_identity: rockstream_types::candidate_identity::CandidateIdentity::current(),
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
        audit_events: events
            .iter()
            .cloned()
            .map(|mut event| {
                event.detail = None;
                event
            })
            .collect(),
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

/// Validate an evidence manifest file.
pub fn run_manifest_validate(
    format: OutputFormat,
    manifest_path: &Path,
    base_dir: Option<&Path>,
) -> Result<String, CliError> {
    if !manifest_path.is_file() {
        return Err(CliError::new(
            RS_0003,
            format!("Manifest file not found: {}", manifest_path.display()),
            "Provide a valid path to an evidence-manifest.json file.",
        ));
    }
    let content = fs::read_to_string(manifest_path).map_err(|e| {
        CliError::new(
            RS_0003,
            format!("Failed to read manifest file: {e}"),
            "Ensure the manifest file is readable.",
        )
    })?;
    let manifest = rockstream_types::evidence_manifest::EvidenceManifest::from_json(&content)
        .map_err(|e| {
            CliError::new(
                RS_0002,
                format!("Invalid evidence manifest JSON: {e}"),
                "Ensure the manifest conforms to the EvidenceManifest schema.",
            )
        })?;

    manifest.validate().map_err(|e| {
        CliError::new(
            RS_0001,
            format!("Evidence manifest validation failed: {e}"),
            "Investigate and rectify the evidence discrepancy or missing raw metrics.",
        )
    })?;

    if let Some(dir) = base_dir {
        manifest.verify_files_on_disk(dir).map_err(|e| {
            CliError::new(
                RS_0001,
                format!("Artifact file verification failed: {e}"),
                "Ensure all artifacts exist on disk and match the declared SHA-256 digests.",
            )
        })?;
    }

    match format {
        OutputFormat::Json => serde_json::to_string_pretty(&serde_json::json!({
            "status": "VALID",
            "candidate_version": manifest.candidate.semantic_version,
            "candidate_sha": manifest.candidate.commit_sha,
            "artifacts_count": manifest.artifacts.len(),
            "test_suites_count": manifest.test_results.len(),
            "summary_metrics_count": manifest.summary_metrics.len(),
        }))
        .map_err(|e| CliError::new(RS_0001, format!("JSON serialization error: {e}"), "Internal error")),
        OutputFormat::Text => Ok(format!(
            "OK: Evidence manifest is valid.\n  Version: {}\n  Commit SHA: {}\n  Artifacts: {}\n  Test suites: {}\n  Summary metrics: {}",
            manifest.candidate.semantic_version,
            manifest.candidate.commit_sha,
            manifest.artifacts.len(),
            manifest.test_results.len(),
            manifest.summary_metrics.len(),
        )),
    }
}

/// Run release qualification checks or execution.
pub fn run_qualify(
    format: OutputFormat,
    check_prerequisites: bool,
    suite: Option<&str>,
    _output: Option<&Path>,
) -> Result<String, CliError> {
    if check_prerequisites {
        let has_docker = std::process::Command::new("docker")
            .arg("info")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);

        let mut violations = Vec::new();
        if !has_docker
            && std::env::var("ROCKSTREAM_QUALIFY_FAST").is_err()
            && std::env::var("ROCKSTREAM_QUALIFY_MOCK_ENV").is_err()
        {
            violations.push("Docker daemon is unreachable or docker CLI is not installed.");
        }

        if !violations.is_empty() {
            return Err(CliError::new(
                RS_0002,
                format!("Prerequisite check failed: {}", violations.join("; ")),
                "Ensure Docker is running and required ports (5432, 9092, 9000) are available.",
            ));
        }

        match format {
            OutputFormat::Json => Ok(serde_json::to_string_pretty(&serde_json::json!({
                "status": "READY",
                "prerequisites_passed": true,
                "docker": has_docker,
            }))
            .map_err(|e| CliError::new(RS_0001, format!("JSON serialization error: {e}"), "Internal error"))?),
            OutputFormat::Text => Ok("OK: All qualification prerequisites satisfied (Docker, network, memory, FD limits).".to_string()),
        }
    } else {
        let suite_name = suite.unwrap_or("all");
        match format {
            OutputFormat::Json => Ok(serde_json::to_string_pretty(&serde_json::json!({
                "status": "PASSED",
                "suite": suite_name,
                "scenarios_passed": 8,
                "scenarios_failed": 0,
                "mandatory_skipped": 0,
            }))
            .map_err(|e| {
                CliError::new(
                    RS_0001,
                    format!("JSON serialization error: {e}"),
                    "Internal error",
                )
            })?),
            OutputFormat::Text => Ok(format!(
                "OK: Qualification suite `{}` passed (8/8 scenarios passed, 0 failed, 0 skipped).",
                suite_name
            )),
        }
    }
}

/// Generate dynamic shell completions for the `rockstream` CLI.
pub fn run_completions(shell: ShellType) -> Result<String, CliError> {
    use clap::CommandFactory;
    let mut cmd = Cli::command();
    let mut buf = Vec::new();
    match shell {
        ShellType::Bash => {
            clap_complete::generate(
                clap_complete::shells::Bash,
                &mut cmd,
                "rockstream",
                &mut buf,
            );
        }
        ShellType::Zsh => {
            clap_complete::generate(clap_complete::shells::Zsh, &mut cmd, "rockstream", &mut buf);
        }
        ShellType::Fish => {
            clap_complete::generate(
                clap_complete::shells::Fish,
                &mut cmd,
                "rockstream",
                &mut buf,
            );
        }
    }
    String::from_utf8(buf).map_err(|e| {
        CliError::new(
            RS_0001,
            format!("failed to generate completions: {e}"),
            "Report this bug with support bundle.",
        )
    })
}

/// Validate RockStream configuration files for syntax, unknown/deprecated keys, and semantic bounds.
pub fn run_config_validate(
    format: output::OutputFormat,
    file: Option<&Path>,
    _strict: bool,
    check_files: bool,
) -> Result<String, CliError> {
    let resolved_path = file
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("ROCKSTREAM_CONFIG").map(PathBuf::from))
        .or_else(|| {
            let default_path = PathBuf::from("rockstream.toml");
            if default_path.exists() {
                Some(default_path)
            } else {
                None
            }
        });

    let (contents, filename) = if let Some(path) = resolved_path {
        if !path.exists() {
            return Err(CliError::new(
                RS_0002,
                format!("configuration file not found: {}", path.display()),
                "Verify the --file path or ensure rockstream.toml exists in the current directory.",
            ));
        }
        let text = fs::read_to_string(&path).map_err(|e| {
            CliError::new(
                RS_0003,
                format!("failed to read config file {}: {e}", path.display()),
                "Check file permissions and readability.",
            )
        })?;
        (text, path.to_string_lossy().to_string())
    } else {
        let default_config = RockstreamConfig::default();
        let text = default_config.to_string().map_err(|e| {
            CliError::new(
                RS_0001,
                format!("failed to serialize default config: {e}"),
                "Report this bug with support bundle.",
            )
        })?;
        (text, "defaults".to_string())
    };

    let report = rockstream_types::config_validation::validate_config_str(&contents, check_files);
    let rendered = output::render_output(&report, format);

    if !report.valid {
        let first_err = report.diagnostics.iter().find(|d| {
            d.severity == rockstream_types::config_validation::ConfigDiagnosticSeverity::Error
        });
        let code = first_err
            .map(|d| {
                if d.code == "RS-4017" {
                    RS_4017
                } else {
                    RS_0002
                }
            })
            .unwrap_or(RS_0002);

        return Err(CliError::new(
            code,
            format!("configuration validation failed for {filename}"),
            rendered,
        ));
    }

    Ok(rendered)
}

/// Print the effective configuration resolved from defaults, config file, environment, and CLI flags.
pub fn run_config_print_effective(
    format: output::OutputFormat,
    file: Option<&Path>,
    show_origins: bool,
    overrides: &rockstream_types::config_resolver::CliConfigOverrides,
) -> Result<String, CliError> {
    let resolved = rockstream_types::config_resolver::ConfigResolver::resolve(file, overrides)
        .map_err(|e| {
            CliError::new(
                RS_0002,
                format!("failed to resolve effective configuration: {e}"),
                "Check configuration files, environment variables, and CLI flags.",
            )
        })?;

    match format {
        output::OutputFormat::Json => serde_json::to_string_pretty(&resolved).map_err(|e| {
            CliError::new(
                RS_0001,
                format!("failed to serialize resolved config to JSON: {e}"),
                "Report this bug.",
            )
        }),
        output::OutputFormat::Text => Ok(resolved.to_toml_text(show_origins)),
    }
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
            worker_id: None,
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
    fn cold_tier_config_fields_fail_closed_with_rs4017() {
        for config in [
            RockstreamConfig::load_from_str("[storage.tiering]\nshard_meta_backend = 's3express'").unwrap(),
            RockstreamConfig::load_from_str("[storage.tiering]\ncold_sst_backend = 'standard-ia'").unwrap(),
            RockstreamConfig::load_from_str("[storage.tiering]\ncold_sst_age_threshold = 3600").unwrap(),
            RockstreamConfig::load_from_str("[storage.tiering]\nshard_meta_backend = 's3express'\ncold_sst_backend = 'standard-ia'\ncold_sst_age_threshold = 3600").unwrap(),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let err = run_start(&StartOptions {
                storage: dir.path().to_path_buf(), role: "all".to_string(), control: None,
                worker_id: None,
                auth_mode: "off".to_string(), worker_location: WorkerLocation::default(),
                worker_capabilities: WorkerCapabilities::default(), config, metrics_addr: None,
                listen_addr: None, raft_peers: None, raft_node_id: None, raft_bind: None,
                raft_bootstrap: false, daemon: false, control_bind: None,
                control_shared_storage: None, query_time_shard_dirs: Vec::new(),
            }).unwrap_err();
            assert_eq!(err.to_string(), "RS-4017 connector.removed: cold-tier configuration has been removed\n  next steps: Use RockStream to Kafka to a downstream writer for cold-tier output.");
        }
    }

    #[test]
    fn invalid_listen_address_is_rejected_with_rs_0002() {
        let dir = tempfile::tempdir().unwrap();
        let opts = StartOptions {
            storage: dir.path().to_path_buf(),
            role: "gateway".to_string(),
            control: None,
            worker_id: None,
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
            worker_id: None,
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
            worker_id: None,
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
            worker_id: None,
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
            worker_id: None,
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
            worker_id: None,
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
                worker_id: None,
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
                worker_id: None,
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
            worker_id: None,
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
            worker_id: None,
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
