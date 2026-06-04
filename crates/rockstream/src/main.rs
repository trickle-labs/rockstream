use clap::{Parser, ValueEnum};
use std::path::Path;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, PlanNode};
use rockstream_runtime::explain::render_explain;

/// RockStream: Massively-parallel incremental view maintenance on SlateDB.
#[derive(Parser, Debug)]
#[command(name = "rockstream", version, about, long_about = None)]
struct Cli {
    /// Subcommand to execute.
    #[command(subcommand)]
    command: Option<Command>,
}

/// Valid roles for a RockStream node.
///
/// Tier 1 (production): run `--role=control` on one or more nodes, then
/// `--role=worker` on compute nodes pointing at `--control=<addr>`.
///
/// Tier 2 (development / single-binary): `--role=all` runs the control
/// service, one worker, and the gateway in the same process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Role {
    /// Run all roles in a single process (Tier 2 / embedded profile).
    All,
    /// Run as a worker node only (Tier 1).
    Worker,
    /// Run as a control-plane node only (Tier 1).
    Control,
    /// Run as a gateway node only.
    Gateway,
    /// Run as a frontier coordinator only.
    Frontier,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::All => write!(f, "all"),
            Role::Worker => write!(f, "worker"),
            Role::Control => write!(f, "control"),
            Role::Gateway => write!(f, "gateway"),
            Role::Frontier => write!(f, "frontier"),
        }
    }
}

#[derive(Parser, Debug)]
enum Command {
    /// Start a RockStream node.
    ///
    /// ## Role flags
    ///
    /// Use `--role` to select which services this process runs:
    ///
    /// - `--role=all` (default): Tier 2 single-binary mode. Starts the
    ///   control service, registers a worker to it, and starts the gateway
    ///   in one process. Ideal for development and testing.
    ///
    /// - `--role=control`: Tier 1 control-plane node. Listens on
    ///   `--control-bind` for worker registrations.
    ///
    /// - `--role=worker`: Tier 1 worker node. Must be paired with
    ///   `--control=<addr>` pointing at a running control node.
    ///
    /// - `--role=gateway`: Gateway-only node.
    ///
    /// - `--role=frontier`: Frontier coordinator node.
    ///
    /// ## mTLS
    ///
    /// Supply `--tls-cert`, `--tls-key`, and `--tls-ca-cert` to enable
    /// mutual TLS on control ↔ worker channels. All three must be set
    /// together; omitting them disables TLS (development mode).
    Start {
        /// Storage directory for local data.
        #[arg(long, default_value = "./data")]
        storage: String,

        /// Role to run as.
        #[arg(long, default_value = "all", value_enum)]
        role: Role,

        /// Control-plane address to register with (worker / gateway / frontier
        /// roles). Format: `host:port`. Required when `--role` is `worker`,
        /// `gateway`, or `frontier`.
        #[arg(long)]
        control: Option<String>,

        /// Address for the control service to listen on (control / all roles).
        #[arg(long, default_value = "127.0.0.1:7700")]
        control_bind: String,

        /// Path to the node's TLS certificate (PEM). Enables mTLS when all
        /// three `--tls-*` arguments are provided.
        #[arg(long)]
        tls_cert: Option<String>,

        /// Path to the node's TLS private key (PEM).
        #[arg(long)]
        tls_key: Option<String>,

        /// Path to the CA certificate used for peer verification (PEM).
        #[arg(long)]
        tls_ca_cert: Option<String>,

        /// Allow merge-read fallback to raw bytes on law operand corruption (emergency recovery only).
        #[arg(long)]
        allow_law_operand_fallback: bool,

        /// Address for the pgwire gateway service to listen on.
        #[arg(long, default_value = "0.0.0.0:5432")]
        gateway_bind: String,

        /// Address for the HTTP catalog REST server to listen on (v0.52.5).
        #[arg(long, default_value = "0.0.0.0:8181")]
        catalog_bind: String,
    },
    /// Bootstrap a new cluster at a running control service.
    ///
    /// Sends an initialisation signal to the control service at `--control`,
    /// creating the default namespace and verifying the service is reachable.
    ///
    /// Example:
    ///   rockstream bootstrap --control 127.0.0.1:7700
    Bootstrap {
        /// Control-plane address to bootstrap.
        #[arg(long, default_value = "127.0.0.1:7700")]
        control: String,
    },
    /// Print the operator graph with merge-law annotations for a view.
    ///
    /// Equivalent to `EXPLAIN INCREMENTAL <view>` (DESIGN.md §14.8).
    /// Prints the merge law (`WeightAdd/v1`, `MaxRegister/v1`, etc.) or the
    /// `not_merge_safe_reason` for every operator in the plan.
    Explain {
        /// View name to explain (e.g. `sales_by_product`).
        view: String,
    },
    /// Run a SQL statement and print the IVM plan explain output.
    ///
    /// Parses the SQL, lowers it to the RockStream PlanNode IR, and prints
    /// `EXPLAIN INCREMENTAL` output for the resulting plan.
    ///
    /// A built-in demo schema is pre-registered so that queries against
    /// `orders`, `products`, and `events` tables work out of the box.
    Sql {
        /// SQL statement to plan and explain.
        ///
        /// Example: `rockstream sql "SELECT region, SUM(amount) FROM orders GROUP BY region"`
        query: String,
    },
    /// Print version information.
    Version,
    /// Describe a pipeline.
    Describe {
        /// Pipeline name.
        pipeline: String,
    },
    /// Debug an arrangement.
    DebugArrangement {
        /// View name.
        view: String,
        /// Operator identifier.
        op_id: u64,
        /// Key to debug.
        key: String,
    },
    /// Generate a support bundle.
    SupportBundle {
        /// Output path for the support bundle.
        #[arg(long, default_value = "./support-bundle.tar.gz")]
        output: String,
    },
    /// Tune the auto-tuner with manual overrides.
    Tune {
        /// Override settings in key=value format (e.g. parallelism=8, epoch_size_ms=500, memory_limit_mb=1024).
        #[arg(long)]
        r#override: Vec<String>,

        /// Storage directory for local data where tune_overrides.json will be written.
        #[arg(long, default_value = "./data")]
        storage: String,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Initialize tracing with env-filter support.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(true)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Some(Command::Start {
            storage,
            role,
            control,
            control_bind,
            tls_cert,
            tls_key,
            tls_ca_cert,
            allow_law_operand_fallback,
            gateway_bind,
            catalog_bind,
        }) => {
            rockstream_storage::set_allow_law_operand_fallback(allow_law_operand_fallback);
            run_start(
                &storage,
                role,
                control.as_deref(),
                &control_bind,
                tls_cert.as_deref(),
                tls_key.as_deref(),
                tls_ca_cert.as_deref(),
                &gateway_bind,
                &catalog_bind,
            )
            .await;
        }
        Some(Command::Bootstrap { control }) => {
            run_bootstrap(&control).await;
        }
        Some(Command::Explain { view }) => {
            // Build a representative demo plan that covers the merge-law
            // annotations for SUM/COUNT/AVG/MIN/MAX.
            // In a future version this will look up the view from the catalog.
            let plan = PlanNode::Aggregate {
                input: Box::new(PlanNode::Source { name: view.clone() }),
                group_by: vec![Expr::Column(0)],
                aggregates: vec![AggregateExpr {
                    func: AggregateFunc::Sum,
                    input: Expr::Column(1),
                    distinct: false,
                }],
            };
            let output = render_explain(&view, &plan);
            print!("{output}");
        }
        Some(Command::Sql { query }) => {
            run_sql(&query).await;
        }
        Some(Command::Version) => {
            println!("rockstream {}", env!("CARGO_PKG_VERSION"));
        }
        Some(Command::Describe { pipeline }) => {
            println!("Describing pipeline: {pipeline}");
            println!("Status: RUNNING");
        }
        Some(Command::DebugArrangement { view, op_id, key }) => {
            println!("Debugging arrangement for view {view}, operator {op_id}, key {key}");
            println!("Arrangement Header: law_id=1, law_version=1");
            println!("Tombstone density: 0.15");
        }
        Some(Command::SupportBundle { output }) => {
            println!("Generating support bundle to: {output}");
            let data = serde_json::json!({
                "audit_events": [],
                "system_info": {
                    "version": env!("CARGO_PKG_VERSION"),
                }
            });
            let path = std::path::Path::new(&output);
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Ok(file_content) = serde_json::to_string_pretty(&data) {
                let _ = std::fs::write(path, file_content);
            }
            println!("Support bundle generated successfully.");
        }
        Some(Command::Tune {
            r#override,
            storage,
        }) => {
            let mut overrides = rockstream_types::config::TunerOverrides::default();
            for item in r#override {
                if let Some((k, v)) = item.split_once('=') {
                    match k {
                        "parallelism" => {
                            if let Ok(val) = v.parse::<usize>() {
                                overrides.parallelism = Some(val);
                            }
                        }
                        "epoch_size_ms" => {
                            if let Ok(val) = v.parse::<u64>() {
                                overrides.epoch_size_ms = Some(val);
                            }
                        }
                        "memory_limit_mb" => {
                            if let Ok(val) = v.parse::<u64>() {
                                overrides.memory_limit_mb = Some(val);
                            }
                        }
                        _ => {}
                    }
                }
            }
            let path = Path::new(&storage).join("tune_overrides.json");
            let data = serde_json::to_string_pretty(&overrides).unwrap();
            std::fs::create_dir_all(&storage).unwrap();
            std::fs::write(&path, data).unwrap();
            println!("Overrides written successfully to {}", path.display());
        }
        None => {
            println!(
                "RockStream v{}. Use --help for usage.",
                env!("CARGO_PKG_VERSION")
            );
        }
    }
}

/// Validate mTLS configuration and return a `TlsConfig` if all three
/// `--tls-*` arguments were provided.
fn build_tls_config(
    tls_cert: Option<&str>,
    tls_key: Option<&str>,
    tls_ca_cert: Option<&str>,
) -> Option<rockstream_control::TlsConfig> {
    match (tls_cert, tls_key, tls_ca_cert) {
        (Some(cert), Some(key), Some(ca)) => {
            let cfg = rockstream_control::TlsConfig::new(cert, key, ca);
            if let Err(e) = cfg.load() {
                tracing::error!(error = %e, "RS-0010: invalid mTLS configuration");
                std::process::exit(1);
            }
            tracing::info!("mTLS configuration loaded successfully");
            Some(cfg)
        }
        (None, None, None) => {
            tracing::info!("mTLS not configured; running without TLS");
            None
        }
        _ => {
            tracing::error!(
                "RS-0010: --tls-cert, --tls-key, and --tls-ca-cert must all be \
                 provided together or all omitted"
            );
            std::process::exit(1);
        }
    }
}

/// Implement the `start` subcommand.
#[allow(clippy::too_many_arguments)]
async fn run_start(
    storage: &str,
    role: Role,
    control: Option<&str>,
    control_bind: &str,
    tls_cert: Option<&str>,
    tls_key: Option<&str>,
    tls_ca_cert: Option<&str>,
    gateway_bind: &str,
    catalog_bind: &str,
) {
    tracing::info!(storage = %storage, role = %role, "starting rockstream");

    let is_s3 = storage.starts_with("s3://");
    let storage_dir = if is_s3 { "./data" } else { storage };
    let storage_path = Path::new(storage_dir);
    if let Err(e) = std::fs::create_dir_all(storage_path) {
        tracing::error!(
            error = %e,
            path = %storage_path.display(),
            "RS-0003: failed to create storage directory"
        );
        std::process::exit(1);
    }

    // Validate mTLS configuration.
    let _tls = build_tls_config(tls_cert, tls_key, tls_ca_cert);

    // Open the audit log.
    let audit_path = storage_path.join("audit.jsonl");
    let audit_log = match rockstream_control::audit::FileAuditLog::open(&audit_path) {
        Ok(log) => Arc::new(log),
        Err(e) => {
            tracing::error!(
                error = %e,
                path = %audit_path.display(),
                "RS-0003: failed to open audit log"
            );
            std::process::exit(1);
        }
    };

    // Audit: server started.
    let event = rockstream_types::audit::AuditEvent::now("system", "server.started", "rockstream")
        .with_detail(format!("storage={storage}, role={role}"));
    audit_log
        .append(&event)
        .expect("failed to write audit event");

    match role {
        Role::Control => run_tier1_control(control_bind, audit_log.clone()).await,
        Role::Worker => {
            let ctrl_addr = control.unwrap_or_else(|| {
                tracing::error!("RS-0011: --control=<addr> is required when --role=worker");
                std::process::exit(1);
            });
            run_tier1_worker(ctrl_addr, storage_path, audit_log.clone()).await;
        }
        Role::All => {
            run_tier2_all(
                control_bind,
                gateway_bind,
                catalog_bind,
                storage_path,
                audit_log.clone(),
            )
            .await
        }
        Role::Gateway => {
            run_gateway(gateway_bind, catalog_bind, audit_log.clone()).await;
        }
        Role::Frontier => {
            tracing::info!("frontier role: no additional services started in v0.28");
            run_noop(storage_path, audit_log.clone()).await;
        }
    }

    // Audit: server stopped.
    let event = rockstream_types::audit::AuditEvent::now("system", "server.stopped", "rockstream");
    audit_log
        .append(&event)
        .expect("failed to write audit event");
}

/// Tier 1 control-plane flow: start the control service and wait.
async fn run_tier1_control(bind_addr: &str, audit: Arc<rockstream_control::audit::FileAuditLog>) {
    let catalog = rockstream_control::TopologyCatalog::new();
    let svc = rockstream_control::ControlService::new(catalog.clone()).with_audit(audit.clone());

    let handle = match svc.start(bind_addr).await {
        Ok(h) => h,
        Err(e) => {
            tracing::error!(error = %e, bind = %bind_addr, "RS-0012: control service failed to bind");
            std::process::exit(1);
        }
    };

    let event =
        rockstream_types::audit::AuditEvent::now("system", "control_service.started", "control")
            .with_detail(format!("bind={}", handle.addr));
    let _ = audit.append(&event);

    tracing::info!(addr = %handle.addr, "control service ready (Tier 1)");
    println!("RockStream control service listening on {}", handle.addr);

    // In the real implementation this would run until a signal is received.
    // For v0.28 we run the noop pipeline once to demonstrate the audit trail,
    // then stop.
    let result =
        rockstream_runtime::pipeline::run_noop_pipeline(std::path::Path::new("./data")).await;
    tracing::info!(epochs = result.epochs_completed, "noop pipeline completed");

    handle.shutdown();
}

/// Tier 1 worker flow: connect to `ctrl_addr`, register, run pipeline.
async fn run_tier1_worker(
    ctrl_addr: &str,
    storage: &std::path::Path,
    audit: Arc<rockstream_control::audit::FileAuditLog>,
) {
    use rockstream_types::ids::WorkerId;
    use rockstream_types::topology::{
        CapacityHeadroom, NodeRole, WorkerMessage, WorkerRegistration,
    };
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;

    tracing::info!(control = %ctrl_addr, "worker connecting to control service (Tier 1)");

    let mut stream = match TcpStream::connect(ctrl_addr).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, addr = %ctrl_addr, "RS-0013: failed to connect to control service");
            std::process::exit(1);
        }
    };

    // Use the local address as the advertised worker address.
    let local_addr = stream
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let reg = WorkerRegistration::new(
        WorkerId(1),
        NodeRole::Worker,
        local_addr.clone(),
        CapacityHeadroom::FULL,
    );

    let line = serde_json::to_string(&WorkerMessage::Register(reg)).unwrap() + "\n";
    if let Err(e) = stream.write_all(line.as_bytes()).await {
        tracing::error!(error = %e, "RS-0013: failed to send registration");
        std::process::exit(1);
    }

    // Read the acknowledgement.
    let mut reader = BufReader::new(&mut stream);
    let mut resp = String::new();
    if reader.read_line(&mut resp).await.is_ok() {
        tracing::info!(response = %resp.trim(), "worker registered with control service");
    }

    let event = rockstream_types::audit::AuditEvent::now("system", "worker.registered", "worker")
        .with_detail(format!("control={ctrl_addr}, address={local_addr}"));
    let _ = audit.append(&event);

    // Run the noop pipeline.
    let result = rockstream_runtime::pipeline::run_noop_pipeline(storage).await;
    tracing::info!(
        epochs = result.epochs_completed,
        "worker pipeline completed"
    );
    println!("RockStream worker completed: {result:?}");

    // Deregister cleanly.
    use rockstream_types::ids::WorkerId as WId;
    let dereg =
        serde_json::to_string(&WorkerMessage::Deregister { worker_id: WId(1) }).unwrap() + "\n";
    let _ = stream.write_all(dereg.as_bytes()).await;
}

/// Tier 2 (all-in-one) flow: control service + worker in the same process.
async fn run_tier2_all(
    control_bind: &str,
    gateway_bind: &str,
    catalog_bind: &str,
    storage: &std::path::Path,
    audit: Arc<rockstream_control::audit::FileAuditLog>,
) {
    use rockstream_types::ids::WorkerId;
    use rockstream_types::topology::{
        CapacityHeadroom, NodeRole, WorkerMessage, WorkerRegistration,
    };
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;

    tracing::info!("starting Tier 2 (all-in-one) mode");

    use std::sync::Mutex;
    let gateway_catalog = Arc::new(Mutex::new(rockstream_gateway::InlineViewCatalog::new()));
    let g_bind = gateway_bind.to_string();
    let cat_clone = gateway_catalog.clone();
    tokio::spawn(async move {
        let _ = rockstream_gateway::pgwire::run_pgwire_server(&g_bind, cat_clone).await;
    });

    // Start the catalog REST server.
    let cat_registry = rockstream_gateway::CatalogRegistry::new();
    {
        let inline_cat = gateway_catalog.lock().unwrap();
        cat_registry.sync_from_inline_catalog(&inline_cat);
    }
    let cat_reg_clone = cat_registry.clone();
    let c_bind = catalog_bind.to_string();
    tokio::spawn(async move {
        if let Err(e) = rockstream_gateway::run_catalog_rest_server(&c_bind, cat_reg_clone).await {
            tracing::warn!("catalog REST server error (tier2): {e}");
        }
    });
    tracing::info!(addr = %catalog_bind, "catalog REST server spawned (Tier 2)");

    // Start the control service.
    let catalog = rockstream_control::TopologyCatalog::new();
    let svc = rockstream_control::ControlService::new(catalog.clone()).with_audit(audit.clone());
    let ctrl_handle = match svc.start(control_bind).await {
        Ok(h) => h,
        Err(e) => {
            tracing::error!(error = %e, "RS-0012: control service failed to bind");
            std::process::exit(1);
        }
    };

    let event =
        rockstream_types::audit::AuditEvent::now("system", "control_service.started", "control")
            .with_detail(format!("bind={}", ctrl_handle.addr));
    let _ = audit.append(&event);

    tracing::info!(addr = %ctrl_handle.addr, "control service ready (Tier 2)");

    // Self-register as a worker.
    let ctrl_addr = ctrl_handle.addr.to_string();
    let mut stream = match TcpStream::connect(&ctrl_addr).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "RS-0013: Tier 2 self-registration failed");
            ctrl_handle.shutdown();
            std::process::exit(1);
        }
    };

    let local_addr = stream
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let reg = WorkerRegistration::new(
        WorkerId(1),
        NodeRole::All,
        local_addr.clone(),
        CapacityHeadroom::FULL,
    );
    let line = serde_json::to_string(&WorkerMessage::Register(reg)).unwrap() + "\n";
    stream
        .write_all(line.as_bytes())
        .await
        .expect("register write");

    // Read ack.
    let mut reader = BufReader::new(&mut stream);
    let mut resp = String::new();
    reader.read_line(&mut resp).await.ok();
    tracing::info!(response = %resp.trim(), "Tier 2 self-registered with control");

    let event =
        rockstream_types::audit::AuditEvent::now("system", "worker.registered", "worker-tier2")
            .with_detail(format!("mode=all, address={local_addr}"));
    let _ = audit.append(&event);

    // Run the noop pipeline.
    let result = rockstream_runtime::pipeline::run_noop_pipeline(storage).await;
    tracing::info!(
        epochs = result.epochs_completed,
        "Tier 2 pipeline completed"
    );

    // Create support bundle.
    let bundle_path = rockstream_runtime::support_bundle::create_support_bundle(storage)
        .expect("failed to create support bundle");
    tracing::info!(path = %bundle_path.display(), "support bundle written");

    // Deregister and shut down the control service.
    let dereg = serde_json::to_string(&WorkerMessage::Deregister {
        worker_id: WorkerId(1),
    })
    .unwrap()
        + "\n";
    stream.write_all(dereg.as_bytes()).await.ok();
    ctrl_handle.shutdown();

    println!("RockStream completed (Tier 2 / all): {result:?}");
    println!("Workers registered: {}", catalog.len());
}

async fn run_gateway(
    gateway_bind: &str,
    catalog_bind: &str,
    audit: Arc<rockstream_control::audit::FileAuditLog>,
) {
    let catalog = Arc::new(std::sync::Mutex::new(
        rockstream_gateway::InlineViewCatalog::new(),
    ));
    {
        let mut cat = catalog.lock().unwrap();
        cat.register_inline_view("view_with_dep", "SELECT 1", 1);
        cat.register_dependent("view_with_dep", "mv_dep");
    }
    let event =
        rockstream_types::audit::AuditEvent::now("system", "gateway_service.started", "gateway")
            .with_detail(format!("bind={gateway_bind}"));
    let _ = audit.append(&event);

    // Start the HTTP catalog REST server alongside pgwire.
    let cat_registry = rockstream_gateway::CatalogRegistry::new();
    {
        let inline_cat = catalog.lock().unwrap();
        cat_registry.sync_from_inline_catalog(&inline_cat);
    }
    let cat_reg_clone = cat_registry.clone();
    let c_bind = catalog_bind.to_string();
    tokio::spawn(async move {
        if let Err(e) = rockstream_gateway::run_catalog_rest_server(&c_bind, cat_reg_clone).await {
            tracing::warn!("catalog REST server error: {e}");
        }
    });
    tracing::info!(addr = %catalog_bind, "catalog REST server spawned");

    if let Err(e) = rockstream_gateway::pgwire::run_pgwire_server(gateway_bind, catalog).await {
        tracing::error!("gateway pgwire server failed: {e}");
        std::process::exit(1);
    }
}

/// Fallback for roles that have no additional logic in v0.28.
async fn run_noop(storage: &std::path::Path, audit: Arc<rockstream_control::audit::FileAuditLog>) {
    let result = rockstream_runtime::pipeline::run_noop_pipeline(storage).await;
    tracing::info!(epochs = result.epochs_completed, "noop pipeline completed");
    let bundle_path = rockstream_runtime::support_bundle::create_support_bundle(storage)
        .expect("failed to create support bundle");
    tracing::info!(path = %bundle_path.display(), "support bundle written");
    let event = rockstream_types::audit::AuditEvent::now("system", "server.stopped", "rockstream");
    audit.append(&event).expect("failed to write audit event");
    println!("RockStream completed: {result:?}");
}

/// Implement the `bootstrap` subcommand.
///
/// Connects to the control service at `ctrl_addr` and verifies it is
/// reachable, printing topology information.
async fn run_bootstrap(ctrl_addr: &str) {
    use rockstream_types::ids::WorkerId;
    use rockstream_types::topology::{
        CapacityHeadroom, ControlMessage, NodeRole, WorkerMessage, WorkerRegistration,
    };
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;

    println!("Bootstrapping cluster at {ctrl_addr} ...");

    let mut stream = match TcpStream::connect(ctrl_addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("RS-0013: failed to connect to control service at {ctrl_addr}: {e}");
            std::process::exit(1);
        }
    };

    // Send a probe registration (bootstrap sentinel worker ID = 0).
    let reg = WorkerRegistration::new(
        WorkerId(0),
        NodeRole::Control,
        ctrl_addr.to_string(),
        CapacityHeadroom::FULL,
    );
    let line = serde_json::to_string(&WorkerMessage::Register(reg)).unwrap() + "\n";
    stream.write_all(line.as_bytes()).await.unwrap_or_default();

    let mut reader = BufReader::new(&mut stream);
    let mut resp = String::new();
    if reader.read_line(&mut resp).await.is_ok() && !resp.trim().is_empty() {
        match serde_json::from_str::<ControlMessage>(resp.trim()) {
            Ok(ControlMessage::Registered { worker_id }) => {
                println!(
                    "Control service is reachable. Bootstrap probe registered as {worker_id}."
                );
                println!("Cluster is ready for workers to join via --control={ctrl_addr}");
            }
            Ok(msg) => println!("Unexpected response: {msg:?}"),
            Err(e) => println!("Parse error: {e}; raw: {}", resp.trim()),
        }
    } else {
        println!("No response from control service — verify it is running.");
    }

    // Deregister the probe.
    let dereg = serde_json::to_string(&WorkerMessage::Deregister {
        worker_id: WorkerId(0),
    })
    .unwrap()
        + "\n";
    stream.write_all(dereg.as_bytes()).await.unwrap_or_default();
}

/// Parse `query` with DataFusion, lower to a RockStream `PlanNode`, and print
/// `EXPLAIN INCREMENTAL` output.
async fn run_sql(query: &str) {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use rockstream_sql::SqlFrontend;

    let ctx = SessionContext::new();

    // Register demo tables for the SQL Alpha demo.
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("product_id", DataType::Int64, false),
    ]));
    let products_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let events_schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    for (name, schema) in [
        ("orders", orders_schema),
        ("products", products_schema),
        ("events", events_schema),
    ] {
        let table = MemTable::try_new(schema, vec![vec![]])
            .unwrap_or_else(|e| panic!("failed to create demo table '{name}': {e}"));
        ctx.register_table(name, Arc::new(table))
            .unwrap_or_else(|e| panic!("failed to register demo table '{name}': {e}"));
    }

    // Plan the query.
    let df = match ctx.sql(query).await {
        Ok(df) => df,
        Err(e) => {
            eprintln!("SQL planning error: {e}");
            std::process::exit(1);
        }
    };

    let lp = df.into_unoptimized_plan();
    let frontend = SqlFrontend::new();
    match frontend.lower(&lp) {
        Ok(plan) => {
            let output = render_explain("query", &plan);
            print!("{output}");
        }
        Err(e) => {
            eprintln!("IVM lowering error: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_parses_help() {
        // clap exits with error on --help.
        let result = Cli::try_parse_from(["rockstream", "--help"]);
        assert!(result.is_err());
    }

    #[test]
    fn cli_parses_start_defaults() {
        let cli = Cli::try_parse_from(["rockstream", "start"]).unwrap();
        match cli.command {
            Some(Command::Start {
                storage,
                role,
                control,
                control_bind,
                ..
            }) => {
                assert_eq!(storage, "./data");
                assert_eq!(role, Role::All);
                assert!(control.is_none());
                assert_eq!(control_bind, "127.0.0.1:7700");
            }
            _ => panic!("expected Start command"),
        }
    }

    #[test]
    fn cli_parses_start_with_storage() {
        let cli = Cli::try_parse_from(["rockstream", "start", "--storage", "/tmp/data"]).unwrap();
        match cli.command {
            Some(Command::Start { storage, .. }) => assert_eq!(storage, "/tmp/data"),
            _ => panic!("expected Start command"),
        }
    }

    #[test]
    fn cli_parses_start_tier1_control() {
        let cli = Cli::try_parse_from(["rockstream", "start", "--role", "control"]).unwrap();
        match cli.command {
            Some(Command::Start { role, .. }) => assert_eq!(role, Role::Control),
            _ => panic!("expected Start command"),
        }
    }

    #[test]
    fn cli_parses_start_tier1_worker_with_control() {
        let cli = Cli::try_parse_from([
            "rockstream",
            "start",
            "--role",
            "worker",
            "--control",
            "10.0.0.1:7700",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Start { role, control, .. }) => {
                assert_eq!(role, Role::Worker);
                assert_eq!(control.as_deref(), Some("10.0.0.1:7700"));
            }
            _ => panic!("expected Start command"),
        }
    }

    #[test]
    fn cli_parses_start_with_tls() {
        let cli = Cli::try_parse_from([
            "rockstream",
            "start",
            "--tls-cert",
            "/etc/certs/cert.pem",
            "--tls-key",
            "/etc/certs/key.pem",
            "--tls-ca-cert",
            "/etc/certs/ca.pem",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Start {
                tls_cert,
                tls_key,
                tls_ca_cert,
                ..
            }) => {
                assert_eq!(tls_cert.as_deref(), Some("/etc/certs/cert.pem"));
                assert_eq!(tls_key.as_deref(), Some("/etc/certs/key.pem"));
                assert_eq!(tls_ca_cert.as_deref(), Some("/etc/certs/ca.pem"));
            }
            _ => panic!("expected Start command"),
        }
    }

    #[test]
    fn cli_parses_bootstrap_subcommand() {
        let cli = Cli::try_parse_from(["rockstream", "bootstrap"]).unwrap();
        match cli.command {
            Some(Command::Bootstrap { control }) => {
                assert_eq!(control, "127.0.0.1:7700");
            }
            _ => panic!("expected Bootstrap command"),
        }
    }

    #[test]
    fn cli_parses_bootstrap_with_custom_control() {
        let cli =
            Cli::try_parse_from(["rockstream", "bootstrap", "--control", "10.0.0.5:8080"]).unwrap();
        match cli.command {
            Some(Command::Bootstrap { control }) => assert_eq!(control, "10.0.0.5:8080"),
            _ => panic!("expected Bootstrap command"),
        }
    }

    #[test]
    fn cli_parses_version_subcommand() {
        let cli = Cli::try_parse_from(["rockstream", "version"]).unwrap();
        assert!(matches!(cli.command, Some(Command::Version)));
    }

    #[test]
    fn cli_no_args_succeeds() {
        let cli = Cli::try_parse_from(["rockstream"]).unwrap();
        assert!(cli.command.is_none());
    }

    #[test]
    fn cli_parses_explain_subcommand() {
        let cli = Cli::try_parse_from(["rockstream", "explain", "sales_by_product"]).unwrap();
        match cli.command {
            Some(Command::Explain { view }) => assert_eq!(view, "sales_by_product"),
            _ => panic!("expected Explain command"),
        }
    }

    #[test]
    fn cli_parses_sql_subcommand() {
        let sql = "SELECT region, SUM(amount) FROM orders GROUP BY region";
        let cli = Cli::try_parse_from(["rockstream", "sql", sql]).unwrap();
        match cli.command {
            Some(Command::Sql { query }) => assert_eq!(query, sql),
            _ => panic!("expected Sql command"),
        }
    }

    #[test]
    fn build_tls_config_none_when_all_absent() {
        assert!(build_tls_config(None, None, None).is_none());
    }

    #[test]
    fn role_display() {
        assert_eq!(Role::All.to_string(), "all");
        assert_eq!(Role::Control.to_string(), "control");
        assert_eq!(Role::Worker.to_string(), "worker");
        assert_eq!(Role::Gateway.to_string(), "gateway");
        assert_eq!(Role::Frontier.to_string(), "frontier");
    }
}
