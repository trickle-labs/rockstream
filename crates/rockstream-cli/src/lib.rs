//! The `rockstream` CLI library.
//!
//! One binary serves every node role; each role is a flag on the same
//! `rockstream` binary. At v0.1 the binary runs an **embedded no-op node**:
//! `rockstream start --storage <dir>` brings up the node, runs a no-op
//! pipeline to completion, writes an audit log and a support bundle, and exits
//! cleanly. Real operators, durability, and the distributed roles are added in
//! later versions.
//!
//! All user/operator-visible failures carry an `RS-XXXX` error code with
//! actionable `next_steps` text (see [`CliError`]).

use rockstream_types::audit::AuditEvent;
use rockstream_types::error_code::{ErrorCode, RS_0002, RS_0003};
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
    /// Optional metrics server listen address.
    pub metrics_addr: Option<String>,
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

/// Run `rockstream start` as an embedded no-op node.
///
/// Creates the storage directory, writes an audit log recording the node and
/// no-op pipeline lifecycle, writes a support bundle, and returns the paths of
/// the artifacts written.
pub fn run_start(opts: &StartOptions) -> Result<StartOutcome, CliError> {
    let started_ms = now_ms();
    validate_role(&opts.role)?;

    if (opts.role == "worker" || opts.role == "gateway") && opts.control.is_none() {
        return Err(CliError::new(
            rockstream_types::error_code::RS_0002,
            format!("role `{}` requires --control=<url>", opts.role),
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

    // Start services in a tokio runtime
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| CliError::new(RS_0003, format!("failed to start tokio runtime: {e}"), ""))?;

    rt.block_on(async {
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
            let service = rockstream_control::ControlService::new(catalog)
                .with_shard_manager(manager)
                .with_audit(Arc::new(
                    rockstream_control::audit::FileAuditLog::open(&audit_path).unwrap(),
                ));
            let handle = service.start("127.0.0.1:8000").await.unwrap();
            control_handle = Some(handle);
        }

        if opts.role == "worker" || opts.role == "all" {
            let url = control_url.as_deref().unwrap_or("127.0.0.1:8000");
            let (client, handle) = rockstream_runtime::start_worker_client(1, url, &opts.storage)
                .await
                .unwrap();

            if opts.role == "all" {
                // Wait for worker registration handshake
                tokio::time::sleep(Duration::from_millis(50)).await;
                // Acquire shard 1 lease to demonstrate fencing setup
                let _ = client
                    .request_shard(rockstream_types::ids::ShardId(1))
                    .await;
                tokio::time::sleep(Duration::from_millis(50)).await;
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

        // Allow live interactions to complete
        let sleep_ms = std::env::var("ROCKSTREAM_E2E_SLEEP_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(50);
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

        if let Some(wh) = worker_handle {
            wh.abort();
        }
        if let Some(ch) = control_handle {
            ch.shutdown();
        }
        if let Some(mh) = metrics_handle {
            mh.shutdown();
        }
    });

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
        assert_eq!(err.code.to_string(), "RS-0002");
        assert!(err.next_steps.contains("all"));
    }

    #[test]
    fn known_roles_are_accepted() {
        for role in KNOWN_ROLES {
            assert!(validate_role(role).is_ok());
        }
    }

    #[test]
    fn run_start_writes_audit_log_and_support_bundle() {
        let dir = tempfile::tempdir().unwrap();
        let opts = StartOptions {
            storage: dir.path().to_path_buf(),
            role: "all".to_string(),
            control: None,
            auth_mode: "off".to_string(),
            metrics_addr: None,
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
            metrics_addr: None,
        };
        run_start(&opts).unwrap();
        assert!(nested.join("audit.jsonl").exists());
    }

    #[test]
    fn worker_or_gateway_role_without_control_fails_with_rs_0002() {
        let dir = tempfile::tempdir().unwrap();
        let opts = StartOptions {
            storage: dir.path().to_path_buf(),
            role: "worker".to_string(),
            control: None,
            auth_mode: "off".to_string(),
            metrics_addr: None,
        };
        let err = run_start(&opts).unwrap_err();
        assert_eq!(err.code.to_string(), "RS-0002");
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
            metrics_addr: None,
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
}
