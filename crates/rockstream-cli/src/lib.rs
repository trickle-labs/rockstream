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
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Node roles recognised by the single binary. v0.1 ships only the embedded
/// `all` profile; the other roles are accepted as valid names so that scripts
/// written against later versions parse, but they run the same embedded node.
pub const KNOWN_ROLES: &[&str] = &["all", "control", "worker", "gateway"];

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

/// Append an audit event as one JSON line to an open writer.
fn write_audit_line(writer: &mut impl Write, event: &AuditEvent) -> Result<(), CliError> {
    let line = serde_json::to_string(event).map_err(|e| {
        CliError::new(
            RS_0003,
            format!("could not serialize audit event: {e}"),
            "This is an internal error; please report it with the support bundle.",
        )
    })?;
    writer
        .write_all(line.as_bytes())
        .and_then(|()| writer.write_all(b"\n"))
        .map_err(|e| {
            CliError::new(
                RS_0003,
                format!("could not write audit log: {e}"),
                "Check that the storage directory is writable and the disk is not full.",
            )
        })
}

/// Run `rockstream start` as an embedded no-op node.
///
/// Creates the storage directory, writes an audit log recording the node and
/// no-op pipeline lifecycle, writes a support bundle, and returns the paths of
/// the artifacts written.
pub fn run_start(opts: &StartOptions) -> Result<StartOutcome, CliError> {
    let started_ms = now_ms();
    validate_role(&opts.role)?;

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

    // Embedded no-op node lifecycle. Every control-plane action is audited.
    let events = vec![
        AuditEvent::now(SYSTEM_ACTOR, "server.started", "rockstream")
            .with_detail(format!("role={}", opts.role)),
        AuditEvent::now(SYSTEM_ACTOR, "pipeline.created", "noop-pipeline")
            .with_detail("embedded no-op pipeline"),
        AuditEvent::now(SYSTEM_ACTOR, "pipeline.started", "noop-pipeline"),
        AuditEvent::now(SYSTEM_ACTOR, "pipeline.stopped", "noop-pipeline"),
        AuditEvent::now(SYSTEM_ACTOR, "server.stopped", "rockstream"),
    ];

    let audit_path = opts.storage.join("audit.jsonl");
    write_audit_log(&audit_path, &events)?;

    let bundle_path = write_support_bundle(&opts.storage, &opts.role, started_ms, &events)?;

    Ok(StartOutcome {
        audit_path,
        bundle_path,
        events_written: events.len(),
    })
}

fn write_audit_log(audit_path: &Path, events: &[AuditEvent]) -> Result<(), CliError> {
    let mut file = fs::File::create(audit_path).map_err(|e| {
        CliError::new(
            RS_0003,
            format!("could not create audit log {}: {e}", audit_path.display()),
            "Check that the storage directory is writable and the disk is not full.",
        )
    })?;
    for event in events {
        write_audit_line(&mut file, event)?;
    }
    Ok(())
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
        let err = validate_role("frontier").unwrap_err();
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
        };
        let outcome = run_start(&opts).unwrap();

        assert_eq!(outcome.events_written, 5);

        let audit = fs::read_to_string(&outcome.audit_path).unwrap();
        for expected in [
            "server.started",
            "pipeline.created",
            "pipeline.started",
            "pipeline.stopped",
            "server.stopped",
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
        };
        run_start(&opts).unwrap();
        assert!(nested.join("audit.jsonl").exists());
    }
}
