//! Error-code registry for RockStream (DOC-01).
//!
//! Every user-visible, client-returned, or operator-logged error carries an `RS-XXXX` code.
//! This module defines the canonical registry, type-safe descriptor lookups, and static constants
//! backed by the authoritative `contracts/errors.toml` specification.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::LazyLock;

/// Severity level for an error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Severity {
    /// Informational — no action required.
    Info,
    /// Warning — degraded but operational.
    Warning,
    /// Error — operation failed, user action required.
    Error,
    /// Fatal — system cannot continue without intervention.
    Fatal,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Info => write!(f, "INFO"),
            Self::Warning => write!(f, "WARN"),
            Self::Error => write!(f, "ERROR"),
            Self::Fatal => write!(f, "FATAL"),
        }
    }
}

/// Recovery and retry classification policy for client operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RetryClass {
    /// Fatal or deterministic semantic failure (do not retry).
    NonRetryable,
    /// Transient lock or optimistic concurrency conflict (retry immediately).
    Immediate,
    /// Buffer capacity, rate limit, or I/O pressure (retry with exponential backoff).
    ExponentialBackoff,
    /// Request routed to non-leader or raft in transition (retry after leader election).
    AfterLeaderElection,
    /// Pipeline or shard undergoing checkpoint/recovery (retry after recovery).
    AfterClusterRecovery,
}

impl fmt::Display for RetryClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonRetryable => write!(f, "NonRetryable"),
            Self::Immediate => write!(f, "Immediate"),
            Self::ExponentialBackoff => write!(f, "ExponentialBackoff"),
            Self::AfterLeaderElection => write!(f, "AfterLeaderElection"),
            Self::AfterClusterRecovery => write!(f, "AfterClusterRecovery"),
        }
    }
}

/// An error code in the `RS-XXXX` format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorCode(pub(crate) u16);

impl ErrorCode {
    /// Create a new error code from a numeric value.
    pub const fn new(code: u16) -> Self {
        Self(code)
    }

    /// Get the numeric value.
    pub const fn value(self) -> u16 {
        self.0
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RS-{:04}", self.0)
    }
}

impl<'de> Deserialize<'de> for ErrorCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ErrorCodeVisitor;
        impl<'de> serde::de::Visitor<'de> for ErrorCodeVisitor {
            type Value = ErrorCode;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a u16 or an RS-XXXX string")
            }
            fn visit_u64<E>(self, v: u64) -> Result<ErrorCode, E>
            where
                E: serde::de::Error,
            {
                Ok(ErrorCode(v as u16))
            }
            fn visit_i64<E>(self, v: i64) -> Result<ErrorCode, E>
            where
                E: serde::de::Error,
            {
                Ok(ErrorCode(v as u16))
            }
            fn visit_str<E>(self, v: &str) -> Result<ErrorCode, E>
            where
                E: serde::de::Error,
            {
                if let Some(num_str) = v.strip_prefix("RS-") {
                    let num = num_str.parse::<u16>().map_err(E::custom)?;
                    Ok(ErrorCode(num))
                } else {
                    let num = v.parse::<u16>().map_err(E::custom)?;
                    Ok(ErrorCode(num))
                }
            }
        }
        deserializer.deserialize_any(ErrorCodeVisitor)
    }
}

impl Serialize for ErrorCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("RS-{:04}", self.0))
    }
}

/// Canonical metadata descriptor for a registered error code (DOC-01).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorDescriptor {
    /// Canonical error code identifier.
    pub code: ErrorCode,
    /// Dot-delimited identifier key (e.g. "query.timeout").
    pub key: String,
    /// Human-readable title summary.
    pub title: String,
    /// Severity classification.
    pub severity: Severity,
    /// 5-character PostgreSQL / ANSI SQLSTATE.
    pub sqlstate: String,
    /// Retry and recovery class.
    pub retry_class: RetryClass,
    /// Actionable next steps guidance.
    pub default_next_steps: String,
    /// Lowercase markdown documentation anchor.
    pub doc_anchor: String,
}

impl ErrorDescriptor {
    /// Lookup an error descriptor by its ErrorCode.
    pub fn lookup(code: ErrorCode) -> Option<&'static ErrorDescriptor> {
        ErrorCatalog::current().lookup(code)
    }

    /// Lookup an error descriptor by its dot-delimited key (e.g. "query.timeout").
    pub fn by_key(key: &str) -> Option<&'static ErrorDescriptor> {
        ErrorCatalog::current().by_key(key)
    }
}

/// Contract header from errors.toml.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorCatalogContract {
    pub version: String,
    pub roadmap: String,
    #[serde(default)]
    pub description: Option<String>,
}

/// Parsed document structure for `contracts/errors.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorCatalogDocument {
    pub contract: ErrorCatalogContract,
    #[serde(rename = "error", default)]
    pub errors: Vec<ErrorDescriptor>,
}

/// Immutable in-memory runtime error catalog.
pub struct ErrorCatalog {
    document: ErrorCatalogDocument,
    by_code: HashMap<ErrorCode, usize>,
    by_key: HashMap<String, usize>,
}

static CURRENT_CATALOG: LazyLock<ErrorCatalog> = LazyLock::new(|| {
    const TOML_CONTENT: &str = include_str!("../../../contracts/errors.toml");
    let document: ErrorCatalogDocument =
        toml::from_str(TOML_CONTENT).expect("embedded contracts/errors.toml must be valid TOML");
    let mut by_code = HashMap::with_capacity(document.errors.len());
    let mut by_key = HashMap::with_capacity(document.errors.len());
    for (i, err) in document.errors.iter().enumerate() {
        by_code.insert(err.code, i);
        by_key.insert(err.key.clone(), i);
    }
    ErrorCatalog {
        document,
        by_code,
        by_key,
    }
});

impl ErrorCatalog {
    /// Retrieve reference to the singleton compile-time embedded error catalog.
    pub fn current() -> &'static Self {
        &CURRENT_CATALOG
    }

    /// Slice of all registered error descriptors.
    pub fn errors(&self) -> &[ErrorDescriptor] {
        &self.document.errors
    }

    /// Contract metadata.
    pub fn contract(&self) -> &ErrorCatalogContract {
        &self.document.contract
    }

    /// Lookup a descriptor by ErrorCode.
    pub fn lookup(&self, code: ErrorCode) -> Option<&ErrorDescriptor> {
        let idx = self.by_code.get(&code)?;
        Some(&self.document.errors[*idx])
    }

    /// Lookup a descriptor by its string key.
    pub fn by_key(&self, key: &str) -> Option<&ErrorDescriptor> {
        let idx = self.by_key.get(key)?;
        Some(&self.document.errors[*idx])
    }
}

// ─── Legacy ErrorCodeMeta for compatibility ──────────────────────────────────
/// Metadata for a registered error code (legacy compatibility).
pub struct ErrorCodeMeta {
    /// The error code.
    pub code: ErrorCode,
    /// Human-readable description.
    pub description: &'static str,
    /// Severity level.
    pub severity: Severity,
    /// Actionable next steps for the operator/user.
    pub next_steps: &'static str,
    /// Documentation URL.
    pub doc_url: &'static str,
}

/// Returns a short slug for a known error code (e.g. "auth.unauthenticated").
pub fn slug(code: ErrorCode) -> &'static str {
    if let Some(desc) = ErrorDescriptor::lookup(code) {
        desc.key.as_str()
    } else {
        "unknown"
    }
}

/// Returns a human-readable description for a known error code.
pub fn description(code: ErrorCode) -> &'static str {
    if let Some(desc) = ErrorDescriptor::lookup(code) {
        desc.title.as_str()
    } else {
        "Unknown error"
    }
}

/// Returns the severity for a known error code.
pub fn severity(code: ErrorCode) -> Severity {
    if let Some(desc) = ErrorDescriptor::lookup(code) {
        desc.severity
    } else {
        Severity::Error
    }
}

/// Returns actionable next steps for a known error code.
pub fn next_steps(code: ErrorCode) -> &'static str {
    if let Some(desc) = ErrorDescriptor::lookup(code) {
        desc.default_next_steps.as_str()
    } else {
        "See documentation for this error code."
    }
}

// ─── Static ErrorCode Constants ──────────────────────────────────────────────
/// Internal error.
pub const RS_0001: ErrorCode = ErrorCode::new(1);
/// Configuration error.
pub const RS_0002: ErrorCode = ErrorCode::new(2);
/// Storage unavailable.
pub const RS_0003: ErrorCode = ErrorCode::new(3);
/// Cluster control plane unreachable.
pub const RS_0004: ErrorCode = ErrorCode::new(4);
/// Destructive command confirmation required.
pub const RS_0005: ErrorCode = ErrorCode::new(5);
/// Pipeline not found.
pub const RS_1001: ErrorCode = ErrorCode::new(1001);
/// Incompatible schema change.
pub const RS_1002: ErrorCode = ErrorCode::new(1002);
/// Record decode error.
pub const RS_1003: ErrorCode = ErrorCode::new(1003);
/// Pipeline already exists.
pub const RS_1004: ErrorCode = ErrorCode::new(1004);
/// Workload not found.
pub const RS_1005: ErrorCode = ErrorCode::new(1005);
/// Workload already exists.
pub const RS_1006: ErrorCode = ErrorCode::new(1006);
/// View is already paused.
pub const RS_1007: ErrorCode = ErrorCode::new(1007);
/// View is not paused.
pub const RS_1008: ErrorCode = ErrorCode::new(1008);
/// Non-monotone delta rejected in monotone recursion.
pub const RS_1009: ErrorCode = ErrorCode::new(1009);
/// Bootstrap interrupted; connector position lost.
pub const RS_1010: ErrorCode = ErrorCode::new(1010);
/// View-on-view DAG contains a cycle.
pub const RS_1011: ErrorCode = ErrorCode::new(1011);
/// SQL statement could not be parsed.
pub const RS_1012: ErrorCode = ErrorCode::new(1012);
/// Query contains a feature not supported by the incremental planner.
pub const RS_1013: ErrorCode = ErrorCode::new(1013);
/// Workload still has assigned views.
pub const RS_1014: ErrorCode = ErrorCode::new(1014);
/// Group-commit capacity exceeded; backpressure applied.
pub const RS_1015: ErrorCode = ErrorCode::new(1015);
/// Aggregate running sum overflowed i64.
pub const RS_1016: ErrorCode = ErrorCode::new(1016);
/// MIN/MAX multiset retraction underflow: value has no positive weight.
pub const RS_1017: ErrorCode = ErrorCode::new(1017);
/// TopK buffer overflow: too many unique rows in a single partition.
pub const RS_1018: ErrorCode = ErrorCode::new(1018);
/// View query could not be compiled into an executable operator pipeline.
pub const RS_1019: ErrorCode = ErrorCode::new(1019);
/// Operator not found in pipeline.
pub const RS_1020: ErrorCode = ErrorCode::new(1020);
/// Arrangement key decoding failed or unsupported.
pub const RS_1021: ErrorCode = ErrorCode::new(1021);
/// Migration state exceeded its configured timeout budget.
pub const RS_1030: ErrorCode = ErrorCode::new(1030);
/// Inner-frontier stall in distributed recursion; per-shard recompute triggered.
pub const RS_1512: ErrorCode = ErrorCode::new(1512);
/// Distributed recursion max-iteration cap exceeded without convergence.
pub const RS_1513: ErrorCode = ErrorCode::new(1513);
/// Lease acquisition rejected: shard already leased or fence token invalid.
pub const RS_1601: ErrorCode = ErrorCode::new(1601);
/// Recovery active for > 60s; pipeline freshness behind SLO.
pub const RS_1603: ErrorCode = ErrorCode::new(1603);
/// Shard is already leased by a different worker.
pub const RS_1701: ErrorCode = ErrorCode::new(1701);
/// Stale lease token; worker has been fenced out.
pub const RS_1702: ErrorCode = ErrorCode::new(1702);
/// Shard has no active lease.
pub const RS_1703: ErrorCode = ErrorCode::new(1703);
/// Write rejected: acting control node is not the current Raft leader.
pub const RS_1731: ErrorCode = ErrorCode::new(1731);
/// Malformed table DDL statement.
pub const RS_2000: ErrorCode = ErrorCode::new(2000);
/// View not found.
pub const RS_2001: ErrorCode = ErrorCode::new(2001);
/// Query timeout.
pub const RS_2002: ErrorCode = ErrorCode::new(2002);
/// Unsupported isolation level.
pub const RS_2003: ErrorCode = ErrorCode::new(2003);
/// Cannot drop inline view: dependent materialized views still exist.
pub const RS_2004: ErrorCode = ErrorCode::new(2004);
/// Query rate limit exceeded.
pub const RS_2005: ErrorCode = ErrorCode::new(2005);
/// Historical query beyond checkpoint retention window.
pub const RS_2006: ErrorCode = ErrorCode::new(2006);
/// Idempotency key required for non-idempotent write.
pub const RS_2007: ErrorCode = ErrorCode::new(2007);
/// Optimistic transaction conflict: a concurrent write committed to the same key.
pub const RS_2008: ErrorCode = ErrorCode::new(2008);
/// Session wait-for deadline exceeded; query proceeded at current frontier.
pub const RS_2012: ErrorCode = ErrorCode::new(2012);
/// Transaction RETURNING read-back could not find the row at the current frontier.
pub const RS_2013: ErrorCode = ErrorCode::new(2013);
/// Index is building.
pub const RS_2014: ErrorCode = ErrorCode::new(2014);
/// Index frontier lag exceeded limit.
pub const RS_2015: ErrorCode = ErrorCode::new(2015);
/// Index name conflict.
pub const RS_2016: ErrorCode = ErrorCode::new(2016);
/// Shard statistics are too stale for safe pruning; query fell back to a full scatter scan.
pub const RS_2017: ErrorCode = ErrorCode::new(2017);
/// Published frontier exceeded the session max_staleness bound; query proceeded.
pub const RS_2018: ErrorCode = ErrorCode::new(2018);
/// Shard write buffer full; backpressure applied.
pub const RS_2019: ErrorCode = ErrorCode::new(2019);
/// Subscribe consumer fell behind the change-log retention window.
pub const RS_2020: ErrorCode = ErrorCode::new(2020);
/// COPY FROM STDIN statement is malformed.
pub const RS_2021: ErrorCode = ErrorCode::new(2021);
/// UPDATE/DELETE RETURNING clause is malformed.
pub const RS_2022: ErrorCode = ErrorCode::new(2022);
/// Hop window state exceeded its configured overlap-aware bound.
pub const RS_2023: ErrorCode = ErrorCode::new(2023);
/// Session window state exceeded its configured open-session bound.
pub const RS_2024: ErrorCode = ErrorCode::new(2024);
/// Query-time DataFusion source scan exceeded its configured bounded row/byte budget.
pub const RS_2025: ErrorCode = ErrorCode::new(2025);
/// Query-time DataFusion planning or execution failed for an ad hoc query.
pub const RS_2026: ErrorCode = ErrorCode::new(2026);
/// CREATE INDEX automatic backfill scan exceeded its configured bounded row budget.
pub const RS_2027: ErrorCode = ErrorCode::new(2027);
/// Late-data side-channel queue reached its configured bound / scatter topology unavailable.
pub const RS_2028: ErrorCode = ErrorCode::new(2028);
/// Query-time scatter scan exceeded pathological row/byte budget.
pub const RS_2029: ErrorCode = ErrorCode::new(2029);
/// Factorized payload exceeded its configured row or byte bound / scatter frontier mismatch.
pub const RS_2030: ErrorCode = ErrorCode::new(2030);
/// Result set exceeded max_in_flight_rows bound.
pub const RS_2040: ErrorCode = ErrorCode::new(2040);
/// Query was cancelled by a client CancelRequest.
pub const RS_2050: ErrorCode = ErrorCode::new(2050);
/// Cursor does not exist.
pub const RS_2051: ErrorCode = ErrorCode::new(2051);
/// Cursor already exists or cursor limit exceeded.
pub const RS_2052: ErrorCode = ErrorCode::new(2052);
/// Per-connection memory limit exceeded.
pub const RS_2053: ErrorCode = ErrorCode::new(2053);
/// Query exceeded the configured statement timeout.
pub const RS_2054: ErrorCode = ErrorCode::new(2054);
/// Server-wide connection limit reached.
pub const RS_2055: ErrorCode = ErrorCode::new(2055);
/// Malformed INSERT VALUES list or schema mismatch.
pub const RS_2056: ErrorCode = ErrorCode::new(2056);
/// Commit epoch reached u64::MAX.
pub const RS_2060: ErrorCode = ErrorCode::new(2060);
/// Unauthenticated: request missing or carrying invalid credentials.
pub const RS_2400: ErrorCode = ErrorCode::new(2400);
/// Permission denied: authenticated principal lacks required RBAC role.
pub const RS_2401: ErrorCode = ErrorCode::new(2401);
/// Namespace access denied: cross-namespace access attempt by non-admin principal.
pub const RS_2402: ErrorCode = ErrorCode::new(2402);
/// --auth=mtls configured without tls_ca_cert_path; gateway refused to start.
pub const RS_2403: ErrorCode = ErrorCode::new(2403);
/// mTLS connection has no verified client certificate CN for its peer address.
pub const RS_2404: ErrorCode = ErrorCode::new(2404);
/// Gateway TLS certificate/key/CA material failed to load or parse.
pub const RS_2405: ErrorCode = ErrorCode::new(2405);
/// mTLS handshake rejected: connection identity map is at capacity.
pub const RS_2406: ErrorCode = ErrorCode::new(2406);
/// Internal mTLS connection rejected: client certificate required.
pub const RS_2410: ErrorCode = ErrorCode::new(2410);
/// Internal mTLS client certificate invalid, expired, or signed by an untrusted CA.
pub const RS_2411: ErrorCode = ErrorCode::new(2411);
/// Presented client certificate node identity does not match registration payload.
pub const RS_2412: ErrorCode = ErrorCode::new(2412);
/// Internal mTLS certificate rotation or reload failed.
pub const RS_2413: ErrorCode = ErrorCode::new(2413);
/// Secret not found in secret catalog.
pub const RS_2420: ErrorCode = ErrorCode::new(2420);
/// Secret already exists in catalog.
pub const RS_2421: ErrorCode = ErrorCode::new(2421);
/// Secret encryption or envelope DEK wrap/unwrap failed.
pub const RS_2422: ErrorCode = ErrorCode::new(2422);
/// Secret token is invalid, expired, or failed node-identity verification.
pub const RS_2423: ErrorCode = ErrorCode::new(2423);
/// Secret DDL syntax or configuration is invalid.
pub const RS_2424: ErrorCode = ErrorCode::new(2424);
/// Secret or KEK rotation failed.
pub const RS_2425: ErrorCode = ErrorCode::new(2425);
/// Secret drop rejected because it is in active use by a source or sink.
pub const RS_2426: ErrorCode = ErrorCode::new(2426);
/// COPY target table does not exist in the catalog.
pub const RS_2500: ErrorCode = ErrorCode::new(2500);
/// COPY row field count does not match declared column count or invalid encoding.
pub const RS_2501: ErrorCode = ErrorCode::new(2501);
/// Query cannot run inside a failed transaction block.
pub const RS_2560: ErrorCode = ErrorCode::new(2560);
/// Savepoint does not exist.
pub const RS_2561: ErrorCode = ErrorCode::new(2561);
/// PREPARE TRANSACTION / XA two-phase commit is not supported.
pub const RS_2562: ErrorCode = ErrorCode::new(2562);
/// Per-transaction savepoint limit exceeded.
pub const RS_2563: ErrorCode = ErrorCode::new(2563);
/// Notify channel limit exceeded.
pub const RS_2564: ErrorCode = ErrorCode::new(2564);
/// Prepared statement limit exceeded for this connection.
pub const RS_2600: ErrorCode = ErrorCode::new(2600);
/// Portal limit exceeded for this connection.
pub const RS_2601: ErrorCode = ErrorCode::new(2601);
/// Shard writer fenced out: lease lost.
pub const RS_3001: ErrorCode = ErrorCode::new(3001);
/// Pipeline blocked: object store brownout, local buffer exhausted.
pub const RS_3003: ErrorCode = ErrorCode::new(3003);
/// Self-fencing configuration invalid: self_fence_after constraint violated.
pub const RS_3005: ErrorCode = ErrorCode::new(3005);
/// Merge operand malformed.
pub const RS_3009: ErrorCode = ErrorCode::new(3009);
/// Legacy durable shuffle error (retired, use RS-3011..3016).
pub const RS_3010: ErrorCode = ErrorCode::new(3010);
/// Durable shuffle rate-limit retry budget exhausted.
pub const RS_3011: ErrorCode = ErrorCode::new(3011);
/// Durable shuffle generic object-store I/O failure.
pub const RS_3012: ErrorCode = ErrorCode::new(3012);
/// Durable shuffle in-memory buffer capacity exceeded.
pub const RS_3013: ErrorCode = ErrorCode::new(3013);
/// Durable shuffle footer serialization failed.
pub const RS_3014: ErrorCode = ErrorCode::new(3014);
/// Durable shuffle footer deserialization failed.
pub const RS_3015: ErrorCode = ErrorCode::new(3015);
/// Durable shuffle footer is corrupt or undersized.
pub const RS_3016: ErrorCode = ErrorCode::new(3016);
/// Exchange IPC shuffle decode error.
pub const RS_3017: ErrorCode = ErrorCode::new(3017);
/// Exchange loopback route target shard has no active ShardDb.
pub const RS_3018: ErrorCode = ErrorCode::new(3018);
/// Same-host shared-memory segment unavailable; exchange fell back to the direct path.
pub const RS_3019: ErrorCode = ErrorCode::new(3019);
/// Shuffle payload codec is unknown or decompression failed.
pub const RS_3020: ErrorCode = ErrorCode::new(3020);
/// Worker locality metadata is missing or stale; exchange fell back to the safe route.
pub const RS_3021: ErrorCode = ErrorCode::new(3021);
/// Cluster checkpoint manifest codec decode error.
pub const RS_3022: ErrorCode = ErrorCode::new(3022);
/// Fast-path shuffle frontier read failed during replay dedup.
pub const RS_3023: ErrorCode = ErrorCode::new(3023);
/// Shuffle frame row budget exceeded worker.max_rows_per_quantum.
pub const RS_3024: ErrorCode = ErrorCode::new(3024);
/// Unverified platform or compatible backend environment warning.
pub const RS_3025: ErrorCode = ErrorCode::new(3025);
/// Insecure container execution (root user or writable rootfs).
pub const RS_3026: ErrorCode = ErrorCode::new(3026);
/// Platform port conflict on required listener port.
pub const RS_3027: ErrorCode = ErrorCode::new(3027);
/// Unsupported host platform, architecture, OS, or filesystem.
pub const RS_3028: ErrorCode = ErrorCode::new(3028);
/// Incompatible external database or broker version.
pub const RS_3029: ErrorCode = ErrorCode::new(3029);
/// Capacity sample batch flush failure.
pub const RS_3030: ErrorCode = ErrorCode::new(3030);
/// Invalid EXPLAIN INCREMENTAL ESTIMATE query or options.
pub const RS_3031: ErrorCode = ErrorCode::new(3031);
/// Release candidate qualification gate rejected candidate evidence.
pub const RS_3032: ErrorCode = ErrorCode::new(3032);
/// Anti-cheat harness mutation or invalid execution detected.
pub const RS_3033: ErrorCode = ErrorCode::new(3033);
/// Merge-law accumulator wire bytes have the wrong size.
pub const RS_3501: ErrorCode = ErrorCode::new(3501);
/// Checkpoint alignment buffer overflowed; bounded buffer capacity exceeded.
pub const RS_3601: ErrorCode = ErrorCode::new(3601);
/// Cluster checkpoint recovery in progress.
pub const RS_3602: ErrorCode = ErrorCode::new(3602);
/// Pipeline freshness recovery SLO exceeded; RECOVERING_SLOW state.
pub const RS_3603: ErrorCode = ErrorCode::new(3603);
/// Worker drain in progress; new shard assignments rejected.
pub const RS_3604: ErrorCode = ErrorCode::new(3604);
/// Shard load factor exceeds skew threshold; adaptive re-sharding scheduled.
pub const RS_3605: ErrorCode = ErrorCode::new(3605);
/// Worker drain deadline exceeded; worker self-fenced.
pub const RS_3606: ErrorCode = ErrorCode::new(3606);
/// Schema change requires blue/green clone; in-place apply rejected.
pub const RS_3607: ErrorCode = ErrorCode::new(3607);
/// A blue/green clone operation is already in progress for this view.
pub const RS_3608: ErrorCode = ErrorCode::new(3608);
/// Clone backfill lag exceeded the allowed threshold before flip.
pub const RS_3609: ErrorCode = ErrorCode::new(3609);
/// Worker drain target does not exist in the current topology.
pub const RS_3610: ErrorCode = ErrorCode::new(3610);
/// Worker drain cannot proceed because no active recipient worker is available.
pub const RS_3611: ErrorCode = ErrorCode::new(3611);
/// Worker drain queue reached its configured bound; backpressure applied.
pub const RS_3612: ErrorCode = ErrorCode::new(3612);
/// View is waiting on source/frontier progress.
pub const RS_3701: ErrorCode = ErrorCode::new(3701);
/// View admission rejected by quota controls.
pub const RS_3702: ErrorCode = ErrorCode::new(3702);
/// View lag is dominated by spill delay.
pub const RS_3703: ErrorCode = ErrorCode::new(3703);
/// View is in over-budget relaxed mode.
pub const RS_3704: ErrorCode = ErrorCode::new(3704);
/// View checkpoint alignment is stalled.
pub const RS_3705: ErrorCode = ErrorCode::new(3705);
/// View sink commit path is blocked.
pub const RS_3706: ErrorCode = ErrorCode::new(3706);
/// View topology transition is in progress.
pub const RS_3707: ErrorCode = ErrorCode::new(3707);
/// View is recovering from checkpoint/reassignment work.
pub const RS_3708: ErrorCode = ErrorCode::new(3708);
/// Source connection failed or table already exists.
pub const RS_4001: ErrorCode = ErrorCode::new(4001);
/// Sink write failed.
pub const RS_4002: ErrorCode = ErrorCode::new(4002);
/// Sink 2PC pre-commit failed; epoch not staged.
pub const RS_4003: ErrorCode = ErrorCode::new(4003);
/// Sink 2PC commit failed after pre-commit; recovery required.
pub const RS_4004: ErrorCode = ErrorCode::new(4004);
/// Sink 2PC duplicate delivery detected and suppressed.
pub const RS_4005: ErrorCode = ErrorCode::new(4005);
/// Source-epoch registry full; too many uncommitted epochs in flight.
pub const RS_4006: ErrorCode = ErrorCode::new(4006);
/// CREATE SINK DDL parse or validation failed.
pub const RS_4007: ErrorCode = ErrorCode::new(4007);
/// CREATE SOURCE DDL parse or validation failed.
pub const RS_4008: ErrorCode = ErrorCode::new(4008);
/// Source not found.
pub const RS_4009: ErrorCode = ErrorCode::new(4009);
/// Source already exists.
pub const RS_4010: ErrorCode = ErrorCode::new(4010);
/// PostgreSQL CDC replication cannot proceed without recovery.
pub const RS_4011: ErrorCode = ErrorCode::new(4011);
/// Source owner registration requires checkpoint recovery.
pub const RS_4012: ErrorCode = ErrorCode::new(4012);
/// PostgreSQL CDC protocol or ownership validation failed.
pub const RS_4013: ErrorCode = ErrorCode::new(4013);
/// Source bounded in-flight capacity was exceeded.
pub const RS_4014: ErrorCode = ErrorCode::new(4014);
/// Source checkpoint fence did not advance monotonically.
pub const RS_4015: ErrorCode = ErrorCode::new(4015);
/// Source checkpoint acknowledgement failed.
pub const RS_4016: ErrorCode = ErrorCode::new(4016);
/// Connector has been removed.
pub const RS_4017: ErrorCode = ErrorCode::new(4017);
/// Source epoch exhausted.
pub const RS_4018: ErrorCode = ErrorCode::new(4018);
/// Source backfill cursor or lifecycle is invalid.
pub const RS_4019: ErrorCode = ErrorCode::new(4019);
/// Backfill live-delta buffer is full.
pub const RS_4020: ErrorCode = ErrorCode::new(4020);
/// Backfill admission reservation rejected.
pub const RS_4021: ErrorCode = ErrorCode::new(4021);
/// Materialized view backfill is not published.
pub const RS_4022: ErrorCode = ErrorCode::new(4022);
/// Quota or backpressure refusal during ingestion burst.
pub const RS_4029: ErrorCode = ErrorCode::new(4029);
/// Incompatible storage format.
pub const RS_5001: ErrorCode = ErrorCode::new(5001);
/// Unknown merge law in arrangement header.
pub const RS_5002: ErrorCode = ErrorCode::new(5002);
/// Legacy validation failure.
pub const RS_5003: ErrorCode = ErrorCode::new(5003);
/// Quota counter overflow detected.
pub const RS_5004: ErrorCode = ErrorCode::new(5004);
/// Resource usage budget warning (80% threshold reached).
pub const RS_5018: ErrorCode = ErrorCode::new(5018);
/// Resource usage budget critical (95% threshold reached).
pub const RS_5019: ErrorCode = ErrorCode::new(5019);
/// Wire protocol version not supported; rolling upgrade version skew.
pub const RS_5021: ErrorCode = ErrorCode::new(5021);
/// Object store latency or amplification budget breach.
pub const RS_5022: ErrorCode = ErrorCode::new(5022);
/// Window partition size exceeded skew warning threshold.
pub const RS_5023: ErrorCode = ErrorCode::new(5023);
/// Illegal shard-migration state transition rejected.
pub const RS_5030: ErrorCode = ErrorCode::new(5030);
/// Shard-migration verify scan window exceeded its configured bound.
pub const RS_5031: ErrorCode = ErrorCode::new(5031);
/// Shard-migration bucket-map version or watcher acknowledgement mismatch.
pub const RS_5032: ErrorCode = ErrorCode::new(5032);
/// Donor reclamation is not frontier-safe in current state.
pub const RS_5033: ErrorCode = ErrorCode::new(5033);
/// Migration verification divergence detected for key.
pub const RS_5034: ErrorCode = ErrorCode::new(5034);
/// Skew-bound SLO cannot be met without composable partial-state splitting.
pub const RS_5035: ErrorCode = ErrorCode::new(5035);
/// Non-composable hot key routed to a single spill shard.
pub const RS_5036: ErrorCode = ErrorCode::new(5036);
/// Incompatible upstream schema evolution detected.
pub const RS_6001: ErrorCode = ErrorCode::new(6001);
/// Frontier aggregator shard registry is full; new shard reports rejected.
pub const RS_8001: ErrorCode = ErrorCode::new(8001);
/// Stale fencing token on frontier-aggregator publisher-lease CAS or publish.
pub const RS_8002: ErrorCode = ErrorCode::new(8002);
/// Sync-flush-before-lease-handoff-read violation on frontier publication.
pub const RS_8003: ErrorCode = ErrorCode::new(8003);
/// Admission control rejected the capacity request.
pub const RS_9001: ErrorCode = ErrorCode::new(9001);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_display() {
        assert_eq!(RS_0001.to_string(), "RS-0001");
        assert_eq!(RS_1002.to_string(), "RS-1002");
        assert_eq!(RS_5001.to_string(), "RS-5001");
    }

    #[test]
    fn error_code_value() {
        assert_eq!(RS_0001.value(), 1);
        assert_eq!(RS_2003.value(), 2003);
    }

    #[test]
    fn description_known_codes() {
        assert_eq!(description(RS_0001), "Internal error");
        assert_eq!(description(RS_1002), "Incompatible schema change");
        assert_eq!(description(RS_5001), "Incompatible storage format");
        assert_eq!(
            description(RS_5021),
            "Wire protocol version not supported; rolling upgrade version skew"
        );
        assert_eq!(
            next_steps(RS_5001),
            "Run rockstream migrate --from=N --to=M --storage=<url> before upgrading the binary."
        );
        assert_eq!(
            next_steps(RS_5021),
            "Use a peer with an overlapping protocol range, or finish the rolling upgrade before retrying."
        );
    }

    #[test]
    fn description_unknown_code() {
        assert_eq!(description(ErrorCode::new(9999)), "Unknown error");
    }

    #[test]
    fn all_codes_have_descriptions_and_actionable_next_steps() {
        let catalog = ErrorCatalog::current();
        assert!(!catalog.errors().is_empty());
        for desc in catalog.errors() {
            assert_ne!(
                description(desc.code),
                "Unknown error",
                "Code {} has no description",
                desc.code
            );
            assert_ne!(
                next_steps(desc.code),
                "See documentation for this error code.",
                "Code {} has no actionable next steps",
                desc.code
            );
            assert!(
                !next_steps(desc.code).is_empty(),
                "Code {} has empty next steps",
                desc.code
            );
        }
    }

    #[test]
    fn debugger_error_codes_registered() {
        assert_eq!(RS_1020.value(), 1020);
        assert_eq!(RS_1021.value(), 1021);

        assert_eq!(slug(RS_1020), "operator.not_found");
        assert_eq!(slug(RS_1021), "arrangement.key_decode_failed");

        assert_eq!(description(RS_1020), "Operator not found in pipeline");
        assert_eq!(
            description(RS_1021),
            "Arrangement key decoding failed or unsupported"
        );

        assert_eq!(severity(RS_1020), Severity::Error);
        assert_eq!(severity(RS_1021), Severity::Error);

        assert!(next_steps(RS_1020).contains("explain <view> --op-ids"));
        assert!(next_steps(RS_1021).contains("key syntax"));
    }

    #[test]
    fn connector_error_codes_registered_and_actionable() {
        let expected = [
            (RS_4001, "source.connection_failed", "Source connection failed or table already exists", "Verify source connection settings and network connectivity."),
            (RS_4002, "sink.write_failed", "Sink write failed", "Check sink availability and credentials."),
            (RS_4003, "sink.pre_commit_failed", "Sink 2PC pre-commit failed; epoch not staged", "Retry the epoch; check sink connector health and connectivity."),
            (RS_4004, "sink.commit_failed", "Sink 2PC commit failed after pre-commit; recovery required", "Trigger manual recovery or restart the connector; check sink idempotency profile."),
            (RS_4005, "sink.duplicate_delivery", "Sink 2PC duplicate delivery detected and suppressed", "This is informational; the duplicate was suppressed. Check the source for duplicate delivery."),
            (RS_4006, "source.epoch_registry_full", "Source-epoch registry full; too many uncommitted epochs in flight", "Reduce source epoch rate or increase max_in_flight_source_epochs."),
            (RS_4007, "sink.ddl_invalid", "CREATE SINK DDL parse or validation failed", "Check CREATE SINK syntax, referenced view name, and WITH option types; use catalog=filesystem|glue|rest|hive|ducklake."),
            (RS_4008, "source.ddl_invalid", "CREATE SOURCE DDL parse or validation failed", "Check CREATE SOURCE syntax, connector options, and source credentials."),
            (RS_4009, "source.not_found", "Source not found", "Check the source name and ensure it has been created."),
            (RS_4010, "source.already_exists", "Source already exists", "Use a different source name or drop the existing source first."),
            (RS_4011, "postgres_cdc.recovery_required", "PostgreSQL CDC replication cannot proceed without recovery", "Repair the PostgreSQL slot or publication, then run the bounded resnapshot workflow."),
            (RS_4012, "source.owner_recovery_required", "Source owner registration requires checkpoint recovery", "Run checkpoint recovery before registering the source owner, then retry owner registration."),
            (RS_4013, "postgres_cdc.protocol_error", "PostgreSQL CDC protocol or ownership validation failed", "Validate pgoutput protocol, source identity, slot ownership, and durable routing before retrying."),
            (RS_4014, "source.bounds_exceeded", "Source bounded in-flight capacity was exceeded", "Drain the source or reduce transaction and epoch size before increasing the configured bound."),
            (RS_4015, "source.fence_mismatch", "Source checkpoint fence did not advance monotonically", "Recover the highest committed source checkpoint and retry with the next fenced epoch."),
            (RS_4016, "source.acknowledgement_failed", "Source checkpoint acknowledgement failed", "Retain source ownership, recover the committed checkpoint, and retry upstream acknowledgement."),
            (RS_4017, "connector.removed", "Connector has been removed", "Use an external loader through pgwire or Kafka for S3 input, an external HTTP-to-Kafka (or HTTP-to-PostgreSQL) adapter for webhooks, or RockStream to Kafka to a downstream writer for sink output."),
            (RS_4018, "source.epoch_exhausted", "Source epoch exhausted", "Create a new connector before retrying."),
            (RS_4019, "source.backfill_cursor_invalid", "Source backfill cursor or lifecycle is invalid", "Recover or recreate the committed backfill cursor or lifecycle, then retry."),
            (RS_4020, "backfill.live_delta_buffer_full", "Backfill live-delta buffer is full", "Wait for snapshot catch-up or reduce live-delta volume before retrying."),
            (RS_4021, "backfill.admission_rejected", "Backfill admission reservation rejected", "Wait for a backfill to finish or reduce BACKFILL_LIVE_DELTA_MAX_BYTES before retrying."),
            (RS_4022, "backfill.not_published", "Materialized view backfill is not published", "Run SHOW BACKFILL STATUS and retry after the materialized view reaches RUNNING, or create it first."),
        ];

        for (code, expected_slug, expected_description, expected_next_steps) in expected {
            assert_eq!(code.to_string(), format!("RS-{:04}", code.value()));
            assert_eq!(slug(code), expected_slug);
            assert_eq!(description(code), expected_description);
            assert_eq!(next_steps(code), expected_next_steps);
        }
    }

    #[test]
    fn auth_error_codes_registered() {
        assert_eq!(RS_2400.value(), 2400);
        assert_eq!(RS_2401.value(), 2401);
        assert_eq!(RS_2402.value(), 2402);

        assert_ne!(description(RS_2400), "Unknown error");
        assert_ne!(description(RS_2401), "Unknown error");
        assert_ne!(description(RS_2402), "Unknown error");

        assert_eq!(slug(RS_2400), "auth.unauthenticated");
        assert_eq!(slug(RS_2401), "auth.permission_denied");
        assert_eq!(slug(RS_2402), "auth.namespace_access_denied");
    }

    #[test]
    fn gateway_tls_error_codes_registered() {
        assert_eq!(RS_2403.value(), 2403);
        assert_eq!(RS_2404.value(), 2404);
        assert_eq!(RS_2405.value(), 2405);

        assert_eq!(slug(RS_2403), "auth.mtls_requires_ca_cert");
        assert_eq!(slug(RS_2404), "auth.mtls_no_verified_cert");
        assert_eq!(slug(RS_2405), "auth.tls_config_invalid");

        assert_ne!(description(RS_2403), "Unknown error");
        assert_ne!(description(RS_2404), "Unknown error");
        assert_ne!(description(RS_2405), "Unknown error");

        assert_eq!(severity(RS_2403), Severity::Fatal);
        assert_eq!(severity(RS_2404), Severity::Fatal);
        assert_eq!(severity(RS_2405), Severity::Fatal);

        assert!(next_steps(RS_2403).contains("tls-ca-cert-path"));
        assert!(next_steps(RS_2404).contains("client certificate"));
        assert!(next_steps(RS_2405).contains("PEM-encoded"));
    }

    #[test]
    fn skew_error_codes_registered() {
        assert_eq!(RS_5035.value(), 5035);
        assert_eq!(RS_5036.value(), 5036);

        assert_eq!(
            description(RS_5035),
            "Skew-bound SLO cannot be met without composable partial-state splitting"
        );
        assert_eq!(
            description(RS_5036),
            "Non-composable hot key routed to a single spill shard"
        );
        assert_eq!(severity(RS_5035), Severity::Error);
        assert_eq!(severity(RS_5036), Severity::Warning);
        assert!(next_steps(RS_5035).contains("composable partial-state semantics"));
        assert!(next_steps(RS_5036).contains("single spill shard"));
    }

    #[test]
    fn advanced_dml_and_scatter_pruning_error_codes_registered() {
        assert_eq!(RS_2013.value(), 2013);
        assert_eq!(RS_2017.value(), 2017);
        assert_eq!(RS_2022.value(), 2022);

        assert_eq!(slug(RS_2013), "transaction.returning_key_not_found");
        assert_eq!(slug(RS_2017), "shard_stats.too_stale");
        assert_eq!(slug(RS_2022), "write.malformed_returning_clause");

        assert_eq!(severity(RS_2017), Severity::Warning);
        assert!(description(RS_2017).contains("too stale"));
        assert!(next_steps(RS_2017).contains("shard_stats"));
        assert!(description(RS_2022).contains("RETURNING"));
        assert!(next_steps(RS_2013).contains("frontier"));
    }

    #[test]
    fn degradation_reason_error_codes_registered() {
        let expected = [
            (
                RS_3701,
                "view.waiting_on_source",
                "View is waiting on source/frontier progress",
            ),
            (
                RS_3702,
                "view.quota_admission_rejected",
                "View admission rejected by quota controls",
            ),
            (
                RS_3703,
                "view.spilling",
                "View lag is dominated by spill delay",
            ),
            (
                RS_3704,
                "view.over_budget_relaxed",
                "View is in over-budget relaxed mode",
            ),
            (
                RS_3705,
                "view.checkpoint_alignment_stalled",
                "View checkpoint alignment is stalled",
            ),
            (
                RS_3706,
                "view.sink_blocked",
                "View sink commit path is blocked",
            ),
            (
                RS_3707,
                "view.topology_transition_in_progress",
                "View topology transition is in progress",
            ),
            (
                RS_3708,
                "view.recovering",
                "View is recovering from checkpoint/reassignment work",
            ),
        ];
        for (code, expected_slug, expected_description) in expected {
            assert_eq!(slug(code), expected_slug);
            assert_eq!(description(code), expected_description);
            assert_eq!(severity(code), Severity::Warning);
            assert!(!next_steps(code).is_empty());
        }
    }

    #[test]
    fn cli_safeguard_error_codes_registered() {
        assert_eq!(RS_0005.value(), 5);
        assert_eq!(slug(RS_0005), "cli.confirmation_required");
        assert_eq!(
            description(RS_0005),
            "Destructive command confirmation required"
        );
        assert_eq!(severity(RS_0005), Severity::Error);
        assert!(next_steps(RS_0005).contains("--yes"));
    }

    #[test]
    fn internal_mtls_error_codes_registered() {
        assert_eq!(RS_2410.value(), 2410);
        assert_eq!(RS_2411.value(), 2411);
        assert_eq!(RS_2412.value(), 2412);
        assert_eq!(RS_2413.value(), 2413);

        assert_eq!(slug(RS_2410), "auth.internal_mtls_required");
        assert_eq!(slug(RS_2411), "auth.internal_mtls_invalid_cert");
        assert_eq!(slug(RS_2412), "auth.internal_mtls_node_identity_mismatch");
        assert_eq!(slug(RS_2413), "auth.internal_mtls_rotation_failed");

        assert_eq!(severity(RS_2410), Severity::Fatal);
        assert_eq!(severity(RS_2411), Severity::Fatal);
        assert_eq!(severity(RS_2412), Severity::Fatal);
        assert_eq!(severity(RS_2413), Severity::Error);

        assert!(next_steps(RS_2410).contains("internal-tls"));
        assert!(next_steps(RS_2411).contains("cluster CA"));
        assert!(next_steps(RS_2412).contains("Common Name"));
        assert!(next_steps(RS_2413).contains("rotation"));
    }

    #[test]
    fn secrets_error_codes_registered() {
        let codes = [
            (RS_2420, 2420, "secret.not_found", Severity::Error),
            (RS_2421, 2421, "secret.already_exists", Severity::Error),
            (RS_2422, 2422, "secret.encryption_failed", Severity::Fatal),
            (RS_2423, 2423, "secret.token_invalid", Severity::Error),
            (RS_2424, 2424, "secret.ddl_invalid", Severity::Error),
            (RS_2425, 2425, "secret.rotation_failed", Severity::Fatal),
            (
                RS_2426,
                2426,
                "secret.in_use_by_source_or_sink",
                Severity::Error,
            ),
        ];

        for (code, val, expected_slug, expected_sev) in codes {
            assert_eq!(code.value(), val);
            assert_eq!(slug(code), expected_slug);
            assert_eq!(severity(code), expected_sev);
            assert!(!description(code).is_empty());
            assert!(!next_steps(code).is_empty());
        }
    }

    #[test]
    fn qualification_error_codes_registered() {
        let codes = [
            (
                RS_3032,
                3032,
                "qualification.release_gate_rejection",
                Severity::Error,
            ),
            (
                RS_3033,
                3033,
                "qualification.harness_invalidation",
                Severity::Fatal,
            ),
        ];

        for (code, val, expected_slug, expected_sev) in codes {
            assert_eq!(code.value(), val);
            assert_eq!(slug(code), expected_slug);
            assert_eq!(severity(code), expected_sev);
            assert!(!description(code).is_empty());
            assert!(!next_steps(code).is_empty());
        }
    }
}
