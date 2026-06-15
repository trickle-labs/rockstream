//! Error-code registry for RockStream.
//!
//! Every user-visible or operator-visible failure carries an `RS-XXXX` code.
//! This module defines the canonical registry.

use std::fmt;

/// Severity level for an error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

/// An error code in the `RS-XXXX` format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorCode(u16);

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

// ─── Registry ────────────────────────────────────────────────────────────────

// 0xxx: Internal / general
/// Internal error.
pub const RS_0001: ErrorCode = ErrorCode::new(1);
/// Configuration error.
pub const RS_0002: ErrorCode = ErrorCode::new(2);
/// Storage unavailable.
pub const RS_0003: ErrorCode = ErrorCode::new(3);

// 1xxx: Pipeline / plan
/// Pipeline not found.
pub const RS_1001: ErrorCode = ErrorCode::new(1001);
/// Incompatible schema change.
pub const RS_1002: ErrorCode = ErrorCode::new(1002);
/// Record decode error (DLQ).
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
/// Non-monotone delta rejected in monotone recursion (DRed escape hatch).
pub const RS_1009: ErrorCode = ErrorCode::new(1009);
/// Bootstrap interrupted; connector position lost and cannot resume.
pub const RS_1010: ErrorCode = ErrorCode::new(1010);
/// View-on-view DAG contains a cycle; rejected at compile time.
pub const RS_1011: ErrorCode = ErrorCode::new(1011);
/// SQL statement could not be parsed (v0.7).
pub const RS_1012: ErrorCode = ErrorCode::new(1012);
/// Query contains a feature not supported by the incremental planner (v0.7).
pub const RS_1013: ErrorCode = ErrorCode::new(1013);
/// Inner-frontier stall in distributed recursion; per-shard recompute triggered (v0.33).
pub const RS_1512: ErrorCode = ErrorCode::new(1512);
/// Distributed recursion max-iteration cap exceeded without convergence (v0.33).
pub const RS_1513: ErrorCode = ErrorCode::new(1513);
/// Checkpoint alignment buffer overflowed; bounded buffer capacity exceeded (v0.34).
pub const RS_3601: ErrorCode = ErrorCode::new(3601);
/// Cluster checkpoint recovery in progress; pipeline is in RECOVERING state (v0.34).
pub const RS_3602: ErrorCode = ErrorCode::new(3602);
/// Pipeline freshness recovery is slower than the 60s SLO; RECOVERING_SLOW state (v0.35).
pub const RS_3603: ErrorCode = ErrorCode::new(3603);

// 1015-1017: Aggregate operators (v0.5 / v0.6)
/// Group-commit capacity exceeded; operator queue is full and back-pressure is applied.
/// next_steps: reduce epoch rate, increase GROUP_COMMIT_MAX_BATCHES, or add more shards.
pub const RS_1015: ErrorCode = ErrorCode::new(1015);
/// Aggregate running sum overflowed i64; epoch rejected.
/// next_steps: reduce value magnitudes or switch to a wider numeric type.
pub const RS_1016: ErrorCode = ErrorCode::new(1016);
/// MIN/MAX multiset retraction underflow: a value was retracted that has no positive weight.
/// next_steps: ensure every retraction is matched by a prior insertion; check source ordering.
pub const RS_1017: ErrorCode = ErrorCode::new(1017);

// 17xx: Lease management
/// Shard is already leased by a different worker; acquire rejected (v0.29).
pub const RS_1701: ErrorCode = ErrorCode::new(1701);
/// Stale lease token; worker has been fenced out (v0.29).
pub const RS_1702: ErrorCode = ErrorCode::new(1702);
/// Shard has no active lease (v0.29).
pub const RS_1703: ErrorCode = ErrorCode::new(1703);

// 2xxx: Gateway / query
/// View not found.
pub const RS_2001: ErrorCode = ErrorCode::new(2001);
/// Query timeout.
pub const RS_2002: ErrorCode = ErrorCode::new(2002);
/// Unsupported isolation level.
pub const RS_2003: ErrorCode = ErrorCode::new(2003);
/// Cannot drop inline view: dependent materialized views still exist (v0.40).
pub const RS_2004: ErrorCode = ErrorCode::new(2004);
/// Query rate limit exceeded; client is sending too many requests (v0.40).
pub const RS_2005: ErrorCode = ErrorCode::new(2005);
/// Historical query references an epoch before the checkpoint retention window (v0.42).
pub const RS_2006: ErrorCode = ErrorCode::new(2006);
/// Idempotency key required for non-idempotent write (v0.44).
pub const RS_2007: ErrorCode = ErrorCode::new(2007);
/// Optimistic transaction conflict detected; a concurrent write committed to the same key (v0.43).
pub const RS_2008: ErrorCode = ErrorCode::new(2008);
/// Index is building.
pub const RS_2014: ErrorCode = ErrorCode::new(2014);
/// Index has exceeded max lag.
pub const RS_2015: ErrorCode = ErrorCode::new(2015);
/// Index name conflict.
pub const RS_2016: ErrorCode = ErrorCode::new(2016);

// 3xxx: Merge / arrangement
/// Merge operand malformed (fail-closed: never silently overwrites).
pub const RS_3009: ErrorCode = ErrorCode::new(3009);
/// Durable shuffle fallback operation failed.
pub const RS_3010: ErrorCode = ErrorCode::new(3010);
/// Pipeline blocked due to object store brownout; local buffer exhausted (v0.36, DESIGN.md §11.7).
pub const RS_3003: ErrorCode = ErrorCode::new(3003);
/// Worker drain in progress; new shard assignments rejected (v0.38).
pub const RS_3604: ErrorCode = ErrorCode::new(3604);
/// Shard load factor exceeds skew threshold; adaptive re-sharding scheduled (v0.38).
pub const RS_3605: ErrorCode = ErrorCode::new(3605);
/// Worker drain deadline exceeded; worker self-fenced (v0.38).
pub const RS_3606: ErrorCode = ErrorCode::new(3606);
/// Schema change requires blue/green clone/backfill/flip; in-place apply rejected (v0.39).
pub const RS_3607: ErrorCode = ErrorCode::new(3607);
/// A blue/green clone operation is already in progress for this view (v0.39).
pub const RS_3608: ErrorCode = ErrorCode::new(3608);
/// Clone backfill lag exceeded the allowed threshold before flip (v0.39).
pub const RS_3609: ErrorCode = ErrorCode::new(3609);
// 4xxx: Connector
/// Source connection failed.
pub const RS_4001: ErrorCode = ErrorCode::new(4001);
/// Sink write failed.
pub const RS_4002: ErrorCode = ErrorCode::new(4002);

// 5xxx: Upgrade / migration
/// Incompatible storage format.
pub const RS_5001: ErrorCode = ErrorCode::new(5001);
/// Unknown merge law referenced in arrangement header.
pub const RS_5002: ErrorCode = ErrorCode::new(5002);
/// Wire protocol version not supported; rolling upgrade version skew (v0.36, DESIGN.md §5.5).
pub const RS_5003: ErrorCode = ErrorCode::new(5003);
/// Resource usage budget warning (80% threshold reached).
pub const RS_5018: ErrorCode = ErrorCode::new(5018);
/// Resource usage budget critical (95% threshold reached).
pub const RS_5019: ErrorCode = ErrorCode::new(5019);

// 6xxx: Connector schema evolution
/// Incompatible upstream schema evolution detected.
pub const RS_6001: ErrorCode = ErrorCode::new(6001);

// 8xxx: Frontier aggregation (v0.18)
/// Frontier aggregator shard registry is full; new shard reports are rejected.
/// next_steps: scale out aggregators or reduce shard count.
pub const RS_8001: ErrorCode = ErrorCode::new(8001);

/// Metadata for a registered error code.
pub struct ErrorCodeMeta {
    /// The error code.
    pub code: ErrorCode,
    /// Human-readable description.
    pub description: &'static str,
    /// Severity level.
    pub severity: Severity,
    /// Actionable next steps for the operator/user.
    pub next_steps: &'static str,
    /// Documentation URL (relative path within docs site).
    pub doc_url: &'static str,
}

/// Returns a human-readable description for a known error code.
pub fn description(code: ErrorCode) -> &'static str {
    match code.0 {
        1 => "Internal error",
        2 => "Configuration error",
        3 => "Storage unavailable",
        1001 => "Pipeline not found",
        1002 => "Incompatible schema change",
        1003 => "Record decode error",
        1004 => "Pipeline already exists",
        1005 => "Workload not found",
        1006 => "Workload already exists",
        1007 => "View is already paused",
        1008 => "View is not paused",
        1009 => "Non-monotone delta rejected in monotone recursion",
        1010 => "Bootstrap interrupted; connector position lost",
        1011 => "View-on-view DAG contains a cycle",
        1012 => "SQL statement could not be parsed",
        1013 => "Query contains a feature not yet supported by the incremental planner",
        1015 => "Group-commit queue full; back-pressure applied",
        1016 => "Aggregate running sum overflowed i64",
        1017 => "MIN/MAX multiset retraction underflow: value has no positive weight",
        1512 => "Inner-frontier stall in distributed recursion; per-shard recompute triggered",
        1513 => "Distributed recursion max-iteration cap exceeded without convergence",
        3601 => "Checkpoint alignment buffer overflowed; bounded buffer capacity exceeded",
        3602 => "Cluster checkpoint recovery in progress",
        3603 => "Pipeline freshness recovery SLO exceeded; RECOVERING_SLOW state",
        1701 => "Shard is already leased by a different worker",
        1702 => "Stale lease token; worker has been fenced out",
        1703 => "Shard has no active lease",
        2001 => "View not found",
        2002 => "Query timeout",
        2003 => "Unsupported isolation level",
        2004 => "Cannot drop inline view: dependent materialized views still exist",
        2005 => "Query rate limit exceeded",
        2006 => "Historical query beyond checkpoint retention window",
        2007 => "Idempotency key required for non-idempotent write",
        2008 => "Optimistic transaction conflict: a concurrent write committed to the same key",
        2014 => "Index is building",
        2015 => "Index frontier lag exceeded limit",
        2016 => "Index name conflict",
        3003 => "Pipeline blocked: object store brownout, local buffer exhausted",
        3009 => "Merge operand malformed",
        3010 => "Durable shuffle fallback operation failed",
        3604 => "Worker drain in progress; new shard assignments rejected",
        3605 => "Shard load factor exceeds skew threshold; adaptive re-sharding scheduled",
        3606 => "Worker drain deadline exceeded; worker self-fenced",
        3607 => "Schema change requires blue/green clone; in-place apply rejected",
        3608 => "A blue/green clone operation is already in progress for this view",
        3609 => "Clone backfill lag exceeded the allowed threshold before flip",
        4001 => "Source connection failed",
        4002 => "Sink write failed",
        5001 => "Incompatible storage format",
        5002 => "Unknown merge law in arrangement header",
        5003 => "Wire protocol version not supported; rolling upgrade version skew",
        5018 => "Resource usage budget warning (80% threshold reached)",
        5019 => "Resource usage budget critical (95% threshold reached)",
        6001 => "Incompatible upstream schema evolution detected",
        8001 => "Frontier aggregator shard registry is full; new shard reports rejected",
        _ => "Unknown error",
    }
}

/// Returns the severity for a known error code.
pub fn severity(code: ErrorCode) -> Severity {
    match code.0 {
        1 => Severity::Fatal,
        2 => Severity::Error,
        3 => Severity::Error,
        3009 => Severity::Error,
        3010 => Severity::Error,
        5001 => Severity::Fatal,
        5002 => Severity::Fatal,
        5018 => Severity::Warning,
        5019 => Severity::Warning,
        6001 => Severity::Warning,
        _ => Severity::Error,
    }
}

/// Returns actionable next steps for a known error code.
pub fn next_steps(code: ErrorCode) -> &'static str {
    match code.0 {
        1 => "Report this bug with the support bundle.",
        2 => "Check configuration file and CLI flags.",
        3 => "Verify storage directory permissions and disk space.",
        1001 => "Check pipeline name and ensure it has been created.",
        1002 => "Review schema evolution rules; a new view may be required.",
        1003 => "Inspect the dead-letter queue for malformed records.",
        1004 => "Use a different pipeline name or drop the existing one.",
        1005 => "Check the workload name; ensure it has been created with CREATE WORKLOAD.",
        1006 => "Use a different workload name or drop the existing workload first.",
        1007 => "The view is already paused; use RESUME MATERIALIZED VIEW to restart it.",
        1008 => "The view is not paused; only paused views can be resumed.",
        1009 => "Ensure the recursive query is monotone or restructure it; check EXPLAIN for recursion rules.",
        1010 => "Verify connector positions, reset offsets, or perform a full bootstrap rebuild.",
        1011 => "Resolve cycle in view dependencies; view-on-view relations must form a DAG.",
        1012 => "Check SQL syntax; see docs/language-features.md for the supported SQL subset.",
        1013 => "Simplify the query or check docs/language-features.md for the supported incremental SQL subset.",
        1015 => "Reduce epoch rate, increase GROUP_COMMIT_MAX_BATCHES, or add more shards.",
        1016 => "Reduce value magnitudes or switch to a wider numeric type.",
        1017 => "Ensure every retraction is matched by a prior insertion; check source event ordering and idempotency.",
        1512 => "Check the step function for infinite cycles or skewed partitioning; review per-shard recompute logs.",
        1513 => "Increase max_iterations or restructure the recursive query to converge faster.",
        1701 => "Check worker assignments; another worker holds the lease. Use force-acquire if the holder is dead.",
        1702 => "Worker has been fenced out; acquire a new lease before retrying.",
        1703 => "No lease exists for this shard; acquire a lease before operating on it.",
        2001 => "Check view name and ensure the pipeline is running.",
        2002 => "Reduce query scope or increase timeout.",
        2003 => "Use a supported isolation level (snapshot or eventual).",
        2004 => "Drop all dependent materialized views first, or use CASCADE.",
        2005 => "Reduce query rate, bundle queries, or increase tenant concurrency limits.",
        2006 => "Query a more recent epoch or timestamp, or increase the catalog's checkpoint_retention_duration.",
        2007 => "Provide a client-supplied idempotency key or an exactly-once source-epoch envelope.",
        2008 => "Retry the transaction; if conflicts persist, reduce write concurrency or switch to a serializable protocol.",
        2014 => "Wait for index backfill to complete.",
        2015 => "Index is too far behind view. Wait for synchronization or increase index_max_lag_ms.",
        2016 => "An index with the same name already exists.",
        3003 => "Reduce input rate or increase local_buffer_max_epochs; check object store availability.",
        3009 => "Inspect the stored arrangement value; possible data corruption or law version mismatch.",
        3010 => "Verify object store connectivity, credentials, and bucket settings.",
        3601 => "Reduce input rate or increase checkpoint alignment buffer capacity; check for slow shards holding up barrier propagation.",
        3602 => "Wait for recovery to complete; monitor shard reassignment and frontier progress via SHOW VIEW STATUS.",
        3603 => "Recovery is exceeding SLO; check worker health, storage latency, and frontier progress. Escalate if recovery does not complete within expected bounds.",
        3604 => "Wait for worker drain to complete, or target active workers for shard assignment.",
        3605 => "Allow adaptive re-sharding to complete or manually trigger partition splitting.",
        3606 => "Investigate slow network/compaction preventing worker from draining; review worker logs.",
        3607 => "Perform a zero-downtime view replacement using a blue/green deployment strategy.",
        3608 => "Wait for the existing clone backfill to finish before starting a new one.",
        3609 => "Reduce write load or check worker resource usage to allow backfill to catch up before flip.",
        4001 => "Verify source connection settings and network connectivity.",
        4002 => "Check sink availability and credentials.",
        5001 => "Run the storage migration tool before upgrading.",
        5002 => "Register the merge law or migrate the arrangement before attaching the shard.",
        5003 => "Ensure N+1 binary is backward compatible with N; check rolling upgrade procedure in DESIGN.md §5.5.",
        5018 => "Examine view resource usage and plan to scale out cluster capacity or adjust memory limits.",
        5019 => "Immediately free unused view resources or scale cluster capacity to prevent pipeline stalls.",
        6001 => "Apply view replacement or run manual migration to match the new upstream schema.",
        8001 => "Scale out frontier aggregators (add more nodes with --role=frontier) or reduce shard count below the configured limit.",
        _ => "See documentation for this error code.",
    }
}

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
    }

    #[test]
    fn description_unknown_code() {
        assert_eq!(description(ErrorCode::new(9999)), "Unknown error");
    }

    #[test]
    fn all_codes_have_descriptions_and_actionable_next_steps() {
        let codes = [
            RS_0001, RS_0002, RS_0003, RS_1001, RS_1002, RS_1003, RS_1004, RS_1005, RS_1006,
            RS_1007, RS_1008, RS_2001, RS_2002, RS_2003, RS_2004, RS_2005, RS_2006, RS_2007,
            RS_2008, RS_2014, RS_2015, RS_2016, RS_3003, RS_3009, RS_3010, RS_4001, RS_4002,
            RS_5001, RS_5002, RS_5003, RS_1512, RS_1513, RS_3601, RS_3602, RS_3603, RS_1701,
            RS_1702, RS_1703, RS_5018, RS_5019, RS_6001, RS_1015, RS_1016, RS_1017, RS_1012,
            RS_1013, RS_8001,
        ];
        for code in codes {
            assert_ne!(
                description(code),
                "Unknown error",
                "Code {code} has no description"
            );
            assert_ne!(
                next_steps(code),
                "See documentation for this error code.",
                "Code {code} has no actionable next steps"
            );
            assert!(
                !next_steps(code).is_empty(),
                "Code {code} has empty next steps"
            );
        }
    }
}
