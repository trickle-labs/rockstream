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
/// Cluster control plane unreachable.
pub const RS_0004: ErrorCode = ErrorCode::new(4);
/// Destructive command confirmation required.
pub const RS_0005: ErrorCode = ErrorCode::new(5);

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
/// Workload drop rejected because views are still assigned.
pub const RS_1014: ErrorCode = ErrorCode::new(1014);
/// Migration state exceeded its configured timeout budget (v0.46).
pub const RS_1030: ErrorCode = ErrorCode::new(1030);
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
/// TopK buffer overflow: more unique rows than TOPK_BUFFER_LIMIT arrived in one partition.
/// next_steps: reduce partition cardinality, increase TOPK_BUFFER_LIMIT, or add partition columns.
pub const RS_1018: ErrorCode = ErrorCode::new(1018);
/// A `CREATE VIEW`/`CREATE MATERIALIZED VIEW`'s query could not be compiled
/// into an executable operator pipeline (v0.51.4 Slice 8 — there is no
/// DataFusion-materializer fallback left to silently serve it from).
pub const RS_1019: ErrorCode = ErrorCode::new(1019);
/// Operator not found in pipeline (v0.53.2 IVM arrangement debugger).
pub const RS_1020: ErrorCode = ErrorCode::new(1020);
/// Arrangement key decoding failed or unsupported family (v0.53.2 IVM arrangement debugger).
pub const RS_1021: ErrorCode = ErrorCode::new(1021);

// 17xx: Lease management
/// Shard is already leased by a different worker; acquire rejected (v0.29).
pub const RS_1701: ErrorCode = ErrorCode::new(1701);
/// Stale lease token; worker has been fenced out (v0.29).
pub const RS_1702: ErrorCode = ErrorCode::new(1702);
/// Shard has no active lease (v0.29).
pub const RS_1703: ErrorCode = ErrorCode::new(1703);
/// Write rejected: the acting control node is not the current Raft-elected
/// leader (v0.45.2, M7-S2 leader-only write gating).
pub const RS_1731: ErrorCode = ErrorCode::new(1731);

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
/// Shard statistics are older than the configured pruning freshness horizon;
/// the query fell back to a full scatter scan instead of pruning (v0.48).
/// next_steps: "Wait for the next checkpoint to publish fresh shard_stats or increase shard_stats_max_age_checkpoints."
pub const RS_2017: ErrorCode = ErrorCode::new(2017);
/// Published frontier exceeded the session max_staleness bound; query continued with the current frontier (v0.45).
/// next_steps: "Increase rockstream.max_staleness, reduce publish lag, or switch to session_wait_for mode."
pub const RS_2018: ErrorCode = ErrorCode::new(2018);
/// Shard write buffer full — backpressure (v0.24).
/// next_steps: "Wait for downstream IVM processing to drain, then retry COMMIT."
pub const RS_2019: ErrorCode = ErrorCode::new(2019);
/// Transaction RETURNING read-back could not find the expected row at the
/// current frontier (v0.48, DESIGN.md §13.5.2 `transaction.returning_key_not_found`).
pub const RS_2013: ErrorCode = ErrorCode::new(2013);
/// Session wait-for deadline exceeded; query proceeded at current frontier (v0.25).
/// next_steps: "Increase session_wait_for_timeout or reduce write latency."
pub const RS_2012: ErrorCode = ErrorCode::new(2012);
/// Subscribe consumer fell behind the change-log retention window (v0.25).
/// next_steps: "Reconnect with AS OF NOW WITH SNAPSHOT or increase CHANGE_LOG_MAX_ENTRIES."
pub const RS_2020: ErrorCode = ErrorCode::new(2020);
/// COPY FROM STDIN statement is malformed and could not be parsed (v0.45.7).
pub const RS_2021: ErrorCode = ErrorCode::new(2021);
/// `UPDATE`/`DELETE ... RETURNING` clause is malformed and could not be
/// parsed (v0.48, `write.malformed_returning_clause`).
pub const RS_2022: ErrorCode = ErrorCode::new(2022);
/// Hop window state exceeded its configured overlap-aware bound (v0.50).
/// next_steps: "Reduce hop overlap, increase HOP_WINDOW_STATE_LIMIT, or shard the windowed stream more finely."
pub const RS_2023: ErrorCode = ErrorCode::new(2023);
/// Session window state exceeded its configured open-session bound (v0.50).
/// next_steps: "Reduce session cardinality, increase SESSION_WINDOW_STATE_LIMIT, or shard the windowed stream more finely."
pub const RS_2024: ErrorCode = ErrorCode::new(2024);
/// Query-time DataFusion source scan exceeded its configured bounded row/byte budget (v0.51.2).
/// next_steps: "Reduce source-table cardinality, add a LIMIT, or materialize the query into a view."
pub const RS_2025: ErrorCode = ErrorCode::new(2025);
/// Query-time DataFusion planning or execution failed for an ad hoc query (v0.51.2).
/// next_steps: "Simplify the query, validate referenced table/view schemas, or materialize the query into a view."
pub const RS_2026: ErrorCode = ErrorCode::new(2026);
/// `CREATE INDEX` automatic backfill scan exceeded its configured bounded
/// row budget (v0.51.2, `index.backfill_row_limit_exceeded`).
/// next_steps: "Reduce table cardinality before indexing, or drop and recreate the index once the table is smaller."
pub const RS_2027: ErrorCode = ErrorCode::new(2027);
/// Late-data side-channel queue reached its bounded capacity (v0.51.12).
pub const RS_2028: ErrorCode = ErrorCode::new(2028);

// 24xx: Auth (v0.26)
/// Unauthenticated: request missing or carrying invalid credentials.
pub const RS_2400: ErrorCode = ErrorCode::new(2400);
/// Permission denied: authenticated principal lacks required RBAC role.
pub const RS_2401: ErrorCode = ErrorCode::new(2401);
/// Namespace access denied: cross-namespace access attempt by non-admin principal.
pub const RS_2402: ErrorCode = ErrorCode::new(2402);
/// `--auth=mtls` configured without `tls_ca_cert_path`; the gateway refuses
/// to start rather than silently accepting an unauthenticated identity
/// (v0.51.5, `auth.mtls_requires_ca_cert`).
/// next_steps: "Set --tls-ca-cert-path (or gateway.tls_ca_cert_path in rockstream.toml) to the CA that signs client certificates."
pub const RS_2403: ErrorCode = ErrorCode::new(2403);
/// `--auth=mtls` connection reached the application layer with no verified
/// client certificate CN recorded for its peer address (v0.51.5,
/// `auth.mtls_no_verified_cert`). The old spoofable `cn` startup-parameter
/// path has been removed; only a real TLS client-cert handshake can
/// authenticate.
/// next_steps: "Connect with a client certificate signed by the configured CA over sslmode=verify-full; a bare TCP or TLS connection without a client cert cannot use --auth=mtls."
pub const RS_2404: ErrorCode = ErrorCode::new(2404);
/// Gateway TLS certificate/key material configured via `tls_cert_path`/
/// `tls_key_path`/`tls_ca_cert_path` failed to load or parse (v0.51.5,
/// `auth.tls_config_invalid`).
/// next_steps: "Verify the configured paths point to valid PEM-encoded certificate/key files readable by the gateway process."
pub const RS_2405: ErrorCode = ErrorCode::new(2405);

/// mTLS connection cap reached; handshake rejected because `MTLS_CN_BY_PEER_ADDR`
/// is at `MAX_CONNECTIONS` capacity (v0.51.26, `auth.mtls_connection_cap_exceeded`).
/// next_steps: "Reduce concurrent connections or raise MAX_CONNECTIONS. The gateway rejected the handshake to avoid silently dropping the peer identity."
pub const RS_2406: ErrorCode = ErrorCode::new(2406);

/// Internal mTLS connection rejected: client certificate required (v0.55, `auth.internal_mtls_required`).
/// next_steps: "Configure internal TLS client certificates (--internal-tls-cert-path, --internal-tls-key-path, --internal-tls-ca-cert-path) so the node can authenticate with the cluster."
pub const RS_2410: ErrorCode = ErrorCode::new(2410);
/// Internal mTLS client certificate invalid, expired, or signed by an untrusted CA (v0.55, `auth.internal_mtls_invalid_cert`).
/// next_steps: "Verify the internal TLS client certificate is valid, not expired, and signed by the trusted cluster CA root certificate."
pub const RS_2411: ErrorCode = ErrorCode::new(2411);
/// Presented client certificate node identity does not match registration payload (v0.55, `auth.internal_mtls_node_identity_mismatch`).
/// next_steps: "Ensure the node ID and role presented in the internal TLS certificate Common Name / SAN match the node registration parameters."
pub const RS_2412: ErrorCode = ErrorCode::new(2412);
/// Internal mTLS certificate rotation or reload failed (v0.55, `auth.internal_mtls_rotation_failed`).
/// next_steps: "Verify the new certificate and private key files exist, have matching keys, and are signed by a trusted CA."
pub const RS_2413: ErrorCode = ErrorCode::new(2413);

/// Merge operand malformed (fail-closed: never silently overwrites).
pub const RS_3009: ErrorCode = ErrorCode::new(3009);
/// Durable shuffle rate-limit retry budget exhausted (v0.45.7, split from RS-3010).
pub const RS_3011: ErrorCode = ErrorCode::new(3011);
/// Durable shuffle generic object-store I/O failure (v0.45.7, split from RS-3010).
pub const RS_3012: ErrorCode = ErrorCode::new(3012);
/// Durable shuffle in-memory buffer capacity exceeded (v0.45.7, split from RS-3010).
pub const RS_3013: ErrorCode = ErrorCode::new(3013);
/// Durable shuffle footer serialization failed (v0.45.7, split from RS-3010).
pub const RS_3014: ErrorCode = ErrorCode::new(3014);
/// Durable shuffle footer deserialization failed (v0.45.7, split from RS-3010).
pub const RS_3015: ErrorCode = ErrorCode::new(3015);
/// Durable shuffle footer is corrupt or undersized (v0.45.7, split from RS-3010).
pub const RS_3016: ErrorCode = ErrorCode::new(3016);
/// Exchange IPC shuffle decode error (v0.45.7).
pub const RS_3017: ErrorCode = ErrorCode::new(3017);
/// Exchange loopback route target shard has no active `ShardDb` (v0.45.7).
pub const RS_3018: ErrorCode = ErrorCode::new(3018);
/// Same-host shared-memory segment unavailable; exchange fell back to the direct path (v0.49).
/// next_steps: "Check same_host_shm_segment_bytes, same_host_shm_segments_per_peer, and host-level shared-memory permissions/capacity."
pub const RS_3019: ErrorCode = ErrorCode::new(3019);
/// Shuffle payload codec is unknown or decompression failed (v0.49).
/// next_steps: "Verify both peers advertise shuffle_codec_v1, inspect the payload bytes for corruption, and retry after rolling the cluster to a compatible build."
pub const RS_3020: ErrorCode = ErrorCode::new(3020);
/// Worker locality metadata is missing or stale; exchange fell back to the safe route (v0.49).
/// next_steps: "Check worker host_id/availability_zone registration, wait for topology refresh, or force the durable path during the rollout."
pub const RS_3021: ErrorCode = ErrorCode::new(3021);
/// Cluster checkpoint manifest codec is unknown or decompression failed (v0.49).
/// next_steps: "Verify the control-plane capability floor, inspect control: checkpoints/ payloads for corruption, and complete the rolling upgrade before enabling manifest compression."
pub const RS_3022: ErrorCode = ErrorCode::new(3022);
/// Fast-path shuffle frontier read failed while deduplicating a replayed frame (v0.51).
/// With fast-path shuffle WAL elision, the receiver dedups replayed frames against the
/// target shard's committed frontier instead of a persisted inbox entry. A failed read
/// means the receiver could not confirm whether the frame was already reflected.
/// next_steps: "Inspect target shard storage health and the committed frontier key; the frame was delivered conservatively, so verify downstream idempotency if the shard is unhealthy."
pub const RS_3023: ErrorCode = ErrorCode::new(3023);
/// Shuffle frame row budget exceeded the configured `worker.max_rows_per_quantum` bound (v0.51).
/// next_steps: "Reduce exchange batch size or rechunking, or raise worker.max_rows_per_quantum within the worker memory/network budget."
pub const RS_3024: ErrorCode = ErrorCode::new(3024);
/// Pipeline blocked due to object store brownout; local buffer exhausted (v0.36, DESIGN.md §11.7).
pub const RS_3003: ErrorCode = ErrorCode::new(3003);
/// Worker drain in progress; new shard assignments rejected (v0.38).
pub const RS_3604: ErrorCode = ErrorCode::new(3604);
/// Shard load factor exceeds skew threshold; adaptive re-sharding scheduled (v0.38).
pub const RS_3605: ErrorCode = ErrorCode::new(3605);
/// Worker drain deadline exceeded; worker self-fenced (v0.38).
pub const RS_3606: ErrorCode = ErrorCode::new(3606);
/// Worker drain target does not exist in the current topology (v0.46).
pub const RS_3610: ErrorCode = ErrorCode::new(3610);
/// Worker drain cannot proceed because no active recipient worker is available (v0.46).
pub const RS_3611: ErrorCode = ErrorCode::new(3611);
/// Worker drain queue reached its configured bound; backpressure applied (v0.46).
pub const RS_3612: ErrorCode = ErrorCode::new(3612);
/// Schema change requires blue/green clone/backfill/flip; in-place apply rejected (v0.39).
pub const RS_3607: ErrorCode = ErrorCode::new(3607);
/// A blue/green clone operation is already in progress for this view (v0.39).
pub const RS_3608: ErrorCode = ErrorCode::new(3608);
/// Clone backfill lag exceeded the allowed threshold before flip (v0.39).
pub const RS_3609: ErrorCode = ErrorCode::new(3609);
/// View is waiting on upstream source/frontier progress (v0.54.1).
pub const RS_3701: ErrorCode = ErrorCode::new(3701);
/// View admission was rejected by quota controls (v0.54.1).
pub const RS_3702: ErrorCode = ErrorCode::new(3702);
/// View is spilling and lag is dominated by spill delay (v0.54.1).
pub const RS_3703: ErrorCode = ErrorCode::new(3703);
/// View is running in over-budget relaxed mode (v0.54.1).
pub const RS_3704: ErrorCode = ErrorCode::new(3704);
/// Checkpoint alignment is stalled on at least one shard/operator (v0.54.1).
pub const RS_3705: ErrorCode = ErrorCode::new(3705);
/// Sink commit path is blocked and dominates freshness lag (v0.54.1).
pub const RS_3706: ErrorCode = ErrorCode::new(3706);
/// Topology transition (migration or drain) is in progress (v0.54.1).
pub const RS_3707: ErrorCode = ErrorCode::new(3707);
/// View is recovering from checkpoint/reassignment work (v0.54.1).
pub const RS_3708: ErrorCode = ErrorCode::new(3708);

// 3500-3999: Merge laws
/// Merge-law accumulator wire bytes have the wrong size and cannot be decoded (v0.45.7).
pub const RS_3501: ErrorCode = ErrorCode::new(3501);

// 4xxx: Connector
/// Source connection failed.
pub const RS_4001: ErrorCode = ErrorCode::new(4001);
/// Sink write failed.
pub const RS_4002: ErrorCode = ErrorCode::new(4002);
/// Sink 2PC pre-commit failed; epoch not staged.
pub const RS_4003: ErrorCode = ErrorCode::new(4003);
/// Sink 2PC commit failed after pre-commit; recovery required.
pub const RS_4004: ErrorCode = ErrorCode::new(4004);
/// Sink 2PC duplicate delivery detected and blocked (CheckBeforeCommit check found existing).
pub const RS_4005: ErrorCode = ErrorCode::new(4005);
/// Source-epoch registry full; too many uncommitted source epochs in flight.
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
/// Connector surface removed; creation and ingress are fail-closed.
pub const RS_4017: ErrorCode = ErrorCode::new(4017);
/// Source epoch exhausted; no further fenced epoch can be created.
pub const RS_4018: ErrorCode = ErrorCode::new(4018);
/// Source backfill cursor or lifecycle is invalid.
pub const RS_4019: ErrorCode = ErrorCode::new(4019);
/// Backfill live-delta buffer exceeded its configured bound.
pub const RS_4020: ErrorCode = ErrorCode::new(4020);
/// Backfill admission reservation exceeded its configured capacity.
pub const RS_4021: ErrorCode = ErrorCode::new(4021);
/// Materialized view backfill is not published.
pub const RS_4022: ErrorCode = ErrorCode::new(4022);

/// Self-fencing configuration invalid: self_fence_after must satisfy
/// dead_after < self_fence_after < 2 × shard_recovery_budget.
pub const RS_3005: ErrorCode = ErrorCode::new(3005);

// 5xxx: Upgrade / migration
/// Incompatible storage format.
pub const RS_5001: ErrorCode = ErrorCode::new(5001);
/// Unknown merge law referenced in arrangement header.
pub const RS_5002: ErrorCode = ErrorCode::new(5002);
/// Wire protocol version not supported; rolling upgrade version skew (v0.36, DESIGN.md §5.5).
pub const RS_5003: ErrorCode = ErrorCode::new(5003);
/// Illegal shard-migration state transition rejected (v0.46).
pub const RS_5030: ErrorCode = ErrorCode::new(5030);
/// Shard-migration verify scan window exceeded its configured bound (v0.46).
pub const RS_5031: ErrorCode = ErrorCode::new(5031);
/// Shard-migration bucket-map version or watcher acknowledgement mismatch (v0.46).
pub const RS_5032: ErrorCode = ErrorCode::new(5032);
/// Skew-bound SLO cannot be met without composable partial-state splitting (v0.47).
pub const RS_5035: ErrorCode = ErrorCode::new(5035);
/// Non-composable hot key routed to a single spill shard (v0.47).
pub const RS_5036: ErrorCode = ErrorCode::new(5036);
/// Resource usage budget warning (80% threshold reached).
pub const RS_5018: ErrorCode = ErrorCode::new(5018);
/// Resource usage budget critical (95% threshold reached).
pub const RS_5019: ErrorCode = ErrorCode::new(5019);

// 6xxx: Connector schema evolution
/// Incompatible upstream schema evolution detected.
pub const RS_6001: ErrorCode = ErrorCode::new(6001);

// 9xxx: Admission control (v0.45.1)
/// Admission control rejected a capacity request: no lower-priority workload
/// available to pause and the requesting workload has no remaining headroom.
pub const RS_9001: ErrorCode = ErrorCode::new(9001);

// 8xxx: Frontier aggregation (v0.18)
/// Frontier aggregator shard registry is full; new shard reports are rejected.
/// next_steps: scale out aggregators or reduce shard count.
pub const RS_8001: ErrorCode = ErrorCode::new(8001);

/// Stale fencing token on the frontier-aggregator's publisher-lease CAS
/// path (v0.45.6, M2-S3): either a lease-acquisition CAS lost the race
/// against a newer token, or a `publish_frontier` write carried a
/// superseded token. next_steps: re-acquire the lease under the current
/// fence token before retrying; the aggregator has been fenced out.
pub const RS_8002: ErrorCode = ErrorCode::new(8002);

/// Sync-flush-before-lease-handoff-read violation (v0.45.6, M2-S3/S4): a
/// newly-elected publisher's first read of the published frontier observed
/// a write that was not confirmed synchronously durable before the lease
/// handoff. next_steps: verify every `publish_frontier` write path uses
/// `WriteOptions { await_durable: true }`; this indicates a durability
/// regression in `FrontierLeaseStore`.
pub const RS_8003: ErrorCode = ErrorCode::new(8003);

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

/// Returns a short slug for a known error code (e.g. "auth.unauthenticated").
pub fn slug(code: ErrorCode) -> &'static str {
    match code.0 {
        2013 => "transaction.returning_key_not_found",
        2017 => "shard_stats.too_stale",
        2022 => "write.malformed_returning_clause",
        2023 => "window.hop_state_overflow",
        2024 => "window.session_state_overflow",
        2400 => "auth.unauthenticated",
        2401 => "auth.permission_denied",
        2402 => "auth.namespace_access_denied",
        2403 => "auth.mtls_requires_ca_cert",
        2404 => "auth.mtls_no_verified_cert",
        2405 => "auth.tls_config_invalid",
        2406 => "auth.mtls_connection_cap_exceeded",
        2410 => "auth.internal_mtls_required",
        2411 => "auth.internal_mtls_invalid_cert",
        2412 => "auth.internal_mtls_node_identity_mismatch",
        2413 => "auth.internal_mtls_rotation_failed",
        3701 => "view.waiting_on_source",
        3702 => "view.quota_admission_rejected",
        3703 => "view.spilling",
        3704 => "view.over_budget_relaxed",
        3705 => "view.checkpoint_alignment_stalled",
        3706 => "view.sink_blocked",
        3707 => "view.topology_transition_in_progress",
        3708 => "view.recovering",
        1014 => "workload.has_assigned_views",
        1020 => "operator.not_found",
        1021 => "arrangement.key_decode_failed",
        9001 => "admission_control.rejected",
        1731 => "control.not_leader",
        4001 => "source.connection_failed",
        4002 => "sink.write_failed",
        4003 => "sink.pre_commit_failed",
        4004 => "sink.commit_failed",
        4005 => "sink.duplicate_delivery",
        4006 => "source.epoch_registry_full",
        4007 => "sink.ddl_invalid",
        4008 => "source.ddl_invalid",
        4009 => "source.not_found",
        4010 => "source.already_exists",
        4011 => "postgres_cdc.recovery_required",
        4012 => "source.owner_recovery_required",
        4013 => "postgres_cdc.protocol_error",
        4014 => "source.bounds_exceeded",
        4015 => "source.fence_mismatch",
        4016 => "source.acknowledgement_failed",
        4017 => "connector.removed",
        4018 => "source.epoch_exhausted",
        4019 => "source.backfill_cursor_invalid",
        4020 => "backfill.live_delta_buffer_full",
        4021 => "backfill.admission_rejected",
        4022 => "backfill.not_published",
        4 => "control.unreachable",
        5 => "cli.confirmation_required",
        _ => "unknown",
    }
}

/// Returns a human-readable description for a known error code.
pub fn description(code: ErrorCode) -> &'static str {
    match code.0 {
        1 => "Internal error",
        2 => "Configuration error",
        3 => "Storage unavailable",
        4 => "Cluster control plane unreachable",
        5 => "Destructive command confirmation required",
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
        1014 => "Workload still has assigned views",
        1030 => "Migration state exceeded its configured timeout budget",
        9001 => "Admission control rejected the capacity request",
        1015 => "Group-commit queue full; back-pressure applied",
        1016 => "Aggregate running sum overflowed i64",
        1017 => "MIN/MAX multiset retraction underflow: value has no positive weight",
        1018 => "TopK buffer overflow: too many unique rows in a single partition",
        1019 => "View query could not be compiled into an executable operator pipeline",
        1020 => "Operator not found in pipeline",
        1021 => "Arrangement key decoding failed or unsupported",
        1512 => "Inner-frontier stall in distributed recursion; per-shard recompute triggered",
        1513 => "Distributed recursion max-iteration cap exceeded without convergence",
        3601 => "Checkpoint alignment buffer overflowed; bounded buffer capacity exceeded",
        3602 => "Cluster checkpoint recovery in progress",
        3603 => "Pipeline freshness recovery SLO exceeded; RECOVERING_SLOW state",
        1701 => "Shard is already leased by a different worker",
        1702 => "Stale lease token; worker has been fenced out",
        1703 => "Shard has no active lease",
        1731 => "Write rejected: acting control node is not the current Raft leader",
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
        2018 => "Published frontier exceeded the session max_staleness bound; query proceeded",
        3003 => "Pipeline blocked: object store brownout, local buffer exhausted",
        3009 => "Merge operand malformed",
        3011 => "Durable shuffle rate-limit retry budget exhausted",
        3012 => "Durable shuffle generic object-store I/O failure",
        3013 => "Durable shuffle in-memory buffer capacity exceeded",
        3014 => "Durable shuffle footer serialization failed",
        3015 => "Durable shuffle footer deserialization failed",
        3016 => "Durable shuffle footer is corrupt or undersized",
        3017 => "Exchange IPC shuffle decode error",
        3018 => "Exchange loopback route target shard has no active ShardDb",
        3022 => "Cluster checkpoint manifest codec decode error",
        3023 => "Fast-path shuffle frontier read failed during replay dedup",
        3024 => "Shuffle frame row budget exceeded worker.max_rows_per_quantum",
        3501 => "Merge-law accumulator wire bytes have the wrong size",
        3604 => "Worker drain in progress; new shard assignments rejected",
        3605 => "Shard load factor exceeds skew threshold; adaptive re-sharding scheduled",
        3606 => "Worker drain deadline exceeded; worker self-fenced",
        3607 => "Schema change requires blue/green clone; in-place apply rejected",
        3608 => "A blue/green clone operation is already in progress for this view",
        3609 => "Clone backfill lag exceeded the allowed threshold before flip",
        3701 => "View is waiting on source/frontier progress",
        3702 => "View admission rejected by quota controls",
        3703 => "View lag is dominated by spill delay",
        3704 => "View is in over-budget relaxed mode",
        3705 => "View checkpoint alignment is stalled",
        3706 => "View sink commit path is blocked",
        3707 => "View topology transition is in progress",
        3708 => "View is recovering from checkpoint/reassignment work",
        4001 => "Source connection failed",
        4002 => "Sink write failed",
        4003 => "Sink 2PC pre-commit failed; epoch not staged",
        4004 => "Sink 2PC commit failed after pre-commit; recovery required",
        4005 => "Sink 2PC duplicate delivery detected and suppressed",
        4006 => "Source-epoch registry full; too many uncommitted epochs in flight",
        4007 => "CREATE SINK DDL parse or validation failed",
        4008 => "CREATE SOURCE DDL parse or validation failed",
        4009 => "Source not found",
        4010 => "Source already exists",
        4011 => "PostgreSQL CDC replication cannot proceed without recovery",
        4012 => "Source owner registration requires checkpoint recovery",
        4013 => "PostgreSQL CDC protocol or ownership validation failed",
        4014 => "Source bounded in-flight capacity was exceeded",
        4015 => "Source checkpoint fence did not advance monotonically",
        4016 => "Source checkpoint acknowledgement failed",
        4017 => "Connector has been removed",
        4018 => "Source epoch exhausted",
        4019 => "Source backfill cursor or lifecycle is invalid",
        4020 => "Backfill live-delta buffer is full",
        4021 => "Backfill admission reservation rejected",
        4022 => "Materialized view backfill is not published",
        3005 => "Self-fencing configuration invalid: self_fence_after constraint violated",
        5001 => "Incompatible storage format",
        5002 => "Unknown merge law in arrangement header",
        5003 => "Wire protocol version not supported; rolling upgrade version skew",
        5030 => "Illegal shard-migration state transition rejected",
        5031 => "Shard-migration verify scan window exceeded its configured bound",
        5032 => "Shard-migration bucket-map version or watcher acknowledgement mismatch",
        5035 => "Skew-bound SLO cannot be met without composable partial-state splitting",
        5036 => "Non-composable hot key routed to a single spill shard",
        5018 => "Resource usage budget warning (80% threshold reached)",
        5019 => "Resource usage budget critical (95% threshold reached)",
        6001 => "Incompatible upstream schema evolution detected",
        8001 => "Frontier aggregator shard registry is full; new shard reports rejected",
        8002 => "Stale fencing token on frontier-aggregator publisher-lease CAS or publish",
        8003 => "Sync-flush-before-lease-handoff-read violation on frontier publication",
        2017 => "Shard statistics are too stale for safe pruning; query fell back to a full scatter scan",
        2019 => "Shard write buffer full; backpressure applied",
        2012 => "Session wait-for deadline exceeded; query proceeded at current frontier",
        2020 => "Subscribe consumer fell behind the change-log retention window",
        2021 => "COPY FROM STDIN statement is malformed",
        2022 => "UPDATE/DELETE RETURNING clause is malformed",
        2023 => "Hop window state exceeded its configured overlap-aware bound",
        2024 => "Session window state exceeded its configured open-session bound",
        2028 => "Late-data side-channel queue reached its configured bound",
        2013 => "Transaction RETURNING read-back could not find the row at the current frontier",
        2400 => "Unauthenticated: request missing or carrying invalid credentials",
        2401 => "Permission denied: authenticated principal lacks required RBAC role",
        2402 => "Namespace access denied: cross-namespace access attempt by non-admin principal",
        2403 => "--auth=mtls configured without tls_ca_cert_path; gateway refused to start",
        2404 => "mTLS connection has no verified client certificate CN for its peer address",
        2405 => "Gateway TLS certificate/key/CA material failed to load or parse",
        2406 => "mTLS handshake rejected: connection identity map is at capacity",
        2410 => "Internal mTLS connection rejected: client certificate required",
        2411 => "Internal mTLS client certificate invalid, expired, or signed by an untrusted CA",
        2412 => "Presented client certificate node identity does not match registration payload",
        2413 => "Internal mTLS certificate rotation or reload failed",
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
        3011 => Severity::Error,
        3012 => Severity::Error,
        3013 => Severity::Error,
        3014 => Severity::Error,
        3015 => Severity::Error,
        3016 => Severity::Error,
        3017 => Severity::Error,
        3018 => Severity::Error,
        3022 => Severity::Error,
        3023 => Severity::Warning,
        3501 => Severity::Error,
        5001 => Severity::Fatal,
        5002 => Severity::Fatal,
        5030 => Severity::Error,
        5031 => Severity::Error,
        5032 => Severity::Error,
        5035 => Severity::Error,
        5036 => Severity::Warning,
        5018 => Severity::Warning,
        5019 => Severity::Warning,
        6001 => Severity::Warning,
        2017 => Severity::Warning,
        2018 => Severity::Warning,
        2403 => Severity::Fatal,
        2404 => Severity::Fatal,
        2405 => Severity::Fatal,
        2406 => Severity::Error,
        2410 => Severity::Fatal,
        2411 => Severity::Fatal,
        2412 => Severity::Fatal,
        2413 => Severity::Error,
        4017 => Severity::Error,
        3701 => Severity::Warning,
        3702 => Severity::Warning,
        3703 => Severity::Warning,
        3704 => Severity::Warning,
        3705 => Severity::Warning,
        3706 => Severity::Warning,
        3707 => Severity::Warning,
        3708 => Severity::Warning,
        _ => Severity::Error,
    }
}

/// Returns actionable next steps for a known error code.
pub fn next_steps(code: ErrorCode) -> &'static str {
    match code.0 {
        1 => "Report this bug with the support bundle.",
        2 => "Check configuration file and CLI flags.",
        3 => "Verify storage directory permissions and disk space.",
        4 => "Verify the control service URL and ensure the control node is running and reachable.",
        5 => "Pass --yes for script execution or answer y at the prompt.",
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
        1014 => "Reassign or drop the workload's views before dropping the workload.",
        1030 => "Check donor/recipient shard health, then retry or abort the migration; increase the specific migration timeout only if the cluster is healthy but the workload is larger than expected.",
        9001 => "Reduce the requesting workload's demand, raise the cluster state budget, or lower the priority of contending workloads so admission control can pause them.",
        1015 => "Reduce epoch rate, increase GROUP_COMMIT_MAX_BATCHES, or add more shards.",
        1016 => "Reduce value magnitudes or switch to a wider numeric type.",
        1017 => "Ensure every retraction is matched by a prior insertion; check source event ordering and idempotency.",
        1018 => "Reduce partition cardinality, increase TOPK_BUFFER_LIMIT, or add more partition columns.",
        1019 => "Simplify the query to a supported shape (see docs/language-features.md), or reference only base tables — views over other views are not yet compiled.",
        1020 => "Run rockstream explain <view> --op-ids to inspect available operator IDs for this view.",
        1021 => "Check arrangement key syntax or verify if the operator family key codec is supported.",
        1512 => "Check the step function for infinite cycles or skewed partitioning; review per-shard recompute logs.",
        1513 => "Increase max_iterations or restructure the recursive query to converge faster.",
        1701 => "Check worker assignments; another worker holds the lease. Use force-acquire if the holder is dead.",
        1702 => "Worker has been fenced out; acquire a new lease before retrying.",
        1703 => "No lease exists for this shard; acquire a lease before operating on it.",
        1731 => "Retry the write against the current Raft leader (query cluster status for the elected leader's address); do not retry against this node until it wins a future election.",
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
        2017 => "Wait for the next checkpoint to publish fresh shard_stats, or increase shard_stats_max_age_checkpoints if this fallback is expected for the workload.",
        2018 => "Increase rockstream.max_staleness, reduce publish lag, or switch to session_wait_for mode.",
        2021 => "Check COPY syntax; the statement must be COPY <table> [(<col>, ...)] FROM STDIN [WITH (...)].",
        2022 => "Check RETURNING syntax; it must be RETURNING * or RETURNING <col>[, <col>...] with no trailing content.",
        2023 => "Reduce hop overlap, increase HOP_WINDOW_STATE_LIMIT, or shard the windowed stream more finely.",
        2024 => "Reduce session cardinality, increase SESSION_WINDOW_STATE_LIMIT, or shard the windowed stream more finely.",
        2028 => "Drain the configured late-data sink, reduce late-event volume, or increase TUMBLE_WINDOW_LATE_ROUTE_LIMIT after verifying available capacity.",
        2013 => "Retry the write; if the row is consistently missing, check that the frontier used for the read-back has advanced past the commit epoch.",
        3003 => "Reduce input rate or increase local_buffer_max_epochs; check object store availability.",
        3009 => "Inspect the stored arrangement value; possible data corruption or law version mismatch.",
        3011 => "Object store is rate-limiting requests; reduce shuffle write concurrency or request a higher rate limit/quota from the object store provider.",
        3012 => "Verify object store connectivity, credentials, and bucket settings.",
        3013 => "Reduce per-epoch shuffle frame size or flush more frequently; increase MAX_DURABLE_BUFFER_SIZE_BYTES if the workload legitimately needs a larger buffer.",
        3014 => "Report this bug with the support bundle; the index footer failed to serialize to JSON.",
        3015 => "The stored footer bytes are not valid JSON; the object may be corrupt or written by an incompatible version. Re-run the shuffle epoch.",
        3016 => "The coalesced shuffle object is truncated or its footer-length header is inconsistent with the object size; re-run the shuffle epoch or restore from a prior checkpoint.",
        3017 => "Inspect the Arrow IPC shuffle payload; possible truncation or a version mismatch between the writer and reader.",
        3018 => "Verify the target shard is registered and its ShardDb has been attached before routing; check shard assignment and worker startup order.",
        3022 => "Verify the control-plane capability floor, inspect the stored checkpoint manifest bytes for corruption, and finish the rolling upgrade before re-enabling manifest compression.",
        3023 => "Inspect target shard storage health and the committed frontier key; the frame was delivered conservatively, so verify downstream idempotency if the shard is unhealthy.",
        3024 => "Reduce exchange batch size/rechunking or raise worker.max_rows_per_quantum only if the worker can safely absorb a larger in-flight row budget.",
        3501 => "Inspect the stored merge-law accumulator bytes; possible data corruption or an accumulator wire-format version mismatch.",
        3601 => "Reduce input rate or increase checkpoint alignment buffer capacity; check for slow shards holding up barrier propagation.",
        3602 => "Wait for recovery to complete; monitor shard reassignment and frontier progress via SHOW VIEW STATUS.",
        3603 => "Recovery is exceeding SLO; check worker health, storage latency, and frontier progress. Escalate if recovery does not complete within expected bounds.",
        3604 => "Wait for worker drain to complete, or target active workers for shard assignment.",
        3605 => "Allow adaptive re-sharding to complete or manually trigger partition splitting.",
        3606 => "Investigate slow network/compaction preventing worker from draining; review worker logs.",
        3607 => "Perform a zero-downtime view replacement using a blue/green deployment strategy.",
        3608 => "Wait for the existing clone backfill to finish before starting a new one.",
        3609 => "Reduce write load or check worker resource usage to allow backfill to catch up before flip.",
        3701 => "Check source/frontier health and producer lag; verify watermark advancement for the upstream source.",
        3702 => "Reduce state pressure, free quota in competing workloads, or adjust admission and memory budgets before retrying.",
        3703 => "Reduce spill pressure by lowering hot-key skew, increasing memory budget, or reducing ingest burst size.",
        3704 => "Reduce view memory usage or increase workload memory limit so the view can exit relaxed mode.",
        3705 => "Inspect checkpoint barrier holders and slow shards; resolve stalled operators before retrying.",
        3706 => "Check sink connectivity/commit latency and transactional backpressure; recover sink health before retrying.",
        3707 => "Wait for migration/drain to complete, or inspect topology transition progress and blocked shard ownership.",
        3708 => "Wait for recovery to complete; monitor checkpoint and shard reassignment progress via SHOW VIEW STATUS.",
        4001 => "Verify source connection settings and network connectivity.",
        4002 => "Check sink availability and credentials.",
        4003 => "Retry the epoch; check sink connector health and connectivity.",
        4004 => "Trigger manual recovery or restart the connector; check sink idempotency profile.",
        4005 => "This is informational; the duplicate was suppressed. Check the source for duplicate delivery.",
        4006 => "Reduce source epoch rate or increase max_in_flight_source_epochs.",
        4007 => "Check CREATE SINK syntax, referenced view name, and WITH option types; use catalog=filesystem|glue|rest|hive|ducklake.",
        4008 => "Check CREATE SOURCE syntax, connector options, and source credentials.",
        4009 => "Check the source name and ensure it has been created.",
        4010 => "Use a different source name or drop the existing source first.",
        4011 => "Repair the PostgreSQL slot or publication, then run the bounded resnapshot workflow.",
        4012 => "Run checkpoint recovery before registering the source owner, then retry owner registration.",
        4013 => "Validate pgoutput protocol, source identity, slot ownership, and durable routing before retrying.",
        4014 => "Drain the source or reduce transaction and epoch size before increasing the configured bound.",
        4015 => "Recover the highest committed source checkpoint and retry with the next fenced epoch.",
        4016 => "Retain source ownership, recover the committed checkpoint, and retry upstream acknowledgement.",
        4017 => "Use an external loader through pgwire or Kafka for S3 input, an external HTTP-to-Kafka (or HTTP-to-PostgreSQL) adapter for webhooks, or RockStream to Kafka to a downstream writer for sink output.",
        4018 => "Create a new connector before retrying.",
        4019 => "Recover or recreate the committed backfill cursor or lifecycle, then retry.",
        4020 => "Wait for snapshot catch-up or reduce live-delta volume before retrying.",
        4021 => "Wait for a backfill to finish or reduce BACKFILL_LIVE_DELTA_MAX_BYTES before retrying.",
        4022 => "Run SHOW BACKFILL STATUS and retry after the materialized view reaches RUNNING, or create it first.",
        3005 => "Set self_fence_after so that: dead_after < self_fence_after < 2 × shard_recovery_budget.",
        5001 => "Run the storage migration tool before upgrading.",
        5002 => "Register the merge law or migrate the arrangement before attaching the shard.",
        5003 => "Ensure N+1 binary is backward compatible with N; check rolling upgrade procedure in DESIGN.md §5.5.",
        5030 => "Drive the migration through the documented next state only, or resume from the persisted record instead of forcing a skipped state.",
        5031 => "Reduce verify_sample_rate, split the migration into fewer buckets, or increase the configured verify scan bound if memory headroom allows.",
        5032 => "Wait for every reader, exchange receiver, and gateway to observe the new bucket_map_version, then retry the migration step under the current version.",
        5035 => "Add composable partial-state semantics for this operator, reduce the hot key's skew at the source, or route the workload to a spill-shard plan that can tolerate the SLO miss.",
        5036 => "Keep the hot key on a single spill shard, watch that shard's pressure, and switch to a composable law before enabling virtual-bucket splitting for this workload.",
        5018 => "Examine view resource usage and plan to scale out cluster capacity or adjust memory limits.",
        5019 => "Immediately free unused view resources or scale cluster capacity to prevent pipeline stalls.",
        6001 => "Apply view replacement or run manual migration to match the new upstream schema.",
        8001 => "Scale out frontier aggregators (add more nodes with --role=frontier) or reduce shard count below the configured limit.",
        8002 => "Re-acquire the publisher lease under the current fence token before retrying; this aggregator has been fenced out by a newer publisher.",
        8003 => "Verify every publish_frontier write path uses WriteOptions { await_durable: true }; this indicates a durability regression in FrontierLeaseStore.",
        2400 => "Provide valid credentials (Bearer token or mTLS certificate)",
        2401 => "Request elevated RBAC role from an admin or contact the namespace owner",
        2402 => "Switch to the correct namespace with SET search_path or request cross-namespace admin role",
        2403 => "Set --tls-ca-cert-path (or gateway.tls_ca_cert_path in rockstream.toml) to the CA that signs client certificates.",
        2404 => "Connect with a client certificate signed by the configured CA over sslmode=verify-full; a bare TCP or TLS connection without a client cert cannot use --auth=mtls.",
        2405 => "Verify the configured paths point to valid PEM-encoded certificate/key files readable by the gateway process.",
        2406 => "Reduce concurrent connections or raise MAX_CONNECTIONS. The gateway rejected the handshake to avoid silently dropping the peer identity.",
        2410 => "Configure internal TLS client certificates (--internal-tls-cert-path, --internal-tls-key-path, --internal-tls-ca-cert-path) so the node can authenticate with the cluster.",
        2411 => "Verify the internal TLS client certificate is valid, not expired, and signed by the trusted cluster CA root certificate.",
        2412 => "Ensure the node ID and role presented in the internal TLS certificate Common Name / SAN match the node registration parameters.",
        2413 => "Verify the new certificate and private key files exist, have matching keys, and are signed by a trusted CA before triggering certificate rotation.",
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
            RS_0001, RS_0002, RS_0003, RS_0004, RS_0005, RS_1001, RS_1002, RS_1003, RS_1004,
            RS_1005, RS_1006, RS_1007, RS_1008, RS_1030, RS_2001, RS_2002, RS_2003, RS_2004,
            RS_2005, RS_2006, RS_2007, RS_2008, RS_2014, RS_2015, RS_2016, RS_2017, RS_2018,
            RS_2021, RS_3003, RS_3009, RS_3011, RS_3012, RS_3013, RS_3014, RS_3015, RS_3016,
            RS_3017, RS_3018, RS_3022, RS_3023, RS_3024, RS_3501, RS_4001, RS_4002, RS_5001,
            RS_5002, RS_5003, RS_5030, RS_5031, RS_5032, RS_5035, RS_5036, RS_1512, RS_1513,
            RS_3601, RS_3602, RS_3603, RS_1701, RS_1702, RS_1703, RS_5018, RS_5019, RS_6001,
            RS_1015, RS_1016, RS_1017, RS_1012, RS_1013, RS_1014, RS_8001, // v0.21
            RS_4003, RS_4004, RS_4005, RS_4006, RS_4007, RS_4008, RS_4009, RS_4010, RS_4011,
            RS_4012, RS_4013, RS_4014, RS_4015, RS_4016, RS_4017, RS_4018, RS_4019, RS_4020,
            RS_4021, RS_4022, RS_3005, RS_1018, RS_2400, RS_2401, RS_2402, // v0.26 auth
            RS_9001, // v0.45.1 admission control
            RS_1731, // v0.45.2 control-plane leader-only write gating (M7-S2)
            RS_8002, RS_8003, // v0.45.6 frontier-lease publisher fencing (M2-S3)
            RS_2013, RS_2022, // v0.48 UPDATE/DELETE RETURNING (Track A)
            RS_1019, // v0.51.4 Slice 8 — CREATE VIEW compile-failure is a real error
            RS_2403, RS_2404, RS_2405, RS_2406, // v0.51.5-v0.51.26 gateway TLS/mTLS
            RS_2028, // v0.51.12 bounded late-data side-channel
            RS_1020, RS_1021, // v0.53.2 IVM arrangement debugger
            RS_3701, RS_3702, RS_3703, RS_3704, RS_3705, RS_3706, RS_3707,
            RS_3708, // v0.54.1 explainability taxonomy
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
            (RS_4001, "source.connection_failed", "Source connection failed", "Verify source connection settings and network connectivity."),
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

    /// S1 green gate: auth_error_codes_registered
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

    /// v0.51.5 gateway TLS/mTLS error codes.
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
}
