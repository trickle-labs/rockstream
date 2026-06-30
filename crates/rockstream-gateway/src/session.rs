//! Per-connection session state: isolation level, frontier pinning, freshness tokens, cursors.

use std::collections::HashMap;

/// Maximum GUC parameters stored per connection.
/// Fill-level metric: `guc_params.len()`.
/// Backpressure: excess SET commands are silently accepted but not stored.
pub const MAX_GUC_PARAMS: usize = 100;

/// SCRAM-SHA-256 per-connection auth state machine.
#[derive(Debug, Clone, Default)]
pub enum ScramAuthState {
    /// No SCRAM exchange in progress.
    #[default]
    Idle,
    /// Server-first message has been sent; waiting for client-final.
    ServerFirstSent {
        /// SaltedPassword from the RoleCatalog.
        salted_password: Vec<u8>,
        /// Full nonce (client_nonce + server_nonce).
        server_nonce: String,
        /// Partial auth-message: `client_first_bare + "," + server_first`.
        auth_message: String,
        /// Username extracted from client-first-message.
        username: String,
    },
}

/// Isolation level for a gateway session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Default. Frontier pin acquired per-statement.
    #[default]
    ReadCommitted,
    /// Frontier pin acquired at BEGIN.
    RepeatableRead,
    /// Not supported — returns `RS-2003`.
    Serializable,
}

impl IsolationLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            IsolationLevel::ReadCommitted => "read committed",
            IsolationLevel::RepeatableRead => "repeatable read",
            IsolationLevel::Serializable => "serializable",
        }
    }
}

/// A freshness token recording the source epoch of a committed write.
///
/// Used for read-your-writes (RYW): after COMMIT the token is stored in the
/// session, and before the next SELECT the gateway waits until the shard
/// frontier reaches `source_epoch`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FreshnessToken {
    /// Table that was written.
    pub table_name: String,
    /// Epoch at which the write was committed.
    pub source_epoch: u64,
}

impl FreshnessToken {
    pub fn new(table_name: impl Into<String>, source_epoch: u64) -> Self {
        Self {
            table_name: table_name.into(),
            source_epoch,
        }
    }
}

/// Maximum number of cursors open simultaneously per connection.
/// Fill-level metric: `cursors.len()`.
/// Backpressure: `RS-2052` / SQLSTATE 42P03.
pub const MAX_CURSORS_PER_CONNECTION: usize = 100;

/// Transaction status byte — mirrors the Postgres ReadyForQuery status byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TxStatus {
    /// Not in a transaction block (`'I'`).
    #[default]
    Idle,
    /// Inside a transaction block (`'T'`).
    InTransaction,
    /// Inside a failed transaction block (`'E'`).
    Failed,
}

impl TxStatus {
    pub fn as_byte(self) -> u8 {
        match self {
            TxStatus::Idle => b'I',
            TxStatus::InTransaction => b'T',
            TxStatus::Failed => b'E',
        }
    }
}

/// State of a single named cursor.
#[derive(Debug, Clone)]
pub struct CursorState {
    /// All rows (tab-separated bytes) collected from the query at DECLARE time.
    pub rows: Vec<Vec<u8>>,
    /// Current read position (0-indexed).
    pub position: usize,
}

/// Per-connection session state.
///
/// Tracks isolation level and the pinned frontier epoch (set at `BEGIN` for
/// `REPEATABLE READ`, cleared at `COMMIT`/`ROLLBACK`).
#[derive(Debug)]
pub struct SessionState {
    /// `None` = READ COMMITTED (pin per-statement).
    /// `Some(epoch)` = REPEATABLE READ (pin at BEGIN).
    pub pinned_frontier: Option<u64>,
    pub isolation_level: IsolationLevel,
    /// Current transaction isolation level name for `SHOW transaction_isolation`.
    pub search_path: String,
    /// First 16 bytes of SHA-256 of user-supplied idempotency key string.
    /// Set via `SET rockstream.idempotency_key = 'str'`.
    pub idempotency_key: Option<[u8; 16]>,
    /// Explicit source epoch envelope for this write.
    /// Set via `SET rockstream.source_epoch = N`.
    pub source_epoch_envelope: Option<u64>,
    /// Freshness token of the most recently committed write (for RYW in v0.25).
    /// Automatically applied before the next SELECT if `session_wait_for_enabled`.
    pub last_written_epoch: Option<FreshnessToken>,
    /// Explicit wait-for token set via `SET rockstream.wait_for = '<json>'`.
    /// Applied once on the next SELECT, then cleared.
    pub wait_for_token: Option<FreshnessToken>,
    /// Timeout for wait_for polling in milliseconds. Default 5 000 ms.
    pub session_wait_for_timeout_ms: u64,
    /// Whether automatic session-scoped RYW is enabled. Default: `true`.
    pub session_wait_for_enabled: bool,
    /// The namespace this session is currently operating in (v0.26).
    pub current_namespace: String,
    /// Authenticated principal for this session (v0.26).
    pub principal: crate::auth::Principal,
    /// Backend PID for this connection (used in BackendKeyData and pg_stat_activity).
    pub backend_pid: u32,
    /// Per-connection cancel secret (used in BackendKeyData / CancelRequest).
    pub cancel_secret: u32,
    /// Current transaction status (for ReadyForQuery status byte).
    pub tx_status: TxStatus,
    /// Open named cursors. Bound: MAX_CURSORS_PER_CONNECTION.
    /// Fill-level metric: `cursors.len()`.
    pub cursors: HashMap<String, CursorState>,
    /// application_name startup parameter (v0.39).
    pub application_name: String,
    /// Per-connection SCRAM-SHA-256 auth state (v0.40).
    pub scram_auth_state: ScramAuthState,
    /// 4-byte salt sent for MD5 auth challenge (v0.40).
    pub md5_auth_salt: Option<[u8; 4]>,
    /// Per-connection GUC parameters set via SET (v0.40). Bound: MAX_GUC_PARAMS.
    pub guc_params: HashMap<String, String>,
    /// True when search_path was explicitly set via SET (v0.40, S9). Enables search_path-aware view resolution.
    pub search_path_set: bool,
    /// True when an explicit BEGIN block is active (v0.41).
    pub in_explicit_block: bool,
    /// SET LOCAL overrides — cleared at COMMIT or ROLLBACK (v0.41).
    pub local_guc_params: HashMap<String, String>,
}

impl Default for SessionState {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionState {
    pub fn new() -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        SessionState {
            pinned_frontier: None,
            isolation_level: IsolationLevel::ReadCommitted,
            search_path: "public".to_string(),
            idempotency_key: None,
            source_epoch_envelope: None,
            last_written_epoch: None,
            wait_for_token: None,
            session_wait_for_timeout_ms: 5_000,
            session_wait_for_enabled: true,
            current_namespace: "public".to_string(),
            principal: crate::auth::Principal::System,
            backend_pid: rng.gen(),
            cancel_secret: rng.gen(),
            tx_status: TxStatus::Idle,
            cursors: HashMap::new(),
            application_name: String::new(),
            scram_auth_state: ScramAuthState::Idle,
            md5_auth_salt: None,
            guc_params: HashMap::new(),
            search_path_set: false,
            in_explicit_block: false,
            local_guc_params: HashMap::new(),
        }
    }

    /// Handle `BEGIN`: pin the frontier for `REPEATABLE READ`.
    pub fn begin(&mut self, current_frontier: Option<u64>) {
        if self.isolation_level == IsolationLevel::RepeatableRead {
            self.pinned_frontier = current_frontier;
        }
    }

    /// Enter an explicit transaction block (BEGIN).
    pub fn begin_explicit(&mut self) {
        self.in_explicit_block = true;
        self.tx_status = TxStatus::InTransaction;
    }

    /// Flip to Failed state — only when inside an explicit block.
    pub fn fail_transaction(&mut self) {
        if self.in_explicit_block {
            self.tx_status = TxStatus::Failed;
        }
    }

    /// Returns true when the session is in a failed explicit transaction block.
    pub fn is_in_failed_block(&self) -> bool {
        self.tx_status == TxStatus::Failed
    }

    /// Look up a GUC value: local_guc_params first, then guc_params.
    pub fn effective_guc<'a>(&'a self, key: &str) -> Option<&'a str> {
        self.local_guc_params
            .get(key)
            .or_else(|| self.guc_params.get(key))
            .map(|s| s.as_str())
    }

    /// Handle `COMMIT` or `ROLLBACK`: clear the pinned frontier, cursors, and local GUC.
    pub fn end_transaction(&mut self) {
        self.pinned_frontier = None;
        self.cursors.clear();
        self.tx_status = TxStatus::Idle;
        self.in_explicit_block = false;
        self.local_guc_params.clear();
    }

    /// Frontier to use for this statement. Returns the pinned frontier if set,
    /// otherwise the `current_frontier` parameter (READ COMMITTED per-statement pin).
    pub fn effective_frontier(&self, current_frontier: Option<u64>) -> Option<u64> {
        self.pinned_frontier.or(current_frontier)
    }
}

// ── S7 unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// S3 green gate: transaction state machine new/begin/fail/end lifecycle.
    #[test]
    fn test_transaction_state_machine() {
        let mut s = SessionState::new();
        assert_eq!(s.tx_status, TxStatus::Idle);
        assert!(!s.in_explicit_block);

        s.begin_explicit();
        assert_eq!(s.tx_status, TxStatus::InTransaction);
        assert!(s.in_explicit_block);

        s.fail_transaction();
        assert_eq!(s.tx_status, TxStatus::Failed);
        assert!(s.is_in_failed_block());

        s.end_transaction();
        assert_eq!(s.tx_status, TxStatus::Idle);
        assert!(!s.in_explicit_block);
        assert!(s.local_guc_params.is_empty());
    }

    /// S4 green gate: test_session_state_scram_fields
    #[test]
    fn test_session_state_scram_fields() {
        let s = SessionState::new();
        assert!(
            matches!(s.scram_auth_state, ScramAuthState::Idle),
            "default scram_auth_state must be Idle"
        );
        assert!(
            s.md5_auth_salt.is_none(),
            "default md5_auth_salt must be None"
        );
        assert!(s.guc_params.is_empty(), "default guc_params must be empty");
        // MAX_GUC_PARAMS constant is accessible
        assert_eq!(MAX_GUC_PARAMS, 100);
    }

    #[test]
    fn last_written_epoch_upgraded_to_freshness_token() {
        let mut session = SessionState::new();
        assert!(session.last_written_epoch.is_none());

        let token = FreshnessToken::new("orders", 42);
        session.last_written_epoch = Some(token.clone());

        let stored = session.last_written_epoch.as_ref().unwrap();
        assert_eq!(stored.source_epoch, 42);
        assert_eq!(stored.table_name, "orders");
        assert_eq!(*stored, token);
    }

    #[test]
    fn freshness_token_json_roundtrip() {
        let token = FreshnessToken::new("my_table", 99);
        let json = serde_json::to_string(&token).unwrap();
        let decoded: FreshnessToken = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, token);
    }

    #[test]
    fn session_wait_for_defaults() {
        let s = SessionState::new();
        assert!(s.session_wait_for_enabled);
        assert_eq!(s.session_wait_for_timeout_ms, 5_000);
        assert!(s.wait_for_token.is_none());
    }
}
