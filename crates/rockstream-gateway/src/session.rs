//! Per-connection session state: isolation level, frontier pinning, freshness tokens.

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
        Self { table_name: table_name.into(), source_epoch }
    }
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
}

impl Default for SessionState {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionState {
    pub fn new() -> Self {
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
        }
    }

    /// Handle `BEGIN`: pin the frontier for `REPEATABLE READ`.
    pub fn begin(&mut self, current_frontier: Option<u64>) {
        if self.isolation_level == IsolationLevel::RepeatableRead {
            self.pinned_frontier = current_frontier;
        }
    }

    /// Handle `COMMIT` or `ROLLBACK`: clear the pinned frontier.
    pub fn end_transaction(&mut self) {
        self.pinned_frontier = None;
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
