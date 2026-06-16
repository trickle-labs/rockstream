//! Per-connection session state: isolation level, frontier pinning.

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

/// Per-connection session state.
///
/// Tracks isolation level and the pinned frontier epoch (set at `BEGIN` for
/// `REPEATABLE READ`, cleared at `COMMIT`/`ROLLBACK`).
#[derive(Debug, Default)]
pub struct SessionState {
    /// `None` = READ COMMITTED (pin per-statement).
    /// `Some(epoch)` = REPEATABLE READ (pin at BEGIN).
    pub pinned_frontier: Option<u64>,
    pub isolation_level: IsolationLevel,
    /// Current transaction isolation level name for `SHOW transaction_isolation`.
    pub search_path: String,
}

impl SessionState {
    pub fn new() -> Self {
        SessionState {
            pinned_frontier: None,
            isolation_level: IsolationLevel::ReadCommitted,
            search_path: "public".to_string(),
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
