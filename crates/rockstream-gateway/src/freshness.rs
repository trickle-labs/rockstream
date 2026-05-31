//! Freshness tokens, read-your-writes sessions, and isolation modes.
//!
//! Implements the v0.42 isolation and freshness surface:
//! - `IsolationMode` — `READ COMMITTED` / `REPEATABLE READ` (DESIGN.md §12.4.1)
//! - `FreshnessToken` — opaque epoch-backed token returned after a write
//! - `WaitForConfig` — configures `wait_for=<token>` behavior
//! - `ReadYourWritesSession` — per-session state for read-your-writes guarantee
//!
//! # Proof criterion (v0.42)
//!
//! Read-your-writes demo passes: a write that advances to epoch N produces a
//! `FreshnessToken(N)`; a subsequent read with `wait_for=<token>` on a session
//! whose frontier has caught up returns immediately; a session whose frontier
//! lags returns `WouldBlock`.

/// Supported transaction isolation modes for the gateway.
///
/// Note: `SERIALIZABLE` is rejected with `RS-2003` (see `pgwire::parse_isolation_level`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationMode {
    /// `READ COMMITTED` — each query sees committed state at its own snapshot.
    ReadCommitted,
    /// `REPEATABLE READ` — all queries in a session see the same snapshot epoch.
    RepeatableRead,
}

impl IsolationMode {
    /// Parse from a SQL string (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().trim() {
            "READ COMMITTED" => Some(Self::ReadCommitted),
            "REPEATABLE READ" => Some(Self::RepeatableRead),
            _ => None,
        }
    }

    /// Returns the canonical SQL name.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReadCommitted => "READ COMMITTED",
            Self::RepeatableRead => "REPEATABLE READ",
        }
    }
}

/// An opaque token representing the committed epoch of a write.
///
/// Returned by the gateway after a write has been durably committed.  The
/// client can pass this token in a subsequent read as `wait_for=<token>` to
/// guarantee read-your-writes semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FreshnessToken {
    /// The epoch at which the write was committed.
    pub epoch: u64,
}

impl FreshnessToken {
    /// Create a freshness token for the given committed epoch.
    pub fn new(epoch: u64) -> Self {
        Self { epoch }
    }
}

/// Outcome of a `wait_for` check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitForOutcome {
    /// The session frontier has caught up to (or past) the required epoch.
    /// The read can proceed immediately.
    Ready,
    /// The session frontier has not yet reached the required epoch.
    /// The client must retry or wait.
    WouldBlock {
        /// Required epoch from the freshness token.
        required: u64,
        /// Current session frontier epoch.
        current: u64,
    },
}

/// Configuration for `wait_for=<token>` behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaitForConfig {
    /// Maximum wall-clock wait in milliseconds before returning `WouldBlock`.
    /// A value of 0 means return immediately without blocking.
    pub max_wait_ms: u64,
}

impl Default for WaitForConfig {
    fn default() -> Self {
        Self { max_wait_ms: 5_000 }
    }
}

/// Per-session state for read-your-writes guarantee.
///
/// Each gateway session tracks:
/// - The current snapshot epoch used for `REPEATABLE READ` sessions.
/// - The last write's freshness token (for `wait_for` checks).
/// - The isolation mode.
#[derive(Debug, Clone)]
pub struct ReadYourWritesSession {
    /// Session isolation mode.
    pub isolation: IsolationMode,
    /// Epoch pinned at session start (for `REPEATABLE READ`).
    /// For `READ COMMITTED` this advances with each query.
    pub snapshot_epoch: u64,
    /// The highest epoch written by this session (from the last `FreshnessToken`).
    pub last_written_epoch: Option<u64>,
    /// Wait-for configuration.
    pub wait_for_config: WaitForConfig,
}

impl ReadYourWritesSession {
    /// Create a new session in `READ COMMITTED` mode starting at the given epoch.
    pub fn new(snapshot_epoch: u64) -> Self {
        Self {
            isolation: IsolationMode::ReadCommitted,
            snapshot_epoch,
            last_written_epoch: None,
            wait_for_config: WaitForConfig::default(),
        }
    }

    /// Create a new session in `REPEATABLE READ` mode pinned at the given epoch.
    pub fn repeatable_read(pinned_epoch: u64) -> Self {
        Self {
            isolation: IsolationMode::RepeatableRead,
            snapshot_epoch: pinned_epoch,
            last_written_epoch: None,
            wait_for_config: WaitForConfig::default(),
        }
    }

    /// Record that this session performed a write committed at `epoch`.
    ///
    /// Updates the `last_written_epoch` so that a subsequent `check_wait_for`
    /// can verify read-your-writes.
    pub fn record_write(&mut self, token: FreshnessToken) {
        match self.last_written_epoch {
            None => self.last_written_epoch = Some(token.epoch),
            Some(prev) if token.epoch > prev => self.last_written_epoch = Some(token.epoch),
            _ => {}
        }
    }

    /// Advance the session snapshot epoch (used for `READ COMMITTED` sessions).
    ///
    /// For `REPEATABLE READ` this is a no-op (snapshot is pinned).
    pub fn advance_snapshot(&mut self, new_epoch: u64) {
        if self.isolation == IsolationMode::ReadCommitted && new_epoch > self.snapshot_epoch {
            self.snapshot_epoch = new_epoch;
        }
    }

    /// Check whether the session frontier (`current_committed_epoch`) has caught
    /// up to the token required by a `wait_for=<token>` request.
    ///
    /// Returns `WaitForOutcome::Ready` when `current_committed_epoch >= token.epoch`.
    /// Returns `WaitForOutcome::WouldBlock` otherwise.
    pub fn check_wait_for(
        &self,
        token: FreshnessToken,
        current_committed_epoch: u64,
    ) -> WaitForOutcome {
        if current_committed_epoch >= token.epoch {
            WaitForOutcome::Ready
        } else {
            WaitForOutcome::WouldBlock {
                required: token.epoch,
                current: current_committed_epoch,
            }
        }
    }

    /// Return the effective read epoch for this session given the current
    /// committed epoch.
    ///
    /// - `READ COMMITTED`: returns `current_committed_epoch`.
    /// - `REPEATABLE READ`: returns the pinned `snapshot_epoch`.
    pub fn effective_read_epoch(&self, current_committed_epoch: u64) -> u64 {
        match self.isolation {
            IsolationMode::ReadCommitted => current_committed_epoch,
            IsolationMode::RepeatableRead => self.snapshot_epoch,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── IsolationMode ─────────────────────────────────────────────────────────

    #[test]
    fn isolation_mode_parse_read_committed() {
        assert_eq!(
            IsolationMode::parse("READ COMMITTED"),
            Some(IsolationMode::ReadCommitted)
        );
        assert_eq!(
            IsolationMode::parse("read committed"),
            Some(IsolationMode::ReadCommitted)
        );
    }

    #[test]
    fn isolation_mode_parse_repeatable_read() {
        assert_eq!(
            IsolationMode::parse("REPEATABLE READ"),
            Some(IsolationMode::RepeatableRead)
        );
    }

    #[test]
    fn isolation_mode_parse_unknown_returns_none() {
        assert_eq!(IsolationMode::parse("SERIALIZABLE"), None);
        assert_eq!(IsolationMode::parse(""), None);
    }

    // ── FreshnessToken ────────────────────────────────────────────────────────

    #[test]
    fn freshness_token_ordering() {
        let t1 = FreshnessToken::new(10);
        let t2 = FreshnessToken::new(20);
        assert!(t1 < t2);
    }

    // ── ReadYourWritesSession ─────────────────────────────────────────────────

    /// **Proof criterion (v0.42)**: Read-your-writes demo passes.
    ///
    /// A write committed at epoch N produces a `FreshnessToken(N)`. A subsequent
    /// read on a session whose frontier has caught up returns `Ready`. A session
    /// whose frontier lags returns `WouldBlock`.
    #[test]
    fn proof_read_your_writes_demo() {
        let mut session = ReadYourWritesSession::new(5);

        // Simulate a write committed at epoch 10.
        let write_token = FreshnessToken::new(10);
        session.record_write(write_token);
        assert_eq!(session.last_written_epoch, Some(10));

        // Frontier has caught up: read-your-writes satisfied.
        let current_epoch = 10;
        assert_eq!(
            session.check_wait_for(write_token, current_epoch),
            WaitForOutcome::Ready,
            "frontier >= token epoch must return Ready"
        );

        // Frontier ahead: still ready.
        assert_eq!(
            session.check_wait_for(write_token, 15),
            WaitForOutcome::Ready,
            "frontier > token epoch must return Ready"
        );

        // Frontier behind: would block.
        let outcome = session.check_wait_for(write_token, 7);
        assert_eq!(
            outcome,
            WaitForOutcome::WouldBlock {
                required: 10,
                current: 7
            },
            "frontier < token epoch must return WouldBlock"
        );
    }

    #[test]
    fn read_committed_advances_snapshot() {
        let mut session = ReadYourWritesSession::new(5);
        assert_eq!(session.isolation, IsolationMode::ReadCommitted);
        session.advance_snapshot(10);
        assert_eq!(session.snapshot_epoch, 10);
        // Going backwards does not regress.
        session.advance_snapshot(8);
        assert_eq!(session.snapshot_epoch, 10);
    }

    #[test]
    fn repeatable_read_snapshot_is_pinned() {
        let mut session = ReadYourWritesSession::repeatable_read(5);
        assert_eq!(session.isolation, IsolationMode::RepeatableRead);
        session.advance_snapshot(20); // no-op for REPEATABLE READ
        assert_eq!(
            session.snapshot_epoch, 5,
            "REPEATABLE READ snapshot must be pinned"
        );
    }

    #[test]
    fn effective_read_epoch_read_committed() {
        let session = ReadYourWritesSession::new(5);
        assert_eq!(session.effective_read_epoch(15), 15);
    }

    #[test]
    fn effective_read_epoch_repeatable_read() {
        let session = ReadYourWritesSession::repeatable_read(5);
        assert_eq!(session.effective_read_epoch(15), 5);
    }

    #[test]
    fn record_write_only_advances() {
        let mut session = ReadYourWritesSession::new(0);
        session.record_write(FreshnessToken::new(10));
        session.record_write(FreshnessToken::new(5)); // lower — ignored
        assert_eq!(session.last_written_epoch, Some(10));
        session.record_write(FreshnessToken::new(15)); // higher — accepted
        assert_eq!(session.last_written_epoch, Some(15));
    }
}
