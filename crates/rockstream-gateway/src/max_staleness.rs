//! Session max-staleness tracking and NOTICE emission for RockStream (v0.43).
//!
//! Implements DESIGN.md §12.5 session max-staleness: when a session's view of
//! the committed frontier lags behind the true committed frontier by more than
//! the configured threshold, the gateway emits a Postgres-style `NOTICE`
//! message to the client.
//!
//! This allows clients to detect stale reads and either wait for the frontier
//! to advance or take compensating action.
//!
//! # Proof criteria (v0.43)
//!
//! - `proof_stale_frontier_emits_notice` — when the session's epoch is behind
//!   the committed epoch by more than `max_staleness_ms` millis worth of
//!   epochs, `check_staleness` returns `StalenessStatus::Stale` and
//!   `format_notice` returns a non-empty NOTICE string.
//! - `proof_fresh_frontier_no_notice` — when the session is within the
//!   threshold, `format_notice` returns `None`.

// ── Configuration ─────────────────────────────────────────────────────────────

/// Session-level max-staleness configuration.
///
/// If a session's snapshot epoch lags behind the committed frontier by more
/// than `max_staleness_epochs`, the gateway emits a Postgres NOTICE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxStalenessConfig {
    /// Maximum number of epochs a session may lag before a NOTICE is emitted.
    ///
    /// In production this is derived from `max_staleness_ms` and the average
    /// epoch duration; for the v0.43 proof we express it directly as epochs.
    ///
    /// Default: 10 epochs.
    pub max_staleness_epochs: u64,
}

impl Default for MaxStalenessConfig {
    fn default() -> Self {
        Self {
            max_staleness_epochs: 10,
        }
    }
}

// ── Staleness status ──────────────────────────────────────────────────────────

/// The staleness status of a session's snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StalenessStatus {
    /// The session's snapshot epoch is within the configured threshold.
    Fresh,
    /// The session's snapshot epoch lags behind the frontier by `behind_epochs`.
    Stale {
        /// Number of epochs the session is behind the committed frontier.
        behind_epochs: u64,
    },
}

// ── Core check ────────────────────────────────────────────────────────────────

/// Check whether a session's snapshot is within the configured max-staleness
/// threshold.
///
/// - `session_epoch` — the epoch at which the session took its snapshot.
/// - `committed_epoch` — the current committed frontier epoch.
/// - `config` — the max-staleness configuration.
pub fn check_staleness(
    session_epoch: u64,
    committed_epoch: u64,
    config: &MaxStalenessConfig,
) -> StalenessStatus {
    if committed_epoch <= session_epoch {
        return StalenessStatus::Fresh;
    }
    let behind = committed_epoch - session_epoch;
    if behind > config.max_staleness_epochs {
        StalenessStatus::Stale {
            behind_epochs: behind,
        }
    } else {
        StalenessStatus::Fresh
    }
}

/// Format a Postgres-style NOTICE message for a stale session.
///
/// Returns `None` if the session is `Fresh`.
/// Returns `Some(<notice>)` if the session is `Stale`.
///
/// The notice follows the pattern used by Materialize / CockroachDB for
/// staleness warnings:
///
/// ```
/// NOTICE: session snapshot is 15 epochs behind the committed frontier (max_staleness_epochs=10); consider refreshing your connection.
/// ```
pub fn format_notice(status: &StalenessStatus, config: &MaxStalenessConfig) -> Option<String> {
    match status {
        StalenessStatus::Fresh => None,
        StalenessStatus::Stale { behind_epochs } => Some(format!(
            "NOTICE: session snapshot is {behind_epochs} epochs behind the \
             committed frontier (max_staleness_epochs={}); \
             consider refreshing your connection.",
            config.max_staleness_epochs
        )),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Stale session → NOTICE ────────────────────────────────────────────────

    /// **Proof criterion (v0.43)**: Session max-staleness NOTICE emits
    /// correctly on a stale frontier.
    ///
    /// Session snapshot at epoch 5; committed frontier at epoch 20; threshold
    /// = 10 epochs → behind by 15 epochs → Stale → NOTICE emitted.
    #[test]
    fn proof_stale_frontier_emits_notice() {
        let config = MaxStalenessConfig {
            max_staleness_epochs: 10,
        };
        let status = check_staleness(5, 20, &config);

        assert_eq!(
            status,
            StalenessStatus::Stale { behind_epochs: 15 },
            "session 15 epochs behind must be Stale"
        );

        let notice = format_notice(&status, &config);
        assert!(notice.is_some(), "stale session must produce a NOTICE");

        let notice_text = notice.unwrap();
        assert!(
            notice_text.starts_with("NOTICE:"),
            "notice must start with NOTICE:"
        );
        assert!(
            notice_text.contains("15 epochs"),
            "notice must mention how far behind: {notice_text}"
        );
        assert!(
            notice_text.contains("max_staleness_epochs=10"),
            "notice must include threshold: {notice_text}"
        );
    }

    // ── Fresh session → no NOTICE ─────────────────────────────────────────────

    /// **Proof criterion (v0.43)**: Fresh session does not emit a NOTICE.
    #[test]
    fn proof_fresh_frontier_no_notice() {
        let config = MaxStalenessConfig {
            max_staleness_epochs: 10,
        };

        // Exactly at threshold (10 epochs behind) — still Fresh.
        let status_at_threshold = check_staleness(10, 20, &config);
        assert_eq!(status_at_threshold, StalenessStatus::Fresh);
        assert!(format_notice(&status_at_threshold, &config).is_none());

        // Well within threshold.
        let status_fresh = check_staleness(18, 20, &config);
        assert_eq!(status_fresh, StalenessStatus::Fresh);
        assert!(format_notice(&status_fresh, &config).is_none());
    }

    // ── Edge cases ────────────────────────────────────────────────────────────

    #[test]
    fn session_at_same_epoch_as_committed_is_fresh() {
        let config = MaxStalenessConfig::default();
        let status = check_staleness(42, 42, &config);
        assert_eq!(status, StalenessStatus::Fresh);
    }

    #[test]
    fn session_ahead_of_committed_is_fresh() {
        // Shouldn't happen in practice but must not panic.
        let config = MaxStalenessConfig::default();
        let status = check_staleness(100, 50, &config);
        assert_eq!(status, StalenessStatus::Fresh);
    }

    #[test]
    fn exactly_one_epoch_over_threshold_is_stale() {
        let config = MaxStalenessConfig {
            max_staleness_epochs: 5,
        };
        // behind = 6 > 5 → Stale
        let status = check_staleness(10, 16, &config);
        assert_eq!(status, StalenessStatus::Stale { behind_epochs: 6 });
    }

    #[test]
    fn default_config_has_10_epoch_threshold() {
        assert_eq!(MaxStalenessConfig::default().max_staleness_epochs, 10);
    }

    #[test]
    fn notice_contains_refresh_hint() {
        let config = MaxStalenessConfig {
            max_staleness_epochs: 3,
        };
        let status = check_staleness(0, 10, &config);
        let notice = format_notice(&status, &config).unwrap();
        assert!(
            notice.contains("refreshing"),
            "notice must hint at refreshing the connection"
        );
    }
}
