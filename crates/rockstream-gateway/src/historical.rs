//! Historical and monotone-partial queries for the gateway.
//!
//! Implements DESIGN.md §12.4.1 for v0.42:
//! - `AS OF EPOCH <n>` — return view state at a specific past epoch.
//! - `AS OF TIMESTAMP <t>` — return view state at the epoch closest to
//!   wall-clock timestamp `t`.
//! - `AS OF MONOTONE PARTIAL` — opt-in for monotone-law views; returns a
//!   partial result along with an explicit `complete_through` token.
//! - `RetentionConfig` — `checkpoint_retention_count` /
//!   `checkpoint_retention_duration_ms` configuration.
//! - `check_retention` — returns `RS-2006` if the requested epoch falls
//!   before the retention window.
//!
//! # Note on RS-2006
//!
//! The ROADMAP.md v0.42 proof criterion refers to "RS-2005" for queries beyond
//! retention, but RS-2005 is already registered as "Query rate limit exceeded"
//! (v0.40). This implementation correctly uses RS-2006 ("Historical query
//! beyond checkpoint retention window") instead.
//!
//! # Proof criteria (v0.42)
//!
//! - `SELECT * FROM orders_mv AS OF EPOCH <past>` returns the correct
//!   historical snapshot.
//! - `AS OF MONOTONE PARTIAL` returns a result with an explicit
//!   `complete_through` token for a monotone reachability view.
//! - Queries beyond retention return RS-2006.

use crate::error::GatewayError;

// ── AS OF clause ──────────────────────────────────────────────────────────────

/// Specifies the point-in-time for a historical or monotone-partial query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoricalAsOf {
    /// `AS OF EPOCH <n>` — exact epoch.
    Epoch(u64),
    /// `AS OF TIMESTAMP <t>` — wall-clock millis since Unix epoch.
    Timestamp(u64),
    /// `AS OF MONOTONE PARTIAL` — opt-in for monotone-law views; returns a
    /// partial result with a `complete_through` token.
    MonotonePartial,
}

// ── Retention config ──────────────────────────────────────────────────────────

/// Checkpoint retention configuration.
///
/// Controls how many checkpoints / how much history the gateway retains.
/// Queries referencing epochs older than the retention window return RS-2006.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionConfig {
    /// Number of checkpoints to retain. Default: 100.
    pub checkpoint_retention_count: u64,
    /// Duration of retention in milliseconds. Default: 86_400_000 (24 hours).
    pub checkpoint_retention_duration_ms: u64,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            checkpoint_retention_count: 100,
            checkpoint_retention_duration_ms: 86_400_000,
        }
    }
}

// ── Retention check ───────────────────────────────────────────────────────────

/// Check whether the requested epoch is within the retention window.
///
/// Returns `Ok(())` if `requested_epoch >= oldest_retained_epoch`.
/// Returns `Err(GatewayError::HistoricalQueryBeyondRetention)` with RS-2006
/// if the epoch is before the retention window.
pub fn check_retention(
    requested_epoch: u64,
    oldest_retained_epoch: u64,
) -> Result<(), GatewayError> {
    if requested_epoch < oldest_retained_epoch {
        Err(GatewayError::HistoricalQueryBeyondRetention {
            requested: requested_epoch,
            oldest_retained: oldest_retained_epoch,
        })
    } else {
        Ok(())
    }
}

/// Compute the oldest retained epoch given the current epoch and config.
pub fn oldest_retained_epoch(current_epoch: u64, config: &RetentionConfig) -> u64 {
    current_epoch.saturating_sub(config.checkpoint_retention_count)
}

// ── Query result types ────────────────────────────────────────────────────────

/// A row from a historical or monotone-partial query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoricalRow {
    /// Column values as strings.
    pub columns: Vec<String>,
}

/// Result of an `AS OF EPOCH` or `AS OF TIMESTAMP` query.
#[derive(Debug, Clone)]
pub struct HistoricalQueryResult {
    /// Rows from the view at the requested point in time.
    pub rows: Vec<HistoricalRow>,
    /// The epoch that was actually used (may differ from requested for
    /// `AS OF TIMESTAMP` which snaps to the nearest checkpoint).
    pub effective_epoch: u64,
}

/// Result of an `AS OF MONOTONE PARTIAL` query.
///
/// For monotone-law views (e.g. reachability, min/max that can only grow), the
/// gateway can return a partial result before the view has fully converged,
/// along with a `complete_through` token that the client can use in a
/// subsequent `wait_for=<token>` to wait for full convergence.
#[derive(Debug, Clone)]
pub struct MonotonePartialResult {
    /// Partial rows (may be incomplete for in-flight epochs).
    pub rows: Vec<HistoricalRow>,
    /// The epoch through which the result is complete.  Rows for epochs
    /// beyond this may still be in-flight.
    pub complete_through: u64,
    /// True if the result is fully converged (no further rows expected).
    pub is_complete: bool,
}

// ── Epoch-to-timestamp mapping ────────────────────────────────────────────────

/// An entry in the epoch→timestamp index used by `AS OF TIMESTAMP` queries.
#[derive(Debug, Clone, Copy)]
pub struct EpochTimestampEntry {
    pub epoch: u64,
    pub wall_clock_ms: u64,
}

/// Find the epoch closest to the requested timestamp in a sorted epoch index.
///
/// Returns the epoch with the smallest `|entry.wall_clock_ms - requested_ms|`.
/// Returns `None` if the index is empty.
pub fn find_epoch_for_timestamp(index: &[EpochTimestampEntry], requested_ms: u64) -> Option<u64> {
    if index.is_empty() {
        return None;
    }
    let best = index
        .iter()
        .min_by_key(|e| e.wall_clock_ms.abs_diff(requested_ms))
        .unwrap();
    Some(best.epoch)
}

// ── Stub query executor ───────────────────────────────────────────────────────

/// Execute a historical query against a simulated view snapshot.
///
/// In production this reads from the checkpoint store; here we simulate a
/// deterministic snapshot: `epoch % stride == 0` rows are present at each
/// epoch, where `stride` is derived from the epoch.
///
/// `available_snapshots` is a map of `epoch → rows` representing the retained
/// checkpoints.
pub fn execute_historical_query(
    as_of: &HistoricalAsOf,
    available_snapshots: &[(u64, Vec<HistoricalRow>)],
    epoch_index: &[EpochTimestampEntry],
    config: &RetentionConfig,
    current_epoch: u64,
) -> Result<HistoricalQueryResult, GatewayError> {
    let oldest = oldest_retained_epoch(current_epoch, config);

    let target_epoch = match as_of {
        HistoricalAsOf::Epoch(e) => {
            check_retention(*e, oldest)?;
            *e
        }
        HistoricalAsOf::Timestamp(ts) => {
            let e = find_epoch_for_timestamp(epoch_index, *ts).ok_or(
                GatewayError::HistoricalQueryBeyondRetention {
                    requested: 0,
                    oldest_retained: oldest,
                },
            )?;
            check_retention(e, oldest)?;
            e
        }
        HistoricalAsOf::MonotonePartial => current_epoch,
    };

    let rows = available_snapshots
        .iter()
        .find(|(e, _)| *e == target_epoch)
        .map(|(_, rows)| rows.clone())
        .unwrap_or_default();

    Ok(HistoricalQueryResult {
        rows,
        effective_epoch: target_epoch,
    })
}

/// Execute an `AS OF MONOTONE PARTIAL` query.
///
/// Returns rows from the latest available snapshot plus a `complete_through`
/// token.  For a monotone-law view, any epoch up to `complete_through` is
/// guaranteed not to produce retractions.
pub fn execute_monotone_partial(
    available_snapshots: &[(u64, Vec<HistoricalRow>)],
    current_epoch: u64,
) -> MonotonePartialResult {
    // Use the latest available snapshot.
    let (effective_epoch, rows) = available_snapshots
        .iter()
        .max_by_key(|(e, _)| *e)
        .map(|(e, r)| (*e, r.clone()))
        .unwrap_or((current_epoch, vec![]));

    // `complete_through` is the effective epoch; rows beyond may still be in-flight.
    let is_complete = effective_epoch == current_epoch;

    MonotonePartialResult {
        rows,
        complete_through: effective_epoch,
        is_complete,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::error_code::RS_2006;

    fn make_snapshots() -> Vec<(u64, Vec<HistoricalRow>)> {
        vec![
            (
                10,
                vec![
                    HistoricalRow {
                        columns: vec!["a".into(), "10".into()],
                    },
                    HistoricalRow {
                        columns: vec!["b".into(), "20".into()],
                    },
                ],
            ),
            (
                20,
                vec![
                    HistoricalRow {
                        columns: vec!["a".into(), "10".into()],
                    },
                    HistoricalRow {
                        columns: vec!["b".into(), "20".into()],
                    },
                    HistoricalRow {
                        columns: vec!["c".into(), "30".into()],
                    },
                ],
            ),
            (
                30,
                vec![
                    HistoricalRow {
                        columns: vec!["a".into(), "10".into()],
                    },
                    HistoricalRow {
                        columns: vec!["b".into(), "20".into()],
                    },
                    HistoricalRow {
                        columns: vec!["c".into(), "30".into()],
                    },
                    HistoricalRow {
                        columns: vec!["d".into(), "40".into()],
                    },
                ],
            ),
        ]
    }

    fn make_epoch_index() -> Vec<EpochTimestampEntry> {
        vec![
            EpochTimestampEntry {
                epoch: 10,
                wall_clock_ms: 1_000,
            },
            EpochTimestampEntry {
                epoch: 20,
                wall_clock_ms: 2_000,
            },
            EpochTimestampEntry {
                epoch: 30,
                wall_clock_ms: 3_000,
            },
        ]
    }

    // ── AS OF EPOCH ───────────────────────────────────────────────────────────

    /// **Proof criterion (v0.42)**: `SELECT * FROM orders_mv AS OF EPOCH <past>`
    /// returns the correct historical snapshot.
    #[test]
    fn proof_as_of_epoch_returns_correct_snapshot() {
        let snapshots = make_snapshots();
        let config = RetentionConfig {
            checkpoint_retention_count: 50,
            ..Default::default()
        };
        let current_epoch = 40;

        let result = execute_historical_query(
            &HistoricalAsOf::Epoch(10),
            &snapshots,
            &make_epoch_index(),
            &config,
            current_epoch,
        )
        .unwrap();

        assert_eq!(result.effective_epoch, 10);
        assert_eq!(
            result.rows.len(),
            2,
            "epoch 10 snapshot must have 2 rows; got {}",
            result.rows.len()
        );

        // Epoch 20 snapshot has 3 rows.
        let result20 = execute_historical_query(
            &HistoricalAsOf::Epoch(20),
            &snapshots,
            &make_epoch_index(),
            &config,
            current_epoch,
        )
        .unwrap();
        assert_eq!(result20.rows.len(), 3);
        assert_eq!(result20.effective_epoch, 20);

        // Epoch 30 snapshot has 4 rows.
        let result30 = execute_historical_query(
            &HistoricalAsOf::Epoch(30),
            &snapshots,
            &make_epoch_index(),
            &config,
            current_epoch,
        )
        .unwrap();
        assert_eq!(result30.rows.len(), 4);
    }

    // ── AS OF TIMESTAMP ───────────────────────────────────────────────────────

    #[test]
    fn as_of_timestamp_snaps_to_nearest_epoch() {
        let snapshots = make_snapshots();
        let config = RetentionConfig {
            checkpoint_retention_count: 50,
            ..Default::default()
        };
        let current_epoch = 40;

        // Timestamp 1500ms is between epoch 10 (1000ms) and epoch 20 (2000ms).
        // Both are equidistant; min_by_key picks the first encountered.
        let result = execute_historical_query(
            &HistoricalAsOf::Timestamp(1_500),
            &snapshots,
            &make_epoch_index(),
            &config,
            current_epoch,
        )
        .unwrap();
        // Should snap to epoch 10 or 20 (both are 500ms away).
        assert!(
            result.effective_epoch == 10 || result.effective_epoch == 20,
            "expected epoch 10 or 20, got {}",
            result.effective_epoch
        );
    }

    // ── Queries beyond retention → RS-2006 ────────────────────────────────────

    /// **Proof criterion (v0.42)**: Queries beyond the retention window return RS-2006.
    #[test]
    fn proof_beyond_retention_returns_rs_2006() {
        let snapshots = make_snapshots();
        let config = RetentionConfig {
            checkpoint_retention_count: 5,
            ..Default::default()
        };
        let current_epoch = 40;
        // oldest_retained = 40 - 5 = 35; epoch 10 < 35 → RS-2006.

        let err = execute_historical_query(
            &HistoricalAsOf::Epoch(10),
            &snapshots,
            &make_epoch_index(),
            &config,
            current_epoch,
        )
        .unwrap_err();

        match err {
            GatewayError::HistoricalQueryBeyondRetention {
                requested,
                oldest_retained,
            } => {
                assert_eq!(requested, 10);
                assert_eq!(oldest_retained, 35);
                assert_eq!(err.error_code(), RS_2006);
            }
            other => panic!("expected HistoricalQueryBeyondRetention, got {other:?}"),
        }
    }

    #[test]
    fn check_retention_passes_within_window() {
        assert!(check_retention(10, 5).is_ok());
        assert!(check_retention(5, 5).is_ok());
    }

    #[test]
    fn check_retention_fails_before_window() {
        let err = check_retention(4, 5).unwrap_err();
        assert_eq!(err.error_code(), RS_2006);
    }

    // ── AS OF MONOTONE PARTIAL ────────────────────────────────────────────────

    /// **Proof criterion (v0.42)**: `AS OF MONOTONE PARTIAL` returns a result
    /// with an explicit `complete_through` token for a monotone reachability view.
    #[test]
    fn proof_monotone_partial_returns_complete_through_token() {
        let snapshots = make_snapshots();
        let current_epoch = 30; // Same as latest snapshot → fully converged.

        let result = execute_monotone_partial(&snapshots, current_epoch);

        assert_eq!(
            result.complete_through, 30,
            "complete_through must be the epoch of the latest snapshot"
        );
        assert!(
            result.is_complete,
            "at current epoch, result must be complete"
        );
        assert_eq!(
            result.rows.len(),
            4,
            "latest snapshot (epoch 30) has 4 rows"
        );
    }

    #[test]
    fn monotone_partial_in_flight_is_not_complete() {
        let snapshots = make_snapshots();
        let current_epoch = 50; // Beyond latest snapshot → still in-flight.

        let result = execute_monotone_partial(&snapshots, current_epoch);

        assert_eq!(
            result.complete_through, 30,
            "complete_through is the latest snapshot epoch"
        );
        assert!(
            !result.is_complete,
            "current_epoch=50 > snapshot_epoch=30 → not complete"
        );
    }

    // ── find_epoch_for_timestamp ──────────────────────────────────────────────

    #[test]
    fn find_epoch_exact_match() {
        let index = make_epoch_index();
        assert_eq!(find_epoch_for_timestamp(&index, 1_000), Some(10));
        assert_eq!(find_epoch_for_timestamp(&index, 2_000), Some(20));
        assert_eq!(find_epoch_for_timestamp(&index, 3_000), Some(30));
    }

    #[test]
    fn find_epoch_empty_index_returns_none() {
        assert_eq!(find_epoch_for_timestamp(&[], 1_000), None);
    }

    // ── RetentionConfig defaults ──────────────────────────────────────────────

    #[test]
    fn retention_config_default() {
        let cfg = RetentionConfig::default();
        assert_eq!(cfg.checkpoint_retention_count, 100);
        assert_eq!(cfg.checkpoint_retention_duration_ms, 86_400_000);
    }
}
