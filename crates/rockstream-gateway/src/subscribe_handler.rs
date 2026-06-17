//! Subscribe handler — snapshot delivery + live-tail loop + replay from epoch.
//!
//! Implements:
//!  - S3: `AS OF NOW WITH SNAPSHOT` — scan current view state then enter live tail.
//!  - S4: Live-tail via `SubscribeRegistry` + `ViewChangeLog`.
//!  - S5: `AS OF EPOCH <n>` replay + RS-2006 when epoch is before retention.
//!
//! # Buffer bounds
//! - `ViewChangeLog` per table: `CHANGE_LOG_MAX_ENTRIES` entries (see `change_log.rs`).
//! - Subscribe backlog per subscriber: `SUBSCRIBE_BACKLOG_MAX` epochs (= `CHANGE_LOG_MAX_ENTRIES`).
//! - Fill metric: `subscribe_backlog_epochs` (AtomicU64 per subscriber handle).
//! - Backpressure: RS-2020 when backlog > `SUBSCRIBE_BACKLOG_MAX`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::time::sleep;

use crate::change_log::{ChangeEntry, ViewChangeLog, CHANGE_LOG_MAX_ENTRIES};
use crate::subscribe_parser::{SubscribeRequest, SubscribeStart};

/// Named upper bound for per-subscriber backlog (epochs behind head).
pub const SUBSCRIBE_BACKLOG_MAX: usize = CHANGE_LOG_MAX_ENTRIES;

/// Poll interval for the live-tail loop.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

// ── SubscribeRegistry ─────────────────────────────────────────────────────────

/// Shared map from `table_name → ViewChangeLog`.
///
/// `GatewayHandler` holds one registry; `handle_commit` pushes entries into it
/// after each successful write; subscribe handlers poll it.
#[derive(Default)]
pub struct SubscribeRegistry {
    logs: Mutex<HashMap<String, ViewChangeLog>>,
}

impl SubscribeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a change entry for `table_name`.  Creates the log if absent.
    pub fn push(&self, table_name: &str, entry: ChangeEntry) {
        let mut logs = self.logs.lock().unwrap();
        let log = logs
            .entry(table_name.to_string())
            .or_insert_with(ViewChangeLog::with_default_capacity);
        log.push(entry);
    }

    /// Returns a snapshot of all entries with `epoch >= since` for `table_name`.
    /// Returns `None` if there is no log for this table yet.
    pub fn since_epoch(&self, table_name: &str, since: u64) -> Option<Vec<ChangeEntry>> {
        let logs = self.logs.lock().unwrap();
        logs.get(table_name)
            .map(|log| log.since_epoch(since).into_iter().cloned().collect())
    }

    /// Returns the earliest retained epoch for `table_name`, or `None`.
    pub fn earliest_epoch(&self, table_name: &str) -> Option<u64> {
        let logs = self.logs.lock().unwrap();
        logs.get(table_name).and_then(|log| log.earliest_epoch())
    }

    /// Returns the latest epoch in the log, or `None`.
    pub fn latest_epoch(&self, table_name: &str) -> Option<u64> {
        let logs = self.logs.lock().unwrap();
        logs.get(table_name)
            .and_then(|log| log.since_epoch(0).last().map(|e| e.epoch))
    }

    /// Entry count (fill-level metric) for `table_name`.
    pub fn entry_count(&self, table_name: &str) -> usize {
        let logs = self.logs.lock().unwrap();
        logs.get(table_name).map_or(0, |log| log.entry_count())
    }
}

// ── SubscribeRow — what gets sent to the subscriber ──────────────────────────

/// A single row emitted by the subscribe stream.
#[derive(Debug, Clone)]
pub struct SubscribeRow {
    pub mz_timestamp: u64,
    pub mz_diff: i8,
    /// Tab-separated column values (projected if projection is set).
    pub encoded_row: Bytes,
}

// ── SubscribeError ────────────────────────────────────────────────────────────

/// Error codes for subscribe operations.
#[derive(Debug, PartialEq, Eq)]
pub enum SubscribeError {
    /// RS-2006: epoch before retention window.
    EpochBeforeRetention { requested: u64, earliest: u64 },
    /// RS-2020: consumer too slow — fell behind the retention window after start.
    ConsumerTooSlow,
    /// Generic.
    Other(String),
}

impl std::fmt::Display for SubscribeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EpochBeforeRetention {
                requested,
                earliest,
            } => write!(
                f,
                "[RS-2006] history.epoch_before_retention: \
                 requested epoch {requested} is before the earliest retained epoch {earliest}. \
                 next_steps: Reconnect with AS OF NOW WITH SNAPSHOT or a more recent epoch."
            ),
            Self::ConsumerTooSlow => write!(
                f,
                "[RS-2020] subscribe.consumer_too_slow: \
                 subscriber backlog exceeded SUBSCRIBE_BACKLOG_MAX. \
                 next_steps: Reconnect with AS OF NOW WITH SNAPSHOT."
            ),
            Self::Other(s) => write!(f, "subscribe error: {s}"),
        }
    }
}

impl std::error::Error for SubscribeError {}

// ── Snapshot delivery (S3) ────────────────────────────────────────────────────

/// Deliver the current snapshot for `view_name` by scanning the registry for
/// rows that have been written.  Used by `AS OF NOW WITH SNAPSHOT`.
///
/// `col_names`: column names for the table, used to apply WHERE and projection.
/// In production this would scan `ShardDb::scan_prefix("view_output/{view}/")`.
/// For the test harness we accept a pre-built snapshot vector.
pub fn deliver_snapshot(
    snapshot_rows: Vec<(Bytes, Bytes)>,
    current_epoch: u64,
    req: &SubscribeRequest,
    col_names: &[&str],
) -> Vec<SubscribeRow> {
    snapshot_rows
        .into_iter()
        .filter(|(_key, val)| passes_where(val, req.where_clause.as_deref(), col_names))
        .map(|(_key, val)| {
            let projected = apply_projection(&val, req.projection.as_deref(), col_names);
            SubscribeRow {
                mz_timestamp: current_epoch,
                mz_diff: 1,
                encoded_row: projected,
            }
        })
        .collect()
}

// ── Live-tail delivery (S4) ───────────────────────────────────────────────────

/// State held by a single live-tail subscriber.
#[derive(Debug)]
pub struct SubscriberHandle {
    pub table_name: String,
    pub last_sent_epoch: u64,
    /// Fill-level metric: epochs behind the head of the log.
    pub backlog_epochs: Arc<AtomicU64>,
    pub req: SubscribeRequest,
    /// Column names for the table (used for WHERE and name-based projection).
    pub col_names: Vec<String>,
}

impl SubscriberHandle {
    pub fn new(
        table_name: String,
        start_epoch: u64,
        req: SubscribeRequest,
        col_names: Vec<String>,
    ) -> Self {
        Self {
            table_name,
            last_sent_epoch: start_epoch,
            backlog_epochs: Arc::new(AtomicU64::new(0)),
            req,
            col_names,
        }
    }

    /// Poll the registry for new entries since `last_sent_epoch`.
    /// Applies server-side WHERE filter and name-based column projection.
    /// Returns `Ok(rows)` or `Err(SubscribeError::ConsumerTooSlow)`.
    pub fn poll(
        &mut self,
        registry: &SubscribeRegistry,
    ) -> Result<Vec<SubscribeRow>, SubscribeError> {
        let next_epoch = self.last_sent_epoch + 1;
        let entries = registry
            .since_epoch(&self.table_name, next_epoch)
            .unwrap_or_default();

        if entries.is_empty() {
            self.backlog_epochs.store(0, Ordering::Relaxed);
            return Ok(vec![]);
        }

        // Update backlog metric.
        let latest = entries.last().map(|e| e.epoch).unwrap_or(0);
        let backlog = latest.saturating_sub(self.last_sent_epoch);
        self.backlog_epochs.store(backlog, Ordering::Relaxed);

        // Check consumer-too-slow bound.
        if backlog as usize > SUBSCRIBE_BACKLOG_MAX {
            return Err(SubscribeError::ConsumerTooSlow);
        }

        let col_names_ref: Vec<&str> = self.col_names.iter().map(|s| s.as_str()).collect();
        let mut rows = Vec::with_capacity(entries.len());
        for entry in &entries {
            if !passes_where(
                &entry.encoded_row,
                self.req.where_clause.as_deref(),
                &col_names_ref,
            ) {
                continue;
            }
            let projected = apply_projection(
                &entry.encoded_row,
                self.req.projection.as_deref(),
                &col_names_ref,
            );
            rows.push(SubscribeRow {
                mz_timestamp: entry.epoch,
                mz_diff: entry.mz_diff,
                encoded_row: projected,
            });
        }
        if let Some(last) = entries.last() {
            self.last_sent_epoch = last.epoch;
        }
        Ok(rows)
    }
}

// ── Replay from epoch (S5) ────────────────────────────────────────────────────

/// Start a subscribe from a specific epoch.
///
/// Returns `Err(SubscribeError::EpochBeforeRetention)` if `start_epoch` is
/// before `ViewChangeLog::earliest_epoch()` for this table.
pub fn start_from_epoch(
    registry: &SubscribeRegistry,
    req: &SubscribeRequest,
    start_epoch: u64,
    col_names: Vec<String>,
) -> Result<SubscriberHandle, SubscribeError> {
    let table_name = req.view_name.clone();

    // Check retention.
    if let Some(earliest) = registry.earliest_epoch(&table_name) {
        if start_epoch < earliest {
            return Err(SubscribeError::EpochBeforeRetention {
                requested: start_epoch,
                earliest,
            });
        }
    }
    // Replay: start at one epoch before start_epoch so that poll() includes start_epoch.
    let replay_from = start_epoch.saturating_sub(1);
    Ok(SubscriberHandle::new(
        table_name,
        replay_from,
        req.clone(),
        col_names,
    ))
}

/// Run the full subscribe lifecycle (snapshot + live tail) for a request.
///
/// `snapshot_rows`: caller-provided snapshot (from ShardDb scan in production).
/// `current_epoch`: epoch at which the snapshot was taken.
/// `registry`: shared change log.
/// `col_names`: column names for WHERE/projection resolution.
/// `on_row`: callback invoked for each emitted row.
/// `stop`: async predicate; loop exits when it returns `true`.
///
/// Returns `Err(SubscribeError)` on RS-2006 or RS-2020.
pub async fn run_subscribe<F, S>(
    req: &SubscribeRequest,
    snapshot_rows: Vec<(Bytes, Bytes)>,
    current_epoch: u64,
    registry: &SubscribeRegistry,
    col_names: Vec<String>,
    mut on_row: F,
    mut stop: S,
) -> Result<(), SubscribeError>
where
    F: FnMut(SubscribeRow),
    S: FnMut() -> bool,
{
    let col_names_ref: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();

    let start_epoch = match &req.start {
        SubscribeStart::NowWithSnapshot => {
            // Deliver snapshot.
            for row in deliver_snapshot(snapshot_rows, current_epoch, req, &col_names_ref) {
                on_row(row);
            }
            current_epoch
        }
        SubscribeStart::Epoch(e) => {
            // Validate and set up replay.
            if let Some(earliest) = registry.earliest_epoch(&req.view_name) {
                if *e < earliest {
                    return Err(SubscribeError::EpochBeforeRetention {
                        requested: *e,
                        earliest,
                    });
                }
            }
            // Replay entries from log.
            let replay = registry.since_epoch(&req.view_name, *e).unwrap_or_default();
            let mut last = e.saturating_sub(1);
            for entry in &replay {
                if !passes_where(
                    &entry.encoded_row,
                    req.where_clause.as_deref(),
                    &col_names_ref,
                ) {
                    last = entry.epoch;
                    continue;
                }
                let projected = apply_projection(
                    &entry.encoded_row,
                    req.projection.as_deref(),
                    &col_names_ref,
                );
                on_row(SubscribeRow {
                    mz_timestamp: entry.epoch,
                    mz_diff: entry.mz_diff,
                    encoded_row: projected,
                });
                last = entry.epoch;
            }
            last
        }
    };

    // Live-tail loop.
    let mut handle =
        SubscriberHandle::new(req.view_name.clone(), start_epoch, req.clone(), col_names);
    loop {
        if stop() {
            break;
        }
        let rows = handle.poll(registry)?;
        for row in rows {
            on_row(row);
        }
        sleep(POLL_INTERVAL).await;
    }
    Ok(())
}

// ── WHERE filter (S6 prep) ────────────────────────────────────────────────────

/// Apply a server-side WHERE filter to an encoded row.
///
/// Supports simple `<col> <op> <literal>` expressions where the row is
/// tab-separated and the column index is resolved by name from `col_names`.
///
/// Returns `true` if the row passes the filter (or if there is no filter).
pub fn passes_where(encoded_row: &Bytes, where_clause: Option<&str>, col_names: &[&str]) -> bool {
    let Some(pred) = where_clause else {
        return true;
    };
    let fields: Vec<&str> = std::str::from_utf8(encoded_row)
        .unwrap_or("")
        .split('\t')
        .collect();

    // Parse "<col> <op> <literal>" — simple evaluator.
    let pred = pred.trim();
    for op in &[">=", "<=", "!=", ">", "<", "="] {
        if let Some(pos) = pred.find(op) {
            let col = pred[..pos].trim();
            let rhs = pred[pos + op.len()..].trim().trim_matches('\'');
            if let Some(idx) = col_names.iter().position(|&c| c.eq_ignore_ascii_case(col)) {
                let lhs = fields.get(idx).copied().unwrap_or("");
                // Try numeric comparison first, then string.
                let cmp = if let (Ok(l), Ok(r)) = (lhs.parse::<f64>(), rhs.parse::<f64>()) {
                    l.partial_cmp(&r)
                } else {
                    lhs.partial_cmp(rhs)
                };
                return match (cmp, *op) {
                    (Some(std::cmp::Ordering::Greater), ">")
                    | (Some(std::cmp::Ordering::Greater), ">=")
                    | (Some(std::cmp::Ordering::Equal), ">=")
                    | (Some(std::cmp::Ordering::Less), "<")
                    | (Some(std::cmp::Ordering::Less), "<=")
                    | (Some(std::cmp::Ordering::Equal), "<=")
                    | (Some(std::cmp::Ordering::Equal), "=")
                    | (Some(std::cmp::Ordering::Less), "!=")
                    | (Some(std::cmp::Ordering::Greater), "!=") => true,
                    _ => false,
                };
            }
        }
    }
    true // unknown predicate → pass
}

// ── Column projection (S6 prep) ───────────────────────────────────────────────

/// Apply column projection to a tab-separated encoded row using column names.
///
/// `col_names` is the ordered list of all column names for the row.
/// `projection` specifies which columns to keep (by name, in the order requested).
/// If `projection` is `None`, the row is returned unchanged.
pub fn apply_projection(
    encoded_row: &Bytes,
    projection: Option<&[String]>,
    col_names: &[&str],
) -> Bytes {
    let Some(cols) = projection else {
        return encoded_row.clone();
    };
    let fields: Vec<&str> = std::str::from_utf8(encoded_row)
        .unwrap_or("")
        .split('\t')
        .collect();
    let projected: Vec<&str> = cols
        .iter()
        .filter_map(|col| {
            col_names
                .iter()
                .position(|&c| c.eq_ignore_ascii_case(col))
                .and_then(|idx| fields.get(idx).copied())
        })
        .collect();
    Bytes::from(projected.join("\t"))
}

// ── Tests (S3 / S4 / S5 green gates + S6) ────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscribe_parser::{parse_subscribe, SubscribeStart};

    fn make_snapshot(n: usize) -> Vec<(Bytes, Bytes)> {
        (1..=n)
            .map(|i| {
                (
                    Bytes::from(format!("key-{i}")),
                    Bytes::from(format!("{i}\trow{i}")),
                )
            })
            .collect()
    }

    fn make_registry_with_entries(table: &str, epochs: &[(u64, i8)]) -> SubscribeRegistry {
        let reg = SubscribeRegistry::new();
        for &(epoch, diff) in epochs {
            reg.push(
                table,
                ChangeEntry {
                    epoch,
                    row_key: Bytes::from(format!("key-{epoch}")),
                    mz_diff: diff,
                    encoded_row: Bytes::from(format!("{epoch}\trow{epoch}")),
                },
            );
        }
        reg
    }

    // S3 green gate: snapshot delivers N rows with mz_diff = +1.
    #[test]
    fn subscribe_snapshot_delivers_current_state() {
        let n = 5;
        let snapshot = make_snapshot(n);
        let req = parse_subscribe("SUBSCRIBE orders_mv AS OF NOW WITH SNAPSHOT").unwrap();
        let rows = deliver_snapshot(snapshot, 10, &req, &[]);
        assert_eq!(rows.len(), n);
        for row in &rows {
            assert_eq!(row.mz_diff, 1);
            assert_eq!(row.mz_timestamp, 10);
        }
    }

    // S4 green gate: live tail emits +1 for insert, -1 for delete.
    #[test]
    fn subscribe_live_tail_emits_deltas() {
        let registry = SubscribeRegistry::new();
        let req = parse_subscribe("SUBSCRIBE orders_mv").unwrap();
        let mut handle = SubscriberHandle::new("orders_mv".to_string(), 0, req, vec![]);

        // Nothing yet.
        let rows = handle.poll(&registry).unwrap();
        assert!(rows.is_empty());

        // Push an insert at epoch 1.
        registry.push(
            "orders_mv",
            ChangeEntry {
                epoch: 1,
                row_key: Bytes::from("k1"),
                mz_diff: 1,
                encoded_row: Bytes::from("1\trow1"),
            },
        );
        let rows = handle.poll(&registry).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].mz_diff, 1);
        assert_eq!(handle.last_sent_epoch, 1);

        // Push a delete at epoch 2.
        registry.push(
            "orders_mv",
            ChangeEntry {
                epoch: 2,
                row_key: Bytes::from("k1"),
                mz_diff: -1,
                encoded_row: Bytes::from("1\trow1"),
            },
        );
        let rows = handle.poll(&registry).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].mz_diff, -1);
    }

    // S5 green gate: replay from epoch replays only epochs >= start.
    #[test]
    fn subscribe_restart_replays_from_epoch() {
        // Write epochs 1-5.
        let registry =
            make_registry_with_entries("orders_mv", &[(1, 1), (2, 1), (3, 1), (4, 1), (5, 1)]);
        let req = parse_subscribe("SUBSCRIBE orders_mv AS OF EPOCH 3").unwrap();

        let mut handle = start_from_epoch(&registry, &req, 3, vec![]).unwrap();
        // Poll collects epochs 3-5.
        let rows = handle.poll(&registry).unwrap();
        assert_eq!(rows.len(), 3, "expected epochs 3-5");
        assert_eq!(rows[0].mz_timestamp, 3);
        assert_eq!(rows[2].mz_timestamp, 5);
    }

    // S5 green gate: AS OF EPOCH outside retention returns RS-2006.
    #[test]
    fn subscribe_as_of_epoch_outside_retention_returns_rs2006() {
        // max_entries = 3, push 10 entries → earliest = 8.
        let reg = SubscribeRegistry::new();
        {
            let mut logs = reg.logs.lock().unwrap();
            let log = logs
                .entry("orders_mv".to_string())
                .or_insert_with(|| ViewChangeLog::new(3));
            for i in 1u64..=10 {
                log.push(ChangeEntry {
                    epoch: i,
                    row_key: Bytes::from(format!("k{i}")),
                    mz_diff: 1,
                    encoded_row: Bytes::from(format!("{i}")),
                });
            }
        }
        let req = parse_subscribe("SUBSCRIBE orders_mv AS OF EPOCH 1").unwrap();
        let err = start_from_epoch(&reg, &req, 1, vec![]).unwrap_err();
        match err {
            SubscribeError::EpochBeforeRetention {
                requested,
                earliest,
            } => {
                assert_eq!(requested, 1);
                assert!(earliest > 1, "earliest should be > 1 after eviction");
            }
            other => panic!("expected EpochBeforeRetention, got {other:?}"),
        }
    }

    // S6 green gate: WHERE filter removes rows that don't match the predicate.
    #[test]
    fn subscribe_where_filters_rows() {
        // 10 rows with format "{id}\tname{id}", col_names = ["id", "name"]
        let registry = SubscribeRegistry::new();
        let col_names_owned = vec!["id".to_string(), "name".to_string()];
        for i in 1u64..=10 {
            registry.push(
                "t",
                ChangeEntry {
                    epoch: i,
                    row_key: Bytes::from(format!("k{i}")),
                    mz_diff: 1,
                    encoded_row: Bytes::from(format!("{i}\tname{i}")),
                },
            );
        }
        // WHERE id > 5 → only rows 6-10 pass
        let req = parse_subscribe("SUBSCRIBE t WHERE id > 5").unwrap();
        let mut handle = SubscriberHandle::new("t".to_string(), 0, req, col_names_owned);
        let rows = handle.poll(&registry).unwrap();
        assert_eq!(
            rows.len(),
            5,
            "expected 5 rows with id > 5, got {}",
            rows.len()
        );
        for row in &rows {
            let id: u64 = std::str::from_utf8(&row.encoded_row)
                .unwrap()
                .split('\t')
                .next()
                .unwrap()
                .parse()
                .unwrap();
            assert!(id > 5, "expected id > 5, got {id}");
        }
    }

    // S6 green gate: projection selects only the named columns.
    #[test]
    fn subscribe_projection_selects_columns() {
        // Rows with format "{id}\t{name}\t{value}", col_names = ["id", "name", "value"]
        // projection = ["id", "value"] → each output row has 2 fields, no "name"
        let registry = SubscribeRegistry::new();
        let col_names_owned = vec!["id".to_string(), "name".to_string(), "value".to_string()];
        for i in 1u64..=3 {
            registry.push(
                "t",
                ChangeEntry {
                    epoch: i,
                    row_key: Bytes::from(format!("k{i}")),
                    mz_diff: 1,
                    encoded_row: Bytes::from(format!("{i}\tname{i}\tval{i}")),
                },
            );
        }
        let req = parse_subscribe("SUBSCRIBE t (id, value)").unwrap();
        let mut handle = SubscriberHandle::new("t".to_string(), 0, req, col_names_owned);
        let rows = handle.poll(&registry).unwrap();
        assert_eq!(rows.len(), 3);
        for row in &rows {
            let s = std::str::from_utf8(&row.encoded_row).unwrap();
            let fields: Vec<&str> = s.split('\t').collect();
            assert_eq!(
                fields.len(),
                2,
                "expected 2 projected fields, got {:?}",
                fields
            );
            // Should NOT contain "name" values
            assert!(
                !fields[0].starts_with("name"),
                "first field should be id, not name"
            );
        }
    }
}
