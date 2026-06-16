//! In-memory bounded change log for a single table view.
//!
//! Records `(epoch, row_key, mz_diff, encoded_row)` tuples.
//! Evicts entries older than the retention window using VecDeque point-eviction
//! (never SlateDB range delete — see invariant test `subscribe_no_range_delete_in_change_log`).
//!
//! # Buffer bound
//! Named upper bound: `CHANGE_LOG_MAX_ENTRIES` (default 10 000).
//! Fill-level metric:  `ViewChangeLog::entry_count()`.
//! Backpressure path:  entries older than retention are evicted from the front;
//!                     subscribers that request epochs before `earliest_epoch()` receive RS-2020.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;

/// Default maximum number of entries retained in a `ViewChangeLog`.
pub const CHANGE_LOG_MAX_ENTRIES: usize = 10_000;

/// A single change-log entry.
#[derive(Debug, Clone)]
pub struct ChangeEntry {
    /// Monotonically increasing epoch at which this change was committed.
    pub epoch: u64,
    /// Storage row key (deterministic from column values).
    pub row_key: Bytes,
    /// Differential multiplicity: +1 for insert/retraction-of-delete, -1 for delete.
    pub mz_diff: i8,
    /// Tab-separated column values for the row.
    pub encoded_row: Bytes,
}

/// Bounded in-memory change log for one table.
///
/// Thread-safety: callers must hold an external `Mutex` when using `push` /
/// `since_epoch` concurrently (see `SubscribeRegistry`).
pub struct ViewChangeLog {
    entries: VecDeque<ChangeEntry>,
    /// Named upper bound.
    max_entries: usize,
    /// Shared fill-level metric — updated on every push/eviction.
    entry_count: Arc<AtomicUsize>,
}

impl ViewChangeLog {
    /// Create a new log with the given capacity.
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            max_entries,
            entry_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create with the default capacity `CHANGE_LOG_MAX_ENTRIES`.
    pub fn with_default_capacity() -> Self {
        Self::new(CHANGE_LOG_MAX_ENTRIES)
    }

    /// Append an entry.  Evicts the oldest entry when `max_entries` is exceeded
    /// (VecDeque pop_front — never a range delete).
    pub fn push(&mut self, entry: ChangeEntry) {
        while self.entries.len() >= self.max_entries {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
        self.entry_count.store(self.entries.len(), Ordering::Relaxed);
    }

    /// Returns all entries with `epoch >= since` in insertion order.
    pub fn since_epoch(&self, since: u64) -> Vec<&ChangeEntry> {
        self.entries
            .iter()
            .filter(|e| e.epoch >= since)
            .collect()
    }

    /// The earliest epoch still retained, or `None` if the log is empty.
    pub fn earliest_epoch(&self) -> Option<u64> {
        self.entries.front().map(|e| e.epoch)
    }

    /// Current number of entries (fill-level metric).
    pub fn entry_count(&self) -> usize {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// A cloneable handle to the fill-level counter (for Prometheus export).
    pub fn entry_count_handle(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.entry_count)
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(epoch: u64, diff: i8) -> ChangeEntry {
        ChangeEntry {
            epoch,
            row_key: Bytes::from(format!("key-{epoch}")),
            mz_diff: diff,
            encoded_row: Bytes::from(format!("val-{epoch}")),
        }
    }

    /// S1 green gate: push > max_entries evicts oldest; since_epoch returns retained only.
    #[test]
    fn view_change_log_push_poll_bound() {
        let max = 5;
        let mut log = ViewChangeLog::new(max);

        // Push max+3 entries — oldest 3 should be evicted.
        for i in 1u64..=8 {
            log.push(make_entry(i, 1));
        }

        // Only max entries retained.
        assert_eq!(log.entry_count(), max);

        // Earliest epoch is 4 (1,2,3 evicted).
        assert_eq!(log.earliest_epoch(), Some(4));

        // since_epoch(1) returns only retained entries (4–8).
        let entries = log.since_epoch(1);
        assert_eq!(entries.len(), max);
        assert_eq!(entries[0].epoch, 4);
        assert_eq!(entries[4].epoch, 8);

        // since_epoch(6) returns only epochs 6–8.
        let entries = log.since_epoch(6);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].epoch, 6);

        // since_epoch(100) returns empty.
        assert!(log.since_epoch(100).is_empty());
    }

    /// Invariant: eviction uses VecDeque pop_front (point-eviction), not range delete.
    /// This test verifies the structural invariant statically via code inspection:
    /// `ViewChangeLog` has no SlateDB field and no range-delete call site.
    #[test]
    fn subscribe_no_range_delete_in_change_log() {
        // The type contains only VecDeque + AtomicUsize + usize — no DB handle.
        // Instantiating and evicting must not panic or call any DB API.
        let mut log = ViewChangeLog::new(2);
        log.push(make_entry(1, 1));
        log.push(make_entry(2, 1));
        log.push(make_entry(3, 1)); // triggers pop_front, no DB call
        assert_eq!(log.entry_count(), 2);
        assert_eq!(log.earliest_epoch(), Some(2));
    }
}
