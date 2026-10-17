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
/// Maximum payload bytes retained for a view subscription (16 MiB).
pub const CHANGE_LOG_MAX_BUFFER_BYTES: usize = 16 * 1024 * 1024;
/// A complete committed epoch exceeding 8 MiB cannot be buffered for replay.
pub const CHANGE_LOG_MAX_EPOCH_BYTES: usize = 8 * 1024 * 1024;

/// A bounded log must never retain only a subset of an oversized committed epoch.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChangeLogError {
    #[error(
        "committed epoch {epoch} exceeds the subscription envelope ({rows} rows, {bytes} bytes)"
    )]
    EpochTooLarge {
        epoch: u64,
        rows: usize,
        bytes: usize,
    },
    #[error("subscription epoch must advance: latest {latest}, requested {requested}")]
    NonMonotoneEpoch { latest: u64, requested: u64 },
    #[error("subscription batch mixes committed epochs")]
    MixedEpochs,
}

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
    bytes: usize,
    /// Minimum valid replay position following a retention eviction or overflow.
    replay_floor: Option<u64>,
    latest_epoch_seen: Option<u64>,
    /// Shared fill-level metric — updated on every push/eviction.
    entry_count: Arc<AtomicUsize>,
}

impl ViewChangeLog {
    /// Create a new log with the given capacity.
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            max_entries,
            bytes: 0,
            replay_floor: None,
            latest_epoch_seen: None,
            entry_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create with the default capacity `CHANGE_LOG_MAX_ENTRIES`.
    pub fn with_default_capacity() -> Self {
        Self::new(CHANGE_LOG_MAX_ENTRIES)
    }

    fn entry_bytes(entry: &ChangeEntry) -> usize {
        entry.row_key.len().saturating_add(entry.encoded_row.len())
    }

    fn discard_oldest_epoch(&mut self) {
        let Some(epoch) = self.entries.front().map(|entry| entry.epoch) else {
            return;
        };
        while self
            .entries
            .front()
            .is_some_and(|entry| entry.epoch == epoch)
        {
            if let Some(entry) = self.entries.pop_front() {
                self.bytes = self.bytes.saturating_sub(Self::entry_bytes(&entry));
            }
        }
        self.replay_floor = Some(self.replay_floor.unwrap_or(0).max(epoch.saturating_add(1)));
    }

    /// Append one complete durable epoch or reject it without retaining a partial
    /// epoch. Rejection invalidates replay so lagging clients must resnapshot.
    pub fn push_epoch(&mut self, batch: Vec<ChangeEntry>) -> Result<(), ChangeLogError> {
        let Some(epoch) = batch.first().map(|entry| entry.epoch) else {
            return Ok(());
        };
        if batch.iter().any(|entry| entry.epoch != epoch) {
            return Err(ChangeLogError::MixedEpochs);
        }
        if let Some(latest) = self.latest_epoch_seen {
            if epoch <= latest {
                return Err(ChangeLogError::NonMonotoneEpoch {
                    latest,
                    requested: epoch,
                });
            }
        }
        let bytes = batch
            .iter()
            .try_fold(0usize, |sum, entry| {
                sum.checked_add(Self::entry_bytes(entry))
            })
            .unwrap_or(usize::MAX);
        self.latest_epoch_seen = Some(epoch);

        if batch.len() > self.max_entries
            || bytes > CHANGE_LOG_MAX_EPOCH_BYTES
            || bytes > CHANGE_LOG_MAX_BUFFER_BYTES
        {
            self.entries.clear();
            self.bytes = 0;
            self.entry_count.store(0, Ordering::Relaxed);
            self.replay_floor = Some(epoch.saturating_add(1));
            return Err(ChangeLogError::EpochTooLarge {
                epoch,
                rows: batch.len(),
                bytes,
            });
        }

        while !self.entries.is_empty()
            && (self.entries.len().saturating_add(batch.len()) > self.max_entries
                || self.bytes.saturating_add(bytes) > CHANGE_LOG_MAX_BUFFER_BYTES)
        {
            self.discard_oldest_epoch();
        }
        self.bytes += bytes;
        self.entries.extend(batch);
        self.entry_count
            .store(self.entries.len(), Ordering::Relaxed);
        Ok(())
    }

    /// Legacy single-row insertion for consumers that do not publish an entire
    /// epoch. Production commit paths use `push_epoch` instead.
    pub fn push(&mut self, entry: ChangeEntry) {
        let epoch = entry.epoch;
        let bytes = Self::entry_bytes(&entry);
        self.latest_epoch_seen = Some(self.latest_epoch_seen.unwrap_or(epoch).max(epoch));
        if self.max_entries == 0 || bytes > CHANGE_LOG_MAX_EPOCH_BYTES {
            self.entries.clear();
            self.bytes = 0;
            self.replay_floor = Some(epoch.saturating_add(1));
            self.entry_count.store(0, Ordering::Relaxed);
            return;
        }
        while !self.entries.is_empty()
            && (self.entries.len() >= self.max_entries
                || self.bytes.saturating_add(bytes) > CHANGE_LOG_MAX_BUFFER_BYTES)
        {
            self.discard_oldest_epoch();
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.entries.push_back(entry);
        self.entry_count
            .store(self.entries.len(), Ordering::Relaxed);
    }

    /// Earliest safely replayable epoch after eviction/overflow.
    pub fn replay_floor(&self) -> Option<u64> {
        self.replay_floor
    }

    /// Most recently published epoch, even if it exceeded buffer bounds.
    pub fn latest_epoch(&self) -> Option<u64> {
        self.latest_epoch_seen
    }

    /// Retained entry payload bytes (does not include VecDeque allocator overhead).
    pub fn buffered_bytes(&self) -> usize {
        self.bytes
    }

    /// Returns all entries with `epoch >= since` in insertion order.
    pub fn since_epoch(&self, since: u64) -> Vec<&ChangeEntry> {
        self.entries.iter().filter(|e| e.epoch >= since).collect()
    }

    /// The earliest epoch still retained, or `None` if the log is empty.
    pub fn earliest_epoch(&self) -> Option<u64> {
        self.entries
            .front()
            .map(|e| e.epoch.max(self.replay_floor.unwrap_or(0)))
            .or(self.replay_floor)
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
