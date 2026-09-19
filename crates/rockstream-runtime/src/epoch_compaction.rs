//! In-memory Z-set epoch compaction filter and accumulator.
//!
//! Under high-frequency ingress (e.g., rapid updates or deletes on the same primary key
//! or identical row payload), multiple offsetting row modifications (+1/-1 weights) can
//! collapse to net-zero or net-single updates in-memory within a micro-batch epoch window
//! (typically 100ms to 300ms).
//!
//! This module provides [`EpochCompactor`] (also aliased as [`EpochCompactionFilter`]
//! and [`InFlightZSetAccumulator`]), which:
//! - Tracks in-flight row updates for active epochs.
//! - Enforces micro-batch window bounds (default 100ms, bounded between 100ms and 300ms).
//! - Accumulates signed row delta weights (`i64`).
//! - Collapses multiple updates with non-zero net weight into a single row.
//! - Cancels offsetting updates where net weight is 0 (`weight == 0`), omitting them on flush.
//! - Tracks metrics for input updates, cancelled zero-weight updates, collapsed updates, and emitted updates.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rockstream_types::data_plane::RuntimeRow;
use rockstream_types::timestamp::Epoch;
use serde::{Deserialize, Serialize};

/// Default micro-batch epoch window duration (100ms).
pub const DEFAULT_EPOCH_WINDOW: Duration = Duration::from_millis(100);

/// Minimum allowable epoch window duration (100ms).
pub const MIN_EPOCH_WINDOW: Duration = Duration::from_millis(100);

/// Maximum allowable epoch window duration (300ms).
pub const MAX_EPOCH_WINDOW: Duration = Duration::from_millis(300);

/// Errors related to epoch compaction configuration and lifecycle.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EpochCompactionError {
    #[error("Epoch window duration {actual:?} is below minimum {minimum:?}")]
    WindowDurationBelowMinimum { actual: Duration, minimum: Duration },
    #[error("Epoch window duration {actual:?} is above maximum {maximum:?}")]
    WindowDurationAboveMaximum { actual: Duration, maximum: Duration },
}

/// Key extraction strategy for identifying identical or primary-key-matched rows.
#[derive(Clone, Default)]
pub enum KeyExtractionStrategy {
    /// Exact match on the entire `values_tsv` string (canonical Z-set element identity).
    #[default]
    ValuesTsv,
    /// Extract a single column index (0-indexed) as the primary key.
    ColumnIndex(usize),
    /// Extract multiple column indices (0-indexed) joined with `\t` as composite primary key.
    ColumnIndices(Vec<usize>),
    /// Custom key extractor function.
    Custom(Arc<dyn Fn(&RuntimeRow) -> String + Send + Sync>),
}

impl std::fmt::Debug for KeyExtractionStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ValuesTsv => write!(f, "ValuesTsv"),
            Self::ColumnIndex(idx) => write!(f, "ColumnIndex({idx})"),
            Self::ColumnIndices(indices) => write!(f, "ColumnIndices({indices:?})"),
            Self::Custom(_) => write!(f, "Custom(<closure>)"),
        }
    }
}

impl KeyExtractionStrategy {
    /// Extract key string from a given `RuntimeRow`.
    pub fn extract_key(&self, row: &RuntimeRow) -> String {
        match self {
            Self::ValuesTsv => row.values_tsv.clone(),
            Self::ColumnIndex(col) => row
                .values_tsv
                .split('\t')
                .nth(*col)
                .unwrap_or("")
                .to_string(),
            Self::ColumnIndices(cols) => {
                let parts: Vec<&str> = row.values_tsv.split('\t').collect();
                cols.iter()
                    .map(|&idx| parts.get(idx).copied().unwrap_or(""))
                    .collect::<Vec<_>>()
                    .join("\t")
            }
            Self::Custom(extractor) => extractor(row),
        }
    }
}

/// Configuration for epoch compaction and micro-batch windowing.
#[derive(Debug, Clone)]
pub struct EpochCompactionConfig {
    /// Configured window duration before an active epoch is eligible for expiration flush.
    pub window_duration: Duration,
    /// Minimum allowed window duration.
    pub min_window_duration: Duration,
    /// Maximum allowed window duration.
    pub max_window_duration: Duration,
    /// Strategy used to match offsetting/collapsing rows.
    pub key_strategy: KeyExtractionStrategy,
}

impl Default for EpochCompactionConfig {
    fn default() -> Self {
        Self {
            window_duration: DEFAULT_EPOCH_WINDOW,
            min_window_duration: MIN_EPOCH_WINDOW,
            max_window_duration: MAX_EPOCH_WINDOW,
            key_strategy: KeyExtractionStrategy::default(),
        }
    }
}

impl EpochCompactionConfig {
    /// Create a new configuration with the specified window duration, validating against bounds.
    pub fn new(window_duration: Duration) -> Result<Self, EpochCompactionError> {
        let config = Self {
            window_duration,
            ..Default::default()
        };
        config.validate()?;
        Ok(config)
    }

    /// Set window duration with bounds validation.
    pub fn with_window_duration(
        mut self,
        window_duration: Duration,
    ) -> Result<Self, EpochCompactionError> {
        self.window_duration = window_duration;
        self.validate()?;
        Ok(self)
    }

    /// Set window duration clamped between `min_window_duration` and `max_window_duration`.
    pub fn with_window_duration_clamped(mut self, window_duration: Duration) -> Self {
        self.window_duration =
            window_duration.clamp(self.min_window_duration, self.max_window_duration);
        self
    }

    /// Override the allowable bounds for window duration (useful for testing or flexible environments).
    pub fn with_bounds(mut self, min: Duration, max: Duration) -> Self {
        self.min_window_duration = min;
        self.max_window_duration = max;
        self
    }

    /// Configure key extraction strategy.
    pub fn with_key_strategy(mut self, strategy: KeyExtractionStrategy) -> Self {
        self.key_strategy = strategy;
        self
    }

    /// Configure primary key extraction using a single column index.
    pub fn with_primary_key_column(mut self, col_idx: usize) -> Self {
        self.key_strategy = KeyExtractionStrategy::ColumnIndex(col_idx);
        self
    }

    /// Configure composite primary key extraction using multiple column indices.
    pub fn with_primary_key_columns(mut self, indices: Vec<usize>) -> Self {
        self.key_strategy = KeyExtractionStrategy::ColumnIndices(indices);
        self
    }

    /// Validate the current configuration against bounds.
    pub fn validate(&self) -> Result<(), EpochCompactionError> {
        if self.window_duration < self.min_window_duration {
            return Err(EpochCompactionError::WindowDurationBelowMinimum {
                actual: self.window_duration,
                minimum: self.min_window_duration,
            });
        }
        if self.window_duration > self.max_window_duration {
            return Err(EpochCompactionError::WindowDurationAboveMaximum {
                actual: self.window_duration,
                maximum: self.max_window_duration,
            });
        }
        Ok(())
    }
}

/// Atomic counters tracking compaction activity.
#[derive(Debug, Default)]
pub struct EpochCompactionMetrics {
    input_updates: AtomicU64,
    cancelled_zero_weight_updates: AtomicU64,
    emitted_updates: AtomicU64,
    collapsed_updates: AtomicU64,
}

impl EpochCompactionMetrics {
    /// Total row updates received as input.
    pub fn input_updates(&self) -> u64 {
        self.input_updates.load(Ordering::Relaxed)
    }

    /// Total entries cancelled out due to net zero weight and omitted on flush.
    pub fn cancelled_zero_weight_updates(&self) -> u64 {
        self.cancelled_zero_weight_updates.load(Ordering::Relaxed)
    }

    /// Alias for `cancelled_zero_weight_updates`.
    pub fn cancelled_updates(&self) -> u64 {
        self.cancelled_zero_weight_updates()
    }

    /// Total rows emitted with non-zero net weight upon flush.
    pub fn emitted_updates(&self) -> u64 {
        self.emitted_updates.load(Ordering::Relaxed)
    }

    /// Total updates that merged into an existing key in the active epoch.
    pub fn collapsed_updates(&self) -> u64 {
        self.collapsed_updates.load(Ordering::Relaxed)
    }

    /// Record input updates.
    pub fn record_input(&self, count: u64) {
        self.input_updates.fetch_add(count, Ordering::Relaxed);
    }

    /// Record cancelled zero-weight updates.
    pub fn record_cancelled(&self, count: u64) {
        self.cancelled_zero_weight_updates
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Record emitted updates.
    pub fn record_emitted(&self, count: u64) {
        self.emitted_updates.fetch_add(count, Ordering::Relaxed);
    }

    /// Record collapsed updates.
    pub fn record_collapsed(&self, count: u64) {
        self.collapsed_updates.fetch_add(count, Ordering::Relaxed);
    }

    /// Take a point-in-time snapshot of the metrics.
    pub fn snapshot(&self) -> EpochCompactionMetricsSnapshot {
        EpochCompactionMetricsSnapshot {
            input_updates: self.input_updates(),
            cancelled_zero_weight_updates: self.cancelled_zero_weight_updates(),
            emitted_updates: self.emitted_updates(),
            collapsed_updates: self.collapsed_updates(),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.input_updates.store(0, Ordering::Relaxed);
        self.cancelled_zero_weight_updates
            .store(0, Ordering::Relaxed);
        self.emitted_updates.store(0, Ordering::Relaxed);
        self.collapsed_updates.store(0, Ordering::Relaxed);
    }
}

/// Snapshot of compaction metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct EpochCompactionMetricsSnapshot {
    pub input_updates: u64,
    pub cancelled_zero_weight_updates: u64,
    pub emitted_updates: u64,
    pub collapsed_updates: u64,
}

#[derive(Debug, Clone)]
struct InFlightRow {
    values_tsv: String,
    weight: i64,
    first_seen_seq: usize,
}

/// State of an active epoch currently accumulating in-flight rows.
#[derive(Debug, Clone)]
pub struct ActiveEpochState {
    pub epoch: Epoch,
    pub opened_at: Instant,
    pub last_updated_at: Instant,
    rows: BTreeMap<String, InFlightRow>,
    next_seq: usize,
}

impl ActiveEpochState {
    fn new(epoch: Epoch, now: Instant) -> Self {
        Self {
            epoch,
            opened_at: now,
            last_updated_at: now,
            rows: BTreeMap::new(),
            next_seq: 0,
        }
    }

    /// Total distinct keys currently tracked.
    pub fn key_count(&self) -> usize {
        self.rows.len()
    }

    /// Count of keys with non-zero net weight.
    pub fn net_non_zero_count(&self) -> usize {
        self.rows.values().filter(|r| r.weight != 0).count()
    }

    /// Count of keys that currently cancel out to zero weight.
    pub fn net_zero_count(&self) -> usize {
        self.rows.values().filter(|r| r.weight == 0).count()
    }
}

/// In-memory Z-set accumulator and compaction coordinator.
///
/// Accumulates row updates per active epoch over a configurable micro-batch window
/// (100ms - 300ms). When flushed, updates for the same key collapse their signed weights;
/// updates that resolve to a net weight of zero are omitted completely.
#[derive(Debug)]
pub struct EpochCompactor {
    config: EpochCompactionConfig,
    metrics: Arc<EpochCompactionMetrics>,
    active_epochs: Mutex<BTreeMap<Epoch, ActiveEpochState>>,
}

impl Clone for EpochCompactor {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            active_epochs: Mutex::new(self.active_epochs.lock().clone()),
        }
    }
}

/// Alias for [`EpochCompactor`].
pub type EpochCompactionFilter = EpochCompactor;

/// Alias for [`EpochCompactor`].
pub type InFlightZSetAccumulator = EpochCompactor;

impl Default for EpochCompactor {
    fn default() -> Self {
        Self::new(EpochCompactionConfig::default())
    }
}

impl EpochCompactor {
    /// Create a new compactor with the provided configuration.
    pub fn new(config: EpochCompactionConfig) -> Self {
        Self {
            config,
            metrics: Arc::new(EpochCompactionMetrics::default()),
            active_epochs: Mutex::new(BTreeMap::new()),
        }
    }

    /// Create a compactor with a specific window duration, validating against standard bounds.
    pub fn with_window_duration(duration: Duration) -> Result<Self, EpochCompactionError> {
        let config = EpochCompactionConfig::new(duration)?;
        Ok(Self::new(config))
    }

    /// Access the configuration.
    pub fn config(&self) -> &EpochCompactionConfig {
        &self.config
    }

    /// Access the shared metrics.
    pub fn metrics(&self) -> &Arc<EpochCompactionMetrics> {
        &self.metrics
    }

    /// Get a point-in-time metrics snapshot.
    pub fn metrics_snapshot(&self) -> EpochCompactionMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Open an active epoch explicitly if not already present.
    pub fn open_epoch(&self, epoch: Epoch) {
        let now = Instant::now();
        self.active_epochs
            .lock()
            .entry(epoch)
            .or_insert_with(|| ActiveEpochState::new(epoch, now));
    }

    /// Ingest a single [`RuntimeRow`] delta for the given epoch.
    pub fn push_row(&self, epoch: Epoch, row: RuntimeRow) {
        self.push_row_at(epoch, row, Instant::now());
    }

    /// Ingest a single row delta specifying timestamp explicitly.
    pub fn push_row_at(&self, epoch: Epoch, row: RuntimeRow, now: Instant) {
        self.metrics.record_input(1);
        let key = self.config.key_strategy.extract_key(&row);
        let mut epochs = self.active_epochs.lock();
        let epoch_state = epochs
            .entry(epoch)
            .or_insert_with(|| ActiveEpochState::new(epoch, now));

        epoch_state.last_updated_at = now;

        if let Some(existing) = epoch_state.rows.get_mut(&key) {
            existing.weight += row.weight;
            existing.values_tsv = row.values_tsv;
            self.metrics.record_collapsed(1);
        } else {
            let seq = epoch_state.next_seq;
            epoch_state.next_seq += 1;
            epoch_state.rows.insert(
                key,
                InFlightRow {
                    values_tsv: row.values_tsv,
                    weight: row.weight,
                    first_seen_seq: seq,
                },
            );
        }
    }

    /// Ingest multiple [`RuntimeRow`] deltas for the given epoch.
    pub fn push_rows<I>(&self, epoch: Epoch, rows: I)
    where
        I: IntoIterator<Item = RuntimeRow>,
    {
        let now = Instant::now();
        for row in rows {
            self.push_row_at(epoch, row, now);
        }
    }

    /// Ingest a row delta by values_tsv string and signed weight.
    pub fn push_delta(&self, epoch: Epoch, values_tsv: impl Into<String>, weight: i64) {
        self.push_row(
            epoch,
            RuntimeRow {
                values_tsv: values_tsv.into(),
                weight,
            },
        );
    }

    /// Ingest multiple deltas by values_tsv string and signed weight.
    pub fn push_deltas<I, S>(&self, epoch: Epoch, deltas: I)
    where
        I: IntoIterator<Item = (S, i64)>,
        S: Into<String>,
    {
        let now = Instant::now();
        for (tsv, weight) in deltas {
            self.push_row_at(
                epoch,
                RuntimeRow {
                    values_tsv: tsv.into(),
                    weight,
                },
                now,
            );
        }
    }

    /// Returns whether the specified epoch is currently active.
    pub fn has_epoch(&self, epoch: Epoch) -> bool {
        self.active_epochs.lock().contains_key(&epoch)
    }

    /// Returns list of all active epoch IDs in ascending order.
    pub fn active_epochs(&self) -> Vec<Epoch> {
        self.active_epochs.lock().keys().copied().collect()
    }

    /// Count of currently active epochs.
    pub fn active_epoch_count(&self) -> usize {
        self.active_epochs.lock().len()
    }

    /// Total distinct keys tracked for an active epoch.
    pub fn epoch_key_count(&self, epoch: Epoch) -> Option<usize> {
        self.active_epochs.lock().get(&epoch).map(|s| s.key_count())
    }

    /// Age of an active epoch measured from when it was opened.
    pub fn epoch_age(&self, epoch: Epoch) -> Option<Duration> {
        self.epoch_age_at(epoch, Instant::now())
    }

    /// Age of an active epoch relative to a provided reference instant.
    pub fn epoch_age_at(&self, epoch: Epoch, now: Instant) -> Option<Duration> {
        self.active_epochs
            .lock()
            .get(&epoch)
            .map(|s| now.saturating_duration_since(s.opened_at))
    }

    /// Check if an epoch's micro-batch window has expired.
    pub fn is_epoch_expired(&self, epoch: Epoch) -> bool {
        self.is_epoch_expired_at(epoch, Instant::now())
    }

    /// Check if an epoch's micro-batch window has expired relative to a provided instant.
    pub fn is_epoch_expired_at(&self, epoch: Epoch, now: Instant) -> bool {
        match self.epoch_age_at(epoch, now) {
            Some(age) => age >= self.config.window_duration,
            None => false,
        }
    }

    /// Returns IDs of all active epochs whose window has expired.
    pub fn expired_epochs(&self) -> Vec<Epoch> {
        self.expired_epochs_at(Instant::now())
    }

    /// Returns IDs of all active epochs whose window has expired relative to `now`.
    pub fn expired_epochs_at(&self, now: Instant) -> Vec<Epoch> {
        self.active_epochs
            .lock()
            .iter()
            .filter_map(|(&epoch, state)| {
                let age = now.saturating_duration_since(state.opened_at);
                if age >= self.config.window_duration {
                    Some(epoch)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Drain and compact an epoch, omitting net-zero weight entries and collapsing non-zero deltas.
    ///
    /// Emitted rows preserve original first-arrival order of their respective keys.
    /// Returns `None` if the epoch was not active.
    pub fn drain_epoch(&self, epoch: Epoch) -> Option<Vec<RuntimeRow>> {
        let state = self.active_epochs.lock().remove(&epoch)?;

        let mut non_zero_rows: Vec<InFlightRow> = Vec::new();
        let mut cancelled_count = 0u64;

        for (_key, row) in state.rows {
            if row.weight == 0 {
                cancelled_count += 1;
            } else {
                non_zero_rows.push(row);
            }
        }

        self.metrics.record_cancelled(cancelled_count);
        self.metrics.record_emitted(non_zero_rows.len() as u64);

        // Sort by first arrival sequence to maintain deterministic arrival ordering
        non_zero_rows.sort_by_key(|r| r.first_seen_seq);

        let result = non_zero_rows
            .into_iter()
            .map(|r| RuntimeRow {
                values_tsv: r.values_tsv,
                weight: r.weight,
            })
            .collect();

        Some(result)
    }

    /// Flush an epoch boundary (alias for `drain_epoch`).
    pub fn flush_epoch(&self, epoch: Epoch) -> Option<Vec<RuntimeRow>> {
        self.drain_epoch(epoch)
    }

    /// Drain all expired active epochs.
    pub fn drain_expired_epochs(&self) -> Vec<(Epoch, Vec<RuntimeRow>)> {
        self.drain_expired_epochs_at(Instant::now())
    }

    /// Drain all expired active epochs relative to `now`.
    pub fn drain_expired_epochs_at(&self, now: Instant) -> Vec<(Epoch, Vec<RuntimeRow>)> {
        let expired = self.expired_epochs_at(now);
        expired
            .into_iter()
            .filter_map(|epoch| self.drain_epoch(epoch).map(|rows| (epoch, rows)))
            .collect()
    }

    /// Flush all expired active epochs (alias for `drain_expired_epochs`).
    pub fn flush_expired_epochs(&self) -> Vec<(Epoch, Vec<RuntimeRow>)> {
        self.drain_expired_epochs()
    }

    /// Drain all active epochs regardless of window expiration.
    pub fn drain_all(&self) -> Vec<(Epoch, Vec<RuntimeRow>)> {
        let epochs = self.active_epochs();
        epochs
            .into_iter()
            .filter_map(|epoch| self.drain_epoch(epoch).map(|rows| (epoch, rows)))
            .collect()
    }

    /// Flush all active epochs (alias for `drain_all`).
    pub fn flush_all(&self) -> Vec<(Epoch, Vec<RuntimeRow>)> {
        self.drain_all()
    }

    /// Stateless compaction helper: compacts an arbitrary slice of [`RuntimeRow`] deltas
    /// using this compactor's key extraction strategy.
    pub fn compact_slice(&self, rows: &[RuntimeRow]) -> Vec<RuntimeRow> {
        self.compact_rows(rows.iter().cloned())
    }

    /// Stateless compaction helper: compacts an iterator of [`RuntimeRow`] deltas.
    pub fn compact_rows<I>(&self, rows: I) -> Vec<RuntimeRow>
    where
        I: IntoIterator<Item = RuntimeRow>,
    {
        let mut map: BTreeMap<String, InFlightRow> = BTreeMap::new();
        let mut next_seq = 0usize;
        let mut input_count = 0u64;
        let mut collapsed_count = 0u64;

        for row in rows {
            input_count += 1;
            let key = self.config.key_strategy.extract_key(&row);
            if let Some(existing) = map.get_mut(&key) {
                existing.weight += row.weight;
                existing.values_tsv = row.values_tsv;
                collapsed_count += 1;
            } else {
                let seq = next_seq;
                next_seq += 1;
                map.insert(
                    key,
                    InFlightRow {
                        values_tsv: row.values_tsv,
                        weight: row.weight,
                        first_seen_seq: seq,
                    },
                );
            }
        }

        let mut non_zero: Vec<InFlightRow> = Vec::new();
        let mut cancelled_count = 0u64;

        for (_key, row) in map {
            if row.weight == 0 {
                cancelled_count += 1;
            } else {
                non_zero.push(row);
            }
        }

        self.metrics.record_input(input_count);
        self.metrics.record_collapsed(collapsed_count);
        self.metrics.record_cancelled(cancelled_count);
        self.metrics.record_emitted(non_zero.len() as u64);

        non_zero.sort_by_key(|r| r.first_seen_seq);

        non_zero
            .into_iter()
            .map(|r| RuntimeRow {
                values_tsv: r.values_tsv,
                weight: r.weight,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_bounds_and_defaults() {
        let default_config = EpochCompactionConfig::default();
        assert_eq!(default_config.window_duration, Duration::from_millis(100));
        assert_eq!(
            default_config.min_window_duration,
            Duration::from_millis(100)
        );
        assert_eq!(
            default_config.max_window_duration,
            Duration::from_millis(300)
        );
        assert!(default_config.validate().is_ok());

        // Valid boundary values
        assert!(EpochCompactionConfig::new(Duration::from_millis(100)).is_ok());
        assert!(EpochCompactionConfig::new(Duration::from_millis(200)).is_ok());
        assert!(EpochCompactionConfig::new(Duration::from_millis(300)).is_ok());

        // Out of bounds
        assert_eq!(
            EpochCompactionConfig::new(Duration::from_millis(50)).unwrap_err(),
            EpochCompactionError::WindowDurationBelowMinimum {
                actual: Duration::from_millis(50),
                minimum: Duration::from_millis(100),
            }
        );
        assert_eq!(
            EpochCompactionConfig::new(Duration::from_millis(350)).unwrap_err(),
            EpochCompactionError::WindowDurationAboveMaximum {
                actual: Duration::from_millis(350),
                maximum: Duration::from_millis(300),
            }
        );

        // Clamping
        let clamped = default_config
            .clone()
            .with_window_duration_clamped(Duration::from_millis(50));
        assert_eq!(clamped.window_duration, Duration::from_millis(100));

        let clamped_high = default_config.with_window_duration_clamped(Duration::from_millis(500));
        assert_eq!(clamped_high.window_duration, Duration::from_millis(300));
    }

    #[test]
    fn test_zero_net_weight_omission() {
        let compactor = EpochCompactor::default();
        let epoch = 1;

        // Ingest offsetting row updates (+1 and -1 for same row)
        compactor.push_delta(epoch, "101\talice\tengineer", 1);
        compactor.push_delta(epoch, "101\talice\tengineer", -1);

        // Ingest another row with offsetting weights (+2, -1, -1)
        compactor.push_delta(epoch, "102\tbob\tdesigner", 2);
        compactor.push_delta(epoch, "102\tbob\tdesigner", -1);
        compactor.push_delta(epoch, "102\tbob\tdesigner", -1);

        // Ingest a row with non-zero net weight
        compactor.push_delta(epoch, "103\tcharlie\tmanager", 1);

        let flushed = compactor.drain_epoch(epoch).expect("epoch should exist");

        // Both 101 and 102 should be completely omitted; only 103 remains
        assert_eq!(
            flushed,
            vec![RuntimeRow {
                values_tsv: "103\tcharlie\tmanager".to_string(),
                weight: 1,
            }]
        );

        let metrics = compactor.metrics_snapshot();
        assert_eq!(metrics.input_updates, 6);
        assert_eq!(metrics.cancelled_zero_weight_updates, 2);
        assert_eq!(metrics.emitted_updates, 1);
        assert_eq!(metrics.collapsed_updates, 3);
    }

    #[test]
    fn test_collapsing_updates_exact_weights() {
        let compactor = EpochCompactor::default();
        let epoch = 42;

        // Multiple increments collapse into single positive net weight
        compactor.push_delta(epoch, "key_a", 1);
        compactor.push_delta(epoch, "key_a", 2);
        compactor.push_delta(epoch, "key_a", 3);

        // Mixed increments and decrements with positive net weight
        compactor.push_delta(epoch, "key_b", 5);
        compactor.push_delta(epoch, "key_b", -2);

        // Negative net weight delta (valid Z-set deletion)
        compactor.push_delta(epoch, "key_c", -1);
        compactor.push_delta(epoch, "key_c", -2);

        let flushed = compactor.drain_epoch(epoch).unwrap();

        assert_eq!(
            flushed,
            vec![
                RuntimeRow {
                    values_tsv: "key_a".to_string(),
                    weight: 6,
                },
                RuntimeRow {
                    values_tsv: "key_b".to_string(),
                    weight: 3,
                },
                RuntimeRow {
                    values_tsv: "key_c".to_string(),
                    weight: -3,
                },
            ]
        );

        let metrics = compactor.metrics_snapshot();
        assert_eq!(metrics.input_updates, 7);
        assert_eq!(metrics.cancelled_zero_weight_updates, 0);
        assert_eq!(metrics.emitted_updates, 3);
        assert_eq!(metrics.collapsed_updates, 4);
    }

    #[test]
    fn test_primary_key_matching_collapse() {
        // Strategy matching on column 0 (primary key)
        let config = EpochCompactionConfig::default().with_primary_key_column(0);
        let compactor = EpochCompactor::new(config);
        let epoch = 10;

        // First an insert (+1), then an update which in Z-set is delete old (-1) and insert new (+1)
        // With primary key matching on col 0 ("1"), the delete offsets the original insert!
        compactor.push_delta(epoch, "1\talice\tv1", 1);
        compactor.push_delta(epoch, "1\talice\tv1", -1);
        compactor.push_delta(epoch, "1\talice\tv2", 1);

        // Key "2": insert (+1) and delete (+-1) -> cancels to 0
        compactor.push_delta(epoch, "2\tbob\tv1", 1);
        compactor.push_delta(epoch, "2\tbob\tv1", -1);

        let flushed = compactor.drain_epoch(epoch).unwrap();

        assert_eq!(
            flushed,
            vec![RuntimeRow {
                values_tsv: "1\talice\tv2".to_string(),
                weight: 1,
            }]
        );

        let metrics = compactor.metrics_snapshot();
        assert_eq!(metrics.input_updates, 5);
        assert_eq!(metrics.cancelled_zero_weight_updates, 1);
        assert_eq!(metrics.emitted_updates, 1);
        assert_eq!(metrics.collapsed_updates, 3);
    }

    #[test]
    fn test_composite_primary_key_matching() {
        // Match on col 0 (tenant_id) and col 1 (user_id)
        let config = EpochCompactionConfig::default().with_primary_key_columns(vec![0, 1]);
        let compactor = EpochCompactor::new(config);
        let epoch = 1;

        compactor.push_delta(epoch, "orgA\tusr1\tactive", 1);
        compactor.push_delta(epoch, "orgA\tusr1\tsuspended", 1);
        compactor.push_delta(epoch, "orgB\tusr1\tactive", 1);

        let flushed = compactor.drain_epoch(epoch).unwrap();
        assert_eq!(
            flushed,
            vec![
                RuntimeRow {
                    values_tsv: "orgA\tusr1\tsuspended".to_string(),
                    weight: 2,
                },
                RuntimeRow {
                    values_tsv: "orgB\tusr1\tactive".to_string(),
                    weight: 1,
                },
            ]
        );
    }

    #[test]
    fn test_custom_key_strategy() {
        let config = EpochCompactionConfig::default().with_key_strategy(
            KeyExtractionStrategy::Custom(Arc::new(|row: &RuntimeRow| {
                // Lowercase key
                row.values_tsv.to_lowercase()
            })),
        );
        let compactor = EpochCompactor::new(config);
        let epoch = 1;

        compactor.push_delta(epoch, "ABC", 1);
        compactor.push_delta(epoch, "abc", -1);

        let flushed = compactor.drain_epoch(epoch).unwrap();
        assert!(flushed.is_empty());
        assert_eq!(compactor.metrics().cancelled_updates(), 1);
    }

    #[test]
    fn test_window_expiration_and_boundary_flushing() {
        let base_time = Instant::now();
        let config = EpochCompactionConfig::default()
            .with_bounds(Duration::from_millis(10), Duration::from_millis(500))
            .with_window_duration(Duration::from_millis(150))
            .expect("valid window duration");

        let compactor = EpochCompactor::new(config);

        // Open epoch 100 at base_time
        compactor.push_row_at(
            100,
            RuntimeRow {
                values_tsv: "row_100".to_string(),
                weight: 1,
            },
            base_time,
        );

        // Open epoch 200 at base_time + 100ms
        let t_100ms = base_time + Duration::from_millis(100);
        compactor.push_row_at(
            200,
            RuntimeRow {
                values_tsv: "row_200".to_string(),
                weight: 2,
            },
            t_100ms,
        );

        // At t = base_time + 120ms:
        // Epoch 100 age is 120ms (< 150ms) -> not expired
        // Epoch 200 age is 20ms (< 150ms) -> not expired
        let t_120ms = base_time + Duration::from_millis(120);
        assert!(!compactor.is_epoch_expired_at(100, t_120ms));
        assert!(!compactor.is_epoch_expired_at(200, t_120ms));
        assert!(compactor.expired_epochs_at(t_120ms).is_empty());

        // At t = base_time + 160ms:
        // Epoch 100 age is 160ms (>= 150ms) -> EXPIRED!
        // Epoch 200 age is 60ms (< 150ms) -> not expired
        let t_160ms = base_time + Duration::from_millis(160);
        assert!(compactor.is_epoch_expired_at(100, t_160ms));
        assert!(!compactor.is_epoch_expired_at(200, t_160ms));
        assert_eq!(compactor.expired_epochs_at(t_160ms), vec![100]);

        // Drain expired epochs at t_160ms
        let drained = compactor.drain_expired_epochs_at(t_160ms);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].0, 100);
        assert_eq!(
            drained[0].1,
            vec![RuntimeRow {
                values_tsv: "row_100".to_string(),
                weight: 1
            }]
        );

        // Epoch 100 is no longer active; Epoch 200 is still active
        assert!(!compactor.has_epoch(100));
        assert!(compactor.has_epoch(200));

        // At t = base_time + 260ms:
        // Epoch 200 age is 160ms (>= 150ms) -> EXPIRED!
        let t_260ms = base_time + Duration::from_millis(260);
        assert!(compactor.is_epoch_expired_at(200, t_260ms));
        let drained_200 = compactor.drain_expired_epochs_at(t_260ms);
        assert_eq!(drained_200.len(), 1);
        assert_eq!(drained_200[0].0, 200);
        assert_eq!(
            drained_200[0].1,
            vec![RuntimeRow {
                values_tsv: "row_200".to_string(),
                weight: 2
            }]
        );

        assert_eq!(compactor.active_epoch_count(), 0);
    }

    #[test]
    fn test_drain_all_forced_flushing() {
        let compactor = EpochCompactor::default();
        compactor.push_delta(1, "item1", 1);
        compactor.push_delta(2, "item2", 2);
        compactor.push_delta(3, "item3", 0); // zero weight

        assert_eq!(compactor.active_epoch_count(), 3);
        let all = compactor.drain_all();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].0, 1);
        assert_eq!(
            all[0].1,
            vec![RuntimeRow {
                values_tsv: "item1".into(),
                weight: 1
            }]
        );
        assert_eq!(all[1].0, 2);
        assert_eq!(
            all[1].1,
            vec![RuntimeRow {
                values_tsv: "item2".into(),
                weight: 2
            }]
        );
        assert_eq!(all[2].0, 3);
        assert!(all[2].1.is_empty()); // zero weight omitted

        assert_eq!(compactor.active_epoch_count(), 0);
    }

    #[test]
    fn test_stateless_compact_slice() {
        let compactor = EpochCompactor::default();
        let rows = vec![
            RuntimeRow {
                values_tsv: "alpha".to_string(),
                weight: 1,
            },
            RuntimeRow {
                values_tsv: "beta".to_string(),
                weight: 2,
            },
            RuntimeRow {
                values_tsv: "alpha".to_string(),
                weight: -1,
            },
            RuntimeRow {
                values_tsv: "beta".to_string(),
                weight: 3,
            },
            RuntimeRow {
                values_tsv: "gamma".to_string(),
                weight: -4,
            },
        ];

        let compacted = compactor.compact_slice(&rows);

        // "alpha" cancelled (1 + -1 = 0)
        // "beta" collapsed (2 + 3 = 5)
        // "gamma" non-zero (-4)
        assert_eq!(
            compacted,
            vec![
                RuntimeRow {
                    values_tsv: "beta".to_string(),
                    weight: 5,
                },
                RuntimeRow {
                    values_tsv: "gamma".to_string(),
                    weight: -4,
                },
            ]
        );
    }
}
