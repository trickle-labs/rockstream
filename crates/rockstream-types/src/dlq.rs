use std::sync::OnceLock;

use parking_lot::Mutex;

/// A record inside the persistent dead-letter queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DlqEntry {
    /// Milliseconds since Unix epoch when record arrived at the queue.
    pub arrived_at: u64,
    /// The name of the source connector.
    pub source_name: String,
    /// Opaque source offset as a string.
    pub source_offset: String,
    /// The error code registered for the decode failure.
    pub error_code: String,
    /// The decode error message.
    pub error_message: String,
    /// Raw payload bytes represented as hexadecimal.
    pub raw_bytes_hex: String,
    /// The count of replay attempts (starts at 0).
    pub replay_attempt: u32,
}

// Audit: DLQ entry appends are synchronous and preserve a valid ordered vector
// if a holder panics; no guard crosses an await or external call.
static GLOBAL_DLQ: OnceLock<Mutex<Vec<DlqEntry>>> = OnceLock::new();

/// Get the thread-safe global DLQ database.
pub fn get_global_dlq() -> &'static Mutex<Vec<DlqEntry>> {
    GLOBAL_DLQ.get_or_init(|| Mutex::new(Vec::new()))
}

/// Named upper bound for maximum persistent in-memory DLQ buffer capacity.
pub const MAX_DLQ_CAPACITY: usize = 10_000;

// Metric counter for rockstream_connector_dlq_growing_total
static DLQ_GROWING_METRICS: OnceLock<Mutex<std::collections::HashMap<String, u64>>> =
    OnceLock::new();

fn get_dlq_growing_metrics_map() -> &'static Mutex<std::collections::HashMap<String, u64>> {
    DLQ_GROWING_METRICS.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

/// Increment metric `rockstream_connector_dlq_growing_total` for a source.
pub fn increment_dlq_growing_metric(source_name: &str) {
    let mut map = get_dlq_growing_metrics_map().lock();
    *map.entry(source_name.to_lowercase()).or_insert(0) += 1;
}

/// Read metric `rockstream_connector_dlq_growing_total` for a source.
pub fn get_dlq_growing_metric(source_name: &str) -> u64 {
    let map = get_dlq_growing_metrics_map().lock();
    map.get(&source_name.to_lowercase()).copied().unwrap_or(0)
}

/// Source DLQ state tracker for rate threshold warnings and BLOCKED state degradation.
#[derive(Debug, Clone)]
pub struct SourceDlqState {
    pub source_name: String,
    pub warn_threshold: u64,
    pub current_quarantine_count: u64,
    pub status: String,
}

impl SourceDlqState {
    pub fn new(source_name: &str, warn_threshold: u64) -> Self {
        Self {
            source_name: source_name.to_string(),
            warn_threshold: if warn_threshold == 0 {
                100
            } else {
                warn_threshold
            },
            current_quarantine_count: 0,
            status: "OK".to_string(),
        }
    }

    /// Record a quarantined record; returns true if status degraded to BLOCKED.
    pub fn record_quarantine(&mut self) -> bool {
        self.current_quarantine_count += 1;
        if self.current_quarantine_count >= self.warn_threshold {
            increment_dlq_growing_metric(&self.source_name);
            if self.status != "BLOCKED" {
                self.status = "BLOCKED".to_string();
                return true;
            }
        }
        false
    }
}

/// Purge records from global DLQ that arrived before `cutoff_ts_millis`. Returns count purged.
pub fn purge_expired_before(cutoff_ts_millis: u64) -> usize {
    let mut dlq = get_global_dlq().lock();
    let len_before = dlq.len();
    dlq.retain(|entry| entry.arrived_at >= cutoff_ts_millis);
    len_before - dlq.len()
}

/// Purge records older than `retention_days` relative to `current_ts_millis`.
pub fn purge_expired_by_retention_days(retention_days: u64, current_ts_millis: u64) -> usize {
    let retention_millis = retention_days * 86400 * 1000;
    let cutoff = current_ts_millis.saturating_sub(retention_millis);
    purge_expired_before(cutoff)
}

/// Check if source DLQ count has exceeded warn threshold. Returns (warned, is_blocked).
pub fn check_dlq_warn_threshold(source_name: &str, threshold: u64) -> (bool, bool) {
    let dlq = get_global_dlq().lock();
    let count = dlq
        .iter()
        .filter(|e| e.source_name.eq_ignore_ascii_case(source_name))
        .count() as u64;
    let warned = count >= threshold;
    let is_blocked = count >= threshold;
    (warned, is_blocked)
}

/// Quarantine a record into the global DLQ table with bounded capacity check.
pub fn quarantine_record(
    source_name: &str,
    source_offset: impl std::fmt::Display,
    error_code: &str,
    error_message: &str,
    raw_bytes: &[u8],
) -> DlqEntry {
    let arrived_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let entry = DlqEntry {
        arrived_at,
        source_name: source_name.to_string(),
        source_offset: source_offset.to_string(),
        error_code: error_code.to_string(),
        error_message: error_message.to_string(),
        raw_bytes_hex: hex::encode(raw_bytes),
        replay_attempt: 0,
    };
    let mut dlq = get_global_dlq().lock();
    if dlq.len() >= MAX_DLQ_CAPACITY {
        dlq.remove(0); // Evict oldest record to preserve bounded size
    }
    dlq.push(entry.clone());
    entry
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    fn entry(source_name: &str, arrived_at: u64) -> DlqEntry {
        DlqEntry {
            arrived_at,
            source_name: source_name.to_string(),
            source_offset: "offset".to_string(),
            error_code: "RS-4008".to_string(),
            error_message: "invalid payload".to_string(),
            raw_bytes_hex: "7b7d".to_string(),
            replay_attempt: 0,
        }
    }

    #[test]
    fn global_dlq_peer_append_and_read_survive_holder_panic() {
        let _guard = TEST_LOCK.lock();
        let dlq = get_global_dlq();
        dlq.lock().clear();

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut entries = dlq.lock();
            entries.push(entry("abandoned", 1));
            panic!("injected global DLQ holder panic");
        }));
        assert!(panic.is_err());

        dlq.lock().push(entry("peer", 2));
        assert_eq!(
            dlq.lock().clone(),
            vec![entry("abandoned", 1), entry("peer", 2)]
        );
        dlq.lock().clear();
    }

    #[test]
    fn test_purge_expired_before_and_by_retention_days() {
        let _guard = TEST_LOCK.lock();
        get_global_dlq().lock().clear();
        let now_ms = 1_700_000_000_000u64;
        let ten_days_ago = now_ms - (10 * 86400 * 1000);
        let two_days_ago = now_ms - (2 * 86400 * 1000);

        {
            let mut dlq = get_global_dlq().lock();
            dlq.push(entry("src1", ten_days_ago));
            dlq.push(entry("src1", two_days_ago));
        }

        assert_eq!(get_global_dlq().lock().len(), 2);

        let purged = purge_expired_by_retention_days(7, now_ms);
        assert_eq!(purged, 1);
        let dlq = get_global_dlq().lock();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0].arrived_at, two_days_ago);
        dlq.clone();
        drop(dlq);
        get_global_dlq().lock().clear();
    }

    #[test]
    fn test_source_dlq_state_degradation_and_metrics() {
        let _guard = TEST_LOCK.lock();
        get_global_dlq().lock().clear();
        let mut state = SourceDlqState::new("test_src", 3);
        assert_eq!(state.status, "OK");

        assert!(!state.record_quarantine());
        assert!(!state.record_quarantine());
        // 3rd quarantine hits threshold -> degrades to BLOCKED
        assert!(state.record_quarantine());
        assert_eq!(state.status, "BLOCKED");

        assert!(get_dlq_growing_metric("test_src") >= 1);
    }
}
