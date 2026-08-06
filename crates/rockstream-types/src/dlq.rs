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

#[cfg(test)]
mod tests {
    use super::*;

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
}
