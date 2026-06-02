use std::sync::Mutex;
use std::sync::OnceLock;

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

static GLOBAL_DLQ: OnceLock<Mutex<Vec<DlqEntry>>> = OnceLock::new();

/// Get the thread-safe global DLQ database.
pub fn get_global_dlq() -> &'static Mutex<Vec<DlqEntry>> {
    GLOBAL_DLQ.get_or_init(|| Mutex::new(Vec::new()))
}
