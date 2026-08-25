//! Bounded runtime diagnostics shared by every public renderer.

use crate::error_code::{ErrorCode, ErrorDescriptor};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;
use uuid::Uuid;

pub const MAX_DIAGNOSTIC_CONTEXT_ENTRIES: usize = 32;
pub const MAX_DIAGNOSTIC_CONTEXT_KEY_BYTES: usize = 64;
pub const MAX_DIAGNOSTIC_CONTEXT_VALUE_BYTES: usize = 1_024;
pub const MAX_DIAGNOSTIC_CONTEXT_BYTES: usize = 8 * 1_024;
pub const MAX_DIAGNOSTIC_OCCURRENCES: usize = 256;
pub const MAX_DIAGNOSTIC_BUNDLE_OCCURRENCES: usize = 256;
pub const MAX_DIAGNOSTIC_BUNDLE_BYTES: usize = 1_024 * 1_024;

static CONTEXT_REJECTED: AtomicU64 = AtomicU64::new(0);
static OCCURRENCES_EVICTED: AtomicU64 = AtomicU64::new(0);

/// Snapshot of bounded diagnostic counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticMetrics {
    pub diagnostic_context_rejected_total: u64,
    pub diagnostic_occurrences_evicted_total: u64,
    pub rockstream_diagnostic_occurrences_retained: usize,
}

/// Context admission failure. Oversized values are redacted and truncated; keys,
/// entry count, and total context size are rejected before storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiagnosticContextError {
    TooManyEntries,
    KeyTooLong { bytes: usize },
    ContextTooLarge,
}

impl fmt::Display for DiagnosticContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyEntries => write!(f, "diagnostic context entry limit exceeded"),
            Self::KeyTooLong { bytes } => write!(f, "diagnostic context key is {bytes} bytes"),
            Self::ContextTooLarge => write!(f, "diagnostic context byte limit exceeded"),
        }
    }
}

impl std::error::Error for DiagnosticContextError {}

/// One public runtime failure with a catalog-backed descriptor and bounded safe context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticOccurrence {
    pub code: ErrorCode,
    pub correlation_id: Uuid,
    pub message: String,
    pub context: BTreeMap<String, String>,
    #[serde(with = "duration_millis")]
    pub retry_after: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<Box<DiagnosticOccurrence>>,
}

impl DiagnosticOccurrence {
    /// Construct an occurrence from a catalog code and safe context values.
    pub fn new(
        code: ErrorCode,
        correlation_id: Uuid,
        context: impl IntoIterator<Item = (String, String)>,
        retry_after: Option<Duration>,
        cause: Option<DiagnosticOccurrence>,
    ) -> Result<Self, DiagnosticContextError> {
        let context = sanitize_context(context)?;
        let message = catalog_message(code, &context);
        Ok(Self {
            code,
            correlation_id,
            message,
            context,
            retry_after,
            cause: cause.map(|mut cause| {
                cause.cause = None;
                Box::new(cause)
            }),
        })
    }

    pub fn descriptor(&self) -> Option<&'static ErrorDescriptor> {
        ErrorDescriptor::lookup(self.code)
    }

    pub fn retry_after_ms(&self) -> Option<u64> {
        self.retry_after
            .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
    }

    /// Re-sanitize values before a renderer publishes them.
    pub fn redacted(&self) -> Self {
        let context = sanitize_context(self.context.clone()).unwrap_or_default();
        let mut redacted = self.clone();
        redacted.context = context.clone();
        redacted.message = catalog_message(self.code, &context);
        redacted.cause = self.cause.as_ref().map(|cause| Box::new(cause.redacted()));
        redacted
    }

    pub fn render_text(&self) -> String {
        let occurrence = self.redacted();
        let descriptor = occurrence.descriptor();
        let mut fields = vec![format!("correlation_id={}", occurrence.correlation_id)];
        if let Some(retry_after_ms) = occurrence.retry_after_ms() {
            fields.push(format!("retry_after_ms={retry_after_ms}"));
        }
        if !occurrence.context.is_empty() {
            fields.push(format!(
                "context={}",
                occurrence
                    .context
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        format!(
            "[{}] {}: {} ({}) next_steps: {}",
            occurrence.code,
            descriptor
                .map(|value| value.key.as_str())
                .unwrap_or("unknown"),
            occurrence.message,
            fields.join(" "),
            descriptor
                .map(|value| value.default_next_steps.as_str())
                .unwrap_or("Check the diagnostic code and retry.")
        )
    }

    pub fn render_json(&self) -> String {
        serde_json::to_string(&self.redacted()).unwrap_or_else(|_| "{}".to_string())
    }
}

/// Bounded process-local journal of recent runtime occurrences.
#[derive(Debug, Default)]
pub struct DiagnosticJournal {
    occurrences: VecDeque<DiagnosticOccurrence>,
}

impl DiagnosticJournal {
    pub fn new() -> Self {
        Self {
            occurrences: VecDeque::with_capacity(MAX_DIAGNOSTIC_OCCURRENCES),
        }
    }

    pub fn record(&mut self, occurrence: DiagnosticOccurrence) {
        if self.occurrences.len() == MAX_DIAGNOSTIC_OCCURRENCES {
            self.occurrences.pop_front();
            OCCURRENCES_EVICTED.fetch_add(1, Ordering::Relaxed);
        }
        self.occurrences.push_back(occurrence.redacted());
    }

    pub fn recent(&self, limit: usize) -> Vec<DiagnosticOccurrence> {
        self.occurrences
            .iter()
            .rev()
            .take(limit.min(MAX_DIAGNOSTIC_OCCURRENCES))
            .cloned()
            .collect()
    }

    pub fn by_code(&self, code: ErrorCode) -> Vec<DiagnosticOccurrence> {
        self.recent(MAX_DIAGNOSTIC_OCCURRENCES)
            .into_iter()
            .filter(|occurrence| occurrence.code == code)
            .collect()
    }

    pub fn by_correlation_id(&self, correlation_id: Uuid) -> Option<DiagnosticOccurrence> {
        self.occurrences
            .iter()
            .rev()
            .find(|occurrence| occurrence.correlation_id == correlation_id)
            .cloned()
    }

    pub fn len(&self) -> usize {
        self.occurrences.len()
    }

    pub fn is_empty(&self) -> bool {
        self.occurrences.is_empty()
    }

    pub fn clear(&mut self) {
        self.occurrences.clear();
    }
}

static GLOBAL_JOURNAL: OnceLock<Mutex<DiagnosticJournal>> = OnceLock::new();

pub fn global_diagnostic_journal() -> &'static Mutex<DiagnosticJournal> {
    GLOBAL_JOURNAL.get_or_init(|| Mutex::new(DiagnosticJournal::new()))
}

pub fn record_diagnostic(occurrence: DiagnosticOccurrence) {
    global_diagnostic_journal().lock().record(occurrence);
}

pub fn diagnostic_metrics() -> DiagnosticMetrics {
    DiagnosticMetrics {
        diagnostic_context_rejected_total: CONTEXT_REJECTED.load(Ordering::Relaxed),
        diagnostic_occurrences_evicted_total: OCCURRENCES_EVICTED.load(Ordering::Relaxed),
        rockstream_diagnostic_occurrences_retained: global_diagnostic_journal().lock().len(),
    }
}

pub fn redact_secrets(input: &str) -> String {
    let mut output = input.to_string();
    for key in ["password", "passwd", "secret", "token", "api_key", "apikey"] {
        output = redact_assignment(&output, key);
    }
    output = redact_bearer(&output);
    output = redact_url_credentials(&output);
    if output.contains("-----BEGIN") {
        "[REDACTED_PRIVATE_KEY_MATERIAL]".to_string()
    } else {
        output
    }
}

fn sanitize_context(
    entries: impl IntoIterator<Item = (String, String)>,
) -> Result<BTreeMap<String, String>, DiagnosticContextError> {
    let mut context = BTreeMap::new();
    for (key, value) in entries {
        if context.len() == MAX_DIAGNOSTIC_CONTEXT_ENTRIES && !context.contains_key(&key) {
            CONTEXT_REJECTED.fetch_add(1, Ordering::Relaxed);
            return Err(DiagnosticContextError::TooManyEntries);
        }
        if key.len() > MAX_DIAGNOSTIC_CONTEXT_KEY_BYTES {
            CONTEXT_REJECTED.fetch_add(1, Ordering::Relaxed);
            return Err(DiagnosticContextError::KeyTooLong { bytes: key.len() });
        }
        let value = truncate_bytes(&redact_secrets(&value), MAX_DIAGNOSTIC_CONTEXT_VALUE_BYTES);
        let existing = context.insert(key.clone(), value);
        let total_bytes: usize = context
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum();
        if total_bytes > MAX_DIAGNOSTIC_CONTEXT_BYTES {
            if let Some(existing) = existing {
                context.insert(key, existing);
            } else {
                context.remove(&key);
            }
            CONTEXT_REJECTED.fetch_add(1, Ordering::Relaxed);
            return Err(DiagnosticContextError::ContextTooLarge);
        }
    }
    Ok(context)
}

fn catalog_message(code: ErrorCode, context: &BTreeMap<String, String>) -> String {
    let title = ErrorDescriptor::lookup(code)
        .map(|descriptor| descriptor.title.as_str())
        .unwrap_or("Unknown error");
    if context.is_empty() {
        title.to_string()
    } else {
        format!(
            "{title} ({})",
            context
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

fn redact_assignment(input: &str, key: &str) -> String {
    let mut output = input.to_string();
    let mut offset = 0;
    loop {
        let lower = output.to_ascii_lowercase();
        let Some(relative) = lower[offset..].find(key) else {
            break;
        };
        let start = offset + relative;
        let after_key = start + key.len();
        let separator = output[after_key..].chars().next();
        if !matches!(separator, Some('=') | Some(':')) {
            offset = after_key;
            continue;
        }
        let value_start = after_key + 1;
        let value_end = output[value_start..]
            .find(|character: char| {
                character.is_whitespace() || matches!(character, ',' | ';' | '&')
            })
            .map(|end| value_start + end)
            .unwrap_or(output.len());
        output.replace_range(value_start..value_end, "[REDACTED]");
        offset = value_start + "[REDACTED]".len();
    }
    output
}

fn redact_bearer(input: &str) -> String {
    let lower = input.to_ascii_lowercase();
    let Some(start) = lower.find("bearer ") else {
        return input.to_string();
    };
    let value_start = start + "bearer ".len();
    let value_end = input[value_start..]
        .find(char::is_whitespace)
        .map(|end| value_start + end)
        .unwrap_or(input.len());
    let mut output = input.to_string();
    output.replace_range(value_start..value_end, "[REDACTED]");
    output
}

fn redact_url_credentials(input: &str) -> String {
    let mut output = input.to_string();
    let mut offset = 0;
    while let Some(scheme_relative) = output[offset..].find("://") {
        let authority_start = offset + scheme_relative + 3;
        let Some(at_relative) = output[authority_start..].find('@') else {
            break;
        };
        let at = authority_start + at_relative;
        let Some(colon_relative) = output[authority_start..at].find(':') else {
            offset = at + 1;
            continue;
        };
        let colon = authority_start + colon_relative;
        output.replace_range(colon + 1..at, "[REDACTED]");
        offset = colon + 1 + "[REDACTED]".len();
    }
    output
}

fn truncate_bytes(input: &str, max_bytes: usize) -> String {
    if input.len() <= max_bytes {
        return input.to_string();
    }
    let suffix = "[TRUNCATED]";
    let end = max_bytes.saturating_sub(suffix.len());
    let mut boundary = end.min(input.len());
    while boundary > 0 && !input.is_char_boundary(boundary) {
        boundary -= 1;
    }
    format!("{}{}", &input[..boundary], suffix)
}

mod duration_millis {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(value: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value
            .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<u64>::deserialize(deserializer).map(|value| value.map(Duration::from_millis))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error_code::{RS_2004, RS_2018};

    fn correlation_id() -> Uuid {
        Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap()
    }

    #[test]
    fn occurrence_serializes_catalog_message_and_ordered_context() {
        let occurrence = DiagnosticOccurrence::new(
            RS_2018,
            correlation_id(),
            [
                ("view".to_string(), "orders_mv".to_string()),
                ("age_ms".to_string(), "42".to_string()),
            ],
            Some(Duration::from_millis(1500)),
            None,
        )
        .unwrap();
        assert_eq!(occurrence.descriptor().unwrap().code, RS_2018);
        assert_eq!(
            serde_json::to_string(&occurrence).unwrap(),
            r#"{"code":"RS-2018","correlation_id":"11111111-1111-4111-8111-111111111111","message":"Published frontier exceeded the session max_staleness bound; query proceeded (age_ms=42, view=orders_mv)","context":{"age_ms":"42","view":"orders_mv"},"retry_after":1500}"#
        );
    }

    #[test]
    fn causal_occurrence_is_one_direct_nested_record() {
        let cause = DiagnosticOccurrence::new(RS_2018, correlation_id(), [], None, None).unwrap();
        let occurrence = DiagnosticOccurrence::new(
            RS_2004,
            Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap(),
            [],
            None,
            Some(cause),
        )
        .unwrap();
        assert_eq!(occurrence.cause.as_ref().unwrap().code, RS_2018);
        assert_eq!(occurrence.cause.as_ref().unwrap().cause, None);
    }

    #[test]
    fn redaction_covers_assignments_bearer_urls_and_truncation() {
        let input = "password=secret bearer token https://user:pass@example.test/api";
        assert_eq!(
            redact_secrets(input),
            "password=[REDACTED] bearer [REDACTED] https://user:[REDACTED]@example.test/api"
        );
        let value = "x".repeat(MAX_DIAGNOSTIC_CONTEXT_VALUE_BYTES + 10);
        assert_eq!(
            truncate_bytes(&value, MAX_DIAGNOSTIC_CONTEXT_VALUE_BYTES).len(),
            MAX_DIAGNOSTIC_CONTEXT_VALUE_BYTES
        );
    }
}
