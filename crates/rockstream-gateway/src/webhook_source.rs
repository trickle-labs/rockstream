//! Bounded HTTP webhook source (`http_webhook`).
//!
//! Accepts authenticated JSON or CSV payloads over POST requests, assigning
//! sequential, monotonic source epochs before passing them to gateway ingestion.

#![deny(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Maximum bytes accepted from one webhook request before it is decoded.
pub const HTTP_WEBHOOK_MAX_REQUEST_BYTES: usize = 1024 * 1024;
/// Maximum uncommitted webhook epochs retained by a source.
pub const HTTP_WEBHOOK_LOCAL_BUFFER_MAX_EPOCHS: usize = 1024;
/// Maximum committed delivery identities retained for exactly-once retries.
pub const HTTP_WEBHOOK_DEDUP_MAX_ENTRIES: usize = 4096;
/// Default TTL for unacknowledged pending webhook deliveries.
pub const HTTP_WEBHOOK_PENDING_DEFAULT_TTL_SECS: u64 = 300;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookFormat {
    Json,
    Csv,
}

impl WebhookFormat {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "json" => Some(Self::Json),
            "csv" => Some(Self::Csv),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookEpoch {
    pub source_epoch: u64,
    pub delivery_id: String,
    pub digest: String,
    pub payload: Vec<u8>,
    pub watermark: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookResult {
    Accepted,
    Duplicate,
    Unauthorized,
    NotFound,
    Paused,
    PayloadTooLarge,
    Full,
    InvalidPayload,
    DurabilityFailed,
    InFlight,
}

impl WebhookResult {
    pub fn status_code(self) -> u16 {
        match self {
            Self::Accepted | Self::Duplicate => 202,
            Self::Unauthorized => 401,
            Self::NotFound => 404,
            Self::Paused => 409,
            Self::PayloadTooLarge => 413,
            Self::Full => 429,
            Self::InvalidPayload => 400,
            Self::DurabilityFailed => 500,
            Self::InFlight => 409,
        }
    }

    pub fn error_code(self) -> Option<&'static str> {
        match self {
            Self::Unauthorized => Some("RS-4012"),
            Self::NotFound => Some("RS-4009"),
            Self::Paused => Some("RS-4013"),
            Self::PayloadTooLarge => Some("RS-4014"),
            Self::Full => Some("RS-4015"),
            Self::InvalidPayload => Some("RS-4016"),
            Self::DurabilityFailed => Some("RS-4017"),
            Self::InFlight => Some("RS-4018"),
            Self::Accepted | Self::Duplicate => None,
        }
    }
}

/// Source-local state. `accepted` is bounded and never acknowledged as
/// committed until `commit_next` moves it to the equally bounded dedup window.
pub struct HttpWebhookSource {
    expected_token: Vec<u8>,
    format: WebhookFormat,
    paused: bool,
    accepted: VecDeque<WebhookEpoch>,
    pending: HashSet<String>,
    pending_timestamps: std::collections::HashMap<String, std::time::Instant>,
    pending_ttl: std::time::Duration,
    committed: HashSet<String>,
    committed_order: VecDeque<String>,
    watermark: Option<u64>,
    next_epoch: u64,
}

impl HttpWebhookSource {
    /// `token` is runtime-only: callers must not put it in catalog metadata.
    pub fn new(token: impl AsRef<[u8]>, format: WebhookFormat) -> Self {
        Self {
            expected_token: token.as_ref().to_vec(),
            format,
            paused: false,
            accepted: VecDeque::new(),
            pending: HashSet::new(),
            pending_timestamps: std::collections::HashMap::new(),
            pending_ttl: std::time::Duration::from_secs(HTTP_WEBHOOK_PENDING_DEFAULT_TTL_SECS),
            committed: HashSet::new(),
            committed_order: VecDeque::new(),
            watermark: None,
            next_epoch: 0,
        }
    }

    pub fn set_pending_ttl(&mut self, ttl: std::time::Duration) {
        self.pending_ttl = ttl;
    }

    pub fn with_pending_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.pending_ttl = ttl;
        self
    }

    fn update_metric(&self) {
        rockstream_types::metrics::set_webhook_pending_size(self.pending.len() as u64);
    }

    /// Evict unacknowledged pending deliveries older than `pending_ttl`.
    pub fn evict_expired_pending(&mut self) -> usize {
        self.evict_expired_pending_at(std::time::Instant::now())
    }

    /// Evict unacknowledged pending deliveries older than `pending_ttl` relative to `now`.
    pub fn evict_expired_pending_at(&mut self, now: std::time::Instant) -> usize {
        let ttl = self.pending_ttl;
        let expired_ids: Vec<String> = self
            .pending_timestamps
            .iter()
            .filter_map(|(id, &created)| {
                if now.duration_since(created) >= ttl {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();

        let count = expired_ids.len();
        for id in &expired_ids {
            self.pending.remove(id);
            self.pending_timestamps.remove(id);
            if let Some(index) = self
                .accepted
                .iter()
                .position(|epoch| epoch.delivery_id == *id)
            {
                self.accepted.remove(index);
            }
        }
        self.update_metric();
        count
    }

    pub fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
    }

    pub fn buffered_epochs(&self) -> usize {
        self.accepted.len()
    }

    pub fn buffer_fill_ratio(&self) -> f64 {
        self.accepted.len() as f64 / HTTP_WEBHOOK_LOCAL_BUFFER_MAX_EPOCHS as f64
    }

    pub fn watermark(&self) -> Option<u64> {
        self.watermark
    }

    pub fn advance_watermark(&mut self, watermark: u64) -> Result<(), &'static str> {
        if self.watermark.is_some_and(|current| watermark < current) {
            return Err("[RS-4016] watermark must not move backwards. Next steps: provide a value at or above the current watermark");
        }
        self.watermark = Some(watermark);
        Ok(())
    }

    pub fn accept(
        &mut self,
        token: &[u8],
        delivery_id: Option<&str>,
        payload: &[u8],
    ) -> WebhookResult {
        self.evict_expired_pending();
        if !constant_time_eq(token, &self.expected_token) {
            return WebhookResult::Unauthorized;
        }
        if self.paused {
            return WebhookResult::Paused;
        }
        if payload.len() > HTTP_WEBHOOK_MAX_REQUEST_BYTES {
            return WebhookResult::PayloadTooLarge;
        }
        if !valid_payload(self.format, payload) {
            return WebhookResult::InvalidPayload;
        }
        let digest = format!("{:x}", Sha256::digest(payload));
        let identity = delivery_id.unwrap_or(&digest).to_string();
        if self.committed.contains(&identity) {
            return WebhookResult::Duplicate;
        }
        if self.pending.contains(&identity) {
            return WebhookResult::InFlight;
        }
        if self.accepted.len() >= HTTP_WEBHOOK_LOCAL_BUFFER_MAX_EPOCHS {
            return WebhookResult::Full;
        }
        self.pending.insert(identity.clone());
        self.pending_timestamps
            .insert(identity.clone(), std::time::Instant::now());
        self.update_metric();
        self.next_epoch += 1;
        self.accepted.push_back(WebhookEpoch {
            source_epoch: self.next_epoch,
            delivery_id: identity,
            digest,
            payload: payload.to_vec(),
            watermark: self.watermark,
        });
        WebhookResult::Accepted
    }

    /// Commit one accepted epoch.  The identity enters dedup only here, which
    /// makes a retry after a failed pre-commit attempt eligible for delivery.
    pub fn commit_next(&mut self) -> Option<WebhookEpoch> {
        let delivery_id = self.accepted.front()?.delivery_id.clone();
        self.commit_pending(&delivery_id)
    }

    /// Mark one specifically persisted delivery committed. The identity is
    /// selected rather than relying on queue position because storage commits
    /// may complete in a different request scheduling order.
    pub fn commit_pending(&mut self, delivery_id: &str) -> Option<WebhookEpoch> {
        let index = self
            .accepted
            .iter()
            .position(|epoch| epoch.delivery_id == delivery_id)?;
        let epoch = self.accepted.remove(index)?;
        self.pending.remove(&epoch.delivery_id);
        self.pending_timestamps.remove(&epoch.delivery_id);
        self.update_metric();
        self.committed.insert(epoch.delivery_id.clone());
        self.committed_order.push_back(epoch.delivery_id.clone());
        while self.committed_order.len() > HTTP_WEBHOOK_DEDUP_MAX_ENTRIES {
            if let Some(expired) = self.committed_order.pop_front() {
                self.committed.remove(&expired);
            }
        }
        Some(epoch)
    }

    /// Inspect the next accepted epoch without advancing the durable-delivery
    /// identity. The caller writes this exact record in its M3 transaction
    /// before calling [`Self::commit_next`].
    pub fn next_pending(&self) -> Option<WebhookEpoch> {
        self.accepted.front().cloned()
    }

    /// Abandon a failed pre-commit delivery so its retry can be accepted.
    pub fn abort_pending(&mut self, delivery_id: &str) -> bool {
        let Some(index) = self
            .accepted
            .iter()
            .position(|epoch| epoch.delivery_id == delivery_id)
        else {
            return false;
        };
        let Some(epoch) = self.accepted.remove(index) else {
            return false;
        };
        self.pending_timestamps.remove(&epoch.delivery_id);
        let removed = self.pending.remove(&epoch.delivery_id);
        self.update_metric();
        removed
    }
}

fn valid_payload(format: WebhookFormat, payload: &[u8]) -> bool {
    match format {
        WebhookFormat::Json => serde_json::from_slice::<serde_json::Value>(payload).is_ok(),
        WebhookFormat::Csv => {
            if payload.is_empty() || std::str::from_utf8(payload).is_err() {
                return false;
            }
            let quote_count = payload.iter().filter(|&&b| b == b'"').count();
            quote_count % 2 == 0
        }
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for index in 0..max {
        diff |= usize::from(*left.get(index).unwrap_or(&0) ^ *right.get(index).unwrap_or(&0));
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_commit_has_exactly_one_committed_epoch() {
        let mut source = HttpWebhookSource::new("secret", WebhookFormat::Json);
        assert_eq!(
            source.accept(b"secret", Some("a"), br#"{"id":1}"#),
            WebhookResult::Accepted
        );
        let commit = source.commit_next();
        assert!(commit.is_some());
        assert_eq!(
            commit.as_ref().map(|c| c.payload.as_slice()),
            Some(br#"{"id":1}"#.as_slice())
        );
        assert_eq!(
            source.accept(b"secret", Some("a"), br#"{"id":1}"#),
            WebhookResult::Duplicate
        );
        assert_eq!((source.buffered_epochs(), source.commit_next()), (0, None));
    }

    #[test]
    fn unacknowledged_delivery_ttl_eviction() {
        let mut source = HttpWebhookSource::new("secret", WebhookFormat::Json)
            .with_pending_ttl(std::time::Duration::from_secs(10));
        let now = std::time::Instant::now();

        assert_eq!(
            source.accept(b"secret", Some("delivery-1"), br#"{"id":1}"#),
            WebhookResult::Accepted
        );
        assert_eq!(
            source.accept(b"secret", Some("delivery-1"), br#"{"id":1}"#),
            WebhookResult::InFlight
        );

        let evicted = source.evict_expired_pending_at(now + std::time::Duration::from_secs(5));
        assert_eq!(evicted, 0);

        let evicted = source.evict_expired_pending_at(now + std::time::Duration::from_secs(15));
        assert_eq!(evicted, 1);

        assert_eq!(
            source.accept(b"secret", Some("delivery-1"), br#"{"id":1}"#),
            WebhookResult::Accepted
        );
    }
}
