//! Kafka source connector mock with Tier 1 contract (OffsetToken, watermark, credits).

use async_trait::async_trait;
use rockstream_types::batch::{OffsetToken, SourceBatch};
use rockstream_types::timestamp::{Epoch, EventTimeWatermark};

use crate::source::Source;

/// A mock Kafka source connector.
pub struct KafkaSource {
    topic: String,
    credits: usize,
    current_offset: u64,
    watermark: EventTimeWatermark,
    clock_skew_ms: i64,
    decode_errors_count: usize,
}

impl KafkaSource {
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            credits: usize::MAX,
            current_offset: 0,
            watermark: 0,
            clock_skew_ms: 0,
            decode_errors_count: 0,
        }
    }

    /// Simulate deliberate clock skew for event time watermarking.
    pub fn set_clock_skew(&mut self, skew_ms: i64) {
        self.clock_skew_ms = skew_ms;
    }

    /// Set a simulated decode error count to trigger DLQ warnings.
    pub fn trigger_decode_errors(&mut self, count: usize) {
        self.decode_errors_count = count;
    }
}

#[async_trait]
impl Source for KafkaSource {
    async fn poll_batch(&mut self, epoch: Epoch) -> Option<SourceBatch> {
        // If credit flow control has saturated consumption, return an empty batch
        if self.credits == 0 {
            tracing::info!(topic = %self.topic, "kafka source: credit-starved, pausing consumption");
            return Some(SourceBatch {
                record_count: 0,
                epoch,
                offset: Some(OffsetToken(format!(
                    "part:0-offset:{}",
                    self.current_offset
                ))),
                watermark: Some(self.watermark),
            });
        }

        // Simulate decode errors
        if self.decode_errors_count > 0 {
            self.decode_errors_count -= 1;
            // Record the decode error to the global persistent DLQ database (B-3)
            let entry = rockstream_types::dlq::DlqEntry {
                arrived_at: 1717315200000,
                source_name: self.name().to_string(),
                source_offset: format!("part:0-offset:{}", self.current_offset),
                error_code: "RS-1003".to_string(),
                error_message: "Record decode error".to_string(),
                raw_bytes_hex: "DEADC0DE".to_string(),
                replay_attempt: 0,
            };
            {
                let mut guard = rockstream_types::dlq::get_global_dlq().lock().unwrap();
                guard.push(entry);
            }
            tracing::warn!(
                topic = %self.topic,
                offset = self.current_offset,
                "RS-1003: kafka source: record decode error routed to DLQ"
            );
        }

        self.current_offset += 10;
        // Watermark closes tumbling window correctly under clock skew
        let time_base = (epoch * 60000) as i64; // 1-minute window
        let adjusted_time = (time_base + self.clock_skew_ms).max(0) as u64;
        self.watermark = adjusted_time;

        Some(SourceBatch {
            record_count: 10,
            epoch,
            offset: Some(OffsetToken(format!(
                "part:0-offset:{}",
                self.current_offset
            ))),
            watermark: Some(self.watermark),
        })
    }

    fn name(&self) -> &str {
        "kafka-source"
    }

    fn credits_available(&self) -> usize {
        self.credits
    }

    fn set_credits(&mut self, credits: usize) {
        self.credits = credits;
    }

    fn current_offset(&self) -> Option<OffsetToken> {
        Some(OffsetToken(format!(
            "part:0-offset:{}",
            self.current_offset
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn kafka_source_credits_control() {
        let mut src = KafkaSource::new("test-topic");
        src.set_credits(0);
        let batch = src.poll_batch(1).await.unwrap();
        assert_eq!(batch.record_count, 0);

        src.set_credits(100);
        let batch2 = src.poll_batch(2).await.unwrap();
        assert_eq!(batch2.record_count, 10);
    }
}
