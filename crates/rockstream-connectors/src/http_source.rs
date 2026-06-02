//! HTTP push webhook source mock with Tier 1 contract.

use async_trait::async_trait;
use rockstream_types::batch::{OffsetToken, SourceBatch};
use rockstream_types::timestamp::{Epoch, EventTimeWatermark};

use crate::source::Source;

/// A mock HTTP webhook source connector.
pub struct HttpSource {
    #[allow(dead_code)]
    endpoint: String,
    credits: usize,
    current_request_id: u64,
    watermark: EventTimeWatermark,
}

impl HttpSource {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            credits: usize::MAX,
            current_request_id: 0,
            watermark: 0,
        }
    }
}

#[async_trait]
impl Source for HttpSource {
    async fn poll_batch(&mut self, epoch: Epoch) -> Option<SourceBatch> {
        if self.credits == 0 {
            return Some(SourceBatch {
                record_count: 0,
                epoch,
                offset: Some(OffsetToken(format!("req-{}", self.current_request_id))),
                watermark: Some(self.watermark),
            });
        }

        self.current_request_id += 1;
        self.watermark = epoch * 250;

        Some(SourceBatch {
            record_count: 8,
            epoch,
            offset: Some(OffsetToken(format!("req-{}", self.current_request_id))),
            watermark: Some(self.watermark),
        })
    }

    fn name(&self) -> &str {
        "http-source"
    }

    fn credits_available(&self) -> usize {
        self.credits
    }

    fn set_credits(&mut self, credits: usize) {
        self.credits = credits;
    }

    fn current_offset(&self) -> Option<OffsetToken> {
        Some(OffsetToken(format!("req-{}", self.current_request_id)))
    }
}
