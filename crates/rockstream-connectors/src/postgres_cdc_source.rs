//! Postgres CDC source connector mock with Tier 1 contract.

use async_trait::async_trait;
use rockstream_types::batch::{OffsetToken, SourceBatch};
use rockstream_types::timestamp::{Epoch, EventTimeWatermark};

use crate::source::Source;

/// A mock Postgres CDC source connector.
pub struct PostgresCdcSource {
    #[allow(dead_code)]
    table: String,
    credits: usize,
    current_lsn: u64,
    watermark: EventTimeWatermark,
}

impl PostgresCdcSource {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            credits: usize::MAX,
            current_lsn: 1000,
            watermark: 0,
        }
    }
}

#[async_trait]
impl Source for PostgresCdcSource {
    async fn poll_batch(&mut self, epoch: Epoch) -> Option<SourceBatch> {
        if self.credits == 0 {
            return Some(SourceBatch {
                record_count: 0,
                epoch,
                offset: Some(OffsetToken(format!("lsn:{}", self.current_lsn))),
                watermark: Some(self.watermark),
            });
        }

        self.current_lsn += 100;
        self.watermark = epoch * 1000;

        Some(SourceBatch {
            record_count: 5,
            epoch,
            offset: Some(OffsetToken(format!("lsn:{}", self.current_lsn))),
            watermark: Some(self.watermark),
        })
    }

    fn name(&self) -> &str {
        "postgres-cdc-source"
    }

    fn credits_available(&self) -> usize {
        self.credits
    }

    fn set_credits(&mut self, credits: usize) {
        self.credits = credits;
    }

    fn current_offset(&self) -> Option<OffsetToken> {
        Some(OffsetToken(format!("lsn:{}", self.current_lsn)))
    }
}
