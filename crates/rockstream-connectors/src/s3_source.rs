//! S3 / object-storage table format source mock with Tier 1 contract.

use async_trait::async_trait;
use rockstream_types::batch::{OffsetToken, SourceBatch};
use rockstream_types::timestamp::{Epoch, EventTimeWatermark};

use crate::source::Source;

/// A mock S3 source connector.
pub struct S3Source {
    #[allow(dead_code)]
    bucket: String,
    #[allow(dead_code)]
    prefix: String,
    credits: usize,
    current_file_idx: u64,
    watermark: EventTimeWatermark,
}

impl S3Source {
    pub fn new(bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: prefix.into(),
            credits: usize::MAX,
            current_file_idx: 0,
            watermark: 0,
        }
    }
}

#[async_trait]
impl Source for S3Source {
    async fn poll_batch(&mut self, epoch: Epoch) -> Option<SourceBatch> {
        if self.credits == 0 {
            return Some(SourceBatch {
                record_count: 0,
                epoch,
                offset: Some(OffsetToken(format!(
                    "file-{}.parquet",
                    self.current_file_idx
                ))),
                watermark: Some(self.watermark),
            });
        }

        self.current_file_idx += 1;
        self.watermark = epoch * 500;

        Some(SourceBatch {
            record_count: 20,
            epoch,
            offset: Some(OffsetToken(format!(
                "file-{}.parquet",
                self.current_file_idx
            ))),
            watermark: Some(self.watermark),
        })
    }

    fn name(&self) -> &str {
        "s3-source"
    }

    fn credits_available(&self) -> usize {
        self.credits
    }

    fn set_credits(&mut self, credits: usize) {
        self.credits = credits;
    }

    fn current_offset(&self) -> Option<OffsetToken> {
        Some(OffsetToken(format!(
            "file-{}.parquet",
            self.current_file_idx
        )))
    }
}
