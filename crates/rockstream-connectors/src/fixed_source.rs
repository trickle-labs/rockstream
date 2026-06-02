//! Fixed in-memory source for testing and the v0.5 IVM kernel.
//!
//! Produces pre-loaded `ZSetBatch`es, one per epoch. Used by property tests
//! and the oracle harness to drive the IVM pipeline with known data.

use async_trait::async_trait;
use rockstream_types::batch::{SourceBatch, ZSet, ZSetBatch};
use rockstream_types::timestamp::Epoch;

use crate::source::Source;

/// A source that emits pre-loaded Z-set deltas, one per epoch.
pub struct FixedSource {
    name: String,
    batches: Vec<ZSet>,
    cursor: usize,
}

impl FixedSource {
    /// Create a fixed source with the given name and per-epoch deltas.
    pub fn new(name: impl Into<String>, batches: Vec<ZSet>) -> Self {
        Self {
            name: name.into(),
            batches,
            cursor: 0,
        }
    }

    /// Poll for the next Z-set delta. Returns `None` when exhausted.
    pub fn poll_zset(&mut self, epoch: Epoch) -> Option<ZSetBatch> {
        if self.cursor >= self.batches.len() {
            return None;
        }
        let zset = self.batches[self.cursor].clone();
        self.cursor += 1;
        Some(ZSetBatch { zset, epoch })
    }
}

#[async_trait]
impl Source for FixedSource {
    async fn poll_batch(&mut self, epoch: Epoch) -> Option<SourceBatch> {
        if self.cursor >= self.batches.len() {
            return None;
        }
        let zset = &self.batches[self.cursor];
        let count = zset.len();
        self.cursor += 1;
        Some(SourceBatch {
            record_count: count,
            epoch,
            offset: None,
            watermark: None,
        })
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::batch::ZSet;

    #[tokio::test]
    async fn fixed_source_emits_batches() {
        let mut zs = ZSet::new();
        zs.insert(b"k1".to_vec(), b"v1".to_vec(), 1);

        let mut src = FixedSource::new("test", vec![zs.clone(), zs.clone()]);

        let b1 = src.poll_zset(1);
        assert!(b1.is_some());
        assert_eq!(b1.unwrap().epoch, 1);

        let b2 = src.poll_zset(2);
        assert!(b2.is_some());

        let b3 = src.poll_zset(3);
        assert!(b3.is_none());
    }
}
