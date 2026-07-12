use std::sync::Arc;

use async_trait::async_trait;
use moka::future::Cache;
use rockstream_types::metrics::{
    record_compaction_write, record_segment_cache_hit, record_segment_cache_miss,
    set_segment_cache_bytes_used,
};
use slatedb::compactor::stats::BYTES_COMPACTED;
use slatedb::db_cache::{
    CachedEntry, CachedKey, DbCache, SplitCache, DEFAULT_BLOCK_CACHE_CAPACITY,
    DEFAULT_META_CACHE_CAPACITY,
};
use slatedb::db_stats::L0_FLUSH_BYTES;
use slatedb_common::metrics::{CounterFn, GaugeFn, HistogramFn, MetricsRecorder, UpDownCounterFn};

pub(crate) fn instrumented_db_cache(worker_id: &str) -> Arc<dyn DbCache> {
    Arc::new(
        SplitCache::new()
            .with_block_cache(Some(Arc::new(InstrumentedMokaCache::new(
                worker_id,
                DEFAULT_BLOCK_CACHE_CAPACITY,
            ))))
            .with_meta_cache(Some(Arc::new(InstrumentedMokaCache::new(
                worker_id,
                DEFAULT_META_CACHE_CAPACITY,
            ))))
            .build(),
    )
}

pub(crate) fn instrumented_metrics_recorder(shard_id: u16) -> Arc<dyn MetricsRecorder> {
    Arc::new(RockstreamSlateDbMetricsRecorder { shard_id })
}

struct InstrumentedMokaCache {
    worker_id: String,
    inner: Cache<CachedKey, CachedEntry>,
}

impl InstrumentedMokaCache {
    fn new(worker_id: &str, max_capacity: u64) -> Self {
        Self {
            worker_id: worker_id.to_string(),
            inner: Cache::builder()
                .weigher(|_, value: &CachedEntry| value.size() as u32)
                .max_capacity(max_capacity)
                .build(),
        }
    }

    fn refresh_bytes_used(&self) {
        set_segment_cache_bytes_used(&self.worker_id, self.inner.weighted_size());
    }

    async fn record_get(
        &self,
        result: Result<Option<CachedEntry>, slatedb::Error>,
    ) -> Result<Option<CachedEntry>, slatedb::Error> {
        match &result {
            Ok(Some(_)) => record_segment_cache_hit(&self.worker_id),
            Ok(None) => record_segment_cache_miss(&self.worker_id),
            Err(_) => {}
        }
        result
    }
}

#[async_trait]
impl DbCache for InstrumentedMokaCache {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.record_get(Ok(self.inner.get(key).await)).await
    }

    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.record_get(Ok(self.inner.get(key).await)).await
    }

    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.record_get(Ok(self.inner.get(key).await)).await
    }

    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.record_get(Ok(self.inner.get(key).await)).await
    }

    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        self.inner.insert(key, value).await;
        self.refresh_bytes_used();
    }

    async fn remove(&self, key: &CachedKey) {
        self.inner.remove(key).await;
        self.refresh_bytes_used();
    }

    fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
}

struct RockstreamSlateDbMetricsRecorder {
    shard_id: u16,
}

impl MetricsRecorder for RockstreamSlateDbMetricsRecorder {
    fn register_counter(
        &self,
        name: &str,
        _description: &str,
        _labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        match name {
            BYTES_COMPACTED => Arc::new(CompactionBytesCounter {
                shard_id: self.shard_id,
            }),
            L0_FLUSH_BYTES => Arc::new(LogicalWriteBytesCounter {
                shard_id: self.shard_id,
            }),
            _ => Arc::new(NoopCounter),
        }
    }

    fn register_gauge(
        &self,
        _name: &str,
        _description: &str,
        _labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        Arc::new(NoopGauge)
    }

    fn register_up_down_counter(
        &self,
        _name: &str,
        _description: &str,
        _labels: &[(&str, &str)],
    ) -> Arc<dyn UpDownCounterFn> {
        Arc::new(NoopUpDownCounter)
    }

    fn register_histogram(
        &self,
        _name: &str,
        _description: &str,
        _labels: &[(&str, &str)],
        _boundaries: &[f64],
    ) -> Arc<dyn HistogramFn> {
        Arc::new(NoopHistogram)
    }
}

struct CompactionBytesCounter {
    shard_id: u16,
}

impl CounterFn for CompactionBytesCounter {
    fn increment(&self, value: u64) {
        record_compaction_write(self.shard_id, value, 0);
    }
}

struct LogicalWriteBytesCounter {
    shard_id: u16,
}

impl CounterFn for LogicalWriteBytesCounter {
    fn increment(&self, value: u64) {
        record_compaction_write(self.shard_id, value, value);
    }
}

struct NoopCounter;

impl CounterFn for NoopCounter {
    fn increment(&self, _value: u64) {}
}

struct NoopGauge;

impl GaugeFn for NoopGauge {
    fn set(&self, _value: i64) {}
}

struct NoopUpDownCounter;

impl UpDownCounterFn for NoopUpDownCounter {
    fn increment(&self, _value: i64) {}
}

struct NoopHistogram;

impl HistogramFn for NoopHistogram {
    fn record(&self, _value: f64) {}
}
