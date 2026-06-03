//! Tests for worker-side arrangement segment cache.

#[cfg(test)]
mod tests {
    use rockstream_runtime::segment_cache::{SegmentCache, SegmentCacheConfig, ShardSegmentKey};

    #[test]
    fn insert_and_get_returns_correct_data() {
        let mut cache = SegmentCache::new(SegmentCacheConfig::default());
        let key = ShardSegmentKey::new(0, 1);
        cache.insert(key, vec![1, 2, 3, 4]);
        assert_eq!(cache.get(&key).unwrap(), &[1, 2, 3, 4]);
    }

    #[test]
    fn evicts_lru_when_at_capacity() {
        let config = SegmentCacheConfig {
            max_segments: 4,
            epoch_ttl: 10,
        };
        let mut cache = SegmentCache::new(config);

        // Fill to capacity.
        for i in 0..4u64 {
            cache.insert(ShardSegmentKey::new(0, i), vec![i as u8]);
        }
        assert_eq!(cache.len(), 4);

        // Touch segment 0 (makes it MRU).
        let _ = cache.get(&ShardSegmentKey::new(0, 0));

        // Insert a 5th entry — should evict segment 1 (LRU).
        cache.insert(ShardSegmentKey::new(0, 100), vec![100]);
        assert_eq!(cache.len(), 4);

        // Segment 1 should be gone.
        assert!(cache.get(&ShardSegmentKey::new(0, 1)).is_none());
        // Segment 0 should still be present.
        assert!(cache.get(&ShardSegmentKey::new(0, 0)).is_some());
    }

    #[test]
    fn evicts_expired_entries_by_ttl() {
        let config = SegmentCacheConfig {
            max_segments: 10,
            epoch_ttl: 2,
        };
        let mut cache = SegmentCache::new(config);

        cache.set_epoch(1);
        cache.insert(ShardSegmentKey::new(0, 1), vec![1]);

        cache.set_epoch(2);
        cache.insert(ShardSegmentKey::new(0, 2), vec![2]);

        // Advance to epoch 4 (epoch 1 is expired because 4 - 1 = 3 > 2)
        cache.set_epoch(4);

        // Segment 1 should be evicted/expired
        assert!(cache.get(&ShardSegmentKey::new(0, 1)).is_none());
        // Segment 2 should still be present (4 - 2 = 2 <= 2)
        assert!(cache.get(&ShardSegmentKey::new(0, 2)).is_some());
    }

    /// Benchmark test asserting > 80% hit rate on join workloads.
    #[test]
    fn proof_segment_cache_hit_ratio_exceeds_80_percent() {
        const HOT_SEGMENTS: u64 = 20;
        const HOT_REPLAYS: u64 = 5000;

        let config = SegmentCacheConfig {
            max_segments: 32,
            epoch_ttl: 100,
        };
        let mut cache = SegmentCache::new(config);

        // Warm up hot set
        for seg in 0..HOT_SEGMENTS {
            cache.insert(ShardSegmentKey::new(0, seg), vec![seg as u8]);
        }

        // Reset stats
        cache.stats = Default::default();

        // Hot replay
        for i in 0..HOT_REPLAYS {
            let seg = i % HOT_SEGMENTS;
            let _ = cache.get(&ShardSegmentKey::new(0, seg));
        }

        let hit_ratio = cache.stats.hit_ratio();
        assert!(
            hit_ratio > 0.80,
            "cache hit ratio must exceed 80%; actual={hit_ratio:.3}",
        );
    }
}
