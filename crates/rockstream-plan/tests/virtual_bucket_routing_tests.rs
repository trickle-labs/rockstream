use std::collections::HashMap;

use rockstream_plan::virtual_bucket::route_virtual_bucket;
use rockstream_plan::{OpKind, OpNode};
use rockstream_types::ids::OperatorId;

#[test]
fn virtual_bucket_routing_is_deterministic_for_same_key_and_bucket_count() {
    let key = b"hot-key:customer-42";
    let first = route_virtual_bucket(key, 8, key.len()).unwrap();
    let second = route_virtual_bucket(key, 8, key.len()).unwrap();
    assert_eq!(first, second);

    let split = OpNode {
        id: OperatorId(7),
        kind: OpKind::VirtualBucketSplit {
            bucket_count: 8,
            key_prefix_len: key.len(),
        },
        merge_law: None,
        not_merge_safe_reason: None,
        inputs: vec![OperatorId(6)],
    };
    let combine = OpNode {
        id: OperatorId(8),
        kind: OpKind::VirtualBucketCombine { source: split.id },
        merge_law: None,
        not_merge_safe_reason: None,
        inputs: vec![split.id],
    };

    assert!(matches!(split.kind, OpKind::VirtualBucketSplit { .. }));
    assert!(
        matches!(combine.kind, OpKind::VirtualBucketCombine { source } if source == OperatorId(7))
    );
}

#[test]
fn virtual_bucket_routing_handles_zero_buckets_and_prefix_bounds() {
    let key = b"customer-42";

    assert_eq!(route_virtual_bucket(key, 0, key.len()), None);
    assert_eq!(route_virtual_bucket(key, 1, 0), Some(0));
    assert_eq!(
        route_virtual_bucket(key, 16, key.len() + 1),
        route_virtual_bucket(key, 16, key.len())
    );
}

#[test]
fn virtual_bucket_routing_spreads_synthetic_keys_across_all_buckets_with_bounded_skew() {
    let bucket_count = 16u16;
    let mut counts: HashMap<u16, usize> = HashMap::new();
    let samples = 4096usize;

    for key_idx in 0..samples {
        let key = format!("customer-{key_idx:04}");
        let bucket = route_virtual_bucket(key.as_bytes(), bucket_count, key.len()).unwrap();
        *counts.entry(bucket).or_default() += 1;
    }

    assert_eq!(counts.len(), bucket_count as usize);
    let expected = samples as f64 / bucket_count as f64;
    let max_skew = 0.35;
    for bucket in 0..bucket_count {
        let observed = *counts.get(&bucket).unwrap_or(&0) as f64;
        let skew = ((observed - expected) / expected).abs();
        assert!(
            skew <= max_skew,
            "bucket {bucket} skew {skew:.3} exceeded bound {max_skew:.3}; counts={counts:?}"
        );
    }
}
