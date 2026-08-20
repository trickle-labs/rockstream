//! v0.59.5 Slice 3: Merge Law Versioned Tag and Unknown Rejection Tests.
//!
//! Asserts merge resolution across SumCount, Min, Max, and fail-closed rejection of unknown tags.

use bytes::Bytes;
use rockstream_storage::{MergeOperatorRegistry, SumCountMergeOperator};
use slatedb::MergeOperator;

#[test]
fn test_sum_count_merge_resolution() {
    let op = SumCountMergeOperator;
    let key = Bytes::from_static(b"k");

    let val1 = Bytes::from(MergeOperatorRegistry::encode_sum(50));
    let val2 = Bytes::from(MergeOperatorRegistry::encode_sum(25));
    let merged = op.merge(&key, Some(val1), val2).unwrap();
    assert_eq!(MergeOperatorRegistry::decode_sum(&merged), Some(75));

    let c1 = Bytes::from(MergeOperatorRegistry::encode_count(10));
    let c2 = Bytes::from(MergeOperatorRegistry::encode_count(5));
    let merged_c = op.merge(&key, Some(c1), c2).unwrap();
    assert_eq!(MergeOperatorRegistry::decode_count(&merged_c), Some(15));
}

#[test]
fn test_min_max_merge_resolution() {
    let op = SumCountMergeOperator;
    let key = Bytes::from_static(b"k");

    let max1 = Bytes::from(MergeOperatorRegistry::encode_max(100));
    let max2 = Bytes::from(MergeOperatorRegistry::encode_max(250));
    let merged_max = op.merge(&key, Some(max1), max2).unwrap();
    assert_eq!(MergeOperatorRegistry::decode_max(&merged_max), Some(250));

    let min1 = Bytes::from(MergeOperatorRegistry::encode_min(100));
    let min2 = Bytes::from(MergeOperatorRegistry::encode_min(25));
    let merged_min = op.merge(&key, Some(min1), min2).unwrap();
    assert_eq!(MergeOperatorRegistry::decode_min(&merged_min), Some(25));
}

#[test]
fn test_unknown_tag_returns_rs_5002() {
    let op = SumCountMergeOperator;
    let key = Bytes::from_static(b"k");

    let unknown1 = Bytes::from_static(&[0xFE, 1, 2, 3, 4, 5, 6, 7, 8]);
    let unknown2 = Bytes::from_static(&[0xFE, 8, 7, 6, 5, 4, 3, 2, 1]);

    let res = op.merge(&key, Some(unknown1), unknown2);
    assert!(res.is_err());
    let err_msg = format!("{:?}", res.unwrap_err());
    assert!(
        err_msg.contains("RS-5002") || err_msg.contains("RS-3009"),
        "Unknown tag must fail with RS-5002/RS-3009: {err_msg}"
    );
}

#[test]
fn test_tag_mismatch_fails_closed() {
    let op = SumCountMergeOperator;
    let key = Bytes::from_static(b"k");

    let sum_val = Bytes::from(MergeOperatorRegistry::encode_sum(10));
    let count_val = Bytes::from(MergeOperatorRegistry::encode_count(5));

    let res = op.merge(&key, Some(sum_val), count_val);
    assert!(res.is_err());
}
