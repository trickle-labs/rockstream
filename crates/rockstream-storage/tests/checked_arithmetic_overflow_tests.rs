//! v0.59.5 Slice 3: Checked Arithmetic Overflow Tests.
//!
//! Asserts that arithmetic overflow in SumCountMergeOperator fails closed with RS-1002
//! instead of silently wrapping.

use bytes::Bytes;
use rockstream_storage::{MergeOperatorRegistry, SumCountMergeOperator};
use slatedb::MergeOperator;

#[test]
fn test_sum_overflow_fails_closed_with_rs_1002() {
    let op = SumCountMergeOperator;
    let key = Bytes::from_static(b"k");

    let val1 = Bytes::from(MergeOperatorRegistry::encode_sum(i64::MAX));
    let val2 = Bytes::from(MergeOperatorRegistry::encode_sum(1));

    let res = op.merge(&key, Some(val1), val2);
    assert!(
        res.is_err(),
        "i64::MAX + 1 must fail with arithmetic overflow"
    );
    let err_msg = format!("{:?}", res.unwrap_err());
    assert!(
        err_msg.contains("RS-1002"),
        "Overflow error message must contain RS-1002: {err_msg}"
    );
}

#[test]
fn test_sum_underflow_fails_closed_with_rs_1002() {
    let op = SumCountMergeOperator;
    let key = Bytes::from_static(b"k");

    let val1 = Bytes::from(MergeOperatorRegistry::encode_sum(i64::MIN));
    let val2 = Bytes::from(MergeOperatorRegistry::encode_sum(-1));

    let res = op.merge(&key, Some(val1), val2);
    assert!(
        res.is_err(),
        "i64::MIN + (-1) must fail with arithmetic underflow"
    );
    let err_msg = format!("{:?}", res.unwrap_err());
    assert!(
        err_msg.contains("RS-1002"),
        "Underflow error message must contain RS-1002: {err_msg}"
    );
}

#[test]
fn test_count_overflow_fails_closed_with_rs_1002() {
    let op = SumCountMergeOperator;
    let key = Bytes::from_static(b"k");

    let val1 = Bytes::from(MergeOperatorRegistry::encode_count(u64::MAX));
    let val2 = Bytes::from(MergeOperatorRegistry::encode_count(1));

    let res = op.merge(&key, Some(val1), val2);
    assert!(
        res.is_err(),
        "u64::MAX + 1 must fail with arithmetic overflow"
    );
    let err_msg = format!("{:?}", res.unwrap_err());
    assert!(
        err_msg.contains("RS-1002"),
        "Count overflow error message must contain RS-1002: {err_msg}"
    );
}
