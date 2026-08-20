//! v0.59.5 Slice 3: State Mutation Unit Tests.
//!
//! Asserts serialization, deserialization, size calculations, and round-tripping of StateMutation variants.

use bytes::Bytes;
use rockstream_storage::{EpochStateDelta, StateMutation};
use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};

#[test]
fn test_state_mutation_put_round_trip() {
    let mutation = StateMutation::Put {
        key: b"group:42".to_vec(),
        value: Bytes::from_static(b"sum_and_count_payload"),
    };
    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: StateMutation = serde_json::from_str(&json).unwrap();
    assert_eq!(mutation, decoded);
    assert_eq!(decoded.key(), b"group:42");
    assert_eq!(decoded.size_bytes(), 8 + 21);
}

#[test]
fn test_state_mutation_delete_round_trip() {
    let mutation = StateMutation::Delete {
        key: b"group:99".to_vec(),
    };
    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: StateMutation = serde_json::from_str(&json).unwrap();
    assert_eq!(mutation, decoded);
    assert_eq!(decoded.key(), b"group:99");
    assert_eq!(decoded.size_bytes(), 8);
}

#[test]
fn test_state_mutation_merge_round_trip() {
    let mutation = StateMutation::Merge {
        key: b"group:100".to_vec(),
        law: MergeLawId(1),
        law_version: MergeLawVersion(1),
        operand: Bytes::from_static(b"delta_operand"),
    };
    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: StateMutation = serde_json::from_str(&json).unwrap();
    assert_eq!(mutation, decoded);
    assert_eq!(decoded.key(), b"group:100");
}

#[test]
fn test_epoch_state_delta_container() {
    let mut delta = EpochStateDelta::new();
    delta.push(StateMutation::Put {
        key: b"k1".to_vec(),
        value: Bytes::from_static(b"v1"),
    });
    delta.push(StateMutation::Delete {
        key: b"k2".to_vec(),
    });

    assert_eq!(delta.len(), 2);
    assert_eq!(delta.dirty_key_count, 2);
    assert!(!delta.is_empty());
}
