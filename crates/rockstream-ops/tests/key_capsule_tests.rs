use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::{ArrowZSet, JoinOp};
use rockstream_storage::{JoinSide, ShardKeyEncoder};
use rockstream_types::ids::OperatorId;
use rockstream_types::{KeyCapsule, KeyValue};
use std::sync::Arc;

#[test]
fn int64_capsule_is_identical_for_every_consumer() {
    let capsule = KeyCapsule::from_values(&[KeyValue::Int64(-7)]).unwrap();
    assert_eq!(
        capsule.typed_bytes(),
        &[1, 0, 0, 0, 8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xf9]
    );
    let lookup = capsule.typed_bytes().to_vec();
    let partition = capsule.stable_hash();
    let shuffle = capsule.stable_hash();
    let persisted = ShardKeyEncoder::join_arr_key(JoinSide::Left, 9, capsule.typed_bytes(), 1);
    assert_eq!(lookup, capsule.typed_bytes());
    assert_eq!(partition, shuffle);
    assert!(persisted
        .windows(capsule.typed_bytes().len())
        .any(|window| window == capsule.typed_bytes()));
}

#[test]
fn utf8_capsule_is_identical_for_every_consumer() {
    let capsule = KeyCapsule::from_values(&[KeyValue::Utf8("å".into())]).unwrap();
    assert_eq!(capsule.typed_bytes(), &[2, 0, 0, 0, 2, 0xc3, 0xa5]);
    assert_eq!(
        capsule,
        KeyCapsule::from_values(&[KeyValue::Utf8("å".into())]).unwrap()
    );
    assert_ne!(
        capsule,
        KeyCapsule::from_values(&[KeyValue::Utf8("a".into())]).unwrap()
    );
}

#[test]
fn composite_capsule_is_unambiguous_and_replay_stable() {
    let first =
        KeyCapsule::from_values(&[KeyValue::Int64(1), KeyValue::Utf8("23".into())]).unwrap();
    let second =
        KeyCapsule::from_values(&[KeyValue::Int64(12), KeyValue::Utf8("3".into())]).unwrap();
    assert_ne!(first.typed_bytes(), second.typed_bytes());
    assert_ne!(first.stable_hash(), second.stable_hash());
    assert_eq!(
        first,
        KeyCapsule::from_values(&[KeyValue::Int64(1), KeyValue::Utf8("23".into())]).unwrap()
    );
}

#[test]
fn null_capsule_never_creates_an_equi_join_match() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
    let left = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef],
    )
    .unwrap();
    let right = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![None, Some(1)])) as ArrayRef],
    )
    .unwrap();
    assert!(KeyCapsule::from_array(left.column(0), 1).unwrap().is_null());
    assert_ne!(
        KeyCapsule::from_array(left.column(0), 0).unwrap(),
        KeyCapsule::from_array(right.column(0), 0).unwrap()
    );
}

#[test]
fn nullable_inner_join_skips_null_keys_instead_of_coercing_to_zero() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, false),
    ]));
    let left = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![None])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10])) as ArrayRef,
        ],
    )
    .unwrap();
    let right = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(0)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![20])) as ArrayRef,
        ],
    )
    .unwrap();
    let op = JoinOp::new(OperatorId(81), vec![0], vec![0]);
    let output = op
        .process_epoch(
            ArrowZSet::new(left, vec![1]),
            ArrowZSet::new(right, vec![1]),
        )
        .unwrap();
    assert!(output.is_empty());
}

#[test]
fn unsupported_key_type_fails_closed_before_capsule_use() {
    let array = Arc::new(Float64Array::from(vec![1.0])) as ArrayRef;
    let error = KeyCapsule::from_array(&array, 0).unwrap_err();
    assert!(error.to_string().contains("Float64"));
}

#[test]
fn partition_shuffle_skew_and_persistence_share_one_capsule() {
    let capsule = KeyCapsule::from_values(&[KeyValue::Utf8("same".into())]).unwrap();
    let reencoded = KeyCapsule::from_values(&[KeyValue::Utf8("same".into())]).unwrap();
    let persisted = ShardKeyEncoder::join_arr_key(JoinSide::Right, 12, capsule.typed_bytes(), 4);
    assert_eq!(capsule.typed_bytes(), reencoded.typed_bytes());
    assert_eq!(capsule.stable_hash(), reencoded.stable_hash());
    assert!(persisted
        .windows(capsule.typed_bytes().len())
        .any(|window| window == capsule.typed_bytes()));
}

#[test]
fn capsule_arrow_columns_preserve_exact_types() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["x"])) as ArrayRef],
    )
    .unwrap();
    assert_eq!(
        KeyCapsule::from_array(batch.column(0), 0)
            .unwrap()
            .typed_bytes(),
        &[2, 0, 0, 0, 1, b'x']
    );
}
