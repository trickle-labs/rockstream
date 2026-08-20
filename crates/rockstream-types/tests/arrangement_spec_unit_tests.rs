//! Unit tests for ArrangementSpec construction and ArrangementId hashing (v0.59.6).

use rockstream_types::arrangement::{
    ArrangementSpec, CanonicalBinaryOp, CanonicalExpr, CanonicalType, CollationId,
    CollationVersion, NullSemantics, PartitioningSpec, SourceIdentity, TimeDomainSemantics,
};
use rockstream_types::ids::TenantId;
use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};

fn sample_spec() -> ArrangementSpec {
    ArrangementSpec {
        tenant_id: TenantId(1),
        security_policy_digest: [0u8; 32],
        source_identity: SourceIdentity::new("orders"),
        source_schema_generation: 1,
        key_expressions: vec![CanonicalExpr::col("customer_id")],
        key_types: vec![CanonicalType::Int64],
        value_projection: vec![
            CanonicalExpr::col("customer_id"),
            CanonicalExpr::col("order_total"),
        ],
        predicate: Some(CanonicalExpr::binary_op(
            CanonicalBinaryOp::Gt,
            CanonicalExpr::col("order_total"),
            CanonicalExpr::lit_int(100),
        )),
        null_semantics: NullSemantics::NullsLast,
        decimal_scale: Some(2),
        collation_identifier: CollationId::utf8_default(),
        collation_version: CollationVersion(1),
        time_domain: TimeDomainSemantics::Utc,
        merge_law_id: MergeLawId(1),
        merge_law_version: MergeLawVersion(1),
        partitioning: PartitioningSpec::Hash { num_shards: 4 },
    }
}

#[test]
fn test_arrangement_spec_serialization_round_trip() {
    let spec = sample_spec();
    let json = serde_json::to_string(&spec).expect("serialize");
    let deserialized: ArrangementSpec = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(spec, deserialized);
}

#[test]
fn test_arrangement_id_deterministic_hashing() {
    let spec1 = sample_spec();
    let spec2 = sample_spec();
    let id1 = spec1.arrangement_id();
    let id2 = spec2.arrangement_id();
    assert_eq!(id1, id2);
    assert_eq!(id1.to_string(), format!("arr-{}", id1.0));
}

#[test]
fn test_canonical_expr_helpers() {
    let expr = CanonicalExpr::binary_op(
        CanonicalBinaryOp::Add,
        CanonicalExpr::lit_int(10),
        CanonicalExpr::lit_int(5),
    );
    // Add is commutative so lit_int(5) < lit_int(10), meaning left should be 5 and right should be 10
    match expr {
        CanonicalExpr::BinaryOp { op, left, right } => {
            assert_eq!(op, CanonicalBinaryOp::Add);
            assert_eq!(*left, CanonicalExpr::lit_int(5));
            assert_eq!(*right, CanonicalExpr::lit_int(10));
        }
        _ => panic!("Expected BinaryOp"),
    }
}
