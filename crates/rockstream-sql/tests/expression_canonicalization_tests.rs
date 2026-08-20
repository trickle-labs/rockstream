//! Expression Canonicalization Tests (v0.59.6).
//!
//! Tests normalization of harmless casts, commutative predicates, aliases,
//! and verification that semantically identical specifications yield the exact same ArrangementId.

use rockstream_sql::ExpressionNormalizer;
use rockstream_types::arrangement::{
    ArrangementSpec, CanonicalBinaryOp, CanonicalExpr, CanonicalType, CollationId,
    CollationVersion, NullSemantics, PartitioningSpec, SourceIdentity, TimeDomainSemantics,
};
use rockstream_types::ids::TenantId;
use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};

#[test]
fn test_canonical_sharing_harmless_cast() {
    let raw_expr1 = CanonicalExpr::Cast {
        expr: Box::new(CanonicalExpr::Cast {
            expr: Box::new(CanonicalExpr::col("amount")),
            target_type: CanonicalType::Int64,
        }),
        target_type: CanonicalType::Int64,
    };
    let raw_expr2 = CanonicalExpr::Cast {
        expr: Box::new(CanonicalExpr::col("amount")),
        target_type: CanonicalType::Int64,
    };

    let norm1 = ExpressionNormalizer::normalize(&raw_expr1);
    let norm2 = ExpressionNormalizer::normalize(&raw_expr2);

    assert_eq!(norm1, norm2);
}

#[test]
fn test_canonical_sharing_commutative_predicate() {
    // Predicate 1: x = 1 AND y = 2
    let p1 = CanonicalExpr::BinaryOp {
        op: CanonicalBinaryOp::And,
        left: Box::new(CanonicalExpr::BinaryOp {
            op: CanonicalBinaryOp::Eq,
            left: Box::new(CanonicalExpr::col("x")),
            right: Box::new(CanonicalExpr::lit_int(1)),
        }),
        right: Box::new(CanonicalExpr::BinaryOp {
            op: CanonicalBinaryOp::Eq,
            left: Box::new(CanonicalExpr::col("y")),
            right: Box::new(CanonicalExpr::lit_int(2)),
        }),
    };

    // Predicate 2: y = 2 AND x = 1
    let p2 = CanonicalExpr::BinaryOp {
        op: CanonicalBinaryOp::And,
        left: Box::new(CanonicalExpr::BinaryOp {
            op: CanonicalBinaryOp::Eq,
            left: Box::new(CanonicalExpr::col("y")),
            right: Box::new(CanonicalExpr::lit_int(2)),
        }),
        right: Box::new(CanonicalExpr::BinaryOp {
            op: CanonicalBinaryOp::Eq,
            left: Box::new(CanonicalExpr::col("x")),
            right: Box::new(CanonicalExpr::lit_int(1)),
        }),
    };

    let norm1 = ExpressionNormalizer::normalize(&p1);
    let norm2 = ExpressionNormalizer::normalize(&p2);

    assert_eq!(norm1, norm2);
}

#[test]
fn test_canonical_spec_identical_fingerprint() {
    let spec1 = ArrangementSpec {
        tenant_id: TenantId(10),
        security_policy_digest: [1u8; 32],
        source_identity: SourceIdentity::new("EVENTS"),
        source_schema_generation: 1,
        key_expressions: vec![CanonicalExpr::col(" USER_ID ")],
        key_types: vec![CanonicalType::Int64],
        value_projection: vec![CanonicalExpr::col("AMOUNT")],
        predicate: Some(CanonicalExpr::BinaryOp {
            op: CanonicalBinaryOp::And,
            left: Box::new(CanonicalExpr::col("a")),
            right: Box::new(CanonicalExpr::col("b")),
        }),
        null_semantics: NullSemantics::NullsFirst,
        decimal_scale: Some(4),
        collation_identifier: CollationId("UTF8_DEFAULT".to_string()),
        collation_version: CollationVersion(1),
        time_domain: TimeDomainSemantics::Utc,
        merge_law_id: MergeLawId(2),
        merge_law_version: MergeLawVersion(1),
        partitioning: PartitioningSpec::SingleShard(0),
    };

    let spec2 = ArrangementSpec {
        tenant_id: TenantId(10),
        security_policy_digest: [1u8; 32],
        source_identity: SourceIdentity::new("events"),
        source_schema_generation: 1,
        key_expressions: vec![CanonicalExpr::col("user_id")],
        key_types: vec![CanonicalType::Int64],
        value_projection: vec![CanonicalExpr::col("amount")],
        predicate: Some(CanonicalExpr::BinaryOp {
            op: CanonicalBinaryOp::And,
            left: Box::new(CanonicalExpr::col("b")),
            right: Box::new(CanonicalExpr::col("a")),
        }),
        null_semantics: NullSemantics::NullsFirst,
        decimal_scale: Some(4),
        collation_identifier: CollationId("utf8_default".to_string()),
        collation_version: CollationVersion(1),
        time_domain: TimeDomainSemantics::Utc,
        merge_law_id: MergeLawId(2),
        merge_law_version: MergeLawVersion(1),
        partitioning: PartitioningSpec::SingleShard(0),
    };

    let norm_spec1 = ExpressionNormalizer::canonicalize_spec(&spec1);
    let norm_spec2 = ExpressionNormalizer::canonicalize_spec(&spec2);

    assert_eq!(norm_spec1, norm_spec2);
    assert_eq!(norm_spec1.arrangement_id(), norm_spec2.arrangement_id());
}
