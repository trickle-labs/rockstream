//! Arrangement Spec Isolation Tests (v0.59.6).
//!
//! Asserts that 10 distinct isolation variation axes never collide into the same ArrangementId:
//! 1. Tenant boundary (`tenant_id`)
//! 2. Security policy digest (`security_policy_digest`)
//! 3. Predicate mismatch (`predicate`)
//! 4. Collation version (`collation_version`)
//! 5. Decimal scale (`decimal_scale`)
//! 6. Schema generation (`source_schema_generation`)
//! 7. Merge law version (`merge_law_version`)
//! 8. Null semantics (`null_semantics`)
//! 9. Time domain (`time_domain`)
//! 10. Source identity (`source_identity`)

use rockstream_types::arrangement::{
    ArrangementSpec, CanonicalBinaryOp, CanonicalExpr, CanonicalType, CollationId,
    CollationVersion, NullSemantics, PartitioningSpec, SourceIdentity, TimeDomainSemantics,
};
use rockstream_types::ids::TenantId;
use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};

fn base_spec() -> ArrangementSpec {
    ArrangementSpec {
        tenant_id: TenantId(1),
        security_policy_digest: [0u8; 32],
        source_identity: SourceIdentity::new("events"),
        source_schema_generation: 1,
        key_expressions: vec![CanonicalExpr::col("user_id")],
        key_types: vec![CanonicalType::Int64],
        value_projection: vec![CanonicalExpr::col("amount")],
        predicate: Some(CanonicalExpr::binary_op(
            CanonicalBinaryOp::Gt,
            CanonicalExpr::col("amount"),
            CanonicalExpr::lit_int(10),
        )),
        null_semantics: NullSemantics::NullsFirst,
        decimal_scale: Some(2),
        collation_identifier: CollationId::utf8_default(),
        collation_version: CollationVersion(1),
        time_domain: TimeDomainSemantics::Utc,
        merge_law_id: MergeLawId(1),
        merge_law_version: MergeLawVersion(1),
        partitioning: PartitioningSpec::SingleShard(0),
    }
}

#[test]
fn test_isolation_tenant_boundary() {
    let spec1 = base_spec();
    let mut spec2 = base_spec();
    spec2.tenant_id = TenantId(2);

    assert_ne!(spec1.arrangement_id(), spec2.arrangement_id());
}

#[test]
fn test_isolation_security_policy_digest() {
    let spec1 = base_spec();
    let mut spec2 = base_spec();
    spec2.security_policy_digest[0] = 0xFF;

    assert_ne!(spec1.arrangement_id(), spec2.arrangement_id());
}

#[test]
fn test_isolation_predicate_mismatch() {
    let spec1 = base_spec();
    let mut spec2 = base_spec();
    spec2.predicate = Some(CanonicalExpr::binary_op(
        CanonicalBinaryOp::Gt,
        CanonicalExpr::col("amount"),
        CanonicalExpr::lit_int(20),
    ));

    assert_ne!(spec1.arrangement_id(), spec2.arrangement_id());
}

#[test]
fn test_isolation_collation_version() {
    let spec1 = base_spec();
    let mut spec2 = base_spec();
    spec2.collation_version = CollationVersion(2);

    assert_ne!(spec1.arrangement_id(), spec2.arrangement_id());
}

#[test]
fn test_isolation_decimal_scale() {
    let spec1 = base_spec();
    let mut spec2 = base_spec();
    spec2.decimal_scale = Some(4);

    assert_ne!(spec1.arrangement_id(), spec2.arrangement_id());
}

#[test]
fn test_isolation_schema_generation() {
    let spec1 = base_spec();
    let mut spec2 = base_spec();
    spec2.source_schema_generation = 2;

    assert_ne!(spec1.arrangement_id(), spec2.arrangement_id());
}

#[test]
fn test_isolation_merge_law_version() {
    let spec1 = base_spec();
    let mut spec2 = base_spec();
    spec2.merge_law_version = MergeLawVersion(2);

    assert_ne!(spec1.arrangement_id(), spec2.arrangement_id());
}

#[test]
fn test_isolation_null_semantics() {
    let spec1 = base_spec();
    let mut spec2 = base_spec();
    spec2.null_semantics = NullSemantics::NullsLast;

    assert_ne!(spec1.arrangement_id(), spec2.arrangement_id());
}

#[test]
fn test_isolation_time_domain() {
    let spec1 = base_spec();
    let mut spec2 = base_spec();
    spec2.time_domain = TimeDomainSemantics::Timezone("America/New_York".to_string());

    assert_ne!(spec1.arrangement_id(), spec2.arrangement_id());
}

#[test]
fn test_isolation_source_identity() {
    let spec1 = base_spec();
    let mut spec2 = base_spec();
    spec2.source_identity = SourceIdentity::new("orders");

    assert_ne!(spec1.arrangement_id(), spec2.arrangement_id());
}
