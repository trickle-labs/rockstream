use arrow::datatypes::{DataType, Field, Schema};
use rockstream_sql::{ExpressionNormalizer, SqlFrontend};
use rockstream_types::arrangement::{
    ArrangementSpec, CanonicalExpr, CanonicalType, CollationId, CollationVersion, NullSemantics,
    PartitioningSpec, SourceIdentity, TimeDomainSemantics,
};
use rockstream_types::ids::TenantId;
use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};
use std::sync::Arc;

#[test]
fn test_binary_v1_collation_ordering() {
    let spec = ArrangementSpec {
        tenant_id: TenantId(1),
        security_policy_digest: [0u8; 32],
        source_identity: SourceIdentity::new("orders"),
        source_schema_generation: 1,
        key_expressions: vec![CanonicalExpr::col("name")],
        key_types: vec![CanonicalType::Utf8],
        value_projection: vec![CanonicalExpr::col("name")],
        predicate: None,
        null_semantics: NullSemantics::NullsFirst,
        decimal_scale: None,
        collation_identifier: CollationId::rockstream_binary_v1(),
        collation_version: CollationVersion(1),
        time_domain: TimeDomainSemantics::Utc,
        merge_law_id: MergeLawId(1),
        merge_law_version: MergeLawVersion(1),
        partitioning: PartitioningSpec::Hash { num_shards: 4 },
    };

    let canonical = ExpressionNormalizer::canonicalize_spec(&spec);
    assert_eq!(canonical.collation_identifier.0, "rockstream_binary_v1");
    assert!(canonical.collation_identifier.is_binary_supported());

    // Verify binary string byte-wise ordering
    let mut strings = vec!["b", "A", "a", "1", "B", "ä", "Z"];
    strings.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    // ASCII numbers < uppercase < lowercase < multi-byte UTF-8
    assert_eq!(strings, vec!["1", "A", "B", "Z", "a", "b", "ä"]);
}

#[test]
fn test_locale_sensitive_collation_rejected() {
    let locale_collate = CollationId("en_US.UTF-8".to_string());
    assert!(!locale_collate.is_binary_supported());

    let french_collate = CollationId("fr_FR".to_string());
    assert!(!french_collate.is_binary_supported());
}

#[tokio::test]
async fn test_float_join_rejected() {
    let frontend = SqlFrontend::new();

    let schema_left = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
    ]));
    let schema_right = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
    ]));

    frontend.register_table("t_left", schema_left).unwrap();
    frontend.register_table("t_right", schema_right).unwrap();

    // Joining on Float64 column 'score' must fail with RS-1019
    let res = frontend
        .sql_to_plan_node(
            "SELECT t_left.id FROM t_left JOIN t_right ON t_left.score = t_right.score",
        )
        .await;

    assert!(res.is_err(), "Float64 equi-join must be rejected");
    let err = res.err().unwrap();
    let err_str = err.to_string();
    assert!(
        err_str.contains("RS-1019") || err.error_code() == rockstream_types::error_code::RS_1019,
        "Expected error code RS-1019, got: {err_str}"
    );
}

#[tokio::test]
async fn test_integer_join_admitted() {
    let frontend = SqlFrontend::new();

    let schema_left = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Utf8, false),
    ]));
    let schema_right = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("info", DataType::Utf8, false),
    ]));

    frontend.register_table("t1", schema_left).unwrap();
    frontend.register_table("t2", schema_right).unwrap();

    let res = frontend
        .sql_to_plan_node("SELECT t1.id, t1.val, t2.info FROM t1 JOIN t2 ON t1.id = t2.id")
        .await;

    assert!(res.is_ok(), "Integer equi-join must be admitted");
}
