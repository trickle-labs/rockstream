//! Arrangement Catalog Tests (v0.59.6).
//!
//! Tests arrangement catalog registration, refcount increment, multi-consumer tracking,
//! and tenant isolation in catalog lookups.

use rockstream_storage::arrangement_catalog::ArrangementCatalog;
use rockstream_types::arrangement::{
    ArrangementSpec, CanonicalExpr, CanonicalType, CollationId, CollationVersion, NullSemantics,
    PartitioningSpec, SourceIdentity, TimeDomainSemantics,
};
use rockstream_types::ids::{TenantId, ViewId};
use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};

fn create_spec(tenant_id: u64, name: &str) -> ArrangementSpec {
    ArrangementSpec {
        tenant_id: TenantId(tenant_id),
        security_policy_digest: [0u8; 32],
        source_identity: SourceIdentity::new(name),
        source_schema_generation: 1,
        key_expressions: vec![CanonicalExpr::col("k")],
        key_types: vec![CanonicalType::Int64],
        value_projection: vec![CanonicalExpr::col("v")],
        predicate: None,
        null_semantics: NullSemantics::NullsFirst,
        decimal_scale: None,
        collation_identifier: CollationId::utf8_default(),
        collation_version: CollationVersion(1),
        time_domain: TimeDomainSemantics::Utc,
        merge_law_id: MergeLawId(1),
        merge_law_version: MergeLawVersion(1),
        partitioning: PartitioningSpec::SingleShard(0),
    }
}

#[tokio::test]
async fn test_arrangement_catalog_registration_and_sharing() {
    let catalog = ArrangementCatalog::new();
    let spec1 = create_spec(1, "orders");
    let spec2 = create_spec(1, "orders"); // Identical spec

    let (arr_id1, is_new1) = catalog.register_consumer(ViewId(101), spec1).await;
    assert!(is_new1);
    assert_eq!(catalog.consumer_count(arr_id1).await, 1);
    assert_eq!(catalog.physical_arrangements_count().await, 1);

    let (arr_id2, is_new2) = catalog.register_consumer(ViewId(102), spec2).await;
    assert!(!is_new2); // Reused!
    assert_eq!(arr_id1, arr_id2);
    assert_eq!(catalog.consumer_count(arr_id1).await, 2);
    assert_eq!(catalog.physical_arrangements_count().await, 1);

    let entry = catalog
        .lookup(arr_id1)
        .await
        .expect("Arrangement must exist");
    assert_eq!(entry.consumer_count, 2);
    assert!(entry.consumers.contains(&ViewId(101)));
    assert!(entry.consumers.contains(&ViewId(102)));
}

#[tokio::test]
async fn test_arrangement_catalog_tenant_isolation() {
    let catalog = ArrangementCatalog::new();
    let spec_tenant1 = create_spec(1, "orders");
    let spec_tenant2 = create_spec(2, "orders");

    let (arr_id1, _) = catalog.register_consumer(ViewId(1), spec_tenant1).await;
    let (arr_id2, _) = catalog.register_consumer(ViewId(2), spec_tenant2).await;

    assert_ne!(arr_id1, arr_id2);
    assert_eq!(catalog.physical_arrangements_count().await, 2);

    let tenant1_list = catalog.list_for_tenant(TenantId(1)).await;
    assert_eq!(tenant1_list.len(), 1);
    assert_eq!(tenant1_list[0].id, arr_id1);

    let tenant2_list = catalog.list_for_tenant(TenantId(2)).await;
    assert_eq!(tenant2_list.len(), 1);
    assert_eq!(tenant2_list[0].id, arr_id2);
}
