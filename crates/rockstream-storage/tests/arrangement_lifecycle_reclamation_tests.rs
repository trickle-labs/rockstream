//! Arrangement Lifecycle & Reclamation Tests (v0.59.6).
//!
//! Asserts that dropping 1 of multiple consumers keeps the physical arrangement active,
//! and dropping the last active consumer triggers deferred reclamation once safe horizon clears.

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
async fn test_refcount_decrement_on_view_drop() {
    let catalog = ArrangementCatalog::new();
    let spec = create_spec(1, "events");

    let (arr_id, _) = catalog.register_consumer(ViewId(1), spec.clone()).await;
    let _ = catalog.register_consumer(ViewId(2), spec.clone()).await;
    let _ = catalog.register_consumer(ViewId(3), spec).await;

    assert_eq!(catalog.consumer_count(arr_id).await, 3);

    // Drop View 1
    let marked1 = catalog
        .deregister_consumer(ViewId(1), arr_id)
        .await
        .expect("deregister");
    assert!(!marked1);
    assert_eq!(catalog.consumer_count(arr_id).await, 2);

    // Drop View 2
    let marked2 = catalog
        .deregister_consumer(ViewId(2), arr_id)
        .await
        .expect("deregister");
    assert!(!marked2);
    assert_eq!(catalog.consumer_count(arr_id).await, 1);

    // Arrangement is still active and NOT reclaimed
    let reclaimed = catalog.reclaim_unreferenced_arrangements(100).await;
    assert!(reclaimed.is_empty());
    assert_eq!(catalog.physical_arrangements_count().await, 1);
}

#[tokio::test]
async fn test_last_consumer_drop_triggers_reclamation() {
    let catalog = ArrangementCatalog::new();
    let spec = create_spec(1, "events");

    let (arr_id, _) = catalog.register_consumer(ViewId(1), spec).await;
    catalog.update_compaction_frontier(arr_id, 10).await;

    // Drop last consumer
    let marked = catalog
        .deregister_consumer(ViewId(1), arr_id)
        .await
        .expect("deregister");
    assert!(marked);
    assert_eq!(catalog.consumer_count(arr_id).await, 0);

    // If safe horizon is 20, compaction_frontier 10 is < 20, not reclaimed yet
    let reclaimed_early = catalog.reclaim_unreferenced_arrangements(20).await;
    assert!(reclaimed_early.is_empty());

    // Advance compaction frontier to 25 >= 20
    catalog.update_compaction_frontier(arr_id, 25).await;
    let reclaimed = catalog.reclaim_unreferenced_arrangements(20).await;
    assert_eq!(reclaimed, vec![arr_id]);
    assert_eq!(catalog.physical_arrangements_count().await, 0);
}
