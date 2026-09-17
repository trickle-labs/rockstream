//! v0.67 Slice 7 tests: Public SQL Shared Execution & Compatible View Arrangements.
//!
//! Verifies:
//! 1. 20 compatible views share a single source arrangement (Exit criterion V067-09).
//! 2. Shared arrangement consumer drop reference count lifecycle (dropping 19 of 20 leaves arrangement intact).
//! 3. Arrangement restores correctly post restart with correct consumer count and frontier meet.

use std::collections::HashMap;

use rockstream_plan::dag::find_compatible_view_groups;
use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, PlanNode};
use rockstream_storage::arrangement_catalog::ArrangementCatalog;
use rockstream_types::arrangement::{
    ArrangementSpec, CanonicalExpr, CanonicalType, CollationId, CollationVersion, NullSemantics,
    PartitioningSpec, SourceIdentity, TimeDomainSemantics,
};
use rockstream_types::ids::{TenantId, ViewId};
use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};

fn create_test_spec(tenant_id: u64, name: &str) -> ArrangementSpec {
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

/// Test 1: 20 compatible views on the same source share a single source arrangement (V067-09).
#[tokio::test]
async fn test_public_sql_twenty_views_share_source_arrangement() {
    let mut view_plans = HashMap::new();

    // Create 20 view plans querying the same "orders" source
    for i in 1..=20 {
        let view_name = format!("view_orders_{i}");
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "orders".to_string(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        };
        view_plans.insert(view_name, plan);
    }

    // DAG planner identifies all 20 views as a compatible view group
    let groups = find_compatible_view_groups(&view_plans);
    assert_eq!(
        groups.len(),
        1,
        "all 20 views must belong to 1 shared arrangement group"
    );
    assert_eq!(groups[0].source_name, "orders");
    assert_eq!(groups[0].view_names.len(), 20);

    // Register into ArrangementCatalog: 1 physical arrangement shared across 20 views
    let catalog = ArrangementCatalog::new();
    let spec = create_test_spec(1, "orders");

    let mut arrangement_id = None;
    for i in 1..=20 {
        let (arr_id, is_new) = catalog.register_consumer(ViewId(i), spec.clone()).await;
        if i == 1 {
            assert!(is_new, "first view creates physical arrangement");
            arrangement_id = Some(arr_id);
        } else {
            assert!(
                !is_new,
                "subsequent views reuse existing physical arrangement"
            );
            assert_eq!(Some(arr_id), arrangement_id);
        }
    }

    let shared_id = arrangement_id.unwrap();
    assert_eq!(
        catalog.physical_arrangements_count().await,
        1,
        "exactly 1 physical arrangement"
    );
    assert_eq!(
        catalog.consumer_count(shared_id).await,
        20,
        "consumer count is 20"
    );
}

/// Test 2: Consumer drop semantics (drop 19 of 20 views, arrangement retained with refcount 1).
#[tokio::test]
async fn test_shared_arrangement_consumer_drop_ref_count_lifecycle() {
    let catalog = ArrangementCatalog::new();
    let spec = create_test_spec(1, "orders");

    // Register 20 consumers
    let mut arr_id = None;
    for i in 1..=20 {
        let (id, _) = catalog.register_consumer(ViewId(i), spec.clone()).await;
        arr_id = Some(id);
    }
    let shared_id = arr_id.unwrap();
    assert_eq!(catalog.consumer_count(shared_id).await, 20);

    // Drop 19 consumers (views 1 through 19)
    for i in 1..=19 {
        let is_last = catalog
            .deregister_consumer(ViewId(i), shared_id)
            .await
            .unwrap();
        assert!(
            !is_last,
            "dropping consumer {i} must not mark arrangement as last"
        );
        assert_eq!(catalog.consumer_count(shared_id).await, 20 - (i as usize));
    }

    // Reference count is now 1: arrangement is NOT marked for reclamation, retained for remaining view 20
    assert_eq!(catalog.consumer_count(shared_id).await, 1);
    let entry = catalog.lookup(shared_id).await.unwrap();
    assert!(!entry.marked_for_reclamation);
    assert_eq!(entry.consumers.len(), 1);
    assert!(entry.consumers.contains(&ViewId(20)));

    // Now drop the last consumer (view 20)
    let is_last = catalog
        .deregister_consumer(ViewId(20), shared_id)
        .await
        .unwrap();
    assert!(
        is_last,
        "dropping view 20 marks arrangement as last for reclamation"
    );
    assert_eq!(catalog.consumer_count(shared_id).await, 0);

    let entry = catalog.lookup(shared_id).await.unwrap();
    assert!(entry.marked_for_reclamation);
}

/// Test 3: Shared arrangement restores correctly post restart with correct consumer count and frontier meet.
#[tokio::test]
async fn test_shared_arrangement_restores_correctly_post_restart() {
    let catalog = ArrangementCatalog::new();
    let spec = create_test_spec(1, "bids");

    // Phase 1: Register 5 consumers and advance compaction frontier to 500
    let mut arr_id = None;
    for i in 1..=5 {
        let (id, _) = catalog.register_consumer(ViewId(i), spec.clone()).await;
        arr_id = Some(id);
    }
    let shared_id = arr_id.unwrap();
    catalog.update_compaction_frontier(shared_id, 500).await;

    // Snapshot catalog to simulate durable storage checkpoint
    let snapshot = catalog.snapshot().await;
    assert_eq!(snapshot.len(), 1);

    // Phase 2: Worker restart: create fresh catalog and restore snapshot
    let restarted_catalog = ArrangementCatalog::new();
    assert_eq!(restarted_catalog.physical_arrangements_count().await, 0);

    restarted_catalog.restore(snapshot).await;

    // Verify post-restart state
    assert_eq!(restarted_catalog.physical_arrangements_count().await, 1);
    assert_eq!(restarted_catalog.consumer_count(shared_id).await, 5);

    let restored_entry = restarted_catalog.lookup(shared_id).await.unwrap();
    assert_eq!(restored_entry.compaction_frontier, 500);
    assert_eq!(restored_entry.consumers.len(), 5);
    for i in 1..=5 {
        assert!(restored_entry.consumers.contains(&ViewId(i)));
    }
}
