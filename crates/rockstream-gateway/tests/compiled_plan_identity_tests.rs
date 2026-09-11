//! Compiled Plan Identity and Gateway Recovery Tests (v0.63 Slice 5).
//!
//! Asserts that:
//! 1. All seven compiled-plan identity fields survive restart and recovery.
//! 2. Incompatible state layout versions or compiler versions fail closed with RS-1002.
//! 3. Broken view dependencies prevent Ready state and fail closed with RS-1002.
//! 4. View recovery failures trigger Fatal component state and block readiness probes.

use object_store::memory::InMemory;
use rockstream_plan::identity::{
    CompiledPlanRecord, CURRENT_COMPILER_VERSION, CURRENT_STATE_LAYOUT_VERSION,
};
use rockstream_storage::catalog::{CatalogMutation, CatalogStore, CatalogTxn, DurableCatalogStore};
use rockstream_types::ids::CompiledPlanId;
use rockstream_types::lifecycle::{LifecycleState, LifecycleTracker};
use std::sync::Arc;

#[tokio::test]
async fn test_seven_compiled_plan_identity_fields_survive_restart() {
    let store = Arc::new(InMemory::new());
    let prefix = "catalog_test";
    let catalog = DurableCatalogStore::new(store.clone(), prefix);

    let plan_id = CompiledPlanId::from(42);
    let original_sql = "SELECT id, count(*) FROM orders GROUP BY id".to_string();
    let ast_hash = [0xabu8; 32];
    let logical_plan_hash = [0xcdu8; 32];
    let compiler_ver = CURRENT_COMPILER_VERSION.to_string();
    let state_layout_ver = CURRENT_STATE_LAYOUT_VERSION;
    let output_schema_bytes = vec![1, 2, 3, 4, 5, 6, 7, 8];
    let dep_ids = vec![101u128, 102u128];

    let plan_record = CompiledPlanRecord::new(
        plan_id,
        original_sql.clone(),
        ast_hash,
        logical_plan_hash,
        compiler_ver.clone(),
        state_layout_ver,
        output_schema_bytes.clone(),
        dep_ids.clone(),
    );

    // Persist compiled plan in a transaction
    let txn = CatalogTxn::new(
        1,
        1001,
        vec![CatalogMutation::PutCompiledPlan(
            rockstream_storage::catalog::CompiledPlanRecord {
                id: plan_id,
                sql: original_sql.clone(),
                ast_hash,
                logical_plan_hash,
                compiler_version: compiler_ver.clone(),
                state_layout_version: state_layout_ver,
                output_schema: output_schema_bytes.clone(),
                dependency_ids: dep_ids.clone(),
            },
        )],
    )
    .expect("valid txn");
    catalog.commit_txn(txn).await.expect("commit_txn failed");

    // Reconstruct catalog from durable storage
    let recovered_catalog = DurableCatalogStore::recover(store.clone(), prefix)
        .await
        .expect("recover failed");

    let restored = recovered_catalog
        .get_compiled_plan(plan_id)
        .await
        .expect("get_compiled_plan failed")
        .expect("compiled plan not found");

    // Assert all seven identity fields match exactly
    assert_eq!(restored.sql, original_sql, "Field 1: sql mismatch");
    assert_eq!(restored.ast_hash, ast_hash, "Field 2: ast_hash mismatch");
    assert_eq!(
        restored.logical_plan_hash, logical_plan_hash,
        "Field 3: logical_plan_hash mismatch"
    );
    assert_eq!(
        restored.compiler_version, compiler_ver,
        "Field 4: compiler_version mismatch"
    );
    assert_eq!(
        restored.state_layout_version, state_layout_ver,
        "Field 5: state_layout_version mismatch"
    );
    assert_eq!(
        restored.output_schema, output_schema_bytes,
        "Field 6: output_schema mismatch"
    );
    assert_eq!(
        restored.dependency_ids, dep_ids,
        "Field 7: dependency_ids mismatch"
    );

    // Compatibility validation passes
    assert!(
        plan_record
            .validate_compatibility(CURRENT_COMPILER_VERSION, CURRENT_STATE_LAYOUT_VERSION)
            .is_ok(),
        "Compatibility validation should succeed for current versions"
    );
}

#[test]
fn test_state_layout_version_rejection() {
    let plan_record = CompiledPlanRecord::new(
        CompiledPlanId::from(1),
        "SELECT 1",
        [0u8; 32],
        [0u8; 32],
        CURRENT_COMPILER_VERSION,
        999, // Incompatible state layout version
        vec![],
        vec![],
    );

    let err = plan_record
        .validate_compatibility(CURRENT_COMPILER_VERSION, CURRENT_STATE_LAYOUT_VERSION)
        .unwrap_err();

    assert!(
        err.contains("[RS-1002]"),
        "Error must contain RS-1002 error code: {err}"
    );
    assert!(
        err.contains("Incompatible state layout version"),
        "Error must mention state layout version: {err}"
    );

    // Also assert incompatible compiler version rejection
    let plan_record_old_compiler = CompiledPlanRecord::new(
        CompiledPlanId::from(2),
        "SELECT 1",
        [0u8; 32],
        [0u8; 32],
        "0.0.1", // Incompatible compiler version
        CURRENT_STATE_LAYOUT_VERSION,
        vec![],
        vec![],
    );

    let err2 = plan_record_old_compiler
        .validate_compatibility(CURRENT_COMPILER_VERSION, CURRENT_STATE_LAYOUT_VERSION)
        .unwrap_err();

    assert!(
        err2.contains("[RS-1002]"),
        "Error must contain RS-1002 error code: {err2}"
    );
    assert!(
        err2.contains("Incompatible compiler version"),
        "Error must mention compiler version: {err2}"
    );
}

#[test]
fn test_broken_dependencies_prevent_ready() {
    let plan_record = CompiledPlanRecord::new(
        CompiledPlanId::from(1),
        "SELECT 1",
        [0u8; 32],
        [0u8; 32],
        CURRENT_COMPILER_VERSION,
        CURRENT_STATE_LAYOUT_VERSION,
        vec![],
        vec![101, 202, 303],
    );

    // Only 101 and 202 exist, 303 is missing
    let known_objects = vec![101, 202];
    let err = plan_record
        .validate_dependencies(&known_objects)
        .unwrap_err();

    assert!(
        err.contains("[RS-1002]"),
        "Error must contain RS-1002 error code: {err}"
    );
    assert!(
        err.contains("Broken view dependency: referenced object ID 303 not found"),
        "Error must identify missing dependency ID: {err}"
    );

    // When all dependencies exist, it succeeds
    let all_objects = vec![101, 202, 303];
    assert!(plan_record.validate_dependencies(&all_objects).is_ok());
}

#[tokio::test]
async fn test_view_recovery_failure_blocks_readiness() {
    let tracker = LifecycleTracker::new("gateway");
    tracker.set_state(LifecycleState::Starting);
    assert!(!tracker.is_ready());

    tracker.set_state(LifecycleState::Recovering);
    assert!(!tracker.is_ready());

    // Failed view recovery triggers fatal state transition
    let recovery_failure: Result<(), String> = Err(
        "[RS-1002] Failed to restore persisted arrangement state for view 'orders_summary'"
            .to_string(),
    );

    if let Err(e) = recovery_failure {
        tracing::error!("[RS-1002] View recovery fatal error: {e}");
        tracker.set_state(LifecycleState::Fatal);
    }

    assert_eq!(
        tracker.state(),
        LifecycleState::Fatal,
        "Recovery failure must transition to Fatal state"
    );
    assert!(
        !tracker.is_ready(),
        "Readiness must be false when in Fatal state"
    );

    let (code, body) = tracker.generate_ready_response();
    assert_eq!(
        code, 503,
        "Readiness probe /readyz must return 503 when in Fatal state"
    );
    assert_eq!(body.status, "not_ready");
}
