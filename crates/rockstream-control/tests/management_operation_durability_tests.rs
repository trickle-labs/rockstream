use std::sync::Arc;

use object_store::local::LocalFileSystem;
use rockstream_control::{
    ManagementOperationStore, OperationKind, OperationLifecycleError, OperationRecord,
    OperationStatus, OperationStoreError, OperationUpdate,
};
use rockstream_test_support::minio::{minio_object_store, start_minio};
use serde_json::{json, Value};

const MINIO_BUCKET: &str = "rockstream-management-operation-test";

fn update(
    status: OperationStatus,
    updated_at_ms: i64,
    progress: Option<u8>,
    phase: Option<&str>,
    error_code: Option<&str>,
    next_steps: &[&str],
) -> OperationUpdate {
    OperationUpdate {
        status,
        updated_at_ms,
        progress,
        phase: phase.map(str::to_owned),
        error_code: error_code.map(str::to_owned),
        next_steps: next_steps.iter().map(|step| (*step).to_owned()).collect(),
    }
}

fn exact_record(record: &OperationRecord) -> Value {
    serde_json::to_value(record).unwrap()
}

#[test]
fn management_operation_lifecycle_has_exact_state_records() {
    let mut succeeded = OperationRecord::accepted("op_01ABC", OperationKind::MigrateShard, 1_000);
    assert_eq!(
        exact_record(&succeeded),
        json!({
            "record_version": 1,
            "operation_id": "op_01ABC",
            "kind": "migrate_shard",
            "status": "pending",
            "started_at": 1000,
            "updated_at": 1000,
            "progress": null,
            "phase": null,
            "error_code": null,
            "next_steps": []
        })
    );
    succeeded
        .apply_update(update(
            OperationStatus::Running,
            2_000,
            Some(25),
            Some("copying"),
            None,
            &[],
        ))
        .unwrap();
    succeeded
        .apply_update(update(
            OperationStatus::Running,
            2_500,
            Some(40),
            Some("copying"),
            None,
            &[],
        ))
        .unwrap();
    succeeded
        .apply_update(update(
            OperationStatus::Waiting,
            3_000,
            Some(40),
            Some("waiting_for_worker"),
            None,
            &["restore worker worker-2"],
        ))
        .unwrap();
    assert_eq!(
        exact_record(&succeeded),
        json!({
            "record_version": 1,
            "operation_id": "op_01ABC",
            "kind": "migrate_shard",
            "status": "waiting",
            "started_at": 1000,
            "updated_at": 3000,
            "progress": 40,
            "phase": "waiting_for_worker",
            "error_code": null,
            "next_steps": ["restore worker worker-2"]
        })
    );
    succeeded
        .apply_update(update(
            OperationStatus::Running,
            4_000,
            Some(75),
            Some("transferring_lease"),
            None,
            &[],
        ))
        .unwrap();
    succeeded
        .apply_update(update(
            OperationStatus::Succeeded,
            5_000,
            Some(100),
            Some("completed"),
            None,
            &[],
        ))
        .unwrap();
    assert_eq!(
        exact_record(&succeeded),
        json!({
            "record_version": 1,
            "operation_id": "op_01ABC",
            "kind": "migrate_shard",
            "status": "succeeded",
            "started_at": 1000,
            "updated_at": 5000,
            "progress": 100,
            "phase": "completed",
            "error_code": null,
            "next_steps": []
        })
    );
    let succeeded_record = exact_record(&succeeded);

    let mut failed = OperationRecord::accepted("op_failed", OperationKind::DrainWorker, 1_000);
    failed
        .apply_update(update(
            OperationStatus::Failed,
            2_000,
            None,
            Some("validation"),
            Some("invalid_destination"),
            &["choose a registered worker"],
        ))
        .unwrap();
    assert_eq!(
        exact_record(&failed),
        json!({
            "record_version": 1,
            "operation_id": "op_failed",
            "kind": "drain_worker",
            "status": "failed",
            "started_at": 1000,
            "updated_at": 2000,
            "progress": null,
            "phase": "validation",
            "error_code": "invalid_destination",
            "next_steps": ["choose a registered worker"]
        })
    );

    let mut cancelled =
        OperationRecord::accepted("op_cancelled", OperationKind::CreateBackup, 1_000);
    cancelled
        .apply_update(update(
            OperationStatus::Running,
            2_000,
            Some(10),
            Some("writing"),
            None,
            &[],
        ))
        .unwrap();
    cancelled
        .apply_update(update(
            OperationStatus::Cancelled,
            3_000,
            Some(10),
            Some("cancelled"),
            None,
            &["backup remains uncommitted"],
        ))
        .unwrap();
    assert_eq!(
        exact_record(&cancelled),
        json!({
            "record_version": 1,
            "operation_id": "op_cancelled",
            "kind": "create_backup",
            "status": "cancelled",
            "started_at": 1000,
            "updated_at": 3000,
            "progress": 10,
            "phase": "cancelled",
            "error_code": null,
            "next_steps": ["backup remains uncommitted"]
        })
    );

    let pending = OperationRecord::accepted("op_illegal", OperationKind::MigrateShard, 1_000);
    let mut unchanged = pending.clone();
    assert_eq!(
        unchanged.apply_update(update(
            OperationStatus::Succeeded,
            2_000,
            Some(100),
            Some("completed"),
            None,
            &[],
        )),
        Err(OperationLifecycleError::IllegalTransition {
            from: OperationStatus::Pending,
            to: OperationStatus::Succeeded,
        })
    );
    assert_eq!(exact_record(&unchanged), exact_record(&pending));
    assert_eq!(
        unchanged.apply_update(update(
            OperationStatus::Pending,
            1_500,
            None,
            None,
            None,
            &[],
        )),
        Err(OperationLifecycleError::IllegalTransition {
            from: OperationStatus::Pending,
            to: OperationStatus::Pending,
        })
    );
    assert_eq!(exact_record(&unchanged), exact_record(&pending));
    assert_eq!(
        succeeded.apply_update(update(
            OperationStatus::Running,
            6_000,
            Some(100),
            Some("restarted"),
            None,
            &[],
        )),
        Err(OperationLifecycleError::TerminalStateIsImmutable(
            OperationStatus::Succeeded
        ))
    );
    assert_eq!(exact_record(&succeeded), succeeded_record);
}

#[test]
fn management_operation_lifecycle_matches_exact_transition_table() {
    let statuses = [
        OperationStatus::Pending,
        OperationStatus::Running,
        OperationStatus::Waiting,
        OperationStatus::Succeeded,
        OperationStatus::Failed,
        OperationStatus::Cancelled,
    ];
    let allowed = [
        (OperationStatus::Pending, OperationStatus::Running),
        (OperationStatus::Pending, OperationStatus::Waiting),
        (OperationStatus::Pending, OperationStatus::Failed),
        (OperationStatus::Pending, OperationStatus::Cancelled),
        (OperationStatus::Running, OperationStatus::Running),
        (OperationStatus::Running, OperationStatus::Waiting),
        (OperationStatus::Running, OperationStatus::Succeeded),
        (OperationStatus::Running, OperationStatus::Failed),
        (OperationStatus::Running, OperationStatus::Cancelled),
        (OperationStatus::Waiting, OperationStatus::Running),
        (OperationStatus::Waiting, OperationStatus::Waiting),
        (OperationStatus::Waiting, OperationStatus::Failed),
        (OperationStatus::Waiting, OperationStatus::Cancelled),
    ];

    for from in statuses {
        for to in statuses {
            let mut record = OperationRecord::accepted(
                "op_transition_table",
                OperationKind::MigrateShard,
                1_000,
            );
            match from {
                OperationStatus::Pending => {}
                OperationStatus::Succeeded => {
                    record
                        .apply_update(update(
                            OperationStatus::Running,
                            2_000,
                            Some(25),
                            Some("preparing"),
                            None,
                            &[],
                        ))
                        .unwrap();
                    record
                        .apply_update(update(
                            OperationStatus::Succeeded,
                            3_000,
                            Some(100),
                            Some("completed"),
                            None,
                            &[],
                        ))
                        .unwrap();
                }
                _ => record
                    .apply_update(update(from, 2_000, Some(25), Some("preparing"), None, &[]))
                    .unwrap(),
            }

            let before = exact_record(&record);
            let result = record.apply_update(update(
                to,
                10_000,
                Some(50),
                Some("matrix_transition"),
                None,
                &["inspect operation"],
            ));
            if allowed.contains(&(from, to)) {
                assert_eq!(result, Ok(()), "{from:?} -> {to:?}");
                let mut expected = before;
                expected["status"] = json!(to);
                expected["updated_at"] = json!(10_000);
                expected["progress"] = json!(50);
                expected["phase"] = json!("matrix_transition");
                expected["next_steps"] = json!(["inspect operation"]);
                assert_eq!(exact_record(&record), expected, "{from:?} -> {to:?}");
            } else {
                let expected_error = if from.is_terminal() {
                    OperationLifecycleError::TerminalStateIsImmutable(from)
                } else {
                    OperationLifecycleError::IllegalTransition { from, to }
                };
                assert_eq!(result, Err(expected_error), "{from:?} -> {to:?}");
                assert_eq!(exact_record(&record), before, "{from:?} -> {to:?}");
            }
        }
    }
}

async fn verify_operation_survives_store_reopen(
    make_store: impl Fn() -> ManagementOperationStore,
    operation_id: &str,
) {
    let store = make_store();
    let accepted = store
        .accept(operation_id, OperationKind::MigrateShard, 1_000)
        .await
        .unwrap();
    assert_eq!(
        exact_record(&accepted),
        json!({
            "record_version": 1,
            "operation_id": operation_id,
            "kind": "migrate_shard",
            "status": "pending",
            "started_at": 1000,
            "updated_at": 1000,
            "progress": null,
            "phase": null,
            "error_code": null,
            "next_steps": []
        })
    );
    store
        .transition(
            operation_id,
            update(
                OperationStatus::Running,
                2_000,
                Some(50),
                Some("copying"),
                None,
                &[],
            ),
        )
        .await
        .unwrap();
    drop(store);

    let reopened = make_store();
    let recovered = reopened.get(operation_id).await.unwrap().unwrap();
    assert_eq!(
        exact_record(&recovered),
        json!({
            "record_version": 1,
            "operation_id": operation_id,
            "kind": "migrate_shard",
            "status": "running",
            "started_at": 1000,
            "updated_at": 2000,
            "progress": 50,
            "phase": "copying",
            "error_code": null,
            "next_steps": []
        })
    );
    reopened
        .transition(
            operation_id,
            update(
                OperationStatus::Succeeded,
                3_000,
                Some(100),
                Some("completed"),
                None,
                &[],
            ),
        )
        .await
        .unwrap();
    let completed = make_store().get(operation_id).await.unwrap().unwrap();
    assert_eq!(
        exact_record(&completed),
        json!({
            "record_version": 1,
            "operation_id": operation_id,
            "kind": "migrate_shard",
            "status": "succeeded",
            "started_at": 1000,
            "updated_at": 3000,
            "progress": 100,
            "phase": "completed",
            "error_code": null,
            "next_steps": []
        })
    );
}

#[tokio::test]
async fn accepted_operation_survives_lfs_store_reopen() {
    let dir = tempfile::tempdir().unwrap();
    verify_operation_survives_store_reopen(
        || {
            ManagementOperationStore::new(Arc::new(
                LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
            ))
        },
        "op_lfs_restart",
    )
    .await;
}

#[tokio::test]
async fn accepted_operation_survives_minio_store_reopen() {
    let (_container, port) = start_minio(MINIO_BUCKET)
        .await
        .expect("MinIO operation-store durability test requires Docker");
    verify_operation_survives_store_reopen(
        || ManagementOperationStore::new(minio_object_store(port, MINIO_BUCKET)),
        "op_minio_restart",
    )
    .await;
}

#[tokio::test]
async fn operation_acceptance_never_overwrites_existing_record() {
    let dir = tempfile::tempdir().unwrap();
    let store = ManagementOperationStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    store
        .accept("op_unique", OperationKind::DrainWorker, 1_000)
        .await
        .unwrap();
    assert!(matches!(
        store
            .accept("op_unique", OperationKind::CreateBackup, 2_000)
            .await,
        Err(OperationStoreError::AlreadyExists(id)) if id == "op_unique"
    ));
    let retained = store.get("op_unique").await.unwrap().unwrap();
    assert_eq!(
        exact_record(&retained),
        json!({
            "record_version": 1,
            "operation_id": "op_unique",
            "kind": "drain_worker",
            "status": "pending",
            "started_at": 1000,
            "updated_at": 1000,
            "progress": null,
            "phase": null,
            "error_code": null,
            "next_steps": []
        })
    );
}

#[tokio::test]
async fn management_operation_list_pages_exact_records_and_rejects_invalid_tokens() {
    let dir = tempfile::tempdir().unwrap();
    let store = ManagementOperationStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    store
        .accept("op_page_a", OperationKind::DrainWorker, 1_000)
        .await
        .unwrap();
    store
        .accept("op_page_b", OperationKind::MigrateShard, 2_000)
        .await
        .unwrap();

    let (first, next) = store.list(1, "").await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(
        exact_record(&first[0]),
        json!({
            "record_version": 1,
            "operation_id": "op_page_a",
            "kind": "drain_worker",
            "status": "pending",
            "started_at": 1000,
            "updated_at": 1000,
            "progress": null,
            "phase": null,
            "error_code": null,
            "next_steps": []
        })
    );
    assert_eq!(next, "1");

    let (second, next) = store.list(1, &next).await.unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(
        exact_record(&second[0]),
        json!({
            "record_version": 1,
            "operation_id": "op_page_b",
            "kind": "migrate_shard",
            "status": "pending",
            "started_at": 2000,
            "updated_at": 2000,
            "progress": null,
            "phase": null,
            "error_code": null,
            "next_steps": []
        })
    );
    assert!(next.is_empty());
    assert_eq!(store.counts().await.unwrap(), (2, 2));
    assert!(matches!(
        store.list(1, "not-an-offset").await,
        Err(OperationStoreError::InvalidPageToken)
    ));
    assert!(matches!(
        store.list(101, "").await,
        Err(OperationStoreError::InvalidPageSize)
    ));
}

#[tokio::test]
async fn management_idempotency_retries_return_one_exact_record() {
    let dir = tempfile::tempdir().unwrap();
    let store = ManagementOperationStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    let request = json!({"shard": "orders-3", "destination": "worker-2"});
    let reordered_request = json!({"destination": "worker-2", "shard": "orders-3"});
    let (accepted, retry) = tokio::join!(
        store.accept_idempotent(
            "request-42",
            1,
            &request,
            "op_first",
            OperationKind::MigrateShard,
            1_000,
        ),
        store.accept_idempotent(
            "request-42",
            1,
            &reordered_request,
            "op_first",
            OperationKind::MigrateShard,
            2_000,
        ),
    );
    let accepted = accepted.unwrap();
    let retry = retry.unwrap();
    assert_eq!(retry, accepted);
    assert_eq!(
        exact_record(&retry),
        json!({
            "record_version": 1,
            "operation_id": "op_first",
            "kind": "migrate_shard",
            "status": "pending",
            "started_at": 1000,
            "updated_at": 1000,
            "progress": null,
            "phase": null,
            "error_code": null,
            "next_steps": []
        })
    );
    assert!(matches!(
        store
            .accept_idempotent(
                "request-42",
                1,
                &json!({"destination": "worker-3", "shard": "orders-3"}),
                "op_conflict",
                OperationKind::MigrateShard,
                2_000,
            )
            .await,
        Err(OperationStoreError::IdempotencyConflict { operation_id }) if operation_id == "op_first"
    ));
}

#[tokio::test]
async fn accepted_operation_and_idempotency_survive_lfs_process_restart() {
    let dir = tempfile::tempdir().unwrap();
    let make_store = || {
        ManagementOperationStore::new(Arc::new(
            LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
        ))
    };
    let accepted = make_store()
        .accept_idempotent(
            "backup-2026-09-12",
            1,
            &json!({"backup": "daily-12", "catalog_revision": 8}),
            "op_backup",
            OperationKind::CreateBackup,
            1_000,
        )
        .await
        .unwrap();
    let reopened = make_store();
    let retry = reopened
        .accept_idempotent(
            "backup-2026-09-12",
            1,
            &json!({"catalog_revision": 8, "backup": "daily-12"}),
            "op_different",
            OperationKind::CreateBackup,
            2_000,
        )
        .await
        .unwrap();
    assert_eq!(retry, accepted);
    assert!(matches!(
        reopened
            .accept_idempotent(
                "backup-2026-09-12",
                1,
                &json!({"backup": "daily-13", "catalog_revision": 8}),
                "op_changed",
                OperationKind::CreateBackup,
                2_000,
            )
            .await,
        Err(OperationStoreError::IdempotencyConflict { operation_id }) if operation_id == "op_backup"
    ));
    assert!(matches!(
        reopened
            .accept_idempotent(
                "backup-2026-09-12",
                1,
                &json!({"backup": "daily-12", "catalog_revision": 8}),
                "op_expired",
                OperationKind::CreateBackup,
                1_000 + 24 * 60 * 60 * 1_000 + 1,
            )
            .await,
        Err(OperationStoreError::IdempotencyExpired { operation_id }) if operation_id == "op_backup"
    ));
    assert_eq!(
        exact_record(&reopened.get("op_backup").await.unwrap().unwrap()),
        json!({
            "record_version": 1,
            "operation_id": "op_backup",
            "kind": "create_backup",
            "status": "pending",
            "started_at": 1000,
            "updated_at": 1000,
            "progress": null,
            "phase": null,
            "error_code": null,
            "next_steps": []
        })
    );
}

#[tokio::test]
async fn accepted_operation_and_idempotency_survive_minio_tc_process_restart() {
    let (_container, port) = start_minio(MINIO_BUCKET)
        .await
        .expect("MinIO idempotency durability test requires Docker");
    let request = json!({"backup": "daily-12", "catalog_revision": 8});
    let accepted = ManagementOperationStore::new(minio_object_store(port, MINIO_BUCKET))
        .accept_idempotent(
            "backup-2026-09-12",
            1,
            &request,
            "op_minio_backup",
            OperationKind::CreateBackup,
            1_000,
        )
        .await
        .unwrap();
    drop(accepted);

    let reopened = ManagementOperationStore::new(minio_object_store(port, MINIO_BUCKET));
    let retry = reopened
        .accept_idempotent(
            "backup-2026-09-12",
            1,
            &json!({"catalog_revision": 8, "backup": "daily-12"}),
            "op_minio_retry",
            OperationKind::CreateBackup,
            2_000,
        )
        .await
        .unwrap();
    assert_eq!(
        exact_record(&retry),
        json!({
            "record_version": 1,
            "operation_id": "op_minio_backup",
            "kind": "create_backup",
            "status": "pending",
            "started_at": 1000,
            "updated_at": 1000,
            "progress": null,
            "phase": null,
            "error_code": null,
            "next_steps": []
        })
    );
}

#[tokio::test]
async fn operation_store_rejects_terminal_transition() {
    let dir = tempfile::tempdir().unwrap();
    let store = ManagementOperationStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    store
        .accept("op_terminal", OperationKind::MigrateShard, 1_000)
        .await
        .unwrap();
    store
        .transition(
            "op_terminal",
            update(
                OperationStatus::Running,
                2_000,
                Some(25),
                Some("copying"),
                None,
                &[],
            ),
        )
        .await
        .unwrap();
    store
        .transition(
            "op_terminal",
            update(
                OperationStatus::Succeeded,
                3_000,
                Some(100),
                Some("completed"),
                None,
                &[],
            ),
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .transition(
                "op_terminal",
                update(
                    OperationStatus::Failed,
                    4_000,
                    Some(50),
                    Some("stale_failure"),
                    Some("worker_lost"),
                    &[],
                ),
            )
            .await
            .unwrap_err(),
        OperationStoreError::Lifecycle(OperationLifecycleError::TerminalStateIsImmutable(
            OperationStatus::Succeeded
        ))
    ));
    let retained = store.get("op_terminal").await.unwrap().unwrap();
    assert_eq!(
        exact_record(&retained),
        json!({
            "record_version": 1,
            "operation_id": "op_terminal",
            "kind": "migrate_shard",
            "status": "succeeded",
            "started_at": 1000,
            "updated_at": 3000,
            "progress": 100,
            "phase": "completed",
            "error_code": null,
            "next_steps": []
        })
    );
}

#[tokio::test]
async fn storage_write_failure_never_acknowledges_management_acceptance() {
    let dir = tempfile::tempdir().unwrap();
    let obstruction = dir.path().join("control");
    std::fs::write(&obstruction, b"not a directory").unwrap();
    let store = ManagementOperationStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));

    assert!(matches!(
        store
            .accept("op_storage_failure", OperationKind::DrainWorker, 1_000)
            .await,
        Err(OperationStoreError::Storage(_))
    ));
    assert_eq!(std::fs::read(&obstruction).unwrap(), b"not a directory");
    assert!(!dir.path().join("control/management-operations").exists());
}
