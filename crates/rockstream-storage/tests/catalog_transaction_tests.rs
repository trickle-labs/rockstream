use object_store::memory::InMemory;
use rockstream_storage::catalog::*;
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, LoginMethod, PermissionPreset, PolicyScope,
    ScopedPresetGrant, SecurityAuditClass, SecurityAuditOutcome, SecurityAuditRecord,
    SecurityPrincipal, SECURITY_AUDIT_FORMAT_VERSION, SECURITY_PRINCIPAL_FORMAT_VERSION,
};
use rockstream_types::ids::*;
use std::sync::Arc;

fn security_principal(
    id: u64,
    display_name: &str,
    preset: Option<PermissionPreset>,
) -> SecurityPrincipal {
    SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(id),
        display_name: display_name.to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Mtls {
            certificate_sha256: [id as u8; 32],
        }],
        preset_grants: preset
            .map(|preset| ScopedPresetGrant {
                preset,
                scope: PolicyScope::Cluster,
            })
            .into_iter()
            .collect(),
        scoped_grants: Vec::new(),
    }
}

fn security_grant_audit(
    event_id: &str,
    operation_id: u128,
    actor: AuditIdentity,
    before: Option<&SecurityPrincipal>,
    after: &SecurityPrincipal,
) -> SecurityAuditRecord {
    let state = |principal: &SecurityPrincipal| {
        format!(
            "revision={};policy={}",
            principal.revision,
            serde_json::to_string(&(
                principal.enabled,
                &principal.preset_grants,
                &principal.scoped_grants
            ))
            .unwrap()
        )
    };

    SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: event_id.to_owned(),
        timestamp_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the Unix epoch")
            .as_millis() as u64,
        actor,
        target: AuditField::Known(format!("principal:{}", after.id.0)),
        scope: AuditField::Known("cluster".to_owned()),
        action: SecurityAuditClass::GrantChange,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::NotApplicable,
        operation_id: AuditField::Known(operation_id.to_string()),
        policy_revision: AuditField::Known(after.revision),
        before_state: before.map_or(AuditField::NotApplicable, |principal| {
            AuditField::Known(state(principal))
        }),
        after_state: AuditField::Known(state(after)),
    }
}

fn put_security_principal(
    principal: SecurityPrincipal,
    expected_revision: Option<u64>,
    grantor_revision: Option<(PrincipalId, u64)>,
    audit: SecurityAuditRecord,
) -> CatalogMutation {
    CatalogMutation::PutSecurityPrincipal {
        principal,
        expected_revision,
        grantor_revision,
        audit,
    }
}

#[tokio::test]
async fn stale_grantor_revision_rejects_grant_change_without_audit_or_state_effects() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "security");
    let initial_admin = security_principal(
        1,
        "Initial administrator",
        Some(PermissionPreset::Administrator),
    );
    catalog
        .preprovision_first_administrator(initial_admin.clone(), 1)
        .await
        .unwrap();

    let second_admin = security_principal(
        2,
        "Second administrator",
        Some(PermissionPreset::Administrator),
    );
    let target = security_principal(3, "Target user", None);
    let second_admin_audit = security_grant_audit(
        "create-second-admin",
        2,
        AuditIdentity::Verified("principal:1".to_owned()),
        None,
        &second_admin,
    );
    let target_audit = security_grant_audit(
        "create-target-user",
        2,
        AuditIdentity::Verified("principal:1".to_owned()),
        None,
        &target,
    );
    catalog
        .commit_txn(
            CatalogTxn::new(
                2,
                2,
                vec![
                    put_security_principal(
                        second_admin.clone(),
                        None,
                        Some((PrincipalId(1), 1)),
                        second_admin_audit,
                    ),
                    put_security_principal(
                        target.clone(),
                        None,
                        Some((PrincipalId(1), 1)),
                        target_audit,
                    ),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let mut downgraded_admin = initial_admin.clone();
    downgraded_admin.revision = 2;
    downgraded_admin.preset_grants = vec![ScopedPresetGrant {
        preset: PermissionPreset::Operator,
        scope: PolicyScope::Cluster,
    }];
    let downgrade_audit = security_grant_audit(
        "downgrade-initial-admin",
        3,
        AuditIdentity::Verified("principal:2".to_owned()),
        Some(&initial_admin),
        &downgraded_admin,
    );
    catalog
        .commit_txn(
            CatalogTxn::new(
                3,
                3,
                vec![put_security_principal(
                    downgraded_admin.clone(),
                    Some(1),
                    Some((PrincipalId(2), 1)),
                    downgrade_audit,
                )],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let mut requested_target = target.clone();
    requested_target.revision = 2;
    requested_target.preset_grants = vec![ScopedPresetGrant {
        preset: PermissionPreset::DataReader,
        scope: PolicyScope::Namespace {
            namespace: "sales".to_owned(),
        },
    }];
    let stale_audit = security_grant_audit(
        "stale-reviewed-grant-change",
        4,
        AuditIdentity::Verified("principal:1".to_owned()),
        Some(&target),
        &requested_target,
    );
    let error = catalog
        .commit_txn(
            CatalogTxn::new(
                4,
                4,
                vec![put_security_principal(
                    requested_target,
                    Some(1),
                    Some((PrincipalId(1), 1)),
                    stale_audit,
                )],
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error,
        CatalogError::OptimisticConflict(
            "[RS-2008] transaction.optimistic_conflict: principal 1 revision was 2, expected 1; retry the transaction against current policy"
                .to_owned()
        )
    );

    let audit_before_recovery = catalog.list_security_audit_records().await.unwrap();
    assert_eq!(catalog.get_revision().await, 3);
    assert_eq!(
        catalog
            .get_security_principal(PrincipalId(1))
            .await
            .unwrap(),
        Some(downgraded_admin.clone())
    );
    assert_eq!(
        catalog
            .get_security_principal(PrincipalId(3))
            .await
            .unwrap(),
        Some(target.clone())
    );
    assert_eq!(
        catalog.list_security_audit_records().await.unwrap(),
        audit_before_recovery
    );

    let recovered = DurableCatalogStore::recover(store, "security")
        .await
        .unwrap();
    assert_eq!(recovered.get_revision().await, 3);
    assert_eq!(
        recovered
            .get_security_principal(PrincipalId(1))
            .await
            .unwrap(),
        Some(downgraded_admin)
    );
    assert_eq!(
        recovered
            .get_security_principal(PrincipalId(3))
            .await
            .unwrap(),
        Some(target)
    );
    assert_eq!(
        recovered.list_security_audit_records().await.unwrap(),
        audit_before_recovery
    );
}

#[tokio::test]
async fn test_create_table_crash_before_commit_rolls_back() {
    let store = Arc::new(InMemory::new());
    let _catalog = DurableCatalogStore::new(store.clone(), "test");

    let tbl = CatalogTable {
        id: TableId(10),
        name: "uncommitted_table".to_string(),
        namespace_id: NamespaceId(1),
        columns: vec![],
        pk_cols: vec![],
    };
    // Transaction prepared but NEVER committed to catalog
    let _txn = CatalogTxn::new(1, 100, vec![CatalogMutation::PutTable(tbl)]).unwrap();

    // Node restarts
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    assert_eq!(recovered.get_revision().await, 0);
    assert_eq!(recovered.get_table(TableId(10)).await.unwrap(), None);
    assert_eq!(
        recovered
            .get_table_by_name("uncommitted_table")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn test_create_table_crash_after_commit_recovers() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let tbl = CatalogTable {
        id: TableId(10),
        name: "committed_table".to_string(),
        namespace_id: NamespaceId(1),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
                ordinal: 0,
            },
            CatalogColumn {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
                nullable: true,
                ordinal: 1,
            },
        ],
        pk_cols: vec!["id".to_string()],
    };
    let txn = CatalogTxn::new(1, 101, vec![CatalogMutation::PutTable(tbl.clone())]).unwrap();
    let rev = catalog.commit_txn(txn).await.unwrap();
    assert_eq!(rev, 1);

    // Node crashes and recovers
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    assert_eq!(recovered.get_revision().await, 1);
    let retrieved = recovered.get_table(TableId(10)).await.unwrap().unwrap();
    assert_eq!(retrieved, tbl);
    assert_eq!(retrieved.columns.len(), 2);
}

#[tokio::test]
async fn test_create_mat_view_atomic_mutations_recovered() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let view_id = ViewId(20);
    let plan_id = CompiledPlanId(30);
    let table_id = TableId(10);

    let view = CatalogView {
        id: view_id,
        name: "active_v".to_string(),
        namespace_id: NamespaceId(1),
        sql: "SELECT * FROM t".to_string(),
        compiled_plan_id: Some(plan_id),
        op_id: Some(42),
        columns: vec![],
    };
    let plan = CompiledPlanRecord {
        id: plan_id,
        sql: "SELECT * FROM t".to_string(),
        ast_hash: [1u8; 32],
        logical_plan_hash: [2u8; 32],
        compiler_version: "0.63.0".to_string(),
        state_layout_version: 1,
        output_schema: vec![1, 2],
        dependency_ids: vec![table_id.0 as u128],
    };
    let dep = ViewDependency {
        parent_id: view_id.0,
        child_id: table_id.0,
        dependency_kind: DependencyKind::Table,
    };

    let table = CatalogTable {
        id: table_id,
        name: "t".to_string(),
        namespace_id: NamespaceId(1),
        columns: vec![],
        pk_cols: vec![],
    };
    let setup_txn = CatalogTxn::new(1, 100, vec![CatalogMutation::PutTable(table)]).unwrap();
    catalog.commit_txn(setup_txn).await.unwrap();

    let txn = CatalogTxn::new(
        2,
        200,
        vec![
            CatalogMutation::PutView(view.clone()),
            CatalogMutation::PutCompiledPlan(plan.clone()),
            CatalogMutation::PutViewDependency(dep.clone()),
        ],
    )
    .unwrap();

    catalog.commit_txn(txn).await.unwrap();

    // Recover after crash
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    assert_eq!(recovered.get_view(view_id).await.unwrap(), Some(view));
    assert_eq!(
        recovered.get_compiled_plan(plan_id).await.unwrap(),
        Some(plan)
    );
    let deps = recovered.list_view_dependencies().await.unwrap();
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0], dep);
}

#[tokio::test]
async fn test_catalog_and_admin_bootstrap_storage_failure_fails_closed() {
    // Failing object store simulator
    use object_store::path::Path as ObjectPath;
    use object_store::{
        CopyOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutOptions, PutPayload,
        PutResult,
    };
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Debug)]
    struct FailingStore {
        inner: InMemory,
        fail_puts: AtomicBool,
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for FailingStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            bytes: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            if self.fail_puts.load(Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "FailingStore",
                    source: "simulated storage failure".into(),
                });
            }
            self.inner.put_opts(location, bytes, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        fn list_with_offset(
            &self,
            prefix: Option<&ObjectPath>,
            offset: &ObjectPath,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    impl std::fmt::Display for FailingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingStore")
        }
    }

    let failing_store = Arc::new(FailingStore {
        inner: InMemory::new(),
        fail_puts: AtomicBool::new(true),
    });

    let catalog = DurableCatalogStore::new(failing_store.clone(), "test");
    let view = CatalogView {
        id: ViewId(99),
        name: "failing_view".to_string(),
        namespace_id: NamespaceId(1),
        sql: "SELECT 1".to_string(),
        compiled_plan_id: None,
        op_id: None,
        columns: vec![],
    };
    let txn = CatalogTxn::new(1, 300, vec![CatalogMutation::PutView(view)]).unwrap();

    let res = catalog.commit_txn(txn).await;
    assert!(
        res.is_err(),
        "Expected commit_txn to fail when storage fails"
    );

    // In-memory state unchanged
    assert_eq!(catalog.get_view(ViewId(99)).await.unwrap(), None);
    assert_eq!(catalog.get_revision().await, 0);

    let administrator = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(77),
        display_name: "bootstrap-admin".into(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::ScramSha256 {
            username: "bootstrap-admin".into(),
            salt: vec![1; 16],
            iterations: 4096,
            stored_key: vec![2; 32],
            server_key: vec![3; 32],
        }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::Administrator,
            scope: PolicyScope::Cluster,
        }],
        scoped_grants: vec![],
    };
    assert!(catalog
        .preprovision_first_administrator(administrator.clone(), 301)
        .await
        .is_err());
    assert_eq!(
        catalog
            .get_security_principal(administrator.id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(catalog.list_security_audit_records().await.unwrap(), vec![]);
    assert_eq!(catalog.get_revision().await, 0);

    let recovered = DurableCatalogStore::recover(failing_store, "test")
        .await
        .unwrap();
    assert_eq!(
        recovered
            .get_security_principal(administrator.id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        recovered.list_security_audit_records().await.unwrap(),
        vec![]
    );
    assert_eq!(recovered.get_revision().await, 0);
}

#[tokio::test]
async fn test_drop_view_crash_before_commit_preserves_view() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let view = CatalogView {
        id: ViewId(5),
        name: "v_to_keep".to_string(),
        namespace_id: NamespaceId(1),
        sql: "SELECT 1".to_string(),
        compiled_plan_id: None,
        op_id: None,
        columns: vec![],
    };
    let txn1 = CatalogTxn::new(1, 1, vec![CatalogMutation::PutView(view.clone())]).unwrap();
    catalog.commit_txn(txn1).await.unwrap();

    // Prepare DROP VIEW txn but crash before committing
    let _drop_txn = CatalogTxn::new(2, 2, vec![CatalogMutation::DeleteView(ViewId(5))]).unwrap();

    // Recover
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    assert_eq!(recovered.get_view(ViewId(5)).await.unwrap(), Some(view));
}

#[tokio::test]
async fn test_drop_view_crash_after_commit_removes_view() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let view = CatalogView {
        id: ViewId(5),
        name: "v_to_drop".to_string(),
        namespace_id: NamespaceId(1),
        sql: "SELECT 1".to_string(),
        compiled_plan_id: None,
        op_id: None,
        columns: vec![],
    };
    let txn1 = CatalogTxn::new(1, 1, vec![CatalogMutation::PutView(view)]).unwrap();
    catalog.commit_txn(txn1).await.unwrap();

    let drop_txn = CatalogTxn::new(2, 2, vec![CatalogMutation::DeleteView(ViewId(5))]).unwrap();
    catalog.commit_txn(drop_txn).await.unwrap();

    // Recover after drop commit
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    assert_eq!(recovered.get_view(ViewId(5)).await.unwrap(), None);
    assert_eq!(recovered.get_revision().await, 2);
}

#[tokio::test]
async fn test_drop_view_clears_outgoing_dependencies_before_table_drop() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");
    let table = CatalogTable {
        id: TableId(10),
        name: "base".to_string(),
        namespace_id: NamespaceId(1),
        columns: vec![],
        pk_cols: vec![],
    };
    let view = CatalogView {
        id: ViewId(5),
        name: "dependent".to_string(),
        namespace_id: NamespaceId(1),
        sql: "SELECT * FROM base".to_string(),
        compiled_plan_id: None,
        op_id: None,
        columns: vec![],
    };
    let dependency = ViewDependency {
        parent_id: view.id.0,
        child_id: table.id.0,
        dependency_kind: DependencyKind::Table,
    };

    catalog
        .commit_txn(
            CatalogTxn::new(
                1,
                1,
                vec![
                    CatalogMutation::PutTable(table),
                    CatalogMutation::PutView(view),
                    CatalogMutation::PutViewDependency(dependency),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let error = catalog
        .commit_txn(CatalogTxn::new(2, 2, vec![CatalogMutation::DeleteTable(TableId(10))]).unwrap())
        .await
        .unwrap_err();
    assert_eq!(
        error,
        CatalogError::ReferencedObjectExists(format!(
            "[RS-1002] cannot drop table {}: dependent views exist; next_steps: drop dependent views first",
            TableId(10)
        ))
    );
    assert_eq!(catalog.get_revision().await, 1);
    catalog
        .commit_txn(CatalogTxn::new(2, 3, vec![CatalogMutation::DeleteView(ViewId(5))]).unwrap())
        .await
        .unwrap();
    catalog
        .commit_txn(CatalogTxn::new(3, 4, vec![CatalogMutation::DeleteTable(TableId(10))]).unwrap())
        .await
        .unwrap();

    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    assert_eq!(recovered.list_view_dependencies().await.unwrap(), vec![]);
    assert_eq!(recovered.get_table(TableId(10)).await.unwrap(), None);
}

#[tokio::test]
async fn test_create_index_crash_during_build_recovers_state() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let idx = CatalogIndexEntry {
        id: IndexId(10),
        name: "building_idx".to_string(),
        table_id: TableId(1),
        index_cols: vec!["col".to_string()],
        pk_cols: vec!["id".to_string()],
        state: CatalogIndexState::Building,
        op_id: None,
    };
    let txn = CatalogTxn::new(1, 1, vec![CatalogMutation::PutIndex(idx.clone())]).unwrap();
    catalog.commit_txn(txn).await.unwrap();

    // Crash occurs while still building
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_index(IndexId(10)).await.unwrap().unwrap();
    assert_eq!(retrieved.state, CatalogIndexState::Building);
    assert_eq!(retrieved.op_id, None);
}

#[tokio::test]
async fn test_transaction_replay_deduplication() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let tbl = CatalogTable {
        id: TableId(1),
        name: "dedup_tbl".to_string(),
        namespace_id: NamespaceId(1),
        columns: vec![],
        pk_cols: vec![],
    };
    let txn1 = CatalogTxn::new(1, 555, vec![CatalogMutation::PutTable(tbl.clone())]).unwrap();
    let rev1 = catalog.commit_txn(txn1.clone()).await.unwrap();
    assert_eq!(rev1, 1);

    // Replay transaction with identical operation_id (555)
    let rev2 = catalog.commit_txn(txn1).await.unwrap();
    // Idempotent no-op
    assert_eq!(rev2, 1);
    assert_eq!(catalog.get_revision().await, 1);

    // Recovering also deduplicates
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    assert_eq!(recovered.get_revision().await, 1);
    let tables = recovered.list_tables().await.unwrap();
    assert_eq!(tables.len(), 1);
}
