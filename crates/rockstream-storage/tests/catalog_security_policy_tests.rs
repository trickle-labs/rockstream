use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStoreExt;
use rockstream_storage::catalog::*;
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, LoginMethod, PermissionPreset, PolicyScope,
    ScopedPresetGrant, SecurityAuditClass, SecurityAuditOutcome, SecurityAuditRecord,
    SecurityPrincipal, SECURITY_AUDIT_FORMAT_VERSION, SECURITY_PRINCIPAL_FORMAT_VERSION,
};
use rockstream_types::ids::PrincipalId;
use std::sync::Arc;

fn principal(id: u64, revision: u64, presets: Vec<ScopedPresetGrant>) -> SecurityPrincipal {
    SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(id),
        display_name: format!("principal-{id}"),
        enabled: true,
        revision,
        login_methods: vec![LoginMethod::ScramSha256 {
            username: format!("user-{id}"),
            salt: vec![1; 16],
            iterations: 4096,
            stored_key: vec![2; 32],
            server_key: vec![3; 32],
        }],
        preset_grants: presets,
        scoped_grants: vec![],
    }
}

fn admin(id: u64, revision: u64) -> SecurityPrincipal {
    principal(
        id,
        revision,
        vec![ScopedPresetGrant {
            preset: PermissionPreset::Administrator,
            scope: PolicyScope::Cluster,
        }],
    )
}

fn policy_state(principal: &SecurityPrincipal) -> String {
    let policy = serde_json::to_string(&(
        principal.enabled,
        &principal.preset_grants,
        &principal.scoped_grants,
    ))
    .unwrap();
    format!("revision={};policy={policy}", principal.revision)
}

fn grant_audit(
    event_id: &str,
    operation_id: u64,
    actor: AuditIdentity,
    principal: &SecurityPrincipal,
    before_state: Option<String>,
    after_state: Option<String>,
) -> SecurityAuditRecord {
    SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: event_id.to_string(),
        timestamp_ms: 1,
        actor,
        target: AuditField::Known(format!("principal:{}", principal.id.0)),
        scope: AuditField::Known("cluster".to_string()),
        action: SecurityAuditClass::GrantChange,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::Unknown,
        operation_id: AuditField::Known(operation_id.to_string()),
        policy_revision: AuditField::Known(principal.revision),
        before_state: before_state.map_or(AuditField::NotApplicable, AuditField::Known),
        after_state: after_state.map_or(AuditField::NotApplicable, AuditField::Known),
    }
}

fn bootstrap_audit(
    event_id: &str,
    operation_id: u64,
    principal: &SecurityPrincipal,
) -> SecurityAuditRecord {
    let mut audit = grant_audit(
        event_id,
        operation_id,
        AuditIdentity::Unknown,
        principal,
        None,
        Some(policy_state(principal)),
    );
    audit.actor = AuditIdentity::Unknown;
    audit.correlation = AuditCorrelation::NotApplicable;
    audit
}

#[tokio::test]
async fn security_principal_and_correlated_audit_survive_log_snapshot_and_restart() {
    let object_store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(object_store.clone(), "security");
    let administrator = admin(1, 1);
    let audit = bootstrap_audit("event-1", 101, &administrator);
    let txn = CatalogTxn::new(
        1,
        101,
        vec![CatalogMutation::PutSecurityPrincipal {
            principal: administrator.clone(),
            expected_revision: None,
            grantor_revision: None,
            audit: audit.clone(),
        }],
    )
    .unwrap();
    assert_eq!(txn.format_version, 2);
    assert_eq!(CatalogTxn::decode(&txn.encode().unwrap()).unwrap(), txn);
    assert_eq!(catalog.commit_txn(txn).await.unwrap(), 1);
    assert_eq!(
        catalog
            .get_security_principal(PrincipalId(1))
            .await
            .unwrap(),
        Some(administrator.clone())
    );
    assert_eq!(
        catalog.list_security_audit_records().await.unwrap(),
        vec![audit.clone()]
    );

    let recovered = DurableCatalogStore::recover(object_store.clone(), "security")
        .await
        .unwrap();
    assert_eq!(
        recovered
            .get_security_principal(PrincipalId(1))
            .await
            .unwrap(),
        Some(administrator.clone())
    );
    assert_eq!(
        recovered.list_security_audit_records().await.unwrap(),
        vec![audit.clone()]
    );

    recovered.create_snapshot().await.unwrap();
    recovered.compact_logs(1).await.unwrap();
    let after_snapshot = DurableCatalogStore::recover(object_store, "security")
        .await
        .unwrap();
    assert_eq!(
        after_snapshot.list_security_principals().await.unwrap(),
        vec![administrator]
    );
    assert_eq!(
        after_snapshot.list_security_audit_records().await.unwrap(),
        vec![audit]
    );
}

#[tokio::test]
async fn first_administrator_preprovision_is_atomic_audited_and_one_time() {
    let object_store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(object_store.clone(), "bootstrap");
    let administrator = admin(77, 1);
    let now_ms = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    };
    let before_ms = now_ms();

    assert_eq!(
        catalog
            .preprovision_first_administrator(administrator.clone(), 901)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        catalog
            .preprovision_first_administrator(administrator.clone(), 901)
            .await
            .unwrap(),
        1
    );
    let committed_audits = catalog.list_security_audit_records().await.unwrap();
    let [stored_audit] = committed_audits.as_slice() else {
        panic!("bootstrap must commit exactly one audit record");
    };
    let expected_audit = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "first-admin-bootstrap:901".into(),
        timestamp_ms: stored_audit.timestamp_ms,
        actor: AuditIdentity::Unknown,
        target: AuditField::Known("principal:77".into()),
        scope: AuditField::Known("cluster".into()),
        action: SecurityAuditClass::GrantChange,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::NotApplicable,
        operation_id: AuditField::Known("901".into()),
        policy_revision: AuditField::Known(1),
        before_state: AuditField::NotApplicable,
        after_state: AuditField::Known(policy_state(&administrator)),
    };
    assert!(stored_audit.timestamp_ms >= before_ms && stored_audit.timestamp_ms <= now_ms());
    assert_eq!(committed_audits, vec![expected_audit.clone()]);
    assert_eq!(
        catalog.list_security_principals().await.unwrap(),
        vec![administrator.clone()]
    );

    assert!(matches!(
        catalog
            .preprovision_first_administrator(admin(78, 1), 902)
            .await,
        Err(CatalogError::InvalidSecurityPolicy(message))
            if message == "principal changes require an authorized grantor"
    ));
    assert_eq!(
        catalog.list_security_principals().await.unwrap(),
        vec![administrator.clone()]
    );
    assert_eq!(
        catalog.list_security_audit_records().await.unwrap(),
        vec![expected_audit.clone()]
    );

    let recovered = DurableCatalogStore::recover(object_store, "bootstrap")
        .await
        .unwrap();
    assert_eq!(
        recovered.list_security_principals().await.unwrap(),
        vec![administrator]
    );
    assert_eq!(
        recovered.list_security_audit_records().await.unwrap(),
        vec![expected_audit]
    );
}

#[tokio::test]
async fn first_administrator_bootstrap_rejects_a_fabricated_verified_actor() {
    let catalog = DurableCatalogStore::new(Arc::new(InMemory::new()), "bootstrap-actor");
    let administrator = admin(88, 1);
    let audit = grant_audit(
        "fabricated-bootstrap-actor",
        903,
        AuditIdentity::Verified("operator".into()),
        &administrator,
        None,
        Some(policy_state(&administrator)),
    );
    let error = catalog
        .commit_txn(
            CatalogTxn::new(
                1,
                903,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: administrator,
                    expected_revision: None,
                    grantor_revision: None,
                    audit,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        CatalogError::InvalidSecurityPolicy(message)
            if message == "grant audit must record the permitted actor identity, target, operation, policy revision, and exact before/after state"
    ));
    assert_eq!(catalog.get_revision().await, 0);
    assert_eq!(
        catalog.list_security_principals().await.unwrap(),
        Vec::new()
    );
    assert_eq!(
        catalog.list_security_audit_records().await.unwrap(),
        Vec::new()
    );
}

#[tokio::test]
async fn stale_policy_change_and_last_admin_lockout_have_no_durable_effect() {
    let object_store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(object_store, "security");
    let administrator = admin(1, 1);
    let initial_audit = bootstrap_audit("event-1", 101, &administrator);
    catalog
        .commit_txn(
            CatalogTxn::new(
                1,
                101,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: administrator.clone(),
                    expected_revision: None,
                    grantor_revision: None,
                    audit: initial_audit.clone(),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let stale = admin(1, 100);
    let stale_audit = grant_audit(
        "event-stale",
        102,
        AuditIdentity::Verified("principal:1".into()),
        &stale,
        Some(policy_state(&administrator)),
        Some(policy_state(&stale)),
    );
    let stale_error = catalog
        .commit_txn(
            CatalogTxn::new(
                2,
                102,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: stale,
                    expected_revision: Some(99),
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit: stale_audit,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(stale_error, CatalogError::OptimisticConflict(_)));
    assert_eq!(catalog.get_revision().await, 1);
    assert_eq!(
        catalog
            .get_security_principal(PrincipalId(1))
            .await
            .unwrap(),
        Some(administrator.clone())
    );
    assert_eq!(
        catalog.list_security_audit_records().await.unwrap(),
        vec![initial_audit.clone()]
    );

    let mut disabled_admin = administrator.clone();
    disabled_admin.enabled = false;
    disabled_admin.revision = 2;
    let lockout_audit = grant_audit(
        "event-lockout",
        103,
        AuditIdentity::Verified("principal:1".into()),
        &disabled_admin,
        Some(policy_state(&administrator)),
        Some(policy_state(&disabled_admin)),
    );
    let lockout_error = catalog
        .commit_txn(
            CatalogTxn::new(
                2,
                103,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: disabled_admin,
                    expected_revision: Some(1),
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit: lockout_audit,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        lockout_error,
        CatalogError::InvalidSecurityPolicy(_)
    ));
    assert_eq!(catalog.get_revision().await, 1);
    assert_eq!(
        catalog
            .get_security_principal(PrincipalId(1))
            .await
            .unwrap(),
        Some(administrator)
    );
    assert_eq!(
        catalog.list_security_audit_records().await.unwrap(),
        vec![initial_audit]
    );
}

#[tokio::test]
async fn ordinary_principal_cannot_self_grant_administrator_authority() {
    let object_store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(object_store, "security");
    let administrator = admin(1, 1);
    let admin_audit = bootstrap_audit("event-1", 101, &administrator);
    catalog
        .commit_txn(
            CatalogTxn::new(
                1,
                101,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: administrator.clone(),
                    expected_revision: None,
                    grantor_revision: None,
                    audit: admin_audit,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let ordinary = principal(2, 1, vec![]);
    let ordinary_audit = grant_audit(
        "event-2",
        102,
        AuditIdentity::Verified("principal:1".into()),
        &ordinary,
        None,
        Some(policy_state(&ordinary)),
    );
    catalog
        .commit_txn(
            CatalogTxn::new(
                2,
                102,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: ordinary.clone(),
                    expected_revision: None,
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit: ordinary_audit,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let elevated = admin(2, 2);
    let self_grant_audit = grant_audit(
        "event-3",
        103,
        AuditIdentity::Verified("principal:2".into()),
        &elevated,
        Some(policy_state(&ordinary)),
        Some(policy_state(&elevated)),
    );
    let error = catalog
        .commit_txn(
            CatalogTxn::new(
                3,
                103,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: elevated,
                    expected_revision: Some(1),
                    grantor_revision: Some((PrincipalId(2), 1)),
                    audit: self_grant_audit,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, CatalogError::InvalidSecurityPolicy(_)));
    assert_eq!(catalog.get_revision().await, 2);
    assert_eq!(
        catalog
            .get_security_principal(PrincipalId(2))
            .await
            .unwrap(),
        Some(ordinary)
    );
    assert_eq!(
        catalog.list_security_audit_records().await.unwrap().len(),
        2
    );
}

#[tokio::test]
async fn recovery_rejects_a_security_transaction_without_a_reachable_login() {
    let object_store = Arc::new(InMemory::new());
    let mut unreachable_admin = admin(1, 1);
    unreachable_admin.login_methods.clear();
    let audit = bootstrap_audit("event-invalid", 501, &unreachable_admin);
    let txn = CatalogTxn::new(
        1,
        501,
        vec![CatalogMutation::PutSecurityPrincipal {
            principal: unreachable_admin,
            expected_revision: None,
            grantor_revision: None,
            audit,
        }],
    )
    .unwrap();
    object_store
        .put(
            &ObjectPath::from("tamper/catalog/log/00000000000000000001_00000000000000000501.log"),
            txn.encode().unwrap().into(),
        )
        .await
        .unwrap();

    let error = DurableCatalogStore::recover(object_store, "tamper")
        .await
        .unwrap_err();
    assert!(matches!(error, CatalogError::InvalidSecurityPolicy(_)));
}
