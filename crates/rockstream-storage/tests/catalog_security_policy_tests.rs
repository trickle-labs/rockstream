use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use rockstream_storage::catalog::*;
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, LoginMethod, PermissionPreset, PolicyScope,
    ScopedPresetGrant, SecurityAuditClass, SecurityAuditOutcome, SecurityAuditRecord,
    SecurityPrincipal, SECURITY_AUDIT_FORMAT_VERSION, SECURITY_PRINCIPAL_FORMAT_VERSION,
};
use rockstream_types::ids::PrincipalId;
use std::collections::BTreeSet;
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
        timestamp_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
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

#[test]
fn client_correlated_grant_mutation_uses_catalog_format_three() {
    let operation_id = "550e8400-e29b-41d4-a716-446655440000";
    let previous = admin(1, 1);
    let mut updated = admin(2, 2);
    updated.display_name = "updated administrator".into();
    let mut audit = grant_audit(
        "grant-operation-format-three",
        102,
        AuditIdentity::Verified("principal:1".into()),
        &updated,
        Some(policy_state(&admin(2, 1))),
        Some(policy_state(&updated)),
    );
    audit.correlation = AuditCorrelation::Observed(operation_id.into());
    audit.operation_id = AuditField::Known(operation_id.into());
    let txn = CatalogTxn::new(
        2,
        102,
        vec![CatalogMutation::PutSecurityPrincipalForOperation {
            principal: updated,
            expected_revision: Some(1),
            grantor_revision: Some((previous.id, previous.revision)),
            operation_id: operation_id.into(),
            audit,
        }],
    )
    .unwrap();

    assert_eq!(txn.format_version, 3);
    assert_eq!(CatalogTxn::decode(&txn.encode().unwrap()).unwrap(), txn);
}

#[tokio::test]
async fn expired_security_audit_append_is_rejected_without_a_catalog_effect() {
    let object_store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(object_store.clone(), "expired-audit");
    let principal = principal(8, 1, vec![]);
    let mut expired = grant_audit(
        "expired-event",
        808,
        AuditIdentity::Unknown,
        &principal,
        None,
        Some(policy_state(&principal)),
    );
    expired.timestamp_ms = 1;

    let error = catalog
        .commit_txn(
            CatalogTxn::new(1, 808, vec![CatalogMutation::AppendSecurityAudit(expired)]).unwrap(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        CatalogError::InvalidSecurityPolicy(message)
            if message == "new security audit record is outside the 90-day retention window"
    ));
    assert_eq!(catalog.get_revision().await, 0);
    assert!(catalog
        .list_security_audit_records()
        .await
        .unwrap()
        .is_empty());
    let mut log_files = object_store.list(Some(&ObjectPath::from("expired-audit/catalog/log")));
    assert!(log_files.next().await.is_none());
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
async fn operator_cannot_self_grant_administrator_or_data_authority() {
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

    let operator = principal(
        2,
        1,
        vec![ScopedPresetGrant {
            preset: PermissionPreset::Operator,
            scope: PolicyScope::Cluster,
        }],
    );
    let operator_audit = grant_audit(
        "event-2",
        102,
        AuditIdentity::Verified("principal:1".into()),
        &operator,
        None,
        Some(policy_state(&operator)),
    );
    catalog
        .commit_txn(
            CatalogTxn::new(
                2,
                102,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: operator.clone(),
                    expected_revision: None,
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit: operator_audit,
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
        Some(policy_state(&operator)),
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
        Some(operator.clone())
    );
    assert_eq!(
        catalog.list_security_audit_records().await.unwrap().len(),
        2
    );

    let mut data_elevated = operator.clone();
    data_elevated.revision = 2;
    data_elevated.preset_grants.push(ScopedPresetGrant {
        preset: PermissionPreset::DataReader,
        scope: PolicyScope::Namespace {
            namespace: "sales".to_owned(),
        },
    });
    let data_grant_error = catalog
        .commit_txn(
            CatalogTxn::new(
                3,
                104,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: data_elevated.clone(),
                    expected_revision: Some(1),
                    grantor_revision: Some((PrincipalId(2), 1)),
                    audit: grant_audit(
                        "event-4",
                        104,
                        AuditIdentity::Verified("principal:2".into()),
                        &data_elevated,
                        Some(policy_state(&operator)),
                        Some(policy_state(&data_elevated)),
                    ),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        data_grant_error,
        CatalogError::InvalidSecurityPolicy(message)
            if message == "grantor lacks current principal and grant management authority"
    ));
    assert_eq!(catalog.get_revision().await, 2);
    assert_eq!(
        catalog
            .get_security_principal(PrincipalId(2))
            .await
            .unwrap(),
        Some(operator)
    );
    assert_eq!(
        catalog.list_security_audit_records().await.unwrap().len(),
        2
    );
}

#[tokio::test]
async fn administrator_cannot_self_grant_but_can_self_revoke_with_another_admin() {
    let catalog = DurableCatalogStore::new(Arc::new(InMemory::new()), "admin-self-grant");
    let administrator = admin(1, 1);
    catalog
        .commit_txn(
            CatalogTxn::new(
                1,
                101,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: administrator.clone(),
                    expected_revision: None,
                    grantor_revision: None,
                    audit: bootstrap_audit("admin-self-grant-bootstrap", 101, &administrator),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let second_admin = admin(2, 1);
    catalog
        .commit_txn(
            CatalogTxn::new(
                2,
                102,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: second_admin.clone(),
                    expected_revision: None,
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit: grant_audit(
                        "admin-self-grant-second-admin",
                        102,
                        AuditIdentity::Verified("principal:1".into()),
                        &second_admin,
                        None,
                        Some(policy_state(&second_admin)),
                    ),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let mut expanded = administrator.clone();
    expanded.revision = 2;
    expanded.preset_grants.push(ScopedPresetGrant {
        preset: PermissionPreset::DataReader,
        scope: PolicyScope::Namespace {
            namespace: "sales".to_owned(),
        },
    });
    let error = catalog
        .commit_txn(
            CatalogTxn::new(
                3,
                103,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: expanded.clone(),
                    expected_revision: Some(1),
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit: grant_audit(
                        "admin-self-grant-data",
                        103,
                        AuditIdentity::Verified("principal:1".into()),
                        &expanded,
                        Some(policy_state(&administrator)),
                        Some(policy_state(&expanded)),
                    ),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        CatalogError::InvalidSecurityPolicy(message)
            if message == "grantor cannot increase its own authority"
    ));
    assert_eq!(catalog.get_revision().await, 2);
    assert_eq!(
        catalog
            .get_security_principal(PrincipalId(1))
            .await
            .unwrap(),
        Some(administrator.clone())
    );
    assert_eq!(
        catalog
            .get_security_principal(PrincipalId(2))
            .await
            .unwrap(),
        Some(second_admin.clone())
    );
    assert_eq!(
        catalog.list_security_audit_records().await.unwrap().len(),
        2
    );

    let mut self_revoked = administrator.clone();
    self_revoked.revision = 2;
    self_revoked.preset_grants.clear();
    assert_eq!(
        catalog
            .commit_txn(
                CatalogTxn::new(
                    3,
                    104,
                    vec![CatalogMutation::PutSecurityPrincipal {
                        principal: self_revoked.clone(),
                        expected_revision: Some(1),
                        grantor_revision: Some((PrincipalId(1), 1)),
                        audit: grant_audit(
                            "admin-self-revoke-authority",
                            104,
                            AuditIdentity::Verified("principal:1".into()),
                            &self_revoked,
                            Some(policy_state(&administrator)),
                            Some(policy_state(&self_revoked)),
                        ),
                    }],
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        catalog
            .get_security_principal(PrincipalId(1))
            .await
            .unwrap(),
        Some(self_revoked)
    );
    assert_eq!(
        catalog
            .get_security_principal(PrincipalId(2))
            .await
            .unwrap(),
        Some(second_admin)
    );
    assert_eq!(
        catalog.list_security_audit_records().await.unwrap().len(),
        3
    );
}

#[tokio::test]
async fn concurrent_last_admin_revocations_commit_only_one_policy_revision() {
    let object_store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(object_store.clone(), "admin-race");
    let first_admin = admin(1, 1);
    let second_admin = admin(2, 1);
    catalog
        .commit_txn(
            CatalogTxn::new(
                1,
                201,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: first_admin.clone(),
                    expected_revision: None,
                    grantor_revision: None,
                    audit: bootstrap_audit("admin-race-bootstrap", 201, &first_admin),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    catalog
        .commit_txn(
            CatalogTxn::new(
                2,
                202,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: second_admin.clone(),
                    expected_revision: None,
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit: grant_audit(
                        "admin-race-second-admin",
                        202,
                        AuditIdentity::Verified("principal:1".into()),
                        &second_admin,
                        None,
                        Some(policy_state(&second_admin)),
                    ),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let first_process = DurableCatalogStore::recover(object_store.clone(), "admin-race")
        .await
        .unwrap();
    let second_process = DurableCatalogStore::recover(object_store.clone(), "admin-race")
        .await
        .unwrap();
    let mut first_revocation = first_admin.clone();
    first_revocation.revision = 2;
    first_revocation.preset_grants.clear();
    let mut second_revocation = second_admin.clone();
    second_revocation.revision = 2;
    second_revocation.preset_grants.clear();
    let first_txn = CatalogTxn::new(
        3,
        203,
        vec![CatalogMutation::PutSecurityPrincipal {
            principal: first_revocation.clone(),
            expected_revision: Some(1),
            grantor_revision: Some((PrincipalId(2), 1)),
            audit: grant_audit(
                "admin-race-revoke-first",
                203,
                AuditIdentity::Verified("principal:2".into()),
                &first_revocation,
                Some(policy_state(&first_admin)),
                Some(policy_state(&first_revocation)),
            ),
        }],
    )
    .unwrap();
    let second_txn = CatalogTxn::new(
        3,
        204,
        vec![CatalogMutation::PutSecurityPrincipal {
            principal: second_revocation.clone(),
            expected_revision: Some(1),
            grantor_revision: Some((PrincipalId(1), 1)),
            audit: grant_audit(
                "admin-race-revoke-second",
                204,
                AuditIdentity::Verified("principal:1".into()),
                &second_revocation,
                Some(policy_state(&second_admin)),
                Some(policy_state(&second_revocation)),
            ),
        }],
    )
    .unwrap();

    let (first_result, second_result) = tokio::join!(
        first_process.commit_txn(first_txn),
        second_process.commit_txn(second_txn)
    );
    assert_ne!(first_result.is_ok(), second_result.is_ok());
    assert!(matches!(
        first_result.as_ref().err().or(second_result.as_ref().err()),
        Some(CatalogError::OptimisticConflict(_))
    ));

    let recovered = DurableCatalogStore::recover(object_store, "admin-race")
        .await
        .unwrap();
    assert_eq!(recovered.get_revision().await, 3);
    let stored_first = recovered
        .get_security_principal(PrincipalId(1))
        .await
        .unwrap()
        .unwrap();
    let stored_second = recovered
        .get_security_principal(PrincipalId(2))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        stored_first.preset_grants.is_empty(),
        stored_second.preset_grants.is_empty()
    );
    let winning_event = if first_result.is_ok() {
        "admin-race-revoke-first"
    } else {
        "admin-race-revoke-second"
    };
    let mut event_ids: Vec<_> = recovered
        .list_security_audit_records()
        .await
        .unwrap()
        .into_iter()
        .map(|record| record.event_id)
        .collect();
    event_ids.sort();
    let mut expected_event_ids = vec![
        "admin-race-bootstrap".to_string(),
        "admin-race-second-admin".to_string(),
        winning_event.to_string(),
    ];
    expected_event_ids.sort();
    assert_eq!(event_ids, expected_event_ids);
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

#[tokio::test]
async fn duplicate_login_identity_is_rejected_for_mutations_and_snapshots() {
    let object_store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(object_store, "duplicate-login");
    let administrator = admin(1, 1);
    catalog
        .preprovision_first_administrator(administrator.clone(), 601)
        .await
        .unwrap();

    let mut duplicate = admin(2, 1);
    duplicate.login_methods = administrator.login_methods.clone();
    let snapshot = CatalogSnapshot::new(
        1,
        2,
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        BTreeSet::new(),
    )
    .unwrap();
    let snapshot_error = snapshot
        .with_security_records(vec![administrator.clone(), duplicate.clone()], vec![])
        .unwrap_err();
    assert!(matches!(
        snapshot_error,
        CatalogError::DecodeError(message)
            if message == "[RS-1003] invalid security login mapping in snapshot: login identity is mapped more than once in the security catalog"
    ));

    let audits_before = catalog.list_security_audit_records().await.unwrap();
    let error = catalog
        .commit_txn(
            CatalogTxn::new(
                2,
                602,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: duplicate.clone(),
                    expected_revision: None,
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit: grant_audit(
                        "event-duplicate-login",
                        602,
                        AuditIdentity::Verified("principal:1".to_string()),
                        &duplicate,
                        None,
                        Some(policy_state(&duplicate)),
                    ),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        CatalogError::InvalidSecurityPolicy(message)
            if message == "invalid security login mapping: login identity is mapped more than once in the security catalog"
    ));
    assert_eq!(catalog.get_revision().await, 1);
    assert_eq!(
        catalog.list_security_principals().await.unwrap(),
        vec![administrator]
    );
    assert_eq!(
        catalog.list_security_audit_records().await.unwrap(),
        audits_before
    );
}
