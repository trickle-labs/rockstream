#![allow(dead_code)]

use std::{sync::Arc, time::SystemTime};

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    role_catalog::{create_role_entry, gen_scram_verifiers, RoleCatalog},
};
use rockstream_storage::catalog::{
    CatalogMutation, CatalogNamespace, CatalogStore, CatalogTxn, DurableCatalogStore,
};
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, LoginMethod, PermissionPreset, PolicyScope,
    ScopedActionGrant, ScopedPresetGrant, SecurityAction, SecurityAuditClass, SecurityAuditOutcome,
    SecurityAuditRecord, SecurityPrincipal, SECURITY_AUDIT_FORMAT_VERSION,
    SECURITY_PRINCIPAL_FORMAT_VERSION,
};
use rockstream_types::ids::{DatabaseId, NamespaceId, PrincipalId};
use tokio_postgres::NoTls;

pub async fn connect_test_builder(port: u16) -> tokio_postgres::Client {
    connect_test_login(port, "test-builder", "test-password").await
}

pub async fn connect_test_admin(port: u16) -> tokio_postgres::Client {
    connect_test_login(port, "bootstrap-admin", "bootstrap-password").await
}

async fn connect_test_login(port: u16, username: &str, password: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user={username} password={password} dbname=test"),
        NoTls,
    )
    .await
    .expect("connect using test principal");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// Build a test SCRAM login backed by an explicitly bootstrapped durable policy.
pub async fn role_catalog_with_authorized_builder(
    catalog: &Arc<CatalogStubs>,
    username: &str,
    password: &str,
) -> Arc<RoleCatalog> {
    role_catalog_with_policy(catalog, username, password, None, false, false).await
}

pub async fn role_catalog_with_authorized_admin(
    catalog: &Arc<CatalogStubs>,
    username: &str,
    password: &str,
) -> Arc<RoleCatalog> {
    role_catalog_with_policy(catalog, username, password, None, false, true).await
}

pub async fn role_catalog_with_admin_and_builder(
    catalog: &Arc<CatalogStubs>,
    username: &str,
    password: &str,
) -> Arc<RoleCatalog> {
    role_catalog_with_policy(catalog, username, password, None, true, true).await
}

pub async fn role_catalog_with_builder_secret_use(
    catalog: &Arc<CatalogStubs>,
    username: &str,
    password: &str,
    secret_name: &str,
) -> Arc<RoleCatalog> {
    role_catalog_with_policy(catalog, username, password, Some(secret_name), false, true).await
}

async fn role_catalog_with_policy(
    catalog: &Arc<CatalogStubs>,
    username: &str,
    password: &str,
    secret_name: Option<&str>,
    admin_namespace: bool,
    seed_public_namespace: bool,
) -> Arc<RoleCatalog> {
    let role_catalog = Arc::new(RoleCatalog::new());
    let admin_role = create_role_entry("bootstrap-admin", "bootstrap-password");
    let admin_salt = admin_role.scram_salt.clone();
    let admin_iterations = admin_role.scram_iterations;
    let (_, admin_stored_key, admin_server_key) =
        gen_scram_verifiers("bootstrap-password", &admin_salt, admin_iterations);
    role_catalog
        .insert(admin_role)
        .expect("insert bootstrap administrator");

    let role = create_role_entry(username, password);
    let salt = role.scram_salt.clone();
    let iterations = role.scram_iterations;
    let (_, stored_key, server_key) =
        gen_scram_verifiers(password, &role.scram_salt, role.scram_iterations);
    role_catalog.insert(role).expect("insert test login");

    let mut admin_grants = vec![ScopedPresetGrant {
        preset: PermissionPreset::Administrator,
        scope: PolicyScope::Cluster,
    }];
    if admin_namespace {
        admin_grants.push(ScopedPresetGrant {
            preset: PermissionPreset::Administrator,
            scope: PolicyScope::Namespace {
                namespace: "public".to_string(),
            },
        });
    }
    let principal = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(1),
        display_name: "Test bootstrap administrator".to_string(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::ScramSha256 {
            username: "bootstrap-admin".to_string(),
            salt: admin_salt,
            iterations: admin_iterations,
            stored_key: admin_stored_key.to_vec(),
            server_key: admin_server_key.to_vec(),
        }],
        preset_grants: admin_grants,
        scoped_grants: vec![],
    };
    let builder = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(2),
        display_name: format!("Test principal {username}"),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::ScramSha256 {
            username: username.to_string(),
            salt,
            iterations,
            stored_key: stored_key.to_vec(),
            server_key: server_key.to_vec(),
        }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::Builder,
            scope: PolicyScope::Namespace {
                namespace: "public".to_string(),
            },
        }],
        scoped_grants: secret_name
            .map(|name| {
                vec![ScopedActionGrant {
                    action: SecurityAction::SecretUse,
                    scope: PolicyScope::Secret {
                        namespace: "public".to_string(),
                        name: name.to_string(),
                        consumer: Some(PrincipalId(2)),
                    },
                }]
            })
            .unwrap_or_default(),
    };
    let builder_policy = serde_json::to_string(&(
        builder.enabled,
        &builder.preset_grants,
        &builder.scoped_grants,
    ))
    .expect("serialize test Builder policy");
    let grant_timestamp_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_millis() as u64;
    let builder_audit = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "test-grant-builder".to_string(),
        timestamp_ms: grant_timestamp_ms.max(1),
        actor: AuditIdentity::Verified("principal:1".to_string()),
        target: AuditField::Known("principal:2".to_string()),
        scope: AuditField::Known("cluster".to_string()),
        action: SecurityAuditClass::GrantChange,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::Unknown,
        operation_id: AuditField::Known("2".to_string()),
        policy_revision: AuditField::Known(1),
        before_state: AuditField::NotApplicable,
        after_state: AuditField::Known(format!("revision=1;policy={builder_policy}")),
    };
    let durable = Arc::new(DurableCatalogStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        format!("test-security-policy-{username}"),
    ));
    durable
        .preprovision_first_administrator(principal, 1)
        .await
        .expect("persist test administrator and bootstrap audit");
    durable
        .commit_txn(
            CatalogTxn::new(
                2,
                2,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: builder,
                    expected_revision: None,
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit: builder_audit,
                }],
            )
            .expect("valid test Builder grant transaction"),
        )
        .await
        .expect("persist test Builder grant and audit");
    if seed_public_namespace {
        durable
            .commit_txn(
                CatalogTxn::new(
                    3,
                    3,
                    vec![CatalogMutation::PutNamespace(CatalogNamespace {
                        id: NamespaceId(1),
                        name: "public".to_string(),
                        database_id: DatabaseId(1),
                        created_at: 1,
                    })],
                )
                .expect("valid public namespace transaction"),
            )
            .await
            .expect("persist test public namespace");
    }
    catalog.set_durable_store(durable);
    role_catalog
}
