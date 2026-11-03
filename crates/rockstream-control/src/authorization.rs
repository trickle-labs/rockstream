//! Shared evaluator for the fixed v0.73 action and scope model.

use rockstream_types::authorization::{PolicyScope, SecurityAction, SecurityPrincipal};

/// Evaluate a principal's current fixed presets and independently granted actions.
///
/// Invalid, disabled, or malformed policy records deny by default. Callers that
/// handle delegated work must evaluate the actor and the authenticated consumer
/// independently before resolving a secret or performing a protected operation.
pub fn allows(
    principal: &SecurityPrincipal,
    action: SecurityAction,
    requested_scope: &PolicyScope,
) -> bool {
    if !principal.enabled || principal.validate().is_err() || requested_scope.validate().is_err() {
        return false;
    }

    principal.preset_grants.iter().any(|grant| {
        grant.preset.actions().contains(&action)
            && scope_covers(action, &grant.scope, requested_scope)
    }) || principal
        .scoped_grants
        .iter()
        .any(|grant| grant.action == action && scope_covers(action, &grant.scope, requested_scope))
}

fn scope_covers(action: SecurityAction, grant: &PolicyScope, requested: &PolicyScope) -> bool {
    if grant == requested {
        return scope_is_applicable(action, grant);
    }

    match (action, grant, requested) {
        (
            SecurityAction::MetadataInspect
            | SecurityAction::DataRead
            | SecurityAction::DataWrite
            | SecurityAction::DataSubscribe
            | SecurityAction::ViewAuthor
            | SecurityAction::TableCreate
            | SecurityAction::SourceLifecycle
            | SecurityAction::WorkloadChange
            | SecurityAction::IndexManage,
            PolicyScope::Namespace {
                namespace: grant_ns,
            },
            PolicyScope::Resource {
                namespace: requested_ns,
                ..
            },
        ) => grant_ns == requested_ns,
        (
            SecurityAction::SecretMetadataRead
            | SecurityAction::SecretManage
            | SecurityAction::SecretRotate,
            PolicyScope::Cluster,
            PolicyScope::Namespace { .. } | PolicyScope::Secret { .. },
        ) => true,
        (
            SecurityAction::SecretMetadataRead
            | SecurityAction::SecretManage
            | SecurityAction::SecretRotate,
            PolicyScope::Namespace {
                namespace: grant_ns,
            },
            PolicyScope::Secret {
                namespace: requested_ns,
                ..
            },
        ) => grant_ns == requested_ns,
        (SecurityAction::SecretRotate, PolicyScope::Cluster, PolicyScope::ClusterKey) => true,
        (
            SecurityAction::ClusterObserve,
            PolicyScope::Cluster,
            PolicyScope::Node { .. } | PolicyScope::Shard { .. },
        ) => true,
        (SecurityAction::NodeDrain, PolicyScope::Cluster, PolicyScope::Node { .. }) => true,
        (SecurityAction::ShardMigrate, PolicyScope::Cluster, PolicyScope::Shard { .. }) => true,
        (
            SecurityAction::OperationCancel | SecurityAction::OperationHistoryRead,
            PolicyScope::Cluster,
            PolicyScope::Operation { .. },
        ) => true,
        (SecurityAction::GrantManage, PolicyScope::Cluster, PolicyScope::Grant { .. }) => true,
        (
            SecurityAction::DiagnosticsRead | SecurityAction::SupportGenerate,
            PolicyScope::Cluster,
            PolicyScope::Node { .. },
        ) => true,
        (
            SecurityAction::SecurityAuditRead,
            PolicyScope::Cluster,
            PolicyScope::AuditRange { .. },
        ) => true,
        (
            SecurityAction::SecurityAuditRead,
            PolicyScope::AuditRange {
                from_ms: grant_from,
                to_ms: grant_to,
            },
            PolicyScope::AuditRange {
                from_ms: requested_from,
                to_ms: requested_to,
            },
        ) => range_contains(*grant_from, *grant_to, *requested_from, *requested_to),
        (
            SecurityAction::CheckpointExport,
            PolicyScope::Cluster,
            PolicyScope::Checkpoint { .. },
        ) => true,
        (
            SecurityAction::BackupCreate,
            PolicyScope::BackupDestination {
                destination: grant_destination,
            },
            PolicyScope::Backup { destination, .. },
        ) => grant_destination == destination,
        _ => false,
    }
}

fn scope_is_applicable(action: SecurityAction, scope: &PolicyScope) -> bool {
    use PolicyScope as Scope;
    use SecurityAction as Action;

    match action {
        Action::MetadataInspect
        | Action::DataRead
        | Action::DataWrite
        | Action::DataSubscribe
        | Action::ViewAuthor
        | Action::TableCreate
        | Action::SourceLifecycle
        | Action::WorkloadChange
        | Action::IndexManage => matches!(scope, Scope::Namespace { .. } | Scope::Resource { .. }),
        Action::SchemaCreate | Action::SchemaDrop | Action::NamespaceCrossAccess => {
            matches!(scope, Scope::Namespace { .. })
        }
        Action::NamespaceCreate | Action::PrincipalManage | Action::AuthConfigManage => {
            matches!(scope, Scope::Cluster)
        }
        Action::GrantManage => matches!(scope, Scope::Cluster | Scope::Grant { .. }),
        Action::SecretMetadataRead | Action::SecretManage => matches!(
            scope,
            Scope::Cluster | Scope::Namespace { .. } | Scope::Secret { .. }
        ),
        Action::SecretRotate => matches!(
            scope,
            Scope::Cluster | Scope::Namespace { .. } | Scope::Secret { .. } | Scope::ClusterKey
        ),
        Action::SecretUse => matches!(
            scope,
            Scope::Secret {
                consumer: Some(_),
                ..
            }
        ),
        Action::SecurityAuditRead => matches!(scope, Scope::Cluster | Scope::AuditRange { .. }),
        Action::ClusterObserve => {
            matches!(
                scope,
                Scope::Cluster | Scope::Node { .. } | Scope::Shard { .. }
            )
        }
        Action::NodeDrain => matches!(scope, Scope::Cluster | Scope::Node { .. }),
        Action::ShardMigrate => matches!(scope, Scope::Cluster | Scope::Shard { .. }),
        Action::OperationCancel | Action::OperationHistoryRead => {
            matches!(scope, Scope::Cluster | Scope::Operation { .. })
        }
        Action::DiagnosticsRead | Action::SupportGenerate => {
            matches!(scope, Scope::Cluster | Scope::Node { .. })
        }
        Action::SupportDownload => matches!(scope, Scope::Artifact { .. }),
        Action::CheckpointExport => matches!(scope, Scope::Cluster | Scope::Checkpoint { .. }),
        Action::CheckpointRestore => matches!(scope, Scope::Checkpoint { .. }),
        Action::BackupCreate => matches!(scope, Scope::BackupDestination { .. }),
        Action::BackupInspect | Action::BackupVerify => {
            matches!(scope, Scope::Backup { .. })
        }
        Action::BackupRestore => matches!(
            scope,
            Scope::Backup {
                target_store: Some(_),
                ..
            }
        ),
    }
}

fn range_contains(
    grant_from: Option<u64>,
    grant_to: Option<u64>,
    requested_from: Option<u64>,
    requested_to: Option<u64>,
) -> bool {
    let starts_before = match (grant_from, requested_from) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(grant), Some(requested)) => grant <= requested,
    };
    let ends_after = match (grant_to, requested_to) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(grant), Some(requested)) => grant >= requested,
    };
    starts_before && ends_after
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::{
        authorization::{
            LoginMethod, PermissionPreset, PolicyScope, ScopedActionGrant, ScopedPresetGrant,
            SECURITY_PRINCIPAL_FORMAT_VERSION,
        },
        ids::PrincipalId,
    };

    fn principal(
        presets: Vec<ScopedPresetGrant>,
        scoped_grants: Vec<ScopedActionGrant>,
    ) -> SecurityPrincipal {
        SecurityPrincipal {
            format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
            id: PrincipalId(12),
            display_name: "service principal".to_string(),
            enabled: true,
            revision: 1,
            login_methods: vec![LoginMethod::Oidc {
                issuer: "https://issuer.example".to_string(),
                subject: "worker-12".to_string(),
            }],
            preset_grants: presets,
            scoped_grants,
        }
    }

    #[test]
    fn operator_preset_does_not_imply_data_secret_or_grant_authority() {
        let policy = principal(
            vec![ScopedPresetGrant {
                preset: PermissionPreset::Operator,
                scope: PolicyScope::Cluster,
            }],
            vec![],
        );

        assert!(allows(
            &policy,
            SecurityAction::NodeDrain,
            &PolicyScope::Node {
                node_id: "worker-1".to_string()
            }
        ));
        assert!(!allows(
            &policy,
            SecurityAction::DataRead,
            &PolicyScope::Resource {
                namespace: "sales".to_string(),
                kind: rockstream_types::authorization::ResourceKind::Table,
                name: "orders".to_string(),
            }
        ));
        assert!(!allows(
            &policy,
            SecurityAction::SecretUse,
            &PolicyScope::Secret {
                namespace: "sales".to_string(),
                name: "warehouse_password".to_string(),
                consumer: Some(PrincipalId(12)),
            }
        ));
        assert!(!allows(
            &policy,
            SecurityAction::SecretMetadataRead,
            &PolicyScope::Namespace {
                namespace: "sales".to_string(),
            }
        ));
        assert!(!allows(
            &policy,
            SecurityAction::SecretManage,
            &PolicyScope::Secret {
                namespace: "sales".to_string(),
                name: "warehouse_password".to_string(),
                consumer: None,
            }
        ));
        assert!(!allows(
            &policy,
            SecurityAction::GrantManage,
            &PolicyScope::Grant {
                principal_id: PrincipalId(15),
                action: SecurityAction::DataRead,
                scope: Box::new(PolicyScope::Namespace {
                    namespace: "sales".to_string(),
                }),
            }
        ));
    }

    #[test]
    fn independently_granted_secret_use_is_consumer_and_secret_scoped() {
        let policy = principal(
            vec![ScopedPresetGrant {
                preset: PermissionPreset::Operator,
                scope: PolicyScope::Cluster,
            }],
            vec![ScopedActionGrant {
                action: SecurityAction::SecretUse,
                scope: PolicyScope::Secret {
                    namespace: "sales".to_string(),
                    name: "warehouse_password".to_string(),
                    consumer: Some(PrincipalId(12)),
                },
            }],
        );
        let granted = PolicyScope::Secret {
            namespace: "sales".to_string(),
            name: "warehouse_password".to_string(),
            consumer: Some(PrincipalId(12)),
        };
        assert!(allows(&policy, SecurityAction::SecretUse, &granted));
        assert!(!allows(
            &policy,
            SecurityAction::SecretUse,
            &PolicyScope::Secret {
                namespace: "sales".to_string(),
                name: "warehouse_password".to_string(),
                consumer: Some(PrincipalId(13)),
            }
        ));
        assert!(!allows(
            &policy,
            SecurityAction::SecretUse,
            &PolicyScope::Secret {
                namespace: "sales".to_string(),
                name: "other_secret".to_string(),
                consumer: Some(PrincipalId(12)),
            }
        ));
        assert!(!allows(
            &policy,
            SecurityAction::SecretManage,
            &PolicyScope::Secret {
                namespace: "sales".to_string(),
                name: "warehouse_password".to_string(),
                consumer: None,
            }
        ));
    }

    #[test]
    fn administrator_preset_does_not_imply_data_secret_use_or_operator_actions() {
        let policy = principal(
            vec![ScopedPresetGrant {
                preset: PermissionPreset::Administrator,
                scope: PolicyScope::Cluster,
            }],
            vec![],
        );
        assert!(allows(
            &policy,
            SecurityAction::GrantManage,
            &PolicyScope::Grant {
                principal_id: PrincipalId(14),
                action: SecurityAction::DataRead,
                scope: Box::new(PolicyScope::Namespace {
                    namespace: "sales".to_string(),
                }),
            }
        ));
        assert!(!allows(
            &policy,
            SecurityAction::DataRead,
            &PolicyScope::Resource {
                namespace: "sales".to_string(),
                kind: rockstream_types::authorization::ResourceKind::Table,
                name: "orders".to_string(),
            }
        ));
        assert!(!allows(
            &policy,
            SecurityAction::SecretUse,
            &PolicyScope::Secret {
                namespace: "sales".to_string(),
                name: "warehouse_password".to_string(),
                consumer: Some(PrincipalId(12)),
            }
        ));
        assert!(!allows(
            &policy,
            SecurityAction::NodeDrain,
            &PolicyScope::Node {
                node_id: "worker-1".to_string(),
            }
        ));
    }

    #[test]
    fn administrator_secret_actions_cover_secret_scopes_without_secret_use() {
        let policy = principal(
            vec![ScopedPresetGrant {
                preset: PermissionPreset::Administrator,
                scope: PolicyScope::Cluster,
            }],
            vec![],
        );
        let secret = PolicyScope::Secret {
            namespace: "sales".to_string(),
            name: "warehouse_password".to_string(),
            consumer: None,
        };

        assert!(allows(
            &policy,
            SecurityAction::SecretMetadataRead,
            &PolicyScope::Namespace {
                namespace: "sales".to_string(),
            }
        ));
        assert!(allows(&policy, SecurityAction::SecretManage, &secret));
        assert!(allows(&policy, SecurityAction::SecretRotate, &secret));
        assert!(allows(
            &policy,
            SecurityAction::SecretRotate,
            &PolicyScope::ClusterKey
        ));
        assert!(!allows(
            &policy,
            SecurityAction::SecretUse,
            &PolicyScope::Secret {
                namespace: "sales".to_string(),
                name: "warehouse_password".to_string(),
                consumer: Some(PrincipalId(12)),
            }
        ));
    }

    #[test]
    fn namespace_secret_admin_grant_cannot_cross_namespaces() {
        let policy = principal(
            vec![ScopedPresetGrant {
                preset: PermissionPreset::Administrator,
                scope: PolicyScope::Namespace {
                    namespace: "sales".to_string(),
                },
            }],
            vec![],
        );
        assert!(allows(
            &policy,
            SecurityAction::SecretManage,
            &PolicyScope::Secret {
                namespace: "sales".to_string(),
                name: "warehouse_password".to_string(),
                consumer: None,
            }
        ));
        assert!(!allows(
            &policy,
            SecurityAction::SecretManage,
            &PolicyScope::Secret {
                namespace: "finance".to_string(),
                name: "ledger_password".to_string(),
                consumer: None,
            }
        ));
        assert!(!allows(
            &policy,
            SecurityAction::SecretMetadataRead,
            &PolicyScope::Cluster
        ));
    }

    #[test]
    fn namespace_data_reader_grant_stays_in_its_namespace() {
        let policy = principal(
            vec![ScopedPresetGrant {
                preset: PermissionPreset::DataReader,
                scope: PolicyScope::Namespace {
                    namespace: "sales".to_string(),
                },
            }],
            vec![],
        );
        assert!(allows(
            &policy,
            SecurityAction::DataRead,
            &PolicyScope::Resource {
                namespace: "sales".to_string(),
                kind: rockstream_types::authorization::ResourceKind::Table,
                name: "orders".to_string(),
            }
        ));
        assert!(!allows(
            &policy,
            SecurityAction::DataRead,
            &PolicyScope::Resource {
                namespace: "finance".to_string(),
                kind: rockstream_types::authorization::ResourceKind::Table,
                name: "ledger".to_string(),
            }
        ));
    }

    #[test]
    fn disabled_and_malformed_principals_deny() {
        let mut policy = principal(vec![], vec![]);
        policy.enabled = false;
        assert!(!allows(
            &policy,
            SecurityAction::DataRead,
            &PolicyScope::Namespace {
                namespace: "sales".to_string(),
            }
        ));
    }
}
