//! Fixed v0.73 authorization actions, presets, login methods, and scopes.
//!
//! These are product policy types, not a user-defined policy language. Runtime
//! callers must supply the authenticated principal and the exact resource scope.

use serde::{Deserialize, Serialize};

use crate::ids::PrincipalId;

pub const SECURITY_PRINCIPAL_FORMAT_VERSION: u16 = 1;
pub const SECURITY_AUDIT_FORMAT_VERSION: u16 = 1;
pub const SECURITY_AUDIT_DEFAULT_PAGE_ROWS: usize = 100;
pub const SECURITY_AUDIT_MAX_PAGE_ROWS: usize = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityAction {
    MetadataInspect,
    DataRead,
    DataWrite,
    DataSubscribe,
    ViewAuthor,
    TableCreate,
    SourceLifecycle,
    WorkloadChange,
    SchemaCreate,
    SchemaDrop,
    NamespaceCreate,
    IndexManage,
    CheckpointExport,
    CheckpointRestore,
    PrincipalManage,
    GrantManage,
    AuthConfigManage,
    SecretMetadataRead,
    SecretManage,
    SecretRotate,
    SecretUse,
    SecurityAuditRead,
    NamespaceCrossAccess,
    ClusterObserve,
    NodeDrain,
    ShardMigrate,
    OperationCancel,
    OperationHistoryRead,
    DiagnosticsRead,
    SupportGenerate,
    SupportDownload,
    BackupCreate,
    BackupInspect,
    BackupVerify,
    BackupRestore,
}

impl SecurityAction {
    /// Actions that can be assigned as individual grants outside fixed presets.
    pub const fn permits_explicit_scoped_grant(self) -> bool {
        matches!(
            self,
            Self::SecretUse
                | Self::CheckpointExport
                | Self::CheckpointRestore
                | Self::BackupCreate
                | Self::BackupInspect
                | Self::BackupVerify
                | Self::BackupRestore
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionPreset {
    DataReader,
    Builder,
    Administrator,
    Operator,
}

const DATA_READER_ACTIONS: &[SecurityAction] = &[
    SecurityAction::MetadataInspect,
    SecurityAction::DataRead,
    SecurityAction::DataSubscribe,
];

const BUILDER_ACTIONS: &[SecurityAction] = &[
    SecurityAction::MetadataInspect,
    SecurityAction::DataRead,
    SecurityAction::DataSubscribe,
    SecurityAction::DataWrite,
    SecurityAction::ViewAuthor,
    SecurityAction::TableCreate,
    SecurityAction::SourceLifecycle,
    SecurityAction::WorkloadChange,
    SecurityAction::SchemaCreate,
];

const ADMINISTRATOR_ACTIONS: &[SecurityAction] = &[
    SecurityAction::PrincipalManage,
    SecurityAction::GrantManage,
    SecurityAction::AuthConfigManage,
    SecurityAction::SecretMetadataRead,
    SecurityAction::SecretManage,
    SecurityAction::SecretRotate,
    SecurityAction::SecurityAuditRead,
    SecurityAction::SchemaDrop,
    SecurityAction::NamespaceCreate,
    SecurityAction::IndexManage,
];

const OPERATOR_ACTIONS: &[SecurityAction] = &[
    SecurityAction::ClusterObserve,
    SecurityAction::NodeDrain,
    SecurityAction::ShardMigrate,
    SecurityAction::OperationCancel,
    SecurityAction::OperationHistoryRead,
    SecurityAction::DiagnosticsRead,
    SecurityAction::SupportGenerate,
    SecurityAction::SupportDownload,
];

impl PermissionPreset {
    pub const fn actions(self) -> &'static [SecurityAction] {
        match self {
            Self::DataReader => DATA_READER_ACTIONS,
            Self::Builder => BUILDER_ACTIONS,
            Self::Administrator => ADMINISTRATOR_ACTIONS,
            Self::Operator => OPERATOR_ACTIONS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Table,
    View,
    Source,
    Sink,
    Workload,
    Schema,
    Index,
}

/// Exact scope names used by the fixed action registry.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum PolicyScope {
    Cluster,
    Namespace {
        namespace: String,
    },
    Resource {
        namespace: String,
        kind: ResourceKind,
        name: String,
    },
    Secret {
        namespace: String,
        name: String,
        consumer: Option<PrincipalId>,
    },
    ClusterKey,
    Node {
        node_id: String,
    },
    Shard {
        shard_id: String,
    },
    Operation {
        operation_id: String,
    },
    Artifact {
        opaque_id: String,
    },
    Checkpoint {
        checkpoint_id: String,
        target_store: Option<String>,
    },
    BackupDestination {
        destination: String,
    },
    Backup {
        destination: String,
        backup_id: String,
        target_store: Option<String>,
    },
    Principal {
        principal_id: PrincipalId,
    },
    Grant {
        principal_id: PrincipalId,
        action: SecurityAction,
        scope: Box<PolicyScope>,
    },
    AuditRange {
        from_ms: Option<u64>,
        to_ms: Option<u64>,
    },
}

impl PolicyScope {
    pub fn validate(&self) -> Result<(), AuthorizationModelError> {
        let nonempty = |value: &str| !value.trim().is_empty();
        match self {
            Self::Cluster | Self::ClusterKey => Ok(()),
            Self::Namespace { namespace } if nonempty(namespace) => Ok(()),
            Self::Resource {
                namespace, name, ..
            } if nonempty(namespace) && nonempty(name) => Ok(()),
            Self::Secret {
                namespace, name, ..
            } if nonempty(namespace) && nonempty(name) => Ok(()),
            Self::Node { node_id } if nonempty(node_id) => Ok(()),
            Self::Shard { shard_id } if nonempty(shard_id) => Ok(()),
            Self::Operation { operation_id } if nonempty(operation_id) => Ok(()),
            Self::Artifact { opaque_id }
                if nonempty(opaque_id)
                    && !opaque_id.contains('/')
                    && !opaque_id.contains('\\')
                    && opaque_id != "."
                    && opaque_id != ".." =>
            {
                Ok(())
            }
            Self::Checkpoint {
                checkpoint_id,
                target_store,
            } if nonempty(checkpoint_id)
                && target_store.as_ref().is_none_or(|store| nonempty(store)) =>
            {
                Ok(())
            }
            Self::BackupDestination { destination } if nonempty(destination) => Ok(()),
            Self::Backup {
                destination,
                backup_id,
                target_store,
            } if nonempty(destination)
                && nonempty(backup_id)
                && target_store.as_ref().is_none_or(|store| nonempty(store)) =>
            {
                Ok(())
            }
            Self::Principal { principal_id } if principal_id.0 > 0 => Ok(()),
            Self::Grant {
                principal_id,
                scope,
                ..
            } if principal_id.0 > 0 => scope.validate(),
            Self::AuditRange { from_ms, to_ms }
                if from_ms.zip(*to_ms).is_none_or(|(from, to)| from <= to) =>
            {
                Ok(())
            }
            _ => Err(AuthorizationModelError::InvalidScope),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedPresetGrant {
    pub preset: PermissionPreset,
    pub scope: PolicyScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedActionGrant {
    pub action: SecurityAction,
    pub scope: PolicyScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum LoginMethod {
    ScramSha256 {
        username: String,
        salt: Vec<u8>,
        iterations: u32,
        stored_key: Vec<u8>,
        server_key: Vec<u8>,
    },
    Oidc {
        issuer: String,
        subject: String,
    },
    Mtls {
        certificate_sha256: [u8; 32],
    },
    Service {
        service_id: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum LoginIdentityKey<'a> {
    Scram(&'a str),
    Oidc(&'a str, &'a str),
    Mtls(&'a [u8; 32]),
    Service(&'a str),
}

impl LoginMethod {
    fn identity_key(&self) -> LoginIdentityKey<'_> {
        match self {
            Self::ScramSha256 { username, .. } => LoginIdentityKey::Scram(username),
            Self::Oidc { issuer, subject } => LoginIdentityKey::Oidc(issuer, subject),
            Self::Mtls { certificate_sha256 } => LoginIdentityKey::Mtls(certificate_sha256),
            Self::Service { service_id } => LoginIdentityKey::Service(service_id),
        }
    }

    fn is_interactive(&self) -> bool {
        !matches!(self, Self::Service { .. })
    }
}

/// Durable principal and its current fixed-preset and scoped grants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityPrincipal {
    pub format_version: u16,
    pub id: PrincipalId,
    pub display_name: String,
    pub enabled: bool,
    pub revision: u64,
    pub login_methods: Vec<LoginMethod>,
    pub preset_grants: Vec<ScopedPresetGrant>,
    pub scoped_grants: Vec<ScopedActionGrant>,
}

impl SecurityPrincipal {
    pub fn validate(&self) -> Result<(), AuthorizationModelError> {
        if self.format_version != SECURITY_PRINCIPAL_FORMAT_VERSION
            || self.id.0 == 0
            || self.display_name.trim().is_empty()
            || self.revision == 0
        {
            return Err(AuthorizationModelError::InvalidPrincipal);
        }
        if self.enabled && self.login_methods.is_empty() {
            return Err(AuthorizationModelError::InvalidLoginMethod);
        }

        let mut login_keys = std::collections::HashSet::new();
        for method in &self.login_methods {
            if !login_keys.insert(method.identity_key()) {
                return Err(AuthorizationModelError::DuplicateLoginIdentity);
            }
            match method {
                LoginMethod::ScramSha256 {
                    username,
                    salt,
                    iterations,
                    stored_key,
                    server_key,
                } if username.trim().is_empty()
                    || salt.len() != 16
                    || *iterations == 0
                    || stored_key.len() != 32
                    || server_key.len() != 32 =>
                {
                    return Err(AuthorizationModelError::InvalidLoginMethod);
                }
                LoginMethod::Oidc { issuer, subject }
                    if issuer.trim().is_empty() || subject.trim().is_empty() =>
                {
                    return Err(AuthorizationModelError::InvalidLoginMethod);
                }
                LoginMethod::Service { service_id } if service_id.trim().is_empty() => {
                    return Err(AuthorizationModelError::InvalidLoginMethod);
                }
                _ => {}
            }
        }

        let service_only = !self.login_methods.is_empty()
            && self
                .login_methods
                .iter()
                .all(|method| !method.is_interactive());
        if service_only
            && self.preset_grants.iter().any(|grant| {
                matches!(
                    grant.preset,
                    PermissionPreset::Administrator | PermissionPreset::Operator
                )
            })
        {
            return Err(AuthorizationModelError::ServicePrincipalPresetDenied);
        }

        let mut preset_grants = std::collections::HashSet::new();
        for grant in &self.preset_grants {
            grant.scope.validate()?;
            if !preset_grants.insert(grant) {
                return Err(AuthorizationModelError::DuplicateGrant);
            }
        }
        let mut scoped_grants = std::collections::HashSet::new();
        for grant in &self.scoped_grants {
            grant.scope.validate()?;
            if !grant.action.permits_explicit_scoped_grant() {
                return Err(AuthorizationModelError::ActionRequiresPreset);
            }
            if matches!(grant.action, SecurityAction::SecretUse)
                && !matches!(
                    &grant.scope,
                    PolicyScope::Secret {
                        consumer: Some(_),
                        ..
                    }
                )
            {
                return Err(AuthorizationModelError::SecretConsumerRequired);
            }
            if !scoped_grants.insert(grant) {
                return Err(AuthorizationModelError::DuplicateGrant);
            }
        }
        Ok(())
    }

    /// Reject login methods that would map one credential identity to multiple
    /// principals in the same durable catalog.
    pub fn validate_unique_login_identities<'a>(
        principals: impl IntoIterator<Item = &'a SecurityPrincipal>,
    ) -> Result<(), AuthorizationModelError> {
        let mut identities = std::collections::HashSet::new();
        for principal in principals {
            for method in &principal.login_methods {
                if !identities.insert(method.identity_key()) {
                    return Err(AuthorizationModelError::DuplicateLoginIdentity);
                }
            }
        }
        Ok(())
    }

    pub fn has_interactive_login(&self) -> bool {
        self.login_methods.iter().any(LoginMethod::is_interactive)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum AuditIdentity {
    Verified(String),
    Unverified(String),
    Unknown,
    NotApplicable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum AuditField<T> {
    Known(T),
    Unverified(T),
    Unknown,
    NotApplicable,
}

/// Correlation is either observed by the listener or explicitly absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum AuditCorrelation {
    Observed(String),
    Unknown,
    NotApplicable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityAuditClass {
    AuthenticationSuccess,
    AuthenticationFailure,
    AuthorizationDenial,
    GrantChange,
    SecretRotation,
    NodeIdentityRejection,
    AdminOperationStart,
    AdminOperationCompletion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityAuditOutcome {
    Success,
    Denied,
    Failure,
}

/// Structured durable security event. The record intentionally carries status
/// values for fields that do not exist or were not verified at the listener.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityAuditRecord {
    pub format_version: u16,
    pub event_id: String,
    pub timestamp_ms: u64,
    pub actor: AuditIdentity,
    pub target: AuditField<String>,
    pub scope: AuditField<String>,
    pub action: SecurityAuditClass,
    pub outcome: SecurityAuditOutcome,
    pub correlation: AuditCorrelation,
    pub operation_id: AuditField<String>,
    pub policy_revision: AuditField<u64>,
    pub before_state: AuditField<String>,
    pub after_state: AuditField<String>,
}

impl SecurityAuditRecord {
    pub fn validate(&self) -> Result<(), AuthorizationModelError> {
        if self.format_version != SECURITY_AUDIT_FORMAT_VERSION
            || self.event_id.trim().is_empty()
            || self.timestamp_ms == 0
        {
            return Err(AuthorizationModelError::InvalidAuditRecord);
        }
        if matches!(&self.actor, AuditIdentity::Verified(id) | AuditIdentity::Unverified(id) if id.trim().is_empty())
            || matches!(&self.correlation, AuditCorrelation::Observed(id) if id.trim().is_empty())
            || [&self.target, &self.scope, &self.operation_id, &self.before_state, &self.after_state]
                .iter()
                .any(|field| matches!(field, AuditField::Known(value) | AuditField::Unverified(value) if value.trim().is_empty()))
        {
            return Err(AuthorizationModelError::InvalidAuditRecord);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationModelError {
    InvalidPrincipal,
    DuplicateLoginIdentity,
    InvalidLoginMethod,
    ServicePrincipalPresetDenied,
    InvalidScope,
    DuplicateGrant,
    ActionRequiresPreset,
    SecretConsumerRequired,
    InvalidAuditRecord,
}

impl std::fmt::Display for AuthorizationModelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidPrincipal => {
                "security principal is invalid or uses an unsupported record version"
            }
            Self::DuplicateLoginIdentity => {
                "login identity is mapped more than once in the security catalog"
            }
            Self::InvalidLoginMethod => "login method is malformed",
            Self::ServicePrincipalPresetDenied => {
                "service-only principals cannot receive administrator or operator presets"
            }
            Self::InvalidScope => "grant scope is malformed",
            Self::DuplicateGrant => "the grant is duplicated",
            Self::ActionRequiresPreset => "action cannot be assigned as an individual scoped grant",
            Self::SecretConsumerRequired => "secret.use requires a consumer-bound secret scope",
            Self::InvalidAuditRecord => "security audit record is malformed",
        })
    }
}

impl std::error::Error for AuthorizationModelError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_preset_action_matrix_matches_the_v073_profile() {
        assert_eq!(
            PermissionPreset::DataReader.actions(),
            &[
                SecurityAction::MetadataInspect,
                SecurityAction::DataRead,
                SecurityAction::DataSubscribe,
            ]
        );
        assert_eq!(
            PermissionPreset::Builder.actions(),
            &[
                SecurityAction::MetadataInspect,
                SecurityAction::DataRead,
                SecurityAction::DataSubscribe,
                SecurityAction::DataWrite,
                SecurityAction::ViewAuthor,
                SecurityAction::TableCreate,
                SecurityAction::SourceLifecycle,
                SecurityAction::WorkloadChange,
                SecurityAction::SchemaCreate,
            ]
        );
        assert_eq!(
            PermissionPreset::Administrator.actions(),
            &[
                SecurityAction::PrincipalManage,
                SecurityAction::GrantManage,
                SecurityAction::AuthConfigManage,
                SecurityAction::SecretMetadataRead,
                SecurityAction::SecretManage,
                SecurityAction::SecretRotate,
                SecurityAction::SecurityAuditRead,
                SecurityAction::SchemaDrop,
                SecurityAction::NamespaceCreate,
                SecurityAction::IndexManage,
            ]
        );
        assert_eq!(
            PermissionPreset::Operator.actions(),
            &[
                SecurityAction::ClusterObserve,
                SecurityAction::NodeDrain,
                SecurityAction::ShardMigrate,
                SecurityAction::OperationCancel,
                SecurityAction::OperationHistoryRead,
                SecurityAction::DiagnosticsRead,
                SecurityAction::SupportGenerate,
                SecurityAction::SupportDownload,
            ]
        );
    }

    #[test]
    fn security_principal_rejects_unscoped_secret_use_and_service_preset_escalation() {
        let principal = SecurityPrincipal {
            format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
            id: PrincipalId(7),
            display_name: "automation".to_string(),
            enabled: true,
            revision: 1,
            login_methods: vec![LoginMethod::Service {
                service_id: "connector/warehouse".to_string(),
            }],
            preset_grants: vec![ScopedPresetGrant {
                preset: PermissionPreset::Operator,
                scope: PolicyScope::Cluster,
            }],
            scoped_grants: vec![],
        };
        assert_eq!(
            principal.validate(),
            Err(AuthorizationModelError::ServicePrincipalPresetDenied)
        );

        let principal = SecurityPrincipal {
            preset_grants: vec![],
            scoped_grants: vec![ScopedActionGrant {
                action: SecurityAction::SecretUse,
                scope: PolicyScope::Secret {
                    namespace: "sales".to_string(),
                    name: "warehouse_password".to_string(),
                    consumer: None,
                },
            }],
            ..principal
        };
        assert_eq!(
            principal.validate(),
            Err(AuthorizationModelError::SecretConsumerRequired)
        );
    }

    #[test]
    fn oidc_login_identity_is_the_issuer_and_subject_pair() {
        let principal = SecurityPrincipal {
            format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
            id: PrincipalId(8),
            display_name: "Alice".to_string(),
            enabled: true,
            revision: 1,
            login_methods: vec![LoginMethod::Oidc {
                issuer: "https://issuer.example".to_string(),
                subject: "alice".to_string(),
            }],
            preset_grants: vec![],
            scoped_grants: vec![],
        };
        assert!(principal.has_interactive_login());
        assert_eq!(principal.validate(), Ok(()));
    }

    #[test]
    fn enabled_principal_requires_a_login_identity() {
        let principal = SecurityPrincipal {
            format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
            id: PrincipalId(9),
            display_name: "unreachable administrator".to_string(),
            enabled: true,
            revision: 1,
            login_methods: vec![],
            preset_grants: vec![ScopedPresetGrant {
                preset: PermissionPreset::Administrator,
                scope: PolicyScope::Cluster,
            }],
            scoped_grants: vec![],
        };
        assert_eq!(
            principal.validate(),
            Err(AuthorizationModelError::InvalidLoginMethod)
        );
    }

    #[test]
    fn audit_record_keeps_unverified_and_absent_fields_explicit() {
        let event = SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "event-1".to_string(),
            timestamp_ms: 1,
            actor: AuditIdentity::Unverified("presented-user".to_string()),
            target: AuditField::Unknown,
            scope: AuditField::NotApplicable,
            action: SecurityAuditClass::AuthenticationFailure,
            outcome: SecurityAuditOutcome::Failure,
            correlation: AuditCorrelation::NotApplicable,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Unknown,
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        };
        assert_eq!(event.validate(), Ok(()));
        assert_ne!(
            event.actor,
            AuditIdentity::Verified("presented-user".to_string())
        );
    }
}
