//! RBAC role and ACL entry types for v0.26.
use serde::{Deserialize, Serialize};

/// Role in the RBAC system. Ordered by privilege level.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Role {
    Viewer,
    PipelineOwner,
    Admin,
}

/// An ACL grant entry persisted under catalog/acl/<namespace>/<principal>.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclEntry {
    pub principal: String, // JWT sub or cert CN
    pub namespace: String,
    pub view_name: Option<String>, // None = namespace-level grant
    pub role: Role,
}
