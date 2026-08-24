//! Embedded runtime capability registry for RockStream (OBS-01).
//!
//! Provides compile-time embedded access to `capabilities.toml` with zero runtime drift.

use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

/// Behavioral specification statement and verification proof for a capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityBehavior {
    pub behavior: String,
    pub statement: String,
    #[serde(default)]
    pub proof: Option<String>,
    #[serde(default)]
    pub paired_proof: Option<String>,
    #[serde(default)]
    pub bound: Option<String>,
    #[serde(default)]
    pub metric: Option<String>,
    #[serde(default)]
    pub on_bound: Option<String>,
}

/// Dispatch mapping entry connecting a capability to implementation code symbols.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityDispatch {
    pub id: String,
    pub path: String,
    pub symbol: String,
    pub surface: String,
}

/// Single capability entry declared in the capability specification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityEntry {
    pub id: String,
    pub kind: String,
    pub name: String,
    pub tier: String,
    pub reachability: String,
    #[serde(default)]
    pub dispatch: Vec<String>,
    #[serde(default)]
    pub proof: Option<String>,
    #[serde(default)]
    pub documentation: Option<String>,
    #[serde(default)]
    pub behavior: Vec<CapabilityBehavior>,
}

impl CapabilityEntry {
    /// Number of dispatch symbols associated with this capability.
    pub fn dispatch_count(&self) -> usize {
        self.dispatch.len()
    }

    /// Verification proof reference identifier or path.
    pub fn proof_ref(&self) -> &str {
        self.proof.as_deref().unwrap_or("")
    }

    /// Documentation anchor reference.
    pub fn doc_anchor(&self) -> &str {
        self.documentation.as_deref().unwrap_or("")
    }
}

/// Tier decision audit record for capability promotion/demotion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierDecision {
    pub capability: String,
    pub old_tier: String,
    pub new_tier: String,
    pub reason: String,
    pub evidence: String,
}

/// Contract header information.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityContract {
    pub version: String,
    pub roadmap: String,
    pub promise: String,
    #[serde(default)]
    pub tier_decision: Vec<TierDecision>,
}

/// Complete parsed `capabilities.toml` document structure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityDocument {
    pub contract: CapabilityContract,
    #[serde(default)]
    pub dispatch: Vec<CapabilityDispatch>,
    #[serde(rename = "capability", default)]
    pub capabilities: Vec<CapabilityEntry>,
}

/// Immutable in-memory runtime capability registry.
pub struct CapabilityRegistry {
    document: CapabilityDocument,
}

static CURRENT_REGISTRY: LazyLock<CapabilityRegistry> = LazyLock::new(|| {
    const TOML_CONTENT: &str = include_str!("../../../capabilities.toml");
    let document: CapabilityDocument =
        toml::from_str(TOML_CONTENT).expect("embedded capabilities.toml must be valid TOML");
    CapabilityRegistry { document }
});

impl CapabilityRegistry {
    /// Retrieve reference to the singleton compile-time embedded capability registry.
    pub fn current() -> &'static Self {
        &CURRENT_REGISTRY
    }

    /// Slice of all registered capability entries.
    pub fn capabilities(&self) -> &[CapabilityEntry] {
        &self.document.capabilities
    }

    /// Slice of all registered dispatch definitions.
    pub fn dispatches(&self) -> &[CapabilityDispatch] {
        &self.document.dispatch
    }

    /// Contract metadata.
    pub fn contract(&self) -> &CapabilityContract {
        &self.document.contract
    }

    /// Lookup a capability by its unique identifier (e.g. "language.query-read").
    pub fn get_by_id(&self, id: &str) -> Option<&CapabilityEntry> {
        self.document.capabilities.iter().find(|c| c.id == id)
    }

    /// Filter capabilities by kind (e.g. "language", "connector", "sink").
    pub fn filter_by_kind(&self, kind: &str) -> Vec<&CapabilityEntry> {
        self.document
            .capabilities
            .iter()
            .filter(|c| c.kind.eq_ignore_ascii_case(kind))
            .collect()
    }

    /// Filter capabilities by tier (e.g. "Core", "Maintain", "Experimental").
    pub fn filter_by_tier(&self, tier: &str) -> Vec<&CapabilityEntry> {
        self.document
            .capabilities
            .iter()
            .filter(|c| c.tier.eq_ignore_ascii_case(tier))
            .collect()
    }
}
