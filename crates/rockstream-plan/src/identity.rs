//! Compiled-plan identity metadata and compatibility validation (v0.63 Slice 5).
//!
//! Captures the 7 mandatory identity fields for safe view reconstruction across restarts:
//! 1. `sql`: original SQL query text
//! 2. `ast_hash`: SHA-256 hash of parsed SQL AST
//! 3. `logical_plan_hash`: SHA-256 hash of normalized DataFusion logical plan
//! 4. `compiler_version`: semantic version of compiler/lowering engine
//! 5. `state_layout_version`: layout format version of operator arrangements
//! 6. `output_schema`: complete serialized Arrow Schema
//! 7. `dependency_ids`: complete list of prerequisite TableId and ViewId dependencies

use rockstream_types::ids::CompiledPlanId;
use serde::{Deserialize, Serialize};

/// Current compiler version of the running engine.
pub const CURRENT_COMPILER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Current state layout format version for operator arrangements.
pub const CURRENT_STATE_LAYOUT_VERSION: u32 = 1;

/// Seven compiled plan identity fields for safe gateway view reconstruction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledPlanRecord {
    pub id: CompiledPlanId,
    pub sql: String,
    pub ast_hash: [u8; 32],
    pub logical_plan_hash: [u8; 32],
    pub compiler_version: String,
    pub state_layout_version: u32,
    pub output_schema: Vec<u8>,
    pub dependency_ids: Vec<u128>,
}

impl CompiledPlanRecord {
    /// Create a new compiled plan record.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: CompiledPlanId,
        sql: impl Into<String>,
        ast_hash: [u8; 32],
        logical_plan_hash: [u8; 32],
        compiler_version: impl Into<String>,
        state_layout_version: u32,
        output_schema: Vec<u8>,
        dependency_ids: Vec<u128>,
    ) -> Self {
        Self {
            id,
            sql: sql.into(),
            ast_hash,
            logical_plan_hash,
            compiler_version: compiler_version.into(),
            state_layout_version,
            output_schema,
            dependency_ids,
        }
    }

    /// Validate compatibility of compiler version and state layout version.
    pub fn validate_compatibility(
        &self,
        runtime_compiler_version: &str,
        runtime_layout_version: u32,
    ) -> Result<(), String> {
        if self.compiler_version != runtime_compiler_version {
            return Err(format!(
                "[RS-1002] Incompatible compiler version: stored {}, runtime {}; next_steps: recompile the view under the current compiler",
                self.compiler_version, runtime_compiler_version
            ));
        }

        if self.state_layout_version != runtime_layout_version {
            return Err(format!(
                "[RS-1002] Incompatible state layout version: stored {}, runtime {}; next_steps: migrate the state layout or recreate the view",
                self.state_layout_version, runtime_layout_version
            ));
        }

        Ok(())
    }

    /// Validate that all referenced dependencies exist.
    pub fn validate_dependencies(&self, known_object_ids: &[u128]) -> Result<(), String> {
        for &dep in &self.dependency_ids {
            if !known_object_ids.contains(&dep) {
                return Err(format!(
                    "[RS-1002] Broken view dependency: referenced object ID {} not found; next_steps: ensure all dependent tables and views exist",
                    dep
                ));
            }
        }
        Ok(())
    }
}
