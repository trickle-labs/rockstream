//! Logical plan binding and view references.

use crate::SqlError;
use datafusion::logical_expr::LogicalPlan;

/// Stub structure for binding logical plans.
pub struct SqlBinder;

impl SqlBinder {
    pub fn new() -> Self {
        Self
    }

    pub fn bind(&self, plan: LogicalPlan) -> Result<LogicalPlan, SqlError> {
        Ok(plan)
    }
}

impl Default for SqlBinder {
    fn default() -> Self {
        Self::new()
    }
}
