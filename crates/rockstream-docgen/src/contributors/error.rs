//! Error surface contributor (DOC-001).

use crate::manifest::{ErrorSurface, ErrorSurfaceEntry};

pub struct ErrorContributor;

impl ErrorContributor {
    /// Extract authoritative error descriptors from `contracts/errors.toml`.
    pub fn extract() -> ErrorSurface {
        let catalog = rockstream_types::error_code::ErrorCatalog::current();
        let mut errors: Vec<ErrorSurfaceEntry> = catalog
            .errors()
            .iter()
            .map(|e| ErrorSurfaceEntry {
                code: e.code.to_string(),
                key: e.key.clone(),
                title: e.title.clone(),
                severity: e.severity.to_string(),
                sqlstate: e.sqlstate.clone(),
                retry_class: e.retry_class.to_string(),
                default_next_steps: e.default_next_steps.clone(),
                doc_anchor: e.doc_anchor.clone(),
            })
            .collect();

        errors.sort_by(|a, b| a.code.cmp(&b.code));
        ErrorSurface { errors }
    }
}
