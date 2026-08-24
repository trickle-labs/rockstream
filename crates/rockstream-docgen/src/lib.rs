//! RockStream Product Surface Manifest & Docgen Engine (DOC-001, DOC-004).
//!
//! Provides canonical data models, surface contributors, deterministic generation,
//! and contract verification for the unified RockStream product surface.

pub mod contributors;
pub mod generator;
pub mod manifest;
pub mod sql_matrix;

pub use generator::generate_manifest;
pub use manifest::ProductSurfaceManifest;
pub use sql_matrix::{SqlMatrixDocument, SqlMatrixError};
