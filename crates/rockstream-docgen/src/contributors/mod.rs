//! Modular product surface contributors (DOC-001).

pub mod catalog;
pub mod cli;
pub mod config;
pub mod error;
pub mod function;
pub mod metric;
pub mod sql_matrix;

pub use catalog::CatalogContributor;
pub use cli::CliContributor;
pub use config::ConfigContributor;
pub use error::ErrorContributor;
pub use function::FunctionContributor;
pub use metric::MetricContributor;
pub use sql_matrix::SqlContractContributor;
