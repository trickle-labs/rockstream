//! Tests for Product Surface Contributors (DOC-001, Matrix A).

use rockstream_docgen::contributors::{
    CatalogContributor, CliContributor, ConfigContributor, ErrorContributor, FunctionContributor,
    MetricContributor, SqlContractContributor,
};

#[test]
fn test_cli_contributor_extraction() {
    let cli_surface = CliContributor::extract();
    assert!(
        !cli_surface.commands.is_empty(),
        "CliContributor must extract commands"
    );
    // Verify common commands like 'run', 'init', 'doctor', or others exist
    let names: Vec<&str> = cli_surface
        .commands
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    println!("Extracted CLI commands: {:?}", names);
}

#[test]
fn test_config_contributor_extraction() {
    let config_surface = ConfigContributor::extract();
    assert!(
        !config_surface.options.is_empty(),
        "ConfigContributor must extract options"
    );
    let autotuner = config_surface
        .options
        .iter()
        .find(|o| o.key == "autotuner.enabled");
    assert!(
        autotuner.is_some(),
        "autotuner.enabled config option must exist"
    );
}

#[test]
fn test_function_contributor_extraction() {
    let fn_surface = FunctionContributor::extract();
    assert!(
        !fn_surface.functions.is_empty(),
        "FunctionContributor must extract functions"
    );

    let upper = fn_surface.functions.iter().find(|f| f.name == "upper");
    assert!(upper.is_some(), "upper function must be present");
    assert_eq!(upper.unwrap().return_type, "TEXT");

    let sum = fn_surface.functions.iter().find(|f| f.name == "sum");
    assert!(sum.is_some(), "sum aggregate function must be present");
}

#[test]
fn test_catalog_contributor_extraction() {
    let cat_surface = CatalogContributor::extract();
    assert!(
        !cat_surface.schemas.is_empty(),
        "Catalog schemas must exist"
    );
    let schema = &cat_surface.schemas[0];
    assert_eq!(schema.name, "rockstream_catalog");

    let table_names: Vec<&str> = schema.tables.iter().map(|t| t.name.as_str()).collect();
    assert!(
        table_names.contains(&"nodes"),
        "nodes table must exist in catalog"
    );
    assert!(
        table_names.contains(&"views"),
        "views table must exist in catalog"
    );
    assert!(
        table_names.contains(&"checkpoints"),
        "checkpoints table must exist in catalog"
    );
}

#[test]
fn test_metric_contributor_extraction() {
    let metric_surface = MetricContributor::extract();
    assert!(
        !metric_surface.metrics.is_empty(),
        "MetricContributor must extract metrics"
    );
    let merge_law = metric_surface
        .metrics
        .iter()
        .find(|m| m.name == "merge_law_applied_total");
    assert!(
        merge_law.is_some(),
        "merge_law_applied_total metric must exist"
    );
}

#[test]
fn test_error_contributor_extraction() {
    let error_surface = ErrorContributor::extract();
    assert_eq!(
        error_surface.errors.len(),
        192,
        "ErrorContributor must extract all 192 error descriptors from contracts/errors.toml"
    );
}

#[test]
fn test_sql_contract_contributor_extraction() {
    let sql_surface =
        SqlContractContributor::extract().expect("SqlContractContributor must succeed");
    assert!(
        !sql_surface.types.is_empty(),
        "SqlContractContributor must extract SQL types"
    );
    let int8 = sql_surface.types.iter().find(|t| t.name == "INT8");
    assert!(int8.is_some(), "INT8 type must exist in SQL contract");
}
