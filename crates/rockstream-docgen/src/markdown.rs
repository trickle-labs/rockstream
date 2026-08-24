use std::collections::BTreeMap;

use crate::manifest::{
    CatalogSurface, CliCommandDescriptor, CliSurface, ConfigSurface, ErrorSurface, FunctionSurface,
    MetricSurface, ProductSurfaceManifest, SqlContractSurface,
};

/// Render the manifest as deterministic Markdown reference files.
pub fn render_reference_docs(manifest: &ProductSurfaceManifest) -> BTreeMap<String, String> {
    [
        ("cli.md", render_cli(&manifest.cli_surface)),
        ("configuration.md", render_config(&manifest.config_surface)),
        ("functions.md", render_functions(&manifest.function_surface)),
        (
            "sql-support.md",
            render_sql_support(&manifest.sql_contract_surface),
        ),
        ("catalog.md", render_catalog(&manifest.catalog_surface)),
        ("metrics.md", render_metrics(&manifest.metric_surface)),
        ("errors.md", render_errors(&manifest.error_surface)),
    ]
    .into_iter()
    .map(|(name, content)| (name.to_string(), content))
    .collect()
}

pub fn render_markdown_references(manifest: &ProductSurfaceManifest) -> BTreeMap<String, String> {
    render_reference_docs(manifest)
}

fn table(headers: &[&str], rows: impl IntoIterator<Item = Vec<String>>) -> String {
    let mut out = format!("| {} |\n", headers.join(" | "));
    out.push_str(&format!(
        "| {} |\n",
        headers
            .iter()
            .map(|_| "---")
            .collect::<Vec<_>>()
            .join(" | ")
    ));
    for row in rows {
        out.push_str(&format!("| {} |\n", row.join(" | ")));
    }
    out.push('\n');
    out
}

fn cell(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', "<br>")
}

fn render_cli(surface: &CliSurface) -> String {
    let mut out = String::from("# CLI reference\n\n");
    for command in &surface.commands {
        render_command(&mut out, command, 2, "rockstream");
    }
    out
}

fn render_command(out: &mut String, command: &CliCommandDescriptor, level: usize, prefix: &str) {
    let name = if prefix.is_empty() {
        command.name.clone()
    } else {
        format!("{prefix} {}", command.name)
    };
    out.push_str(&format!(
        "{} `{}`\n\n{}\n\n",
        "#".repeat(level),
        name,
        command.about
    ));
    if !command.options.is_empty() {
        out.push_str("Options\n\n");
        out.push_str(&table(
            &[
                "Name",
                "Short",
                "Long",
                "Value",
                "Required",
                "Default",
                "Values",
                "Description",
            ],
            command.options.iter().map(|o| {
                vec![
                    cell(&o.name),
                    o.short
                        .map(|short| format!("-{short}"))
                        .unwrap_or_else(|| "—".into()),
                    o.long
                        .as_deref()
                        .map(|long| format!("--{long}"))
                        .unwrap_or_else(|| "—".into()),
                    o.value_name.as_deref().unwrap_or("—").into(),
                    if o.required {
                        "yes".into()
                    } else {
                        "no".into()
                    },
                    o.default_value.as_deref().unwrap_or("—").into(),
                    if o.possible_values.is_empty() {
                        "—".into()
                    } else {
                        cell(&o.possible_values.join(", "))
                    },
                    cell(&o.help),
                ]
            }),
        ));
    }
    if !command.exit_codes.is_empty() {
        out.push_str("Exit codes\n\n");
        out.push_str(&table(
            &["Code", "Title", "Description", "Error codes"],
            command.exit_codes.iter().map(|e| {
                vec![
                    e.code.to_string(),
                    cell(&e.title),
                    cell(&e.description),
                    cell(&command.error_codes.join(", ")),
                ]
            }),
        ));
    }
    if !command.error_codes.is_empty() {
        out.push_str(&format!(
            "Error codes: `{}`\n\n",
            command.error_codes.join("`, `")
        ));
    }
    for subcommand in &command.subcommands {
        render_command(out, subcommand, level + 1, &name);
    }
}

fn render_config(surface: &ConfigSurface) -> String {
    let mut out = String::from("# Configuration reference\n\n");
    out.push_str(&table(
        &[
            "Key",
            "Type",
            "Default",
            "Environment",
            "Deprecated",
            "Source",
            "Description",
        ],
        surface.options.iter().map(|o| {
            vec![
                cell(&o.key),
                cell(&o.data_type),
                cell(&o.default_value),
                o.env_var.as_deref().unwrap_or("—").into(),
                o.deprecated.to_string(),
                cell(&o.source_origin),
                cell(&o.description),
            ]
        }),
    ));
    out
}

fn render_functions(surface: &FunctionSurface) -> String {
    let mut out = String::from("# Functions reference\n\n");
    out.push_str(&table(
        &[
            "Name",
            "Category",
            "Signature",
            "Arguments",
            "Returns",
            "Null handling",
            "Examples",
            "Description",
        ],
        surface.functions.iter().map(|f| {
            vec![
                cell(&f.name),
                cell(&f.category),
                cell(&f.signature),
                cell(&f.argument_types.join(", ")),
                cell(&f.return_type),
                cell(&f.null_handling),
                cell(&f.examples.join("<br>")),
                cell(&f.description),
            ]
        }),
    ));
    out
}

fn render_sql_support(surface: &SqlContractSurface) -> String {
    let mut out = String::from("# SQL support reference\n\n");
    for ty in &surface.types {
        out.push_str(&format!(
            "## `{}`\n\n**Family:** {}  \n**Aliases:** {}\n\n{}\n\n",
            ty.name,
            ty.family,
            if ty.aliases.is_empty() {
                "—".into()
            } else {
                ty.aliases.join(", ")
            },
            ty.description
        ));
        out.push_str(&table(
            &["Operation", "Status", "Rejection code", "Notes"],
            ty.operations.iter().map(|o| {
                vec![
                    cell(&o.operation),
                    cell(&o.status),
                    o.rejection_code.as_deref().unwrap_or("—").into(),
                    o.notes.as_deref().map(cell).unwrap_or_else(|| "—".into()),
                ]
            }),
        ));
    }
    out
}

fn render_catalog(surface: &CatalogSurface) -> String {
    let mut out = String::from("# Catalog reference\n\n");
    for schema in &surface.schemas {
        out.push_str(&format!(
            "## `{}`\n\n{}\n\n",
            schema.name, schema.description
        ));
        for table_def in &schema.tables {
            out.push_str(&format!(
                "### `{}`\n\n{}  \n**Cardinality bound:** {}\n\n",
                table_def.name,
                table_def.description,
                table_def.cardinality_bound.as_deref().unwrap_or("—")
            ));
            out.push_str(&table(
                &["Column", "Position", "Type", "Nullable", "Description"],
                table_def.columns.iter().map(|c| {
                    vec![
                        cell(&c.name),
                        c.ordinal_position.to_string(),
                        cell(&c.data_type),
                        c.nullable.to_string(),
                        cell(&c.description),
                    ]
                }),
            ));
        }
    }
    out
}

fn render_metrics(surface: &MetricSurface) -> String {
    let mut out = String::from("# Metrics reference\n\n");
    out.push_str(&table(
        &["Name", "Type", "Unit", "Labels", "Stability", "Description"],
        surface.metrics.iter().map(|m| {
            vec![
                cell(&m.name),
                cell(&m.metric_type),
                cell(&m.unit),
                cell(&m.labels.join(", ")),
                cell(&m.stability),
                cell(&m.description),
            ]
        }),
    ));
    out
}

fn render_errors(surface: &ErrorSurface) -> String {
    let mut out = String::from("# Errors reference\n\n");
    out.push_str(&table(
        &[
            "Code",
            "Key",
            "Title",
            "Severity",
            "SQLSTATE",
            "Retry",
            "Next steps",
            "Anchor",
        ],
        surface.errors.iter().map(|e| {
            vec![
                cell(&e.code),
                cell(&e.key),
                cell(&e.title),
                cell(&e.severity),
                cell(&e.sqlstate),
                cell(&e.retry_class),
                cell(&e.default_next_steps),
                cell(&e.doc_anchor),
            ]
        }),
    ));
    out
}
