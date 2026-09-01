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
        (
            "sql-type-matrix.md",
            render_sql_type_matrix(&manifest.sql_contract_surface),
        ),
        (
            "sql-semantics.md",
            render_sql_semantics(&manifest.sql_contract_surface),
        ),
        ("limits.md", render_limits(&manifest.sql_contract_surface)),
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

fn render_limits(_surface: &SqlContractSurface) -> String {
    let mut out = String::from("# System limits reference\n\n");
    out.push_str("Authoritative operational, architectural, protocol, and parser limits enforced across RockStream.\n\n");
    let limits = rockstream_types::limits::SystemLimitsCatalog::all();
    out.push_str(&table(
        &[
            "Limit Identifier",
            "Name",
            "Canonical Value",
            "Unit",
            "Enforcement Level",
            "Metric Name",
            "Error Code",
            "Description",
        ],
        limits.iter().map(|l| {
            vec![
                format!("`{}`", l.id),
                cell(&l.name),
                format!("{}", l.canonical_value),
                cell(&l.unit),
                cell(&l.enforcement_level),
                format!("`{}`", l.metric_name),
                format!("`{}`", l.error_code),
                cell(&l.description),
            ]
        }),
    ));
    out
}

fn render_sql_semantics(surface: &SqlContractSurface) -> String {
    let mut out = String::from("# SQL semantics and PostgreSQL compatibility\n\n");
    out.push_str("Authoritative v1 SQL semantics, PostgreSQL 18.0 differential compatibility, and system boundaries.\n\n");

    if let Some(ref_db) = &surface.reference_database {
        out.push_str("## Reference database\n\n");
        out.push_str(&format!(
            "- **Engine:** {}\n- **Version:** {}\n- **Canonical image:** `{}`\n- **AMD64 digest:** `{}`\n- **ARM64 digest:** `{}`\n\n",
            ref_db.engine, ref_db.version, ref_db.canonical_image, ref_db.amd64_digest, ref_db.arm64_digest
        ));
    }

    if let Some(col) = &surface.collation {
        out.push_str("## Collation and string ordering\n\n");
        out.push_str(&format!(
            "- **Active collation:** `{}`\n- **Semantics:** {}\n- **Unsupported collations:** Rejected fail-closed with `{}`.\n\n",
            col.name, col.description, col.rejection_code
        ));
    }

    if let Some(num) = &surface.numeric_bounds {
        out.push_str("## Numeric precision and decimal bounds\n\n");
        out.push_str(&format!(
            "- **Admitted precision:** `DECIMAL(p, s)` where {} <= p <= {}, {} <= s <= p.\n- **Arithmetic overflow:** Fails closed with `{}`.\n- **Invalid precision/scale:** Fails closed with `{}`.\n\n",
            num.min_precision, num.max_precision, num.min_scale, num.overflow_code, num.invalid_precision_code
        ));
    }

    if let Some(temp) = &surface.temporal_policy {
        out.push_str("## Temporal policy and time zones\n\n");
        out.push_str(&format!(
            "- **Fractional precision:** {} digits (resolution: {}).\n- **Internal storage:** {}.\n- **Invalid format:** Fails closed with `{}`.\n\n",
            temp.precision, temp.resolution, temp.timezone_storage, temp.invalid_format_code
        ));
    }

    if let Some(ident) = &surface.identifier_folding {
        out.push_str("## Identifier case folding\n\n");
        out.push_str(&format!(
            "- **Unquoted identifiers:** Folded to {}.\n- **Quoted identifiers:** Preserved verbatim (case-sensitive).\n- **Maximum byte length:** {} bytes (exceeding rejected with `{}`).\n\n",
            ident.unquoted_folding, ident.max_byte_length, ident.length_exceeded_code
        ));
    }

    if let Some(nulls) = &surface.null_logic {
        out.push_str("## Three-valued logic and NULL semantics\n\n");
        out.push_str(&format!(
            "- **Evaluation logic:** {} (`TRUE`, `FALSE`, `UNKNOWN`).\n- **Equality:** `NULL = NULL` evaluates to `UNKNOWN`.\n- **Distinctness:** `IS NOT DISTINCT FROM` is {}.\n\n",
            nulls.logic, nulls.distinct_from
        ));
    }

    if let Some(arr) = &surface.parameter_arrays {
        out.push_str("## Prepared statement array parameters\n\n");
        out.push_str(&format!(
            "- **Supported array dimensions:** {}.\n- **Array membership:** `col = ANY($1)` / `col IN (SELECT UNNEST($1))` supported.\n- **Invalid array parameter:** Fails closed with `{}`.\n\n",
            arr.dimensions, arr.invalid_array_code
        ));
    }

    out.push_str("## Multiset bag semantics and IVM retractions\n\n");
    out.push_str("RockStream preserves exact bag/multiset duplicate counts under incremental view maintenance. Retraction underflow fails closed with `RS-1017`.\n\n");

    out.push_str("## Unmatched DML\n\n");
    out.push_str("`UPDATE` or `DELETE` statements matching zero rows succeed without error and return command tags `UPDATE 0` and `DELETE 0`.\n\n");

    out.push_str("## Floating-point join restrictions\n\n");
    out.push_str("Floating-point equality joins (`FLOAT4`/`FLOAT8`) are explicitly rejected fail-closed with `RS-1019` due to non-total IEEE-754 ordering.\n\n");

    out
}

fn render_sql_type_matrix(surface: &SqlContractSurface) -> String {
    let mut out = String::from("# SQL type and operation compatibility matrix\n\n");
    out.push_str("Authoritative matrix of admitted SQL types, operations, support tiers, and rejection error codes.\n\n");
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
