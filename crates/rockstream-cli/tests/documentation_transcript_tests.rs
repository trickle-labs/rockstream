use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use rockstream_cli::demo::{run_demo, DemoOptions, DemoOutcome};
use rockstream_cli::init::{run_init, InitOptions};
use rockstream_cli::output::OutputFormat;
use rockstream_gateway::pgoutput_coordinator::{
    ColumnRoute, RelationChange, RelationRoute, ReplicaIdentity,
};
use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, PlanNode};
use rockstream_sql::catalog::{ColumnDef, SchemaCatalog};
use rockstream_storage::ShardDb;
use tempfile::TempDir;

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
}

fn fenced_blocks(path: &Path) -> Vec<String> {
    let source = fs::read_to_string(path).unwrap();
    let mut blocks = Vec::new();
    let mut current: Option<Vec<String>> = None;
    for line in source.lines() {
        if line.trim_start().starts_with("```") {
            if let Some(block) = current.take() {
                blocks.push(block.join("\n") + "\n");
            } else {
                current = Some(Vec::new());
            }
        } else if let Some(lines) = current.as_mut() {
            lines.push(line.to_string());
        }
    }
    blocks
}

fn block_output(block: &str, command: &str) -> String {
    let mut lines = block.lines();
    assert_eq!(lines.next(), Some(command));
    let output = lines.collect::<Vec<_>>().join("\n");
    if output.is_empty() {
        output
    } else {
        format!("{output}\n")
    }
}

fn block_for_command<'a>(blocks: &'a [String], command: &str) -> &'a str {
    blocks
        .iter()
        .find(|block| block.lines().next() == Some(command))
        .unwrap()
}

fn canonical_demo_text(output: &str) -> String {
    output
        .lines()
        .map(|line| {
            if line.starts_with("RockStream Demo:") {
                "RockStream Demo: scenario='orders' status=passed in <duration>ms".to_string()
            } else if line.starts_with("Storage:") {
                "Storage: <temporary storage> (retained: false)".to_string()
            } else if line.starts_with("[Step ") {
                let start = line.find(" (").unwrap();
                let end = line[start..].find("ms)").unwrap() + start + 3;
                format!("{}<duration>ms){}", &line[..start + 2], &line[end..])
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

fn canonical_demo_json(output: &str) -> String {
    let mut value: DemoOutcome = serde_json::from_str(output).unwrap();
    value.total_duration_ms = 0;
    value.storage_path = "<temporary storage>".to_string();
    for step in &mut value.steps {
        step.duration_ms = 0;
    }
    serde_json::to_string_pretty(&value).unwrap() + "\n"
}

#[test]
fn readme_and_getting_started_match_exact_golden_path() {
    let root = repo_root();
    let readme = fs::read_to_string(root.join("README.md")).unwrap();
    assert!(
        readme.contains("| Evaluating RockStream | [Getting started](docs/getting-started.md) |\n")
    );
    assert!(readme.contains(
        "The [getting-started guide](docs/getting-started.md) includes the checked-in\n"
    ));

    let blocks = fenced_blocks(&root.join("docs/getting-started.md"));
    assert!(blocks
        .iter()
        .any(|block| block.lines().next() == Some("$ rockstream demo")));
    assert!(blocks
        .iter()
        .any(|block| block.lines().next() == Some("$ rockstream demo --output json")));
    assert!(blocks.iter().any(|block| {
        block.lines().next() == Some("$ rockstream init my-project --template local")
    }));
    assert!(blocks.iter().any(|block| block == "$ cd my-project\n"));
    assert!(blocks
        .iter()
        .any(|block| block.starts_with("my-project/\n")));
}

#[tokio::test]
async fn readme_demo_transcript_is_exact() {
    let blocks = fenced_blocks(&repo_root().join("docs/getting-started.md"));
    let opts = DemoOptions {
        scenario: "orders".to_string(),
        storage: None,
        listen: Some("127.0.0.1:0".to_string()),
        keep: false,
        step_delay_ms: 0,
    };

    let text = run_demo(OutputFormat::Text, &opts).unwrap();
    assert_eq!(
        block_output(
            block_for_command(&blocks, "$ rockstream demo"),
            "$ rockstream demo"
        ),
        canonical_demo_text(&text)
    );
    let json = run_demo(OutputFormat::Json, &opts).unwrap();
    assert_eq!(
        block_output(
            block_for_command(&blocks, "$ rockstream demo --output json"),
            "$ rockstream demo --output json",
        ),
        canonical_demo_json(&json)
    );
}

#[test]
fn local_init_transcript_and_layout_are_exact() {
    let root = repo_root();
    let temp = TempDir::new().unwrap();
    let target = temp.path().join("my-project");
    let opts = InitOptions {
        name: "my-project".to_string(),
        template: "local".to_string(),
        dir: Some(target.clone()),
        force: false,
    };
    let output = run_init(OutputFormat::Text, &opts).unwrap();
    let expected = output.replace(target.to_str().unwrap(), "my-project") + "\n";
    let blocks = fenced_blocks(&root.join("docs/getting-started.md"));
    assert_eq!(
        block_output(
            block_for_command(&blocks, "$ rockstream init my-project --template local"),
            "$ rockstream init my-project --template local",
        ),
        expected
    );
    assert_eq!(
        *blocks.iter().find(|block| block.starts_with("my-project/\n")).unwrap(),
        "my-project/\n├── README.md\n├── data/seed.csv\n├── queries.sql\n├── rockstream.toml\n├── schema.sql\n└── scripts/\n    ├── cleanup.sh\n    └── verify.sh\n"
    );

    let expected_files = [
        (
            "rockstream.toml",
            r#"# RockStream Configuration — Local Standalone Deployment
[gateway]
listen = "127.0.0.1:5432"

[storage]
backend = "lfs"
path = "./storage"

[metrics]
listen = "127.0.0.1:9090"
enabled = true

[logging]
level = "info"
"#,
        ),
        (
            "schema.sql",
            r#"-- Local Standalone Project Schema
CREATE TABLE orders (
    id BIGINT,
    store_id BIGINT,
    amount BIGINT
);

CREATE MATERIALIZED VIEW sales_by_store AS
SELECT
    store_id,
    SUM(amount) AS total_amount
FROM orders
GROUP BY store_id;
"#,
        ),
        (
            "queries.sql",
            "-- Diagnostic & Verification Queries\nSELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;\n",
        ),
        ("data/seed.csv", "id,store_id,amount\n1,100,50\n2,100,70\n3,200,40\n"),
        (
            "README.md",
            r#"# RockStream Local Standalone Project

This project runs a single-node RockStream instance maintaining incremental materialized views over local storage.

## Quick Start

1. Start the RockStream node:
   ```bash
   rockstream start --storage ./storage --listen 127.0.0.1:5432
   ```

2. Run automated verification:
   ```bash
   bash scripts/verify.sh
   ```

3. Teardown and cleanup:
   ```bash
   bash scripts/cleanup.sh
   ```
"#,
        ),
        (
            "scripts/verify.sh",
            r#"#!/usr/bin/env bash
set -euo pipefail

GATEWAY_PORT="${ROCKSTREAM_PORT:-5432}"
GATEWAY_HOST="${ROCKSTREAM_HOST:-127.0.0.1}"

echo "==> Verifying RockStream local standalone deployment on ${GATEWAY_HOST}:${GATEWAY_PORT}..."

if ! command -v psql >/dev/null 2>&1; then
    echo "Notice: psql not found in PATH, skipping psql query checks."
    exit 0
fi

psql -h "${GATEWAY_HOST}" -p "${GATEWAY_PORT}" -U rockstream -d rockstream -c "SELECT store_id, total_amount, order_count FROM sales_by_store ORDER BY store_id;"
echo "==> Verification completed successfully."
"#,
        ),
        (
            "scripts/cleanup.sh",
            r#"#!/usr/bin/env bash
set -euo pipefail

echo "==> Cleaning up local RockStream project state..."
rm -rf ./storage
echo "==> Cleanup complete."
"#,
        ),
    ];
    let mut json_opts = opts.clone();
    json_opts.force = true;
    let json: rockstream_cli::init::InitOutcome =
        serde_json::from_str(&run_init(OutputFormat::Json, &json_opts).unwrap()).unwrap();
    assert_eq!(
        json.generated_files,
        expected_files
            .iter()
            .map(|(path, _)| (*path).to_string())
            .collect::<Vec<_>>()
    );
    for (path, expected) in expected_files {
        assert_eq!(fs::read_to_string(target.join(path)).unwrap(), expected);
    }

    let verify = Command::new("/bin/bash")
        .arg("scripts/verify.sh")
        .current_dir(&target)
        .env("PATH", "/nonexistent")
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8(verify.stdout).unwrap(),
        block_output(
            block_for_command(&blocks, "$ bash scripts/verify.sh"),
            "$ bash scripts/verify.sh",
        )
    );
    let cleanup = Command::new("/bin/bash")
        .arg("scripts/cleanup.sh")
        .current_dir(&target)
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8(cleanup.stdout).unwrap(),
        block_output(
            block_for_command(&blocks, "$ bash scripts/cleanup.sh"),
            "$ bash scripts/cleanup.sh",
        )
    );
}

#[test]
fn reference_app_transcript_is_exact() {
    let root = repo_root();
    let output = Command::new("/bin/bash")
        .arg("scripts/verify.sh")
        .current_dir(root.join("examples/reference-app"))
        .env("PATH", "/nonexistent")
        .output()
        .unwrap();
    assert!(output.status.success());
    let blocks = fenced_blocks(&root.join("examples/reference-app/README.md"));
    assert_eq!(
        block_output(&blocks[3], "$ bash scripts/verify.sh"),
        String::from_utf8(output.stdout).unwrap()
    );
}

fn sum_plan() -> PlanNode {
    PlanNode::Aggregate {
        input: Box::new(PlanNode::Source {
            name: "orders".to_string(),
        }),
        group_by: vec![Expr::Column(0)],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Sum,
            input: Expr::Column(1),
            distinct: false,
        }],
    }
}

fn col(name: &str, data_type: &str, nullable: bool) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type: data_type.to_string(),
        nullable,
    }
}

async fn lfs_catalog() -> (TempDir, SchemaCatalog) {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        ShardDb::builder("documentation-schema", store)
            .build()
            .await
            .unwrap(),
    );
    let catalog = SchemaCatalog::new(Arc::clone(&db));
    (dir, catalog)
}

fn route(columns: Vec<ColumnRoute>) -> RelationRoute {
    RelationRoute {
        version: 1,
        relation_id: 52,
        upstream_namespace: "public".to_string(),
        upstream_relation: "orders".to_string(),
        imported_table_id: 52,
        imported_table_name: "orders".to_string(),
        columns,
        replica_identity: ReplicaIdentity::Full,
        schema_version: 1,
    }
}

fn route_column(name: &str, oid: u32, nullable: bool, key: bool) -> ColumnRoute {
    ColumnRoute {
        upstream_name: name.to_string(),
        imported_name: name.to_string(),
        type_oid: oid,
        type_modifier: -1,
        nullable,
        has_default: false,
        key,
    }
}

#[tokio::test]
async fn compatible_schema_cookbook_is_exact() {
    let blocks = fenced_blocks(&repo_root().join("docs/schema-evolution.md"));
    let expected = "backend=lfs\nchange=add nullable column\nresult=accepted\nschema_version=2\ncolumns=k:Int64,s:Int64,note:Utf8?\nbackend=postgres-cdc\nchange=add nullable column\nresult=accepted\nschema_version=2\nhistory_entries=1\n";
    assert_eq!(blocks[0], expected);

    let (_dir, catalog) = lfs_catalog().await;
    let plan = sum_plan();
    catalog
        .register_view(
            "v",
            "SELECT k, SUM(v) FROM orders GROUP BY k",
            &plan,
            vec![col("k", "Int64", false), col("s", "Int64", false)],
        )
        .await
        .unwrap();
    catalog
        .register_view(
            "v",
            "SELECT k, SUM(v) FROM orders GROUP BY k",
            &plan,
            vec![
                col("k", "Int64", false),
                col("s", "Int64", false),
                col("note", "Utf8", true),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        catalog
            .load_view("v")
            .await
            .unwrap()
            .unwrap()
            .schema_version,
        2
    );

    let old = route(vec![route_column("id", 23, false, true)]);
    let next = route(vec![
        route_column("id", 23, false, true),
        route_column("note", 25, true, false),
    ]);
    assert_eq!(old.classify(&next), RelationChange::Compatible);
}

#[tokio::test]
async fn incompatible_schema_cookbook_is_exact() {
    let blocks = fenced_blocks(&repo_root().join("docs/schema-evolution.md"));
    let expected = "backend=lfs\nchange=rename column s to renamed\nresult=error\ncode=RS-1002\nrows=[]\nschema_version=1\nbackend=postgres-cdc\nchange=drop column value\nresult=error\ncode=RS-1002\nrows=[]\n";
    assert_eq!(blocks[1], expected);

    let (_dir, catalog) = lfs_catalog().await;
    let plan = sum_plan();
    let original = vec![col("k", "Int64", false), col("s", "Int64", false)];
    catalog
        .register_view(
            "sum_view",
            "SELECT k, SUM(v) FROM orders GROUP BY k",
            &plan,
            original.clone(),
        )
        .await
        .unwrap();
    let err = catalog
        .register_view(
            "sum_view",
            "SELECT k, SUM(v) FROM orders GROUP BY k",
            &plan,
            vec![col("k", "Int64", false), col("renamed", "Int64", false)],
        )
        .await
        .unwrap_err();
    assert_eq!(err.error_code().to_string(), "RS-1002");
    assert_eq!(
        catalog
            .load_view("sum_view")
            .await
            .unwrap()
            .unwrap()
            .schema_version,
        1
    );

    let old = route(vec![
        route_column("id", 20, false, true),
        route_column("value", 20, false, false),
    ]);
    let next = route(vec![route_column("id", 20, false, true)]);
    assert_eq!(
        old.classify(&next),
        RelationChange::Breaking("column was dropped".to_string())
    );
    assert!(Vec::<String>::new().is_empty());
}
