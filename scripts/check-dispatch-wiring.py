#!/usr/bin/env python3
"""Validate the 4-stage dispatch-wiring pipeline across all documented SQL features.

Checks:
1. Parser: SQL parsing and statement recognition.
2. Dispatcher: Routing to appropriate handler in the gateway.
3. Executor: Execution logic in engine/catalog/operator layer.
4. Response Encoder: Protocol wire encoding into pgwire responses.

Ensures zero MISSING paths exist across capabilities.toml and documented language features.
"""

from __future__ import annotations

import argparse
import sys
import tomllib
from pathlib import Path

# The 21 documented statement / command families and their required 4-stage pipeline anchors.
PIPELINE_AUDIT_SPEC = [
    {
        "family": "SELECT ... WHERE ... (Base & Views)",
        "parser": ("crates/rockstream-sql/src/frontend.rs", "sql_to_plan_node"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_async_with_conn"),
        "executor": ("crates/rockstream-ops/src/compile.rs", "compile_node"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "Scalar Expressions (CAST, CASE, NOW(), Interval)",
        "parser": ("crates/rockstream-sql/src/lower.rs", "Cast"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_async_with_conn"),
        "executor": ("crates/rockstream-ops/src/expr.rs", "eval_i64"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "Aggregates (SUM, COUNT, AVG, MIN, MAX)",
        "parser": ("crates/rockstream-sql/src/lower.rs", "AggregateExpr"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_async_with_conn"),
        "executor": ("crates/rockstream-ops/src/aggregate.rs", "AggregateOp"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "Joins (Inner, Left, Right, Full, Semi, Anti, Cross)",
        "parser": ("crates/rockstream-sql/src/lower.rs", "JoinType"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_async_with_conn"),
        "executor": ("crates/rockstream-ops/src/join.rs", "JoinOp"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "Set Operations (UNION, INTERSECT, EXCEPT, DISTINCT)",
        "parser": ("crates/rockstream-sql/src/lower.rs", "LogicalPlan::Union"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_async_with_conn"),
        "executor": ("crates/rockstream-ops/src/distinct.rs", "DistinctOp"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "Window Functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD)",
        "parser": ("crates/rockstream-sql/src/lower.rs", "WindowFunc"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_async_with_conn"),
        "executor": ("crates/rockstream-ops/src/window.rs", "WindowOp"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "SUBSCRIBE <view> [AS OF ...]",
        "parser": ("crates/rockstream-gateway/src/subscribe_parser.rs", "parse_subscribe"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "handle_subscribe"),
        "executor": ("crates/rockstream-gateway/src/subscribe_handler.rs", "SubscribeRegistry"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "Session Variables (SET rockstream.*)",
        "parser": ("crates/rockstream-gateway/src/server.rs", "set rockstream."),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_sync"),
        "executor": ("crates/rockstream-gateway/src/session.rs", "Session"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "Fence Tokens (rockstream.write_fence(), rockstream.after_fence())",
        "parser": ("crates/rockstream-gateway/src/server.rs", "write_fence"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_sync"),
        "executor": ("crates/rockstream-gateway/src/session.rs", "FreshnessToken"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "DML (INSERT, UPDATE, DELETE, RETURNING)",
        "parser": ("crates/rockstream-sql/src/frontend.rs", "sql_to_plan_node"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_async_with_conn"),
        "executor": ("crates/rockstream-gateway/src/write_buffer.rs", "WriteBuffer"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "Transaction Semantics (idempotency_key, source_epoch)",
        "parser": ("crates/rockstream-gateway/src/server.rs", "idempotency_key"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_async_with_conn"),
        "executor": ("crates/rockstream-gateway/src/session.rs", "Session"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "Views (CREATE [OR REPLACE] [MATERIALIZED] VIEW, REFRESH)",
        "parser": ("crates/rockstream-sql/src/frontend.rs", "DdlStatement"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "handle_create_view"),
        "executor": ("crates/rockstream-gateway/src/server.rs", "refresh_backfill_progress"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "Workload DDL (CREATE/ALTER/DROP WORKLOAD, WITH WORKLOAD)",
        "parser": ("crates/rockstream-gateway/src/server.rs", "create workload "),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_sync"),
        "executor": ("crates/rockstream-sql/src/workload_catalog.rs", "WorkloadCatalog"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "SHOW WORKLOAD STATUS [FOR <name>]",
        "parser": ("crates/rockstream-gateway/src/server.rs", "show workload status"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_sync"),
        "executor": ("crates/rockstream-gateway/src/catalog_stubs.rs", "CatalogWorkloadStatusEntry"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "SHOW RESOURCE USAGE [FOR WORKLOAD / CLUSTER]",
        "parser": ("crates/rockstream-gateway/src/server.rs", "show resource usage"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_sync"),
        "executor": ("crates/rockstream-gateway/src/catalog_stubs.rs", "CatalogWorkloadResourceUsageEntry"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "SHOW VIEW STATUS [FOR <name>]",
        "parser": ("crates/rockstream-gateway/src/server.rs", "show view status"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_sync"),
        "executor": ("crates/rockstream-gateway/src/catalog_stubs.rs", "CatalogView"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "Index DDL (CREATE/DROP/REBUILD/MARK INDEX)",
        "parser": ("crates/rockstream-sql/src/frontend.rs", "DdlStatement"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_sync"),
        "executor": ("crates/rockstream-ops/src/index_arrange.rs", "IndexArrangeOp"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "Diagnostics (EXPLAIN, EXPLAIN INCREMENTAL)",
        "parser": ("crates/rockstream-gateway/src/server.rs", "explain "),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_sync"),
        "executor": ("crates/rockstream-sql/src/explain_incremental.rs", "explain_incremental"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "Secret DDL (CREATE/ALTER/SHOW/DROP SECRET)",
        "parser": ("crates/rockstream-gateway/src/server.rs", "create secret "),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_sync"),
        "executor": ("crates/rockstream-control/src/secret_store.rs", "SecretStore"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "System Catalog (rockstream_catalog.view/workload_resource_usage)",
        "parser": ("crates/rockstream-sql/src/frontend.rs", "sql_to_plan_node"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_async_with_conn"),
        "executor": ("crates/rockstream-gateway/src/catalog_stubs.rs", "handle_query"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
    {
        "family": "Removed Connectors Rejection (RS-4017)",
        "parser": ("crates/rockstream-gateway/src/server.rs", "is_removed_connector_ddl"),
        "dispatcher": ("crates/rockstream-gateway/src/server.rs", "dispatch_sync"),
        "executor": ("crates/rockstream-gateway/src/catalog_stubs.rs", "REMOVED_CONNECTOR_REMEDIATION"),
        "encoder": ("crates/rockstream-gateway/src/server.rs", "DataRowEncoder"),
    },
]


def verify_target(root: Path, path_rel: str, symbol: str) -> tuple[bool, str]:
    file_path = root / path_rel
    if not file_path.is_file():
        return False, f"file not found: {path_rel}"
    try:
        content = file_path.read_text(encoding="utf-8")
    except OSError as e:
        return False, f"cannot read {path_rel}: {e}"

    if symbol not in content:
        return False, f"symbol/pattern {symbol!r} missing in {path_rel}"

    return True, "OK"


def audit_capabilities_toml(root: Path, errors: list[str]) -> None:
    cap_file = root / "capabilities.toml"
    if not cap_file.is_file():
        errors.append("capabilities.toml is missing")
        return

    try:
        data = tomllib.loads(cap_file.read_text(encoding="utf-8"))
    except Exception as e:
        errors.append(f"failed to parse capabilities.toml: {e}")
        return

    dispatches = data.get("dispatch", [])
    dispatch_map: dict[str, dict] = {}
    for d in dispatches:
        d_id = d.get("id")
        if not d_id:
            errors.append("dispatch entry missing 'id'")
            continue
        dispatch_map[d_id] = d
        path = d.get("path")
        symbol = d.get("symbol")
        if not path or not symbol:
            errors.append(f"dispatch {d_id} missing path or symbol")
            continue
        ok, msg = verify_target(root, path, symbol)
        if not ok:
            errors.append(f"dispatch {d_id}: {msg}")

    capabilities = data.get("capability", [])
    for cap in capabilities:
        cap_id = cap.get("id")
        refs = cap.get("dispatch", [])
        for ref in refs:
            if ref not in dispatch_map:
                errors.append(f"capability {cap_id} references undeclared dispatch {ref}")


def audit_4_stage_pipeline(root: Path, errors: list[str]) -> None:
    for item in PIPELINE_AUDIT_SPEC:
        family = item["family"]
        stages = [
            ("Parser", item["parser"]),
            ("Dispatcher", item["dispatcher"]),
            ("Executor", item["executor"]),
            ("Response Encoder", item["encoder"]),
        ]
        for stage_name, (rel_path, sym) in stages:
            ok, msg = verify_target(root, rel_path, sym)
            if not ok:
                errors.append(f"MISSING path for [{family}] at stage [{stage_name}]: {msg}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit 4-stage dispatch-wiring across SQL surface.")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="Repository root directory",
    )
    args = parser.parse_args()
    root = args.root

    errors: list[str] = []

    audit_capabilities_toml(root, errors)
    audit_4_stage_pipeline(root, errors)

    if errors:
        print("FAIL: Dispatch-wiring audit found errors:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1

    print(
        f"OK: {len(PIPELINE_AUDIT_SPEC)}/{len(PIPELINE_AUDIT_SPEC)} statement families and all declared dispatch endpoints verified with 0 MISSING paths."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
