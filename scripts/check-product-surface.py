#!/usr/bin/env python3
"""check-product-surface.py — validate RockStream single-source product surface and drift gates (DOC-001, DOC-004).

Checks:
1. Validates contracts/sql-type-matrix.toml structure, types, operations, and rejection error codes.
2. Validates ID resolution (CLI errors, SQL matrix rejection codes, errors.toml).
3. Asserts zero drift between live code/contracts and docs/product-surface.json with exact section-level diagnosis.

Usage:
    python3 scripts/check-product-surface.py [ROOT_DIR]
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

VALID_STATUSES = {"Core", "Supported", "Experimental", "Unsupported"}


def parse_code_number(code_str: str) -> int:
    m = re.match(r"^RS-(\d{4})$", code_str)
    if not m:
        raise ValueError(f"Invalid error code format: '{code_str}' (expected RS-XXXX)")
    return int(m.group(1))


def check_sql_matrix_toml(matrix_path: Path, errors: list[str]) -> tuple[dict, set[str]]:
    if not matrix_path.exists():
        errors.append(f"SQL type matrix not found at {matrix_path}")
        return {}, set()

    try:
        data = tomllib.loads(matrix_path.read_text(encoding="utf-8"))
    except Exception as e:
        errors.append(f"Error parsing {matrix_path}: {e}")
        return {}, set()

    types = data.get("type", [])
    if not types:
        errors.append("contracts/sql-type-matrix.toml must contain at least one [[type]] entry")

    rejection_codes = set()
    for ty in types:
        name = ty.get("name", "<unknown>")
        ops = ty.get("operations", [])
        for op in ops:
            op_name = op.get("operation", "<unknown>")
            status = op.get("status")
            if status not in VALID_STATUSES:
                errors.append(
                    f"Invalid status '{status}' for type '{name}' operation '{op_name}' in sql-type-matrix.toml"
                )
            if status == "Unsupported":
                code = op.get("rejection_code")
                if not code:
                    errors.append(
                        f"Unsupported type '{name}' operation '{op_name}' missing mandatory rejection_code"
                    )
                else:
                    try:
                        parse_code_number(code)
                        rejection_codes.add(code)
                    except ValueError as e:
                        errors.append(str(e))
    return data, rejection_codes


def check_error_catalog_ids(errors_path: Path, errors: list[str]) -> set[str]:
    if not errors_path.exists():
        errors.append(f"Error catalog not found at {errors_path}")
        return set()

    try:
        data = tomllib.loads(errors_path.read_text(encoding="utf-8"))
    except Exception as e:
        errors.append(f"Error parsing {errors_path}: {e}")
        return set()

    err_list = data.get("error", [])
    valid_codes = set()
    for err in err_list:
        code = err.get("code")
        if code:
            valid_codes.add(code)
    return valid_codes


def check_manifest_id_resolution(
    manifest: dict, valid_error_codes: set[str], errors: list[str]
) -> None:
    # 1. Check CLI error codes
    cli = manifest.get("cli_surface", {})
    for cmd in cli.get("commands", []):
        for code in cmd.get("error_codes", []):
            if code not in valid_error_codes:
                errors.append(
                    f"Unresolved reference: error code '{code}' not found in catalog"
                )

    # 2. Check SQL contract surface rejection codes
    sql = manifest.get("sql_contract_surface", {})
    for ty in sql.get("types", []):
        for op in ty.get("operations", []):
            code = op.get("rejection_code")
            if code and code not in valid_error_codes:
                errors.append(
                    f"Unresolved reference: error code '{code}' not found in catalog"
                )

    # 3. Check error surface codes
    err_surface = manifest.get("error_surface", {})
    for err in err_surface.get("errors", []):
        code = err.get("code")
        if code and code not in valid_error_codes:
            errors.append(
                f"Unresolved reference: error code '{code}' not found in catalog"
            )


def compare_manifest_surfaces(existing: dict, live: dict, errors: list[str]) -> None:
    # 1. Compare CLI surface
    existing_cli = existing.get("cli_surface", {}).get("commands", [])
    live_cli = live.get("cli_surface", {}).get("commands", [])
    existing_cli_map = {c["name"]: c for c in existing_cli}
    live_cli_map = {c["name"]: c for c in live_cli}

    for name, live_cmd in live_cli_map.items():
        if name not in existing_cli_map:
            errors.append(f"Drift detected in cli_surface: unexpected command '{name}'")
        else:
            # Check options
            ex_cmd = existing_cli_map[name]
            ex_opts = {o["name"]: o for o in ex_cmd.get("options", [])}
            live_opts = {o["name"]: o for o in live_cmd.get("options", [])}
            for opt_name, ex_opt in ex_opts.items():
                if opt_name not in live_opts:
                    long_flag = ex_opt.get("long")
                    flag_str = f"--{long_flag}" if long_flag else opt_name
                    errors.append(
                        f"Drift detected in cli_surface: unexpected flag '{flag_str}' in command '{name}'"
                    )
            for opt_name, live_opt in live_opts.items():
                if opt_name not in ex_opts:
                    long_flag = live_opt.get("long")
                    flag_str = f"--{long_flag}" if long_flag else opt_name
                    errors.append(
                        f"Drift detected in cli_surface: missing flag '{flag_str}' in command '{name}'"
                    )

    for name in existing_cli_map:
        if name not in live_cli_map:
            errors.append(f"Drift detected in cli_surface: missing command '{name}'")

    # 2. Compare Config surface
    existing_config = {
        o["key"]: o for o in existing.get("config_surface", {}).get("options", [])
    }
    live_config = {
        o["key"]: o for o in live.get("config_surface", {}).get("options", [])
    }
    for k in existing_config:
        if k not in live_config:
            errors.append(f"Drift detected in config_surface: unknown key '{k}'")
        elif existing_config[k] != live_config[k]:
            errors.append(f"Drift detected in config_surface: configuration mismatch for '{k}'")
    for k in live_config:
        if k not in existing_config:
            errors.append(f"Drift detected in config_surface: missing key '{k}'")

    # 3. Compare Function surface
    existing_funcs = {
        f["name"]: f for f in existing.get("function_surface", {}).get("functions", [])
    }
    live_funcs = {
        f["name"]: f for f in live.get("function_surface", {}).get("functions", [])
    }
    for name, ex_fn in existing_funcs.items():
        if name not in live_funcs:
            errors.append(f"Drift detected in function_surface: missing function '{name}'")
        else:
            live_fn = live_funcs[name]
            if ex_fn.get("signature") != live_fn.get("signature") or ex_fn.get("return_type") != live_fn.get("return_type"):
                errors.append(
                    f"Drift detected in function_surface: signature mismatch for '{name}'"
                )
    for name in live_funcs:
        if name not in existing_funcs:
            errors.append(f"Drift detected in function_surface: unexpected function '{name}'")

    # 4. Compare Error surface
    existing_errs = {
        e["code"]: e for e in existing.get("error_surface", {}).get("errors", [])
    }
    live_errs = {
        e["code"]: e for e in live.get("error_surface", {}).get("errors", [])
    }
    for code in live_errs:
        if code not in existing_errs:
            errors.append(f"Drift detected in error_surface: missing error code '{code}' in manifest")
        elif existing_errs[code] != live_errs[code]:
            errors.append(f"Drift detected in error_surface: error descriptor mismatch for '{code}'")
    for code in existing_errs:
        if code not in live_errs:
            errors.append(f"Drift detected in error_surface: unexpected error code '{code}'")

    # 5. Compare SQL Contract surface
    existing_sql_types = {
        t["name"]: t for t in existing.get("sql_contract_surface", {}).get("types", [])
    }
    live_sql_types = {
        t["name"]: t for t in live.get("sql_contract_surface", {}).get("types", [])
    }
    for type_name, ex_t in existing_sql_types.items():
        if type_name not in live_sql_types:
            errors.append(
                f"Drift detected in sql_contract_surface: unexpected SQL type '{type_name}'"
            )
        else:
            live_t = live_sql_types[type_name]
            ex_ops = {o["operation"]: o for o in ex_t.get("operations", [])}
            live_ops = {o["operation"]: o for o in live_t.get("operations", [])}
            for op_name, ex_op in ex_ops.items():
                if op_name not in live_ops:
                    errors.append(
                        f"Drift detected in sql_contract_surface: unexpected operation '{op_name}' for type '{type_name}'"
                    )
                elif live_ops.get(op_name) != ex_op:
                    errors.append(
                        f"Drift detected in sql_contract_surface: cell mismatch for {type_name} {op_name}"
                    )
    for type_name in live_sql_types:
        if type_name not in existing_sql_types:
            errors.append(
                f"Drift detected in sql_contract_surface: missing SQL type '{type_name}'"
            )

    # 6. Compare Catalog & Metric surfaces
    if existing.get("catalog_surface") != live.get("catalog_surface"):
        errors.append("Drift detected in catalog_surface: catalog schema mismatch")
    if existing.get("metric_surface") != live.get("metric_surface"):
        errors.append("Drift detected in metric_surface: telemetry metric mismatch")


def find_repo_root(start: Path) -> Path:
    cur = start.resolve()
    while cur != cur.parent:
        if (cur / "Cargo.toml").exists() and (cur / "crates").exists():
            return cur
        cur = cur.parent
    try:
        out = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
        if out:
            return Path(out)
    except Exception:
        pass
    return start


def main() -> None:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent
    errors: list[str] = []

    sql_matrix_path = root / "contracts/sql-type-matrix.toml"
    errors_path = root / "contracts/errors.toml"
    manifest_path = root / "docs/product-surface.json"

    # Step 1: Check SQL matrix TOML
    _, sql_rejection_codes = check_sql_matrix_toml(sql_matrix_path, errors)

    # Step 2: Check error catalog IDs
    valid_error_codes = check_error_catalog_ids(errors_path, errors)

    # Assert SQL rejection codes exist in error catalog
    for code in sql_rejection_codes:
        if code not in valid_error_codes:
            errors.append(
                f"Unresolved reference: rejection error code '{code}' in sql-type-matrix.toml not found in catalog"
            )

    if not manifest_path.exists():
        errors.append(f"Product surface manifest does not exist at {manifest_path}")
        print("VIOLATION:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        sys.exit(1)

    try:
        existing_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as e:
        errors.append(f"Error parsing {manifest_path}: {e}")
        existing_manifest = {}

    # Step 3: Check ID resolution inside manifest
    check_manifest_id_resolution(existing_manifest, valid_error_codes, errors)

    # Step 4: Generate live manifest to temporary file and compare
    repo_root = find_repo_root(Path(__file__).resolve().parent)
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)

    try:
        cmd = [
            "cargo",
            "run",
            "-q",
            "--manifest-path",
            str(repo_root / "Cargo.toml"),
            "-p",
            "rockstream-docgen",
            "--",
            "generate",
            "--output",
            str(tmp_path),
        ]
        result = subprocess.run(cmd, cwd=repo_root, capture_output=True, text=True)
        if result.returncode != 0:
            errors.append(f"Failed to generate live manifest via docgen: {result.stderr}")
        else:
            try:
                live_manifest = json.loads(tmp_path.read_text(encoding="utf-8"))
                compare_manifest_surfaces(existing_manifest, live_manifest, errors)
            except Exception as e:
                errors.append(f"Failed to parse live generated manifest: {e}")
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    if errors:
        print("VIOLATION: Product surface validation failed:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        sys.exit(1)

    print("OK: Product surface manifest and contract validation passed with zero drift.")


if __name__ == "__main__":
    main()
