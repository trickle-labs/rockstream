#!/usr/bin/env bash
# check-product-surface.test.sh — self-test check-product-surface against deliberate mutations (DOC-004).
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-product-surface.sh"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

# 1. Base check: real repo tree must pass cleanly
bash "$CHECKER" "$ROOT" >/dev/null || fail "real product surface did not pass"

setup_tree() {
  rm -rf "$TMP_ROOT"
  mkdir -p "$TMP_ROOT/contracts" "$TMP_ROOT/docs" "$TMP_ROOT/scripts"
  cp "$ROOT/contracts/sql-type-matrix.toml" "$TMP_ROOT/contracts/"
  cp "$ROOT/contracts/errors.toml" "$TMP_ROOT/contracts/"
  cp "$ROOT/docs/product-surface.json" "$TMP_ROOT/docs/"
  cp "$ROOT/scripts/check-product-surface.py" "$TMP_ROOT/scripts/"
  cp "$ROOT/scripts/check-product-surface.sh" "$TMP_ROOT/scripts/"
}

run_bad() {
  local name="$1"
  local expected="${2:-VIOLATION:}"
  if bash "$TMP_ROOT/scripts/check-product-surface.sh" "$TMP_ROOT" >"$TMP_ROOT/$name.out" 2>&1; then
    cat "$TMP_ROOT/$name.out"
    fail "$name mutation was accepted"
  fi
  grep -q "$expected" "$TMP_ROOT/$name.out" || {
    cat "$TMP_ROOT/$name.out"
    fail "$name did not report expected error: '$expected'"
  }
}

# 2. Mutation: unexpected CLI flag
setup_tree
python3 -c '
import json, sys
from pathlib import Path
p = Path("'"$TMP_ROOT"'/docs/product-surface.json")
data = json.loads(p.read_text())
data["cli_surface"]["commands"][0].setdefault("options", []).append({
    "name": "experimental_flag",
    "short": None,
    "long": "experimental-flag",
    "help": "Experimental test flag",
    "required": False,
    "value_name": None,
    "default_value": None,
    "possible_values": []
})
p.write_text(json.dumps(data, indent=2))
'
run_bad "cli_flag_drift" "Drift detected in cli_surface: unexpected flag '--experimental-flag'"

# 3. Mutation: undocumented config key
setup_tree
python3 -c '
import json, sys
from pathlib import Path
p = Path("'"$TMP_ROOT"'/docs/product-surface.json")
data = json.loads(p.read_text())
data["config_surface"]["options"].append({
    "key": "storage.secret_bypass",
    "data_type": "bool",
    "default_value": "false",
    "description": "Bypass storage security checks",
    "deprecated": False,
    "env_var": None,
    "source_origin": "rockstream-runtime"
})
p.write_text(json.dumps(data, indent=2))
'
run_bad "config_key_drift" "Drift detected in config_surface: unknown key 'storage.secret_bypass'"

# 4. Mutation: altered function signature
setup_tree
python3 -c '
import json, sys
from pathlib import Path
p = Path("'"$TMP_ROOT"'/docs/product-surface.json")
data = json.loads(p.read_text())
for fn in data["function_surface"]["functions"]:
    if fn["name"].upper() == "UPPER":
        fn["return_type"] = "INT4"
p.write_text(json.dumps(data, indent=2))
'
run_bad "function_signature_drift" "Drift detected in function_surface: signature mismatch for 'upper'"

# 5. Mutation: missing error code
setup_tree
python3 -c '
import json, sys
from pathlib import Path
p = Path("'"$TMP_ROOT"'/docs/product-surface.json")
data = json.loads(p.read_text())
data["error_surface"]["errors"] = [e for e in data["error_surface"]["errors"] if e["code"] != "RS-0001"]
p.write_text(json.dumps(data, indent=2))
'
run_bad "missing_error_code" "Drift detected in error_surface: missing error code 'RS-0001'"

# 6. Mutation: tampered SQL matrix cell
setup_tree
python3 -c '
import json, sys
from pathlib import Path
p = Path("'"$TMP_ROOT"'/docs/product-surface.json")
data = json.loads(p.read_text())
for ty in data["sql_contract_surface"]["types"]:
    if ty["name"] == "FLOAT8":
        for op in ty["operations"]:
            if op["operation"] == "joins":
                op["status"] = "Core"
                op["rejection_code"] = None
p.write_text(json.dumps(data, indent=2))
'
run_bad "sql_matrix_cell_drift" "Drift detected in sql_contract_surface: cell mismatch for FLOAT8 joins"

# 7. Mutation: dangling error ID reference
setup_tree
python3 -c '
import json, sys
from pathlib import Path
p = Path("'"$TMP_ROOT"'/docs/product-surface.json")
data = json.loads(p.read_text())
data["cli_surface"]["commands"][0].setdefault("error_codes", []).append("RS-9999")
p.write_text(json.dumps(data, indent=2))
'
run_bad "dangling_id_reference" "Unresolved reference: error code 'RS-9999' not found in catalog"

# 8. Mutation: invalid SQL matrix status
setup_tree
python3 -c '
from pathlib import Path
p = Path("'"$TMP_ROOT"'/contracts/sql-type-matrix.toml")
s = p.read_text()
p.write_text(s.replace("status = \"Core\"", "status = \"InvalidStatus\"", 1))
'
run_bad "invalid_sql_matrix_status" "Invalid status 'InvalidStatus'"

echo "OK: check-product-surface.sh self-test passed."
