#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import struct
import sys
from pathlib import Path

import tomllib

EXPECTED_THRESHOLDS = {
    "contract_version": 1,
    "sampling": {"repetitions": 5, "max_sample_cv": 0.15},
    "one_key_persistence": {
        "live_groups": [1000, 100000, 10000000],
        "max_logical_byte_ratio": 1.10,
        "max_full_state_entries_visited": 0,
        "require_identical_mutation_counts": True,
    },
    "shared_state": {
        "max_shared_20_to_shared_1_ratio": 1.50,
        "max_shared_20_to_private_20_ratio": 0.20,
    },
    "shared_source_index": {
        "max_key_build_ratio": 1.50,
        "max_trace_rows_written_ratio": 1.50,
        "max_cpu_per_accepted_change_ratio": 1.75,
    },
    "factorized": {
        "min_classic_to_factorized_intermediate_ratio": 10.0,
        "min_throughput_ratio": 1.50,
        "max_cpu_per_visible_change_ratio": 0.67,
        "max_exchange_bytes_per_visible_change_ratio": 0.67,
    },
    "ordinary_regression": {
        "min_throughput_ratio": 0.85,
        "max_p99_freshness_ratio": 1.15,
    },
    "worker_scaling": {
        "min_four_to_one_throughput_ratio": 2.00,
        "yellow_min_four_to_one_throughput_ratio": 1.50,
        "require_nonzero_work_per_worker": True,
    },
}

EXPECTED_WORKLOADS = [
    "workloads/factorized-join.toml",
    "workloads/one-key-persistence.toml",
    "workloads/ordinary-aggregate.toml",
    "workloads/ordinary-join.toml",
    "workloads/shared-arrangement.toml",
    "workloads/uniform-worker-scaling.toml",
]
EXPECTED_SQL = [path.replace("workloads/", "sql/").replace(".toml", ".sql") for path in EXPECTED_WORKLOADS]
EXPECTED_WORKLOAD_CONFIGS = {
    "factorized-join.toml": {
        "name": "factorized-join",
        "seed": 5907003,
        "source_rows": 100000,
        "dimension_rows": 1000,
        "changed_rows": 10000,
        "fan_out": 100,
        "strategies": ["classic", "factorized"],
        "sql": "sql/factorized-join.sql",
    },
    "one-key-persistence.toml": {
        "name": "one-key-persistence",
        "seed": 5907001,
        "kind": "structural",
        "live_groups": [1000, 100000, 10000000],
        "operations": ["insert", "update", "delete"],
        "sql": "sql/one-key-persistence.sql",
    },
    "ordinary-aggregate.toml": {
        "name": "ordinary-aggregate",
        "seed": 5907004,
        "source_rows": 100000,
        "workers": 1,
        "candidates": ["b0-v0.59.4-local-rebuild", "current"],
        "sql": "sql/ordinary-aggregate.sql",
    },
    "ordinary-join.toml": {
        "name": "ordinary-join",
        "seed": 5907005,
        "source_rows": 100000,
        "dimension_rows": 1000,
        "workers": 1,
        "candidates": ["b0-v0.59.4-local-rebuild", "current"],
        "sql": "sql/ordinary-join.sql",
    },
    "shared-arrangement.toml": {
        "name": "shared-arrangement",
        "seed": 5907002,
        "source_rows": 100000,
        "consumer_counts": [1, 20],
        "arrangement_modes": ["shared", "private"],
        "sql": "sql/shared-arrangement.sql",
    },
    "uniform-worker-scaling.toml": {
        "name": "uniform-worker-scaling",
        "seed": 5907006,
        "source_rows": 100000,
        "live_groups": 100000,
        "workers": [1, 2, 4],
        "distribution": "uniform",
        "sql": "sql/uniform-worker-scaling.sql",
    },
}


def load_toml(path: Path) -> dict:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def group_digest(root: Path, paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths):
        relative = path.relative_to(root).as_posix().encode()
        digest.update(relative + b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest()


def contract_paths(root: Path) -> tuple[list[Path], list[Path], list[Path]]:
    base = root / "benchmarks" / "r1-local"
    workloads = sorted((base / "workloads").glob("*.toml"))
    sql = sorted((base / "sql").glob("*.sql"))
    corpus = [base / "corpus.toml", *workloads]
    all_paths = [base / "profile.toml", base / "thresholds.toml", *corpus, *sql]
    return corpus, sql, all_paths


def contract_digests(root: Path) -> str:
    base = root / "benchmarks" / "r1-local"
    corpus, sql, all_paths = contract_paths(root)
    values = {
        "profile_sha256": hashlib.sha256((base / "profile.toml").read_bytes()).hexdigest(),
        "corpus_sha256": group_digest(root, corpus),
        "sql_sha256": group_digest(root, sql),
        "thresholds_sha256": hashlib.sha256((base / "thresholds.toml").read_bytes()).hexdigest(),
        "contract_sha256": group_digest(root, all_paths),
    }
    return "".join(f"{name}={value}\n" for name, value in values.items())


def generated_digests(root: Path) -> str:
    base = root / "benchmarks" / "r1-local"
    corpus = load_toml(base / "corpus.toml")
    digest = hashlib.sha256()
    row = struct.Struct(">QQQqB")
    change = struct.Struct(">BQQq")

    for name in EXPECTED_WORKLOAD_CONFIGS:
        workload = load_toml(base / "workloads" / name)
        digest.update(json.dumps(workload, sort_keys=True, separators=(",", ":")).encode() + b"\n")
        scales = workload.get("live_groups")
        if not isinstance(scales, list):
            scales = [workload["source_rows"]]
        seed = workload["seed"]
        for scale in scales:
            digest.update(struct.pack(">Q", scale))
            for first in range(0, scale, 8192):
                chunk = bytearray()
                for record_id in range(first, min(first + 8192, scale)):
                    chunk.extend(
                        row.pack(
                            record_id,
                            record_id % scale,
                            (record_id * 17 + seed) % 1000,
                            (record_id * 31 + seed) % 2001 - 1000,
                            1,
                        )
                    )
                digest.update(chunk)

        dimension_rows = workload.get("dimension_rows", 0)
        for first in range(0, dimension_rows, 8192):
            chunk = bytearray()
            for record_id in range(first, min(first + 8192, dimension_rows)):
                chunk.extend(struct.pack(">QQ", record_id, (record_id * 19 + seed) % 100))
            digest.update(chunk)

        initial_rows = max(scales)
        change_rows = corpus["generation"]["change_rows_per_workload"]
        for first in range(0, change_rows, 8192):
            chunk = bytearray()
            for offset in range(first, min(first + 8192, change_rows)):
                percentile = offset % 100
                operation = 0 if percentile < 50 else 1 if percentile < 80 else 2
                record_id = initial_rows + offset if operation == 0 else (offset * 97 + seed) % initial_rows
                chunk.extend(
                    change.pack(
                        operation,
                        record_id,
                        record_id % initial_rows,
                        (offset * 43 + seed) % 2001 - 1000,
                    )
                )
            digest.update(chunk)

    _, sql, _ = contract_paths(root)
    return f"input_sha256={digest.hexdigest()}\nsql_sha256={group_digest(root, sql)}\n"


def validate(root: Path) -> str | None:
    base = root / "benchmarks" / "r1-local"
    required = [
        base / name
        for name in (
            "profile.toml",
            "corpus.toml",
            "thresholds.toml",
            "contract.sha256",
            "generated-digests.sha256",
        )
    ]
    missing = [path.relative_to(root).as_posix() for path in required if not path.is_file()]
    if missing:
        return f"missing contract file: {missing[0]}"

    try:
        thresholds = load_toml(base / "thresholds.toml")
        corpus = load_toml(base / "corpus.toml")
        profile = load_toml(base / "profile.toml")
    except (OSError, tomllib.TOMLDecodeError) as error:
        return f"invalid contract TOML: {error}"

    if thresholds != EXPECTED_THRESHOLDS:
        return "thresholds.toml differs from R1 Section 3"
    if (
        profile.get("contract_version") != 1
        or profile.get("profile_id") != "MBP-M5Pro-48GB-v1"
        or profile.get("revision") != 1
        or profile.get("state") != "unsealed"
    ):
        return "profile.toml has unexpected profile identity or revision"

    load = corpus.get("load", {})
    changes = corpus.get("changes", {})
    repetitions = corpus.get("repetitions", {})
    if load != {
        "protocol": "pgwire",
        "lanes": 8,
        "transaction_rows": 256,
        "max_in_flight_transactions_per_lane": 1,
        "visibility_barrier": "output_frontier_query_visible",
        "warm_up_seconds": 30,
        "measurement_seconds": 60,
    }:
        return "corpus.toml has unexpected closed-loop load settings"
    if changes != {
        "insert_percent": 50,
        "update_percent": 30,
        "delete_percent": 20,
        "delete_semantics": "retraction",
        "oracle_comparison": "complete_multiset",
    }:
        return "corpus.toml has unexpected change mix"
    if repetitions != {
        "count": 5,
        "order": ["a_then_b", "b_then_a", "a_then_b", "b_then_a", "a_then_b"],
    }:
        return "corpus.toml has unexpected repetition order"
    if corpus.get("freshness") != {
        "histogram_buckets_ms": [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000]
    }:
        return "corpus.toml has unexpected freshness buckets"
    if corpus.get("generation") != {
        "format": "r1-input-v1-big-endian",
        "change_rows_per_workload": 10000,
    }:
        return "corpus.toml has unexpected generation settings"
    if corpus.get("schemas") != {
        "source": {
            "primary_key": "id",
            "columns": [
                "id BIGINT NOT NULL",
                "group_id BIGINT NOT NULL",
                "dimension_id BIGINT NOT NULL",
                "value BIGINT NOT NULL",
                "active BOOLEAN NOT NULL",
            ],
        },
        "dimension": {
            "primary_key": "id",
            "columns": ["id BIGINT NOT NULL", "bucket BIGINT NOT NULL"],
        },
    }:
        return "corpus.toml has unexpected schemas"
    if corpus.get("inputs") != {"workloads": EXPECTED_WORKLOADS, "sql": EXPECTED_SQL}:
        return "corpus.toml has unexpected workload or SQL inputs"

    for relative in [*EXPECTED_WORKLOADS, *EXPECTED_SQL]:
        if not (base / relative).is_file():
            return f"missing corpus input: benchmarks/r1-local/{relative}"
    for name, expected in EXPECTED_WORKLOAD_CONFIGS.items():
        try:
            actual_workload = load_toml(base / "workloads" / name)
        except (OSError, tomllib.TOMLDecodeError) as error:
            return f"invalid workload TOML: {error}"
        if actual_workload != expected:
            return f"workloads/{name} differs from the frozen corpus"

    try:
        actual_generated = generated_digests(root)
    except (OSError, tomllib.TOMLDecodeError) as error:
        return f"cannot generate corpus digests: {error}"
    expected_generated = (base / "generated-digests.sha256").read_text(encoding="utf-8")
    if actual_generated != expected_generated:
        return "generated input or SQL digests differ from the frozen corpus"

    try:
        actual = contract_digests(root)
    except OSError as error:
        return f"cannot digest contract: {error}"
    expected = (base / "contract.sha256").read_text(encoding="utf-8")
    if actual != expected:
        return "contract digests do not match benchmarks/r1-local/contract.sha256"
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--digest", action="store_true")
    parser.add_argument("--contract-digest", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()

    if args.digest:
        try:
            sys.stdout.write(generated_digests(root))
        except OSError as error:
            print(f"VIOLATION: cannot digest contract: {error}")
            return 1
        return 0
    if args.contract_digest:
        try:
            sys.stdout.write(contract_digests(root))
        except OSError as error:
            print(f"VIOLATION: cannot digest contract: {error}")
            return 1
        return 0

    violation = validate(root)
    if violation:
        print(f"VIOLATION: {violation}")
        return 1
    print("R1 local contract verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
