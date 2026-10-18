# v0.72 observed-run bundle integrity (unqualified)

This command cross-checks evidence files from one separately executed workload run. **A pass is only structural evidence integrity; it does not qualify #132, #138 or #141.** An attacker or erroneous generator can forge both the manifest and its referenced files. A complete independent oracle and release-process provenance must still be established outside this verifier.

Run from the repository root:

```sh
rtk cargo run --manifest-path tools/r1-local-harness/Cargo.toml --bin verify-v072-run-bundle -- --manifest <bundle-directory>/manifest.json
```

## Manifest v1

All paths are relative to the manifest's directory. They must resolve to **distinct files inside that directory**, with no absolute paths, parent traversal or symlink escape. Expected SHA-256 strings are lowercase hex. The tool reads and rehashes every referenced file, not merely the manifest strings.

| Field | Meaning |
| --- | --- |
| `schema_version` | Exactly 1 |
| `run_id` | Nonblank name (at most 128 characters) |
| `profile_path` | Byte-identical frozen `benchmarks/r1-local/profile-v072.toml` (digest `d5cf97b3d0a181cddd78dbf811e22224c4767bd1948143b45beb38390464719a`) |
| `executable_path`, `executable_sha256` | Candidate file and its SHA-256 |
| `capture_path`, `capture_sha256` | Observed-process JSON and its SHA-256 |
| `expected_rows_path`, `expected_rows_sha256` | Entire independently produced expected result as JSON array of string rows and its SHA-256 |
| `observed_rows_path`, `observed_rows_sha256` | Entire observed result with the same representation and its SHA-256 |

The process capture must be schema v1, label itself `observation_only_not_capacity_qualified`, match the executable digest, contain a positive PID and at least two strictly time-ordered samples with non-decreasing user/system CPU counters and some nonzero RSS. This identifies internal inconsistencies; it cannot prove that the PID really ran the workload or that each counter came from that PID.

The verifier independently compares the expected and observed row **multisets**, including duplicates, rejecting mismatches and empty expected outputs. It does not accept result counts, prefixes, or sample hashes as complete-result oracles. It rejects an invalid profile, partial manifest, changed binary/capture, forged file digest, path alias, traversal, and non-monotonic counters.

Success is always printed as `"status":"cross_file_structural_only_not_capacity_qualified"`. Full #132 acceptance additionally requires process-capture provenance, eight workload adapters, all applicable frozen matrix cells, repeated release runs on the specified host, costs, noise/regression gates and separate per-criterion sign-off. Do not use fixture-generated files as qualification evidence.
