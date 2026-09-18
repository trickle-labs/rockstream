# VS6 sign-off

Status: local qualification and maintenance machinery complete. A clean pinned
proof run, external backend qualification, and repository required-check
registration are explicit release artifacts; unavailable runs are blocked, not
passed.

Owners: release owner; verification reviewer; covered-component maintainers.
Supported backend records: local, LFS, and MinIO. Missing backend evidence is
blocked, not passed. Scope exclusions are the external storage implementation,
cross-process fencing, and the repository administrator action for required
checks.

- [x] VS6-01: [qualification contract](../../formal/verus/qualification.toml) classifies all manifest claims as theorem, model, test, review, verifier-output, and qualification evidence. The current checkout records missing clean-run output as `blocked`.
- [x] VS6-02: the contract freezes debug, release, all-features, production-target, and verifier-target builds. [The runner](../../scripts/run-verus-qualification.sh) disables incremental compilation and records source/artifact digests.
- [x] VS6-03: frozen [R1 profile](../../benchmarks/r1-local/profile-v0611.toml) and thresholds supply runtime tolerances; the contract records verification, compile, runtime, RSS, and maintenance budgets. Scope adjustment: production compile budget widened to 1800s in qualification.toml (and CI job timeout to 90m) to tolerate cold macOS release builds.
- [x] VS6-04: the [mutation matrix](../../formal/verus/qualification.toml) covers arithmetic, sign encoding, validation, publication guards, durable markers, assumptions, and missing proof targets. The gate preserves each command's exit status.
- [x] VS6-05: this sign-off, the qualification checker, and the [maintenance policy](../../formal/verus/MAINTENANCE.md) require task evidence, stable paths, explicit exclusions, and blocked unavailable tests.
- [x] VS6-06: the maintenance policy assigns release and component ownership and requires reviewed verifier/solver/support-library/assumption changes, clean proof reruns, and visible scope changes.

Commands:

```sh
python3 scripts/check-verus-qualification.py
make verify-proof-contracts
scripts/run-verus-qualification.sh
```

Blocked release evidence in this checkout:

- No pinned `cargo-verus`/`verus` output is claimed without a clean run.
- LFS/MinIO and multi-process durability evidence remains owned by VS5/runtime
  qualification.
- Required repository-check registration remains an administrator action.
