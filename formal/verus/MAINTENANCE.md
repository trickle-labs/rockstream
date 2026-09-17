# Verus maintenance policy

The release owner owns the pinned verifier, solver, support-library, and
production-compiler records. Maintainers of a covered crate own its manifest
claim, executable contract, callers, adapter tests, and sign-off evidence.

Every change to `formal/verus/`, `crates/rockstream-verified/`, a covered
caller, `Cargo.lock`, `rust-toolchain.toml`, or the Verus workflow must run the
manifest checker, the negative fixtures, the complete required verifier target,
and the supported production build matrix. A verifier, solver, or support-
library upgrade requires a reviewed update to `toolchain.lock.toml`, a clean
non-incremental proof run, and refreshed claim evidence. Assumption changes
require the same review and an updated `assumptions.toml` entry.

Use the frozen R1 profile and thresholds for runtime, allocation, RSS, commit,
and freshness comparisons. A missing backend, unavailable tool, skipped test,
or absent raw log is `blocked`; it is never a pass. Do not weaken a contract,
remove a target from the manifest, or disable a required job to make a run
green. Temporary scope reductions must name the affected claims and be
reviewed in the matching sign-off.

The local workflow is:

```sh
python3 scripts/check-verus-qualification.py
make verify-rust
make verify-proof-contracts
cargo check --workspace --locked
cargo build --workspace --locked --release
```

The clean qualification record must include the source revision, digests for
the manifest, lockfile, toolchain, and normal Cargo artifacts, exact commands,
tool versions, raw verifier output, negative-fixture results, and the measured
configuration. Store those run artifacts under `evidence/verus/` and link them
from `sign-offs/verus/VS6.md`.
