# Byte-decoder fuzzing

The target registry is `fuzz/Cargo.toml`. The scheduled workflow runs **every
registered target** every six hours, on main pushes, and on manual dispatch.
Removing one of the seven mandatory targets, its source, or its known-valid seed
fails the run. PR CI retains the fixed corpus replay in
`rockstream-gateway/tests/fuzz_corpus_regression_tests.rs` and runs the engine
runner's deliberate-failure and zero-execution fixtures.

These targets measure byte-decoder panic and sanitizer safety. They do **not**
establish stateful protocol correctness, semantic equivalence, or a source-line
coverage percentage. PGWire calls the startup and frontend byte decoders directly
so asynchronous connection teardown cannot hide parser execution or panics. CDC
exercises the existing single-payload PgOutput and Wal2Json entry points.

## Run locally

Use Python 3.11+ and Rust `nightly-2026-02-01` with `cargo-fuzz` 0.13.1:

```sh
cargo install cargo-fuzz --version 0.13.1 --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_DEV_INCREMENTAL=false cargo +nightly-2026-02-01 fuzz build --dev --target-dir target/fuzz
RUSTUP_TOOLCHAIN=nightly-2026-02-01 python3 scripts/run-fuzz.py --bin-dir target/fuzz/<host-triple>/debug
```

Each target gets 120 engine seconds, a five-second per-input timeout, a 2048 MiB
RSS limit, a 65536-byte maximum generated input, and seed 130. A process-group
watchdog allows the engine budget plus the input timeout plus 15 seconds, then
kills a stuck engine and its children. Failure minimization gets 30 seconds plus
the same watchdog allowance. `--seconds N` changes the engine duration for smoke
runs. All targets are attempted even if an earlier target fails.

Exit zero requires a coverage-bearing `DONE` event and a completed execution
count beyond initial corpus replay. A crash, sanitizer failure, memory limit,
per-input timeout, watchdog timeout, missing binary, or missing execution evidence
fails the job. No compile-only soak can pass.

## Seeds and retained evidence

`valid-*` files are checked against exact decoder outputs by
`known_valid_fuzz_seeds_decode_exactly`. They cover SQL DDL, PGWire startup/query,
both CDC formats, worker messages, Raft RPCs, Kafka rows, and a correctly signed
HS256 JWT. The JWT deliberately omits expiration so this parsing seed does not
expire. Original malformed seeds remain checked in and replayed.

The workflow restores accumulated `fuzz/corpus` using GitHub Actions cache, then
restores all checked-in seeds before execution. New coverage-bearing inputs are
written by libFuzzer into the corpus and saved even when a target fails. Cache
eviction can reset accumulated inputs; checked-in regressions are always restored.
The 90-day run artifact separately includes the complete corpus and
`target/fuzz-results`:

- `run.json`: candidate revision, exact compiler identity, cargo-fuzz version,
  build command/environment, registered targets, coverage scope, and overall status.
- `<target>/report.json`: exact commands and resource budgets, engine exit status,
  binary/input SHA-256 hashes, before/after corpus inventories, minimization
  status, and two replay commands/results per failure.
- Original and minimized failure bytes, engine/minimizer logs, and replay logs.

Minimization is bounded; a retained minimized input is the smallest found within
that budget. An unsuccessful minimization retains the original and fails visibly.
The replay gate requires matching nonzero exit codes and libFuzzer/sanitizer
summaries on both replays; it does not promise deterministic mutation sequences.
Download artifacts before retention expires to preserve them indefinitely, and
promote useful failures into the checked-in regression corpus after fixing them.

To reproduce, check out `candidate_revision`, install the recorded toolchain and
cargo-fuzz version, rebuild with the command above, then run the recorded replay
command with the downloaded input and rebuilt binary paths. The input bytes,
rather than the random seed alone, are the authoritative reproducer.

## Runner self-test

```sh
cargo +nightly-2026-02-01 fuzz build --dev --fuzz-dir scripts/fixtures/fuzz-engine --target-dir target/fuzz-fixtures
python3 scripts/run-fuzz.test.py --bin-dir target/fuzz-fixtures/<host-triple>/debug -q
```

The independent fixture panics on `!`. Tests require the original
`padding!padding`, the minimized `!`, and two matching crash replays, and require
the zero-execution fixture to fail despite exiting zero. PR CI uploads these
actual failure inputs and reports as `fuzz-runner-self-test-<revision>`.
