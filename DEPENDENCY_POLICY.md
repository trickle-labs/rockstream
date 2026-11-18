# Dependency Policy

RockStream follows a strict dependency policy to maintain security, license
compliance, and build reproducibility.

## Rules

1. **License allowlist.** Permitted licenses: MIT, Apache-2.0,
   Apache-2.0 WITH LLVM-exception, BSD-2-Clause, BSD-3-Clause, ISC,
   Unicode-3.0, Unicode-DFS-2016, Zlib, CC0-1.0, bzip2-1.0.6, and
   CDLA-Permissive-2.0. `webpki-root-certs` brings in CDLA-Permissive-2.0
   through `object_store` 0.14. CC0-1.0 and bzip2-1.0.6 come from Apache Arrow
   and DataFusion dependencies.
   Copyleft licenses (GPL, LGPL, AGPL) are denied.

2. **No wildcard versions.** All dependencies must use exact or bounded
   version ranges.

3. **Advisory compliance.** Known vulnerabilities are denied. Unmaintained
   and yanked crates produce warnings.

4. **Source restrictions.** Only crates.io is an allowed registry. No git
   dependencies in production builds.

5. **MSRV.** The minimum supported Rust version is pinned in
   `rust-toolchain.toml` and enforced by `rust-version` in workspace
   `Cargo.toml`.

## Enforcement

- `cargo deny check` runs in CI on every PR, plus a scheduled daily run via
  `.github/workflows/dependency-audit.yml` (cron, independent of PR/push
  activity) so a CVE disclosed against an already-merged, otherwise-untouched
  dependency is caught within 24h instead of waiting for the next unrelated
  PR.
- `cargo audit` runs alongside cargo deny on every PR and in the same daily
  workflow. The checked-in vulnerable fixture and self-test prove that both
  gates reject an advisory-bearing lockfile.
- `.github/dependabot.yml` keeps `cargo` dependencies current with a weekly
  version-bump PR schedule.
- MSRV is tested in CI by using the pinned toolchain.

## Reviewed advisory exceptions

Every advisory ignored in `deny.toml` has an accountable owner, a review/expiry
date, a rationale, and a removal condition. The machine-readable ignore list
stays in `deny.toml`; this table is the human review record checked by CI.

| Advisory | Owner | Rationale | Review/expiry | Removal condition |
|---|---|---|---|---|
| RUSTSEC-2025-0141 | Runtime maintainers | bincode is an unmaintained SlateDB transitive dependency; no supported direct replacement is available in the current storage stack | 2027-01-31 | Remove when SlateDB no longer resolves bincode or provides a supported replacement |
| RUSTSEC-2024-0436 | Runtime maintainers | paste is an unmaintained SlateDB transitive dependency and is not directly called by RockStream | 2027-01-31 | Remove when SlateDB drops paste from the dependency graph |
| RUSTSEC-2025-0111 | Test infrastructure maintainers | tokio-tar is a testcontainers transitive dependency; the affected archive path is not used by production code | 2027-01-31 | Remove when testcontainers or its bollard stack upgrades to a fixed or replacement crate |
| RUSTSEC-2025-0134 | Test infrastructure maintainers | rustls-pemfile is a testcontainers transitive dependency; production uses the maintained rustls parsing path | 2027-01-31 | Remove when testcontainers no longer resolves rustls-pemfile |
| RUSTSEC-2026-0194 | Connector maintainers | quick-xml is pinned by object_store 0.12.x and the current use is limited to its supported object-store integration | 2027-01-31 | Remove when object_store permits quick-xml >= 0.41.0 |
| RUSTSEC-2026-0195 | Connector maintainers | quick-xml is pinned by object_store 0.12.x and the current use is limited to its supported object-store integration | 2027-01-31 | Remove when object_store permits quick-xml >= 0.41.0 |
| RUSTSEC-2026-0235 | Runtime maintainers | rkyv is an inactive optional rust_decimal feature retained in Cargo.lock; RockStream does not enable it | 2027-01-31 | Remove when rust_decimal no longer locks rkyv 0.7, or upgrade before enabling the rkyv feature |
| RUSTSEC-2026-0435 | Runtime maintainers | thrift 0.17 is pulled by DataFusion 54's Parquet 58 dependency; RockStream and DataFusion do not call Parquet's legacy `format`/Thrift API, while normal Parquet reads use the replacement codec | 2027-01-31 | Remove when the DataFusion/Parquet dependency graph no longer includes thrift 0.17 and re-review the replacement path |
| RUSTSEC-2026-0543 | Runtime maintainers | Chrono maintainers announced a maintenance wind-down; the crate remains used across workspace APIs and Arrow/DataFusion. This is a maintenance advisory, not a reported vulnerability; replacing date/time behavior needs a separate compatibility change. See the [upstream announcement](https://github.com/chronotope/chrono/issues/1768). | 2027-01-31 | Remove when `chrono` is replaced by a maintained date/time library and date, parsing, and serialization compatibility tests pass |
| RUSTSEC-2026-0555 | Runtime maintainers | Chrono-tz shares the upstream maintenance wind-down, including future timezone database updates. It remains in the Arrow/DataFusion dependency graph; there is no fixed release. See the [upstream announcement](https://github.com/chronotope/chrono/issues/1768). | 2027-01-31 | Remove when the timezone dependency is replaced and timezone/DST behavior tests pass |
| RUSTSEC-2026-0542 | Runtime maintainers | Humantime's advisory follows its use of chrono; it is currently transitive through object_store and has no fixed release. This is a maintenance advisory, not a reported vulnerability. | 2027-01-31 | Remove when the dependency graph no longer resolves `humantime` or a maintained replacement is adopted |
| RUSTSEC-2024-0449 | Runtime maintainers | unsafe-libyaml is a pre-existing unmaintained transitive dependency of serde_yaml; this is a maintenance advisory rather than a known exploitable vulnerability | 2027-01-31 | Remove when serde_yaml is replaced by a maintained YAML implementation that does not depend on unsafe-libyaml |
| RUSTSEC-2026-0542 | Runtime maintainers | humantime is pulled by object_store and its advisory is maintenance-only with no patched release; the current storage stack has no supported replacement | 2027-01-31 | Remove when object_store no longer resolves humantime and the replacement path is reviewed |
| RUSTSEC-2026-0543 | Runtime maintainers | chrono is used by workspace code and remains in the Arrow/DataFusion dependency graph; the advisory is maintenance-only and has no patched release | 2027-01-31 | Remove when workspace callers and the Arrow/DataFusion graph no longer resolve chrono |
| RUSTSEC-2026-0555 | Runtime maintainers | chrono-tz is in the Arrow/DataFusion dependency graph; the advisory is maintenance-only and has no patched release | 2027-01-31 | Remove when Arrow/DataFusion no longer resolves chrono-tz and the replacement path is reviewed |
