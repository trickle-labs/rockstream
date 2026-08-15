# Dependency Policy

RockStream follows a strict dependency policy to maintain security, license
compliance, and build reproducibility.

## Rules

1. **License allowlist.** Permitted licenses: MIT, Apache-2.0,
   Apache-2.0 WITH LLVM-exception, BSD-2-Clause, BSD-3-Clause, ISC, Unicode,
   CC0-1.0, and bzip2-1.0.6. The last two are permissive/public-domain
   licenses pulled in as transitive dependencies of Apache Arrow / DataFusion.
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
