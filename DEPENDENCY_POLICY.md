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
- `.github/dependabot.yml` keeps `cargo` dependencies current with a weekly
  version-bump PR schedule.
- MSRV is tested in CI by using the pinned toolchain.
