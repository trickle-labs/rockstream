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

- `cargo deny check` runs in CI on every PR.
- **Not yet true — found by the 2026-07-11 testing-quality review**: this
  section previously stated "Dependabot or Renovate keeps dependencies
  current" as settled fact. Neither `.github/dependabot.yml` nor
  `renovate.json` exists in this repository today, and `cargo deny check` has
  no scheduled (cron) trigger — it runs only when a PR happens to touch the
  repo, so a CVE disclosed against an already-merged, otherwise-untouched
  dependency produces no signal until the next unrelated change runs CI.
  Scheduled to be made real at `NEW_ROADMAP.md` v0.45.3 (a scheduled
  `cargo deny check` workflow plus a real `dependabot.yml`).
- MSRV is tested in CI by using the pinned toolchain.
