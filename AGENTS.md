# RockStream agent guidance

- This is a Rust Cargo workspace; product crates live under `crates/`. Start with the affected crate and its nearby tests.
- Use `ARCHITECTURE.md` for system structure, relevant `docs/adr/` records for decisions, and `CONTRIBUTING.md` for setup and required checks.
- New user or operator facing errors need an `RS-XXXX` registry entry. Follow `DEPENDENCY_POLICY.md` for dependency changes.
- For GitHub issue work, follow `docs/agents/issue-tracker.md` and `docs/agents/triage-labels.md`; make tracker changes when requested.
- `openwiki/` is generated reference material. Consult it for a concrete uncertainty; source and tests are authoritative, and generated pages should not be edited manually.
