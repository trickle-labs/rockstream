# Release Governance and History Protection

## 1. Branch Protection Policy

All production and release branches (`main`, `release/*`) enforce mandatory branch protection rules:
- **Required Status Checks**: Full workspace unit tests, SlateDB LFS suite, MinIO S3 integration tests, capability contract validation, dispatch wiring audits, and release candidate gates must pass prior to merge.
- **No Force Pushes**: History is append-only and linear; force pushing to protected branches is permanently disabled.
- **Linear History**: Require squash merging or rebase merging to ensure clean and traceable commit graphs.

## 2. Signed Release Tag Policy

Release tags identify immutable release artifacts and must adhere to strict cryptographic standards:
- **Tag Naming**: Release tags must follow standard semantic versioning prefixed with `v` (e.g. `v0.59.1`, `v1.0.0`).
- **Cryptographic Signatures**: All release tags must be annotated and GPG/SSH signed by an authorized maintainer key.
- **SHA Binding**: Every release tag permanently points to the exact commit SHA verified during the release qualification run. Mutating or moving existing release tags is forbidden.

## 3. Ownership Review (CODEOWNERS)

Mandatory peer review and approval from designated code owners (`.github/CODEOWNERS`) is required for critical subsystems:
- **Release Workflows & Prompts**: `/.github/workflows/` and `/.github/prompts/` require approval from `@rockstream-maintainers`.
- **Formal Specifications**: `/formal/` models and `/FIZZBEE_TEST_PLAN.md` require approval from `@rockstream-formal-methods`.
- **Security Policies**: `/SECURITY.md`, `/SECURITY_REVIEW_COMMISSION.md`, and `/docs/threat-model.md` require approval from `@rockstream-security`.
- **Capability Contracts & Roadmap**: `/capabilities.toml`, `/docs/language-features.md`, and `/NEW_ROADMAP.md` require approval from `@rockstream-maintainers`.
