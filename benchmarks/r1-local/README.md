# R1 local contract

These files freeze the `MBP-M5Pro-48GB-v1` workload contract before R1
measurements begin. `profile.toml` remains unsealed until the harness records
the observed machine and candidate values.

Run `./scripts/run-r1-local.sh digest` to regenerate the input and SQL digests.
Run `./scripts/run-r1-local.sh verify` to compare the contract with
`contract.sha256`.
Run `./scripts/run-r1-local.sh candidates` after committing product changes to
build the detached B0 rebuild and current candidate and write
`evidence/r1-local/candidates.json`. Run `./scripts/run-r1-local.sh
verify-candidates` before every workload run; it re-hashes both source trees,
lockfiles, toolchains, and binaries and re-queries each public version and
effective-configuration surface.

B0 is always labelled `b0-v0.59.4-local-rebuild`; it is a clean local source
rebuild, not an archived historical binary. There is no B1 comparator. B0 is
admitted only to the ordinary one-worker aggregate and join workloads.

Candidate binaries are written under `evidence/r1-local/artifacts/` and are
ignored by Git; their SHA-256 values remain in the compact candidate record.
Changing the profile, corpus, thresholds, workload, or SQL after a scored run
requires a new profile revision and invalidates every earlier sample.
