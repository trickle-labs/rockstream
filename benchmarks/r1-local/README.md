# R1 local contract

These files freeze the `MBP-M5Pro-48GB-v1` workload contract before R1
measurements begin. `profile.toml` remains unsealed until the harness records
the observed machine and candidate values.

Run `./scripts/run-r1-local.sh digest` to regenerate the input and SQL digests.
Run `./scripts/run-r1-local.sh verify` to compare the contract with
`contract.sha256`.
Changing the profile, corpus, thresholds, workload, or SQL after a scored run
requires a new profile revision and invalidates every earlier sample.
