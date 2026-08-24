# Test commands

Run the checks that match the change. The commands below are the repository's
current command taxonomy.

```console
$ make build
$ make fmt
$ make clippy
$ make test
$ make error-codes
$ make exit-criteria
$ make documentation
$ make check
$ cargo deny check
$ cargo audit
$ bash scripts/check-documentation.test.sh
$ cargo test -p rockstream-cli --test documentation_transcript_tests
```

The CI workflow runs the same Rust checks and the scheduled dependency audit.
Use the [dependency policy](../DEPENDENCY_POLICY.md) for license and advisory
rules.
