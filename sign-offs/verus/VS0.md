# VS0 sign-off

Status: local implementation complete; repository required-check enforcement is
pending repository administrator action.

- [x] VS0-01: pinned Verus release, commit, archive hashes, support crates, solver source, and Rust 1.88 record.
- [x] VS0-02: production-called bucket-normalization pilot with a checked contract and boundary tests.
- [x] VS0-03: claim manifest links implementation, theorem, caller, domain, assumptions, and regression test.
- [x] VS0-04: application assumption scan and negative fixtures for unchecked assumptions and `cfg(verus_only)` substitution.
- [x] VS0-05: `verify-rust`, `verify-proof-contracts`, and `verify-protocol` are separate gates; FizzBee path coupling ignores `formal/verus`.
- [x] VS0-06: positive smoke and isolated invalid-theorem fixtures verify that the gate does real work.
- [ ] VS0-06-admin: register `verus-verify` as a required check in the repository ruleset.

Commands:

```sh
make verify-rust
make verify-proof-contracts
make verify-protocol
cargo test --locked -p rockstream-plan --test virtual_bucket_routing_tests
cargo build --locked --release -p rockstream-cli
```
