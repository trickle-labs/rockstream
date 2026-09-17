# VS1 sign-off

Status: local implementation complete; repository required-check enforcement is
still owned by the VS0 administrator action.

- [x] VS1-01: accepted checked-arithmetic and merge-law admission ADR.
- [x] VS1-02: shared verified arithmetic and exact-width scalar codecs.
- [x] VS1-03: law descriptors carry version, domain, evidence, and admission state.
- [x] VS1-04: law and tagged-storage merge paths share checked kernels and validate first operands.
- [x] VS1-05: regrouping admission fails closed and skew planning spills without concrete admission.
- [x] VS1-06: `SumCount/v1` uses `ExactOnly`; SQL-boundary and floating-point limits are documented.

Commands:

```sh
.cache/verus/0.2026.09.13.671956e/cargo-verus verify -p rockstream-verified --locked
python3 scripts/check-verus-manifest.py
cargo test --locked -p rockstream-types --tests
cargo test --locked -p rockstream-storage merge_registry --lib
cargo test --locked -p rockstream-control --test hot_key_planning_tests --test skew_split_control_tests
```
