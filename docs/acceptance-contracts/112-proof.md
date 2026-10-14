# Evidence: #112 nullable compiled outer joins

Contract: [112.md](112.md), revision v1.  
Implementation candidate: `1cde238f0c6e8181dd93dc27e60c3eea5a345181`.  
Validation date: 2026-10-14.

## Requirement evidence

| ID | Evidence | Result |
|---|---|---|
| R1 | `compiled_null_keys_never_match_and_retract_exactly` compares the complete declared LEFT, FULL, SEMI, and ANTI relations for identical `(NULL,7)` inputs. RIGHT is also covered. | Pass |
| R2 | The same test compares every signed delta after retracting the right row, then the left row. | Pass |
| R3 | `compiled_null_payload_is_distinct_from_zero_after_reopen` asserts the exact `(1,NULL,1,0):1` relation and subsequent retractions; all five join kinds are covered. | Pass |
| R4 | Both tests call `compile_plan`, execute its `JoinCompiled.pipeline`, and compare complete Arrow schemas plus every nullable value and weight. Recovery uses persist, flush, close, reopen, recompilation, and pipeline restore. | Pass |
| R5 | The temporary NULL-matching and NULL-to-zero mutations described below each fail their corresponding exact-relation assertion. Neither mutation remains in the candidate. | Pass |

## Mutation evidence

- Giving right-side NULL keys the left-side namespace changes LEFT output from `(NULL,7,NULL,NULL):1` to `(NULL,7,NULL,7):1`. The NULL-key regression fails.
- Ignoring row validity during decoding changes `(1,NULL,1,0):1` to `(1,0,1,0):1`. The payload regression fails.

The unmodified production implementation also failed both new regressions on its incorrect non-nullable output schemas.

## Checks

- `rtk cargo test -p rockstream-ops --test compiled_nullable_outer_join_tests --test lfs_outer_join --test operator_state_accounting_tests`: 18 passed.
- `rtk cargo test -p rockstream-ops --lib`: 185 passed, including legacy row decoding, canonical NULL serialization, malformed-encoding rejection, compiler tests, and existing outer-join tests.
- Rust 1.88, explicitly selected through its toolchain binary directory: `cargo clippy -p rockstream-ops --lib --test compiled_nullable_outer_join_tests --no-deps -- -D warnings -A clippy::uninlined_format_args`: passed.
- Formatting, diff whitespace, documentation admission, and operator state-byte gates: passed.

## Boundaries

All-valid persisted row bytes and row identities are unchanged. NULL-containing arrangements written by the old implementation require rebuilding: their input validity was already discarded. Output schemas now permit nullable input columns.

This evidence covers compiled production operators and local-filesystem persistence. Full workspace CI, distributed recovery, and a live pgwire server were not run locally.
