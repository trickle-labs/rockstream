# Evidence: #123 production operator qualification

Contract: [123.md](123.md), revision v1.

## Requirement evidence

| ID | Executed path and exact assertion |
|---|---|
| R1 | The three `delta_native_*_tests` suites construct production operators with declared Int64 inputs. Every emitted delta is compared with an independently declared full Arrow schema and typed signed bag using the shared #111 comparator. NULL padding and nullable LAG/LEAD results are included. |
| R2 | `test_delta_native_join_left_i64` checks matched rows of weight two and an unmatched NULL-extended row, then retracts the full bag. `left_join_wrong_inner_operator_reports_exact_missing_null_row` runs both production operators and asserts the precise missing-row mismatch. |
| R3 | `test_delta_native_topk_i64` constructs `TopKOp(k=2, rank_col=1)`, checks ranks 30 and 20, then checks exactly rank 30 at weight -1 and rank 10 at weight +1. |
| R4 | `test_delta_native_distinct_i64_zero_crossings` checks duplicate insertion, partial deletion, and final retractions. `test_delta_native_index_arrange_i64_i64` checks full passthrough deltas and every expected encoded lookup result before and after deletion. Five analytic window cases check initial and retraction deltas, including rank ties and NULL LAG/LEAD boundaries. Runtime qualification executes production aggregate, min/max, classic/factorized join, tumble, hop, and session pipelines across insertion and retraction. |
| R5 | Operator tests assert `Operator::name()`; outer joins also assert `kind()`. Runtime cases directly construct typed `Stage` variants, and join pipelines assert their actual strategy. Expected maps and arithmetic supply expected values only. |
| R6 | Removed false i32/text matrix labels from Int64-only cases. The old UUID/decimal model is replaced by `scaled_decimal_avg_text_pipeline_exact`, which names its actual scaled-integer input and Utf8 decimal output. Corrected the v0.59.5 matrix proof claim and annotated the v0.59.24 runtime qualification reference. The wrong-inner-join regression checks the exact missing NULL row. |

## Checks

- Operator qualification: 15 passed across the join, distinct/TopK/arrangement, and analytic window suites.
- Runtime operator pipelines: 10 passed.
- SQL oracle (`sql_fuzzer::tests::`): 6 passed, including the #111 typed mutations and nullable/fractional planner cases.
- Rust 1.88 Clippy for the four changed crates and selected integration targets (`--no-deps -- -D warnings -A clippy::uninlined_format_args`): passed.
- Formatting, diff whitespace, documentation admission, and operator-state-byte gates: passed.

The temporary wrong-operator mutation replaced the outer-join execution in `qualify_outer` with `JoinOp` and retained the declared output schema to isolate row correctness. `test_delta_native_join_left_i64` failed with exactly `Rows([RowWeightMismatch { row: [Int64(2), Int64(20), Int64(NULL), Int64(NULL)], expected_weight: 1, actual_weight: 0 }])`. The mutation was restored after the run; the committed wrong-operator regression independently checks this mismatch.

The default Homebrew Rust 1.99 Clippy run encounters existing deprecation and new-lint errors in unchanged code; validation uses the CI-pinned Rust 1.88 toolchain.

The initial runtime fixture run caught two mistakes in expected results: hopping windows before timestamp zero had been omitted, and decimal text had assumed fixed trailing zeros. Expected results now explicitly include all five containing hop windows and canonical exact decimal text.

## Boundaries

These cases qualify in-process production operators and pipelines plus a local-filesystem index. They do not establish additional Arrow type support, SQL reachability, distributed runtime behavior, or external-service compatibility. No production execution semantics or storage formats change.

The SQL oracle retains its positional schema comparison because planner aliases differ; the operator qualification helper additionally compares complete Arrow schemas, including names and metadata. Both paths use the same typed nullable row and signed multiplicity comparator introduced by #111. Existing NULL, decimal, type, duplicate, and equal-count/equal-sum mutation tests continue to exercise that comparator.
