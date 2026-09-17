# ADR 0004: Checked arithmetic and merge-law admission

## Status

Accepted for VS1.

## Decision

RockStream treats mathematical merge laws and executable accumulator behavior as
separate contracts. `WeightAdd/v1` and `SumCount/v1` use checked `i64`
accumulators. A merge succeeds only when every scalar result is representable;
overflow is a non-retryable error and never wraps or saturates.

Per-row multiplication is checked before it enters an accumulator. Negative
weights are valid provisional Z-set deltas. A materialized group is valid only
when its count is positive; signed sum/count pairs remain valid as staging
states and may be negative before application. An inverse is checked too:
`i64::MIN` has no representable `i64` inverse and is rejected.

Regrouping or partial pushdown is admitted only after an executable operand
check proves an absolute-sum budget no greater than `i64::MAX`. The checked
kernel computes that budget without overflowing. Law descriptors carry the
implementation version, executable domain, evidence reference, and whether a
concrete admission is available. A composable property alone cannot enable an
optimization; without admission, skew splitting falls back to one spill shard.

The existing fixed-width big-endian formats remain unchanged. Decoders require
exact widths and reject truncation or trailing bytes. Tagged storage merges
validate the incoming operand even when no existing value is present, reject
unknown tags, and use the same checked arithmetic kernels as the law bundles.

`SumCount/v1` uses `ExactOnly` for complete query-visible progress. Provisional
deltas, committed snapshots, `COUNT(*)`, `COUNT(expr)`, empty groups, decimal
conversion, and floating-point finalization are separate contracts; VS1 proves
only the integer accumulator frontier described here.

## Consequences

- Existing persisted tags and accumulator widths are compatible.
- Arithmetic failures surface through the existing `RS-1002`, `RS-3009`, and
  `RS-3501` error paths.
- Unadmitted regrouping may reduce optimization opportunities until a caller
  supplies concrete operands and validates the budget.
