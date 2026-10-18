# v0.72 itemized cost arithmetic — partial #141

The separately runnable `verify-v072-cost` command validates arithmetic from
explicitly supplied **local-observed** costs. It does not invent pricing,
measure throughput, or qualify capacity.

```sh
cargo run --manifest-path tools/r1-local-harness/Cargo.toml \
  --bin verify-v072-cost -- --input evidence/v072/local-cost.json
```

The input is a strict `CostBreakdown` JSON record with seven hourly costs:
gateway, control nodes, workers, object-store requests, retained storage,
network transfer, and compaction. Validation rejects missing fields, negative
or non-finite costs/rates, mismatched item totals, incorrect per-million
calculations, invalid calendar dates, and unsupported pricing scopes.

A sample file is **not** provided: supplying numerical amounts without
measured usage and dated prices would create misleading evidence. The command
always states that measured-source and capacity qualification remain separate.

`cloud-production-dated` claims fail closed, even with a date, because the
current schema does not bind prices to an authenticated, measured cloud
deployment and corresponding usage records. Implement and qualify that
cross-file linkage before enabling cloud cost claims.

Full #141 requires the frozen v0.72 matrix, complete raw release-process
samples, correct-result oracles, actual costs and datestamped price sources,
noise/regression decisions, and all V072 sign-off evidence. Arithmetic
validation alone does not meet the acceptance contract.
