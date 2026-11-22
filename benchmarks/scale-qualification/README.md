# Independent IVM Scale Qualification Program (Issue #405)

This directory defines the canonical multi-node scale qualification contract
extending `benchmarks/r1-local/` for:

1. **Strict Provenance Separation (R1):**
   - `QualificationProvenance::SyntheticFixture` (`sample_reference_run`,
     `manifest_rc1_reference`) is restricted to unit tests and is ALWAYS rejected
     fail-closed by `QualificationReleaseGate` and `IndependentQualificationChecker`.
   - Production release claims require `QualificationProvenance::MeasuredExecution`
     with exact commit SHA, binary SHA-256, hardware/cloud spec, distinct OS worker
     PIDs, oracle multiset verification, verified fault firing, and passing negative
     controls.
2. **Flagship End-to-End CDC Cloud Demonstration (R2):**
   - Repeatable end-to-end run using the ordinary release `rockstream` binary,
     live PostgreSQL CDC, multiple workers, and S3/MinIO object storage.
   - Exercises live load, worker scale-out, worker kill, online backup, clean
     restore, and exact PostgreSQL oracle verification via
     `./scripts/run-flagship-cdc-demo.sh`.
3. **Expanding Qualification Ladder (R3):**
   - Measures qualified throughput $Q_n$ (maximum sustained input rate on $n$
     workers meeting correctness, freshness SLO, bounded backlog/compaction debt,
     and durability) and $\text{Scaling efficiency}(n) = Q_n / (n Q_1)$.
   - Dimensions frozen in `ladder-profile.toml`:
     - Maintenance workers: `1, 2, 4, 8, 16, 32, 64` (fixed-size speedup and
       proportional scale-out).
     - Retained state: `1x`, `4x`, `10x` configured worker RAM through compaction
       and worker crash/failover.
     - Distributions & shapes: uniform vs. 50x skewed hot keys;
       inserts/updates/deletes/large transactions/bursts; aggregation,
       co-partitioned join, repartitioned join, dependent views (1 vs. 20 shared
       arrangements), bounded windows, concurrent high-QPS reads & `SUBSCRIBE`
       via stateless gateways.
     - Duration: short diagnostic runs plus repeatable 72-hour mixed-load soak.
4. **Complete Cost Accounting & Negative-Controlled Checker (R4):**
   - Every published run archives commit SHA, binary digest, hardware/cloud spec,
     offered vs. achieved rate, freshness/read latency distributions (including
     errors/timeouts), full RSS, network/storage requests, compaction debt, and
     complete workload cost (`cost per million correct durable changes at freshness F`)
     covering serving gateways, control plane, maintenance workers, compaction,
     retained history storage, object-store requests, backups, failover headroom,
     and cross-AZ traffic.
   - Verified by `verify-scale-qualification` (in `tools/r1-local-harness`), which
     also runs the 6-case negative-control suite proving rejection of:
     1. 1 dropped row
     2. 1 duplicate row
     3. Stale-owner write
     4. Torn visibility
     5. Fault-never-triggered teardown
     6. Synthetic `sample_reference_run` fixture

## Running the Checker & Negative-Control Suite

```bash
cargo test --manifest-path tools/r1-local-harness/Cargo.toml --test scale_qualification_program_tests
```

To verify an archived `ScaleQualificationBundle` JSON artifact:

```bash
cargo run --manifest-path tools/r1-local-harness/Cargo.toml --bin verify-scale-qualification -- --bundle <bundle.json>
```
