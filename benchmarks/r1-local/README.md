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

---

## v0.61.1 External Performance Baseline Contract

### Versioned Profile & Frozen Thresholds
- **Profile**: `profile-v0611.toml` (`MBP-M5Pro-48GB-v2`), retaining historical `profile.toml` (`MBP-M5Pro-48GB-v1`) intact.
- **Payload & State**: 128-byte payload size, durable commit mode (`Durable`), 60s measurement window, 10s warmup, 5 alternating repetitions (max CV <= 0.15).
- **Write-Amplification Rule**: Allowed write-amplification variation for "approximately constant" state writes between 1K and 100K live groups is frozen before measurement at a max ratio of 1.10. Lookup cost is reported separately.

### Decoupled Metric Targets
Measurements decouple the offered-load generation ticker from completion, reporting three separate p99 metrics:
- `read_p99_ms`: Client query execution round-trip time (target <= 10.0 ms).
- `commit_p99_ms`: Transaction commit submission to durable storage ACK (target <= 25.0 ms).
- `freshness_p99_ms`: Scheduled event instant to visible correct committed result multiset (target <= 100.0 ms).
- Generator metrics: `generator_delay_p99_ms`, bounded queue drops (`generator_queue_drops`), timeouts, and errors.

### Complete Multiset Oracle Alignment
Freshness deciders require complete result multiset equality against an independent SQLite oracle engine at the committed epoch. Visibility markers or single-row probes alone are rejected. Adversarial validation fixtures prove rejection of corrupted values, extra rows, missing rows, and stale epochs.

### Six-Area Experiment Matrix Accounting
Every cell across the six required experiment areas is accounted for. Runnable standalone cells are measured; unavailable distributed or large-state cells are assigned to their owning milestone with concrete blockers:
1. **One-Key Updates**:
   - 1K groups: Standalone measured.
   - 100K groups: Standalone measured.
   - 10M groups: Blocked to `v0.67.1` (requires spillable arrangements and state beyond RAM).
2. **Compatible Views**:
   - 1 view: Standalone measured.
   - 20 views: Standalone unshared baseline measured.
   - Shared execution benefit: Blocked to `v0.67` (requires multi-consumer shared execution and direct data plane).
3. **Worker Scaling**:
   - 1 worker: Standalone measured with verified OS worker PID activity.
   - 2, 4, 8 workers: Blocked to `v0.67` (requires distributed data plane without control-plane bottleneck).
4. **Concurrent Load**:
   - Mixed Ingest + Queries: Standalone measured, reporting separate read-p99 and freshness targets.
5. **Beyond RAM, Compaction & Restart**:
   - Compaction & Restart: Standalone measured with bounded backlog and flush tracking.
   - State beyond RAM: Blocked to `v0.67.1` (requires spillable arrangements and paging to completion).
6. **Overload & Loss**:
   - Standalone Overload: Standalone measured with bounded queue backpressure and recovery timing.
   - Worker Loss / Failover: Blocked to `v0.67` (requires distributed worker supervision and failover).
   - Shard Migration: Blocked to `v0.68` (requires durable distributed migration sagas).

### Negative Qualification Rules
Harness integrity rejects qualification if:
- Any required matrix cell is omitted without an owning milestone and concrete blocker.
- Worker processes have 0 CPU/work or fabricated PIDs.
- Synthetic runs (`sample_reference_run`) are presented in place of release binaries.
- Oracle multiset comparisons detect any value, cardinality, or epoch discrepancies.
- Checksum digests of binaries, profiles, or thresholds do not verify.

### Cost Model Reporting & Scope Disclaimer
Published reports calculate the cost per million changes formula:
`cost_per_million = (total_hourly_cost * 1_000_000.0) / (sustainable_changes_per_sec * 3600.0)`.

Itemized infrastructure costs account for gateways, control nodes, workers, storage requests, retained storage, network transfer, and compaction. Local developer run metrics establish standalone local performance only; cloud cost claims require a measured production deployment with dated pricing.
