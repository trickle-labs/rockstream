# Automatic skew process qualification

The executable qualification is `crates/rockstream-cli/tests/automatic_skew_process_tests.rs`.
It launches the actual CLI control process and separate worker processes with
independent exchange listeners. The materialized result is compared with the
DataFusion batch oracle, including signed retractions and identical replay.

## Correctness gates

`cargo test --locked -p rockstream-cli --test automatic_skew_process_tests -- --test-threads=1`
runs four-worker promotion, fallback, and process crash recovery. The promotion
and latency inputs have a 50x hot key. The promotion test asserts the exact four-worker assignment and
100 hot rows per partial bucket, in addition to complete output equality.
Compressed empty epoch markers advance every logical participant.

The crash test suspends a real non-anchor worker, waits until the next epoch's
routing plan is durably pending, then kills that process. The pending epoch
cannot complete while the participant is suspended. The replacement must replay
the same request and preserve all logical shard IDs and the exact batch result.
The explicit cached development transport is used for this fault injection so
the pause occurs in partial execution, after the durable prepare boundary.

## Release and shared-store gates

The `Automatic skew and elasticity qualification` workflow runs:

`cargo test --locked --release -p rockstream-cli --test automatic_skew_process_tests -- --include-ignored --test-threads=1`

The latency test compares equal-size 464-row epochs on four workers and eight
logical shards. After five warmup epochs per profile, it records 100 uniform and
100 skewed epochs, measuring submission through the complete materialized
snapshot. It rejects a skewed nearest-rank p99 above twice the uniform p99. Its
JSON artifact contains the individual measurements, worker process IDs, batch
geometry, latency boundary and exact-oracle outcome. The workflow records its
source revision beside the measurements. Unoptimized correctness runs are not
release-performance evidence.

The elasticity test starts a real MinIO container and two worker processes,
adds two more workers, then explicitly drains the added workers. Every accepted
epoch is checked against the batch oracle; stable logical shard IDs must survive
both transitions. It uses zero minimum residency to keep the fixture bounded;
the default residency and multi-frontier hysteresis are covered separately by
the policy tests. Docker is required: missing MinIO is a failure when this case
is explicitly selected, not a passing skipped scenario.

## Runtime diagnostics

`EXPLAIN INCREMENTAL ANALYZE <view>` includes a bounded recent decision history
under the same delegated view-read authorization as operator statistics. Events
identify routing generations, committed epochs, thresholds, hashed hot keys,
partial input counts, admission fallback counts, join decisions and automatic
movement requests. The control process retains at most 32 events and 256 KiB per
workload; its durable audit retains accepted decisions. An empty in-memory
history after control restart is reported explicitly. Diagnostic events do not
serve as correctness or latency evidence by themselves.

Actual cache behavior is covered by `heat_cache` tests: a foreground immutable
SST range is observed, the recipient warms those exact bytes before cutover,
then its foreground read must hit the warmed cache. Stale, future, malformed,
unowned and oversized hints cannot trigger unbounded I/O. The canonical transfer
test verifies the warm acknowledgment still carries the donor fence before
ownership changes.
