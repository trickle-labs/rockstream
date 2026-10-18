# v0.72 real-process resource capture — incremental evidence helper

**Status: observation only, not v0.72 capacity qualification.**

`r1-local-harness capture-v072-process` launches the actual supplied executable
under an owned process group and observes its OS-reported CPU and resident memory.
It verifies the executable digest before and after the capture, records the
real child PID, elapsed monotonic timestamps, raw per-process counters, and
digests of its stdout/stderr logs. It rejects premature exit, unavailable or
decreasing counters, invalid sampling windows, and overwriting prior captures.
The capture bound is 100 milliseconds through 1 hour; the interval is 10–1000
milliseconds and total observations are capped at 6001.

```sh
cargo run --release --manifest-path tools/r1-local-harness/Cargo.toml -- \
  capture-v072-process \
  --binary target/release/rockstream \
  --output evidence/v072/process-capture-001 \
  --duration-ms 300000 \
  --sample-interval-ms 250 \
  -- [production node arguments]
```

The output contains `process-capture.json`, `stdout.log`, and `stderr.log`.
`qualification_status` is always
`observation_only_not_capacity_qualified`, including successful captures.
Never treat this command's output as a qualified `RawSample`, and never
relax the frozen v0.72 thresholds to fit a measured candidate.

Process capture sources: Linux `/proc/<pid>/stat` (`utime`, `stime` in
clock ticks and RSS in pages) or macOS `libproc` `PROC_PIDTASKINFO`
(user/system CPU and resident size). The old `ps -o stime` path was invalid
because `stime` denotes process *start time* on Linux and may be mistaken
for a system CPU measurement. Both platforms fail closed when required
counters are unavailable. Mac-specific counter accuracy still requires
actual runner validation.

Remaining work for issues #138, #139, #140 and #141 includes validated
release binary provenance, release-process workload adapters, all eight
canonical workloads, frozen matrix cells, independent *complete* result
oracles, ingestion and commit/visible latency samples, actual worker activity,
overload/fault recovery, cost and regression reports, and hardware-qualified
sign-off. This helper establishes none of those outcomes by itself.
