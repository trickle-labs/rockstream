# Automatic elasticity

Control nodes with durable shard storage and the canonical migration executor automatically
rebalance existing virtual shards after complete durable workload epochs. The
default shared-store deployment allocates 32 virtual shards independently of
worker count. An explicit deployment shard count is retained. Existing workloads
keep their shard identities and routing while individual leases move.
The internal executor and restart reconciler run even when the public management
listener is disabled.
Setting `enabled = false` disables automatic placement decisions and the default
virtual-shard expansion. Explicit drains continue to use the durable canonical
handoff and idle-boundary protocol.

```toml
[control.elasticity]
enabled = true
initial_virtual_shards = 32
hysteresis_frontiers = 3
minimum_residency_ms = 60000
minimum_headroom_gap_bps = 2000
```

The scheduler requires three distinct, increasing committed frontiers with the
same candidate and at least 60 seconds of residency. Only healthy, active,
compatible workers sharing the same verified object store are recipients.
Complete, recent CPU samples can trigger movement with balanced shard counts;
a move that worsens the measured CPU gap is rejected. Memory headroom remains a
separate pressure signal. Samples from former owners or older than 60 seconds
cannot influence a decision.
Every participant must have committed under its current lease. Partial epochs,
repeated reports, stale fences, missing members and recovery-required workloads
cannot trigger movement. A membership change resets hysteresis. Workload epoch
clocks are scoped separately; worker maximum frontiers are not completeness
evidence.

The source coordination path supplies exact per-shard commit receipts after its
durable owned-epoch finish. Direct Arrow routing supplies the same evidence from
the current anchor's durable routing journal through `RoutingEpochCommitted`.
The anchor identity and every current member fence are checked before policy
evaluation. Legacy uncoordinated direct writers cannot authorize rebalancing.

The decision is stored as a normal `MigrateShard` operation with a deterministic
idempotency key, expected donor fence, membership generation, committed frontier,
shard counts and headroom gap. The existing management executor performs the
online handoff, workload restoration and routing publication. Restart reconciles
the stored operation; it does not recreate a decision from a worker maximum.
Only one automatic move is admitted while a migration or drain is active.
An explicit worker drain keeps its current workloads running while the same
executor moves one shard at successive committed frontiers. Draining workers
are donors only. The control plane decommissions the worker after its final
lease has moved. A failed migration can retry at a later committed frontier
with a new durable operation ID and the same expected-donor fence.

An explicit drain also moves unused leases and active workloads that have
become idle. Those operations record `idle_drain`, rather than manufacturing
a committed frontier. The executor atomically freezes admission, excludes
active source epochs and reads, and asks every affected workload's durable
anchor to prove that its owned and routing journals have no unfinished epoch.
Expired direct grants alone are insufficient proof. The donor serializes its
flush and close with application execution; queued requests cannot reopen the
old deployment. New topology is available only after recipient deployment
readiness. Failed idle moves receive a fresh durable retry ID under the same
donor fence.

`shard.auto_rebalance` audit entries and management operation request details
expose the decision metrics. A failed audit append or ambiguous operation-store
read prevents dispatch. An advisory heat update does not change ownership, and
rollback checks the fenced owner identity rather than full heat equality.
