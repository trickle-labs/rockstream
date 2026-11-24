# Automatic skew routing

The direct source path records each epoch's routing decision on the lowest
stable workload shard before sending application frames. The journal contains
the input digest, monotonically increasing routing generation, stable shard
IDs, selected strategy, concrete admission result, and next routing state.
Worker ownership can change without changing the logical shard assignment.

## Aggregate execution

The integer `SUM`, `COUNT`, and `AVG` path partially aggregates a heavy key
when the compiled shape is a single source with one integer grouping column
and one non-distinct aggregate. The value is a separate integer source column,
or the existing integer literal used by `COUNT(*)`. Unsupported expressions,
nullable operands, distinct aggregates, and other scalar types use the normal
single-owner pipeline. The supported shape is checked again on each partial
worker against its actual deployment descriptor.

A key becomes eligible after three consecutive committed source epochs with
at least 64 rows and more than 20 times the lower-median key row count. The
detector tracks at most 1,024 keys per input epoch. More diverse epochs retain
ordinary routing. At most 16 distinct worker owners receive salted tasks.
The source ordinal determines the salt; the persisted plan fixes the eligible
keys and target shard list, so even identical rows distribute and replay with
the same assignments. Transient bursts reset the consecutive-epoch evidence.

Before splitting, the router checks each row's integer product and proves
that the sums of absolute sum operands and absolute count operands are each
at most `i64::MAX`. This is executable ADR 0004 admission for the new epoch's
delta. The receiver independently checks the same domain on every task.
Unadmitted groups are sent unchanged to the canonical group owner.

Partials are pure, signed epoch deltas. They do not own historical aggregate
state and do not advance an application frontier. Therefore a promotion or
demotion does not relocate old rows, and a deletion does not need to recover
an earlier salt. Partial workers return sufficient statistics. The router
converts each statistic to at most two canonical input operands, preserving
both sum and count, and sends the final owner one complete epoch batch. The
existing staged aggregate transition remains the authority for checked
addition to historical state and for the old-row retraction/new-row insertion.
Only reduced partials reach the final hot-key aggregate mutex.

Tasks carry at most 1 MiB of estimated input. Independent tasks run through
the bounded shuffle flow controller concurrently. Responses are sorted back
to task order before the final frame digest is created, so response timing
cannot change crash-replay identity.

## Commit and replay

The prepare operation is a compare-and-set against the completed generation.
An identical prepare returns the stored decision. A different request cannot
pass a pending epoch. Every workload shard receives a final epoch frame,
including empty frames, and the source waits for each durable application ACK.
The journal finish then atomically installs the next routing state, point-key
mutations, and completed epoch. Join row ledgers can use these point mutations
without loading or rewriting their whole history.

A crash before finish leaves a durable pending decision. Resubmitting the
same source request reuses that plan; workers reject a mismatching digest and
deduplicate already committed frames. A crash after finish returns the stored
completion without applying any deltas again. Reads of an anchor with a
pending journal fail closed rather than present an unfinished epoch.

Each worker captures output rows and its durable epoch while holding its
execution gate. The anchor also reports its completed routing epoch, or its
durable frontier before the first routing journal. Control returns a complete
snapshot only when every current shard reports that exact epoch. A read that
straddles a prepare or commit fails with a retryable incomplete-snapshot result;
an older, complete epoch remains a valid result. Mixed old/new report formats
are rejected. Entirely legacy reports remain readable, but upgraded workers
whose legacy frontiers differ fail closed until the next complete direct epoch
advances all participants. This prevents a mixed first-epoch read during upgrade.

Routing command payloads and results are bounded to 4 MiB. Commands use the
authenticated owned-shuffle transport, validate workload, shard index, epoch,
lease identity and storage fence, and carry data in a flow-controlled payload
rather than HTTP/2 metadata. Historical routing point records are individually
addressed; there is no fixed total-history scan limit.

## Diagnostics and tests

`DataPlaneClient::routing_diagnostics` returns the exact persisted generation,
pending/completed epoch, trigger thresholds, promoted key set, fallback keys,
and partial input rows by worker-owned bucket. Partial execution contributes
actual processed-row counters and bounded shard-load reports. The anchor
sends the committed epoch and complete lease membership to the elastic
scheduler only after finish.

Focused runtime tests compare all emitted aggregate rows with the unsplit
staged kernel through promotion and retraction, verify all four partial owners
receive equal heavy-key work, exercise absolute-budget fallback, and reopen a
pending durable journal before matching and mismatching replay attempts.
