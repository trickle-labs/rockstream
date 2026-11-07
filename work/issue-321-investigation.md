# Issue 321 investigation (in progress)

Base revision: `febf9c7c` (clean checkout). The initial reproductions ran with
only the failure injection and observation tests; no repair had been applied. The SQL test
uses a local pgwire gateway and a SlateDB shard backed by LocalFileSystem.

The injection rejects the first batch containing a view-output key, after the
base transaction batch succeeds. With a compiled projection over `t`, COMMIT
returned `CommandComplete(1)`, and the frontier advanced from 1 to 2. The
table contained `[(1, 10), (2, 20)]`, while the view contained `[(1, 10)]`.
After reopening the shard and compiled view, the same discrepancy remained.
After inserting `(3, 30)`, the table contained `[(1, 10), (2, 20), (3, 30)]`
and the view contained `[(1, 10), (3, 30)]`. The regression deliberately
fails on this observation. The fault was injected before any production repair.

The existing failed-base-write test also exposed an allocated epoch as the
frontier: it advanced from 1 to 2 despite the rejected batch. Separating
allocation from publication makes that focused test pass. This alone does not
repair the view-output failure above; the base batch still precedes refresh.

On candidate `ccd2ac7b`, COPY FROM STDIN acknowledged two rows and the base
table contained `[(1, 10), (2, 20)]`, but its compiled projection was empty.
COPY batches now use the same commit path as SQL writes. The filesystem-backed
wire test checks exact rows after COPY, an injected view-output failure, a later
insert, and reopening. COPY can still flush multiple batches; this test does not
claim whole-COPY atomicity across those batches.

Older composite-key checkpoints contain only encoded key bytes and surrogate
IDs. Recovery now decodes those bytes using known compiled key types; tests
cover mixed text and numeric keys across a real gateway restart and exercise
all supported scalar encodings. Unknown or unsupported types and malformed
records remain explicit recovery errors rather than guessed values. Expressions
use the actual evaluator on an empty typed batch to derive their output type;
tests restore arithmetic, boolean, and supported scalar-function keys with their
original surrogate IDs. A pgwire restart test also restores legacy `upper()`
group keys and verifies the complete post-restart result. Unknown functions
remain explicit recovery errors.

On the local pgwire path, a real CancelRequest was observed by the gateway at
each of three paused commit boundaries: before the atomic write, after that
write, and before flush. In each case the commit returned `CommandComplete(1)`;
the complete table and view, a subsequent insert, and the reopened results
matched exactly. These observations do not claim the request aborts a commit.
Separate handler-future cancellation tests cover recovery when execution stops.

The real PostgreSQL 11 CDC fixture was run locally with Docker. After backfill,
live updates, and a gateway restart, a source commit was paused at its atomic
write while a local insert attempted to commit. The local insert waited; after
release the complete table was `[(1,7),(2,20),(3,5),(4,11),(5,13),(6,17)]`
and the aggregate was `73`. Both results survived another gateway restart.
This covers the tested pgoutput path, not every source or failure mode.

A one-shot source view-output failure reported `BLOCKED` with the injected
storage error and left the frontier at its previous value. Without the recovery
lock, a local commit completed while the source's pipeline reload was paused.
The reload now takes the commit lock: the same test observes the local commit
wait, then verifies the exact table rows and aggregate `96`. The failed source
does not automatically retry in this fixture; its blocked status is explicit.

A separate local fixture drives the production pgoutput commit routine without
a PostgreSQL process. On candidate `1bddd8e9`, inserting `(2,20)` into a base
table produced `(2,80)` in a transitive view instead of `(2,60)`: the inlined
pipeline received an already-transformed delta. The source path now supplies
the base delta. The fixture verifies exact rows through an injected write
failure, checkpoint recovery, replay, gateway reopen, and a later SQL commit.
This synthetic fixture does not replace release-process CDC qualification.

The same transitive views were exercised against a real local PostgreSQL 18
replication stream. Initially the source blocked because the outer view had no
lifecycle cursor; after attaching it to the inlined base source, backfill failed
with an Int64/Utf8 mismatch because it read the intermediate view. Attachment
and backfill now use the base source. Exact rows pass through backfill, live
updates, gateway restart, a concurrent local commit, and the injected source
write failure. This runs the source and gateway in the test process, not a
release-process qualification.

The gateway component CI suite exposed stale test expectations: five
text-key/float-value aggregate cases expected an acknowledged write with an
empty view, and two catalog fixtures described a constant query as an already
compiled view. The aggregate cases now check the exact explicit error and empty
base and view results. The catalog fixtures use a recoverable table-backed view
while preserving their identifier checks. The complete operator semantics,
local filesystem catalog, and MinIO catalog suites passed locally.

DISTINCT checkpoint recovery also resurrected a removed row: after a persisted
insert and persisted delete, reloading suppressed the next insert of that row.
The checkpoint now records point deletions and supports in-place restore;
filesystem tests cover failed-write retry and deletions made after a snapshot.
Other operator families and the atomic commit path still require review.

An aggregate restored under a one-group memory limit retained dirty keys from
the abandoned attempt. Its next checkpoint deleted an evicted but committed
group. In-place restoration now clears those keys after a successful reload;
the regression compares all stored records before and after checkpointing.

Work in progress: local SQL stages base rows, view output, state, and frontier
in one batch. Projection and aggregate write-failure tests now pass. A failed
state restore previously let the next aggregate commit produce 50 instead of
30; a recovery gate now rejects reads while the failed scan persists and
restores state before the next commit. This is not yet release evidence.
TopK restoration scans checkpoint pages within the per-partition buffer bound;
spill-backed operators rebuild into an inactive slot before publishing recovery.
Filesystem tests cover failed scans and writes, cancellation, repeated recovery,
large K, tied ranks and multiplicity, another operator sharing storage, and
close/reopen. Other source paths, the distributed path, and shuffle owning-epoch
recovery remain unqualified.
The spill-ID index still grows with overflow, processing materializes a spilled
partition, and abandoned spill namespaces are not yet garbage-collected.
An over-retracted row becoming positive previously exceeded the 100,000-row
buffer limit. It now spills when storage is configured, or fails without
changing state when it is not; tests also cover a failed spill write.

A test-only storage pause cancels the local handler before the combined write,
after storage accepts it but before the durable-write future completes, and
after that future but before flush. The first case restores the old rows; the
latter cases block a concurrent pgwire read until recovery flushes and exposes
the complete committed rows, which also survive reopen. Recovery refreshes the
frontier from storage after the ambiguous write. This test directly cancels the
handler future; the separate pgwire CancelRequest observations are above.
A separate injected flush failure rejects reads until a successful recovery
flush, after which complete table and view rows survive a later commit and reopen.

A join-view write failure is followed by an exact update, reopen, and an update
to the other join input; all resulting table and view rows match expectations.
A transitive view initially backfilled no rows because its inlined pipeline was
fed from the intermediate view name instead of the base relation. Initial
backfill and local commit refresh now use the inlined base input; exact rows
are checked across an injected failure, retry, reopen, and later commit.

A local pgwire test confirms its incremental plan includes TopK, then checks
exact rows after an injected write failure, a valid transaction, reopen, and
another valid transaction. An in-memory-only operator rejects an oversized
checkpoint without replacing live state.

Analytic-window checkpoints retained old output-cache rows after a rank change,
causing stale retractions after reload. Point deletions and in-place restore now
cover that boundary, including failed writes, concurrent snapshot acknowledgement,
corrupt records, and a pgwire rollback/restart test.

MIN/MAX checkpoint scans previously ignored truncation. Recovery and stale-key
removal now scan in bounded pages; the loader rejects malformed or inconsistent
records. Page continuation seeks from the prior key rather than rescanning
earlier pages. Tests compare all records across multiple pages and exercise a failed
MIN view commit followed by recovery and reopen.

Window rollback tests found session restoration retained uncommitted rows.
Tumble and session restores now replace the old state after a successful read.
Session checkpointing also omitted deleted rows without deleting their old
records; point deletions, failed-write retry, and acknowledgement after a
snapshot are covered by filesystem tests. Tumble and session checkpoints now
retain the complete input frontier in a versioned record; malformed records
fail restoration without replacing live state. The remaining operator families
still need examination before using restoration as a general rollback mechanism.

Hopping-window recovery re-emitted already committed rows on the next update
because its output cache was not restored. Recovery now reconstructs that
cache and restores the input frontier; exact deltas are checked after both
in-place restore and reload.

The direct gRPC closed-inlet observation received exactly one ACK with fields
`(exchange_id, src_shard, target_shard, epoch, seq, credit_grant) =
(100, 0, 1, 1, 1, 2)` and then end-of-stream. Healthy direct delivery and the
existing filesystem-backed committed-frontier replay test passed. These do
not establish recovery of a rejected frame by an owning epoch. The multiplexer
returned `Ok(())` for a closed-inlet frame and released its in-flight credits.
After reopening the inlet, no frame arrived during a 100 ms observation;
explicit replay delivered exactly `[(1, 10), (2, 20)]`. This manual replay is
not an owning recovery path. A repository
search found `WorkerStreamMultiplexer::send_frame` callers in tests,
simulations and benchmarks, but no production caller; the owning recovery
observation remains inconclusive.

Management authentication remains owned by open issue #303. A separate
qualification used the unmerged management candidate at commit
`66e9ff6ea0e64800609ae5c6076afb1dfda87a2d`, a release image built from that
revision, and an isolated control process with two simulated worker peers.
The `linux/amd64` image ID was
`sha256:cc9bf1b13bdd8c58f2651afff9cfd1009e4bf33ea608a9706fc2eb664a6f4b57`.
The temporary test checkout added a second CLI certificate and a grantless
durable principal to the existing drain fixture; those test-only changes were
not part of the image. The management endpoint used an ephemeral loopback host
port mapped to the control container's port 9201.
The draft subsequently advanced to `d698bdfc88c1f4f4434e6e4254b3885e0dc3e6e7`;
that later commit changes CLI TLS reload code and tests, not the catalog store
or management service. A separate unsequenced test on its clean-source release
image `sha256:130672615eb2ec76df148ce95ab791e67d02a83f0287c488513e8640e3709813`
reproduced the same catalog failure described below; it did not establish
the serialized positive behavior for that later commit. A subsequent temporary
test used that image for a real control process and two real worker containers,
with shard 77 initially assigned to worker 601 and backed by MinIO. Two
unauthenticated gRPC attempts, for status and drain, each returned
`Unknown error: transport error`; the test waited for each failure audit before
the next attempt. A mapped principal without grants was denied both calls.
The complete shard and operation lists, and node fields other than changing
headroom and topology version, matched before and after rejection. Denial audit
records matched exactly after normalizing generated IDs and timestamps. An
authorized drain moved shard 77 to worker 602, completed at 100%, and the
seeded key retained its exact value after the workers stopped and storage was
reopened. The test passed twice. An extended run also passed after checking
that plaintext received the TLS fatal decode-error alert and that the client
refused both missing CA trust and an explicit plaintext URL. It does not
eliminate the unsequenced audit race or establish the whole #303 release-endpoint
matrix.
The [real-worker test patch](issue-321-real-worker-reproducer.patch) applies
with `rtk git apply --unidiff-zero` to the tested draft commit and expects the
clean-source image tagged
`rockstream-321-qual:d698bdfc88c1`. Run its selected test with
`rtk cargo test -p rockstream-sim --test issue_321_real_process management_denial_and_drain_with_real_release_workers -- --exact`;
Docker and the fixture's MinIO image are required. The patch is a qualification
fixture, not a catalog repair or an assertion that the unsequenced race passes.
An unauthenticated TLS handshake and a plaintext HTTP/2 preface were rejected;
the latter received the TLS fatal decode-error alert `15 03 03 00 02 02 32`.
When the test waited for each authentication-failure audit record before the
next attempt, an authenticated principal with no grants was denied both a
cluster-status read and a worker drain. The complete observable status and
operation list were unchanged, the donor's original shard lease still fenced
successfully, and the denial audit records matched the actor, actions, scopes,
outcomes, and policy revision. An authorized drain then completed and moved
the shard to the recipient. This serialized experiment passed; its simulated
peers do not prove real worker lifecycle or full release qualification.

Without that audit sequencing, the same two rejected handshakes were followed
by an administrator request failing twice in separate runs. The control log
reported incomplete catalog history: expected revision 4, found revision 3.
An independent in-memory storage reproducer on main
`9cae51408ad42252c793a165a18e6eaed43352d6` committed two different
transactions at revision 1 and then received `RS-1003` on recovery, for both
one catalog instance and two instances sharing a store. A clean, dedicated
main build repeated this result after an earlier run used a shared Cargo target.
The diagnostic test asserts the observed defect; it is not a passing regression
for a repair. A clean build of the draft's security catalog also accepted two
different principal changes at revision 2 and then failed recovery on the
duplicate revision. Another main diagnostic committed table 1 at revision 1, snapshotted and
compacted that revision, and then committed table 2 from a stale catalog instance
at revision 1. Both calls returned success. Recovery returned exactly table 1
and no table 2: the stale acknowledged mutation was omitted because replay skips
logs already covered by the snapshot. A separate test repeated this outcome
with a filesystem object store and two OS processes: the second process opened
its catalog before the first committed and compacted the snapshot, then its
stale revision-1 commit returned success but was absent after recovery. These
are diagnostic tests, not release-process evidence of this particular loss.
Regression checks requiring rejection of stale writes failed in the clean main
build both before and after compaction. Cross-process fencing, compaction,
crash recovery, and compatibility of a fix remain open.
The workspace's `object_store` 0.14 local filesystem backend returns
`NotImplemented` for conditional `PutMode::Update`; a local test verified
that the old object remains unchanged. A portable fix cannot assume that
conditional update is available on every supported store.
On current main, `CatalogStubs::persist_mutations` also discards errors from
`commit_txn` in its synchronous and spawned paths. It is called by multiple
gateway catalog mutations. Adding a new conflict error solely in the store
would therefore be insufficient: those paths must propagate or safely resolve
the failure before reporting the affected mutation as successful.
The pgwire `CREATE VIEW` path calls `add_view_with_deps`, which publishes the
view in the in-memory catalog before invoking that error-discarding persistence
path; by contrast, durable `CREATE TABLE` uses `add_table_durably`. A pgwire
test in the clean main build injected a catalog-log write failure after
successfully creating a base table. `CREATE VIEW uncommitted_view AS SELECT id FROM durable_rows`
returned `CommandComplete(0)` and `pg_views` contained exactly
`uncommitted_view`; after restarting the gateway, `pg_views` was empty. The
injected catalog append failed exactly once. This is a separate view-creation
failure observation, not proof about the requested DML rollback paths. Source
inspection also shows that view compilation may publish a local directory entry
or deploy a distributed plan before catalog registration; a complete repair
must handle those effects, not just return the catalog error.

On clean main `9cae51408ad42252c793a165a18e6eaed43352d6`, the [durable catch-up diagnostic](issue-321-durable-rejection.patch) passed with a filesystem-backed store. For uncommitted epoch 1, a closed inlet made `catch_up_durable` return `Ok(())` after persisting the exact payload as an inbox marker. After reopening with frontier 0 and a live inlet, a second call returned `Ok(())` but delivered no rows within 150 ms; the marker still held the original payload. The patch asserts the observed behavior, not the desired behavior. It exercises the helper directly and does not demonstrate an owning epoch or a committed result. Run it after `git apply work/issue-321-durable-rejection.patch` using `cargo test -p rockstream-runtime --test fast_path_wal_elision_tests issue_321_durable_catch_up_closed_inlet_observation`.
