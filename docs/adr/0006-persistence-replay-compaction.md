# ADR 0006: Persistence, replay, and compaction contracts

## Status

Accepted for VS5.

## Decision

RockStream separates logical epoch admission from physical durability. A
candidate epoch is accepted only when its authority, ordering, and duplicate
checks pass. Consecutive source epochs and globally allocated shard epochs use
different ordering contracts; both reject wraparound.

To guarantee that commits are authoritative and durable across restarts and
crashes, the persistence boundary explicitly separates three distinct layers:

### 1. Verified Decision Logic (`coupled_commit_is_durable`)
- The pure verified kernel in `rockstream-verified` models the conditional
  durability predicate: it mathematically proves that a commit is durable *if and
  only if* all component mutations are present (operator state, view output,
  source checkpoint marker, and frontier advancement) and physical storage write
  and flush outcomes are both successful.
- The verified logic proves that a successful write followed by a failed or
  ambiguous flush is an outcome-unknown state: it does not advance the in-memory
  frontier or acknowledge the source. The caller must recover durable history
  before retrying.

### 2. Runtime Adapter Guarantees (`rockstream-connectors`)
- The runtime adapter (`CoupledBatchDescriptor` and `CoupledTransactionBuilder`)
  mechanically derives component presence directly by inspecting transaction
  keys in the batch (`ShardPrefix::State`, `ShardPrefix::ViewOutput`,
  `ShardPrefix::SourceMarker`, `ShardPrefix::Frontier`).
- The adapter enforces a fail-closed boundary: batches missing any required
  mutations are rejected with an explicit error before invoking the verified
  kernel or committing writes to physical storage.
- Storage write and flush outcomes passed to the verified decision kernel are
  derived strictly from real storage backend results (`db.write(batch)` and
  `db.flush()`) rather than passing unvalidated constants.

### 3. Storage Backend & Durability Assumptions (`rockstream-storage` / SlateDB / ObjectStore)
- Atomic batch write: SlateDB and ObjectStore guarantee that an atomic batch
  write either writes all operations or none.
- Flush durability: `db.flush()` persists written memtable/WAL state to durable
  storage (local disk or object store) before reporting success.
- Process crash recovery: any un-flushed or partially written data is recovered
  or discarded cleanly by SlateDB / WAL replay without silent corruption.

### Replay and Compaction

Replay accepts committed records with matching identity and scope. A duplicate
is a no-op; prepared, failed, malformed, or outcome-unknown records cannot
advance state. Recovery is ready only after the bounded scan completes, metadata
and fence checks pass, and the restored state is complete.

Compaction may reclaim an arrangement only when its compaction frontier covers
both the retained-reader and replay horizons and no snapshot or in-flight delta
still needs the state. Consumer registration and removal are idempotent.

## Assumptions

The persistence architecture clearly separates assumptions across three boundaries:

1. **Verified Decision Logic Assumptions**:
   - Assumes that inputs supplied to `coupled_commit_is_durable` accurately
     reflect the batch contents and storage I/O outcomes. The formal kernel does
     not inspect raw I/O or byte streams itself.

2. **Runtime Adapter Guarantees & Assumptions**:
   - The adapter guarantees mechanical key inspection of `WriteBatch` entries to
     classify component presence, avoiding assumptions on caller discipline.
   - The adapter must fail closed: an adapter that cannot distinguish a durable
     result from an ambiguous I/O result must report an error, withhold source
     acknowledgment, and require recovery.

3. **Storage Backend & Durability Assumptions**:
   - SlateDB and ObjectStore provide atomic batch write, WAL persistence, and
     flush durability under their documented contracts.
   - Tokio and cross-process fencing are not formally verified by the Verus
     kernel.
   - The storage adapters must preserve their documented atomic-write, flush,
     conditional-write, snapshot, scan, and error semantics across process crash
     and restart.

## Consequences

The VS5 claim is about the executable decision boundary and its adapters, not a
formal verification of SlateDB internals or distributed liveness. LFS, MinIO,
crash, handoff, and compaction tests remain required qualification evidence.
