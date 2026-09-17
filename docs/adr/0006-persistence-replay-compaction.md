# ADR 0006: Persistence, replay, and compaction contracts

## Status

Accepted for VS5.

## Decision

RockStream separates logical epoch admission from physical durability. A
candidate epoch is accepted only when its authority, ordering, and duplicate
checks pass. Consecutive source epochs and globally allocated shard epochs use
different ordering contracts; both reject wraparound.

A commit is authoritative only after the complete batch containing operator
state, materialized outputs, source markers, and the frontier has been written
and flushed. A successful write followed by a failed or ambiguous flush is an
outcome-unknown state: it does not advance the in-memory frontier or acknowledge
the source. The caller must recover durable history before retrying.

Replay accepts committed records with matching identity and scope. A duplicate
is a no-op; prepared, failed, malformed, or outcome-unknown records cannot
advance state. Recovery is ready only after the bounded scan completes, metadata
and fence checks pass, and the restored state is complete.

Compaction may reclaim an arrangement only when its compaction frontier covers
both the retained-reader and replay horizons and no snapshot or in-flight delta
still needs the state. Consumer registration and removal are idempotent.

## Assumptions

The verified kernels do not verify SlateDB, ObjectStore, Tokio, or cross-process
fencing. The storage adapters must preserve their documented atomic-write,
flush, conditional-write, snapshot, scan, and error semantics. An adapter that
cannot distinguish a durable result from an ambiguous I/O result must fail
closed and require recovery.

## Consequences

The VS5 claim is about the executable decision boundary and its adapters, not a
formal verification of SlateDB internals or distributed liveness. LFS, MinIO,
crash, handoff, and compaction tests remain required qualification evidence.
