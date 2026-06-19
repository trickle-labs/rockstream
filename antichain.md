# Replacing RockStream's frontier primitives with the `antichain` dependency

**Status:** Proposal / migration design
**Date:** 2026-06-19
**Author:** generated analysis
**Scope:** `crates/rockstream-types/src/frontier.rs` and its dependents

---

## 1. Executive summary

We extracted the generic lattice / progress-tracking primitives from RockStream
into a standalone crate, [`antichain`](https://github.com/trickle-labs/antichain)
(v0.3.1). The crate is a near drop-in superset of the **generic** half of
[`crates/rockstream-types/src/frontier.rs`](crates/rockstream-types/src/frontier.rs):
it provides `Lattice`, `Antichain<T>`, `Frontier<T>`, and `ProductTimestamp<T1, T2>`
with the same algebra, plus a large composition toolkit (`Lexicographic`, `Max`,
`Min`, `Bounded`, `WithTop`, `WithBottom`, `MapLattice`, `SetLattice`) and a
companion `antichain-intervals` crate.

We can delete the generic primitives from RockStream and re-export them from
`antichain`, keeping all the **domain-specific** progress types
(`SourceProgress`, `FreshnessToken`, `ShardFrontierReport`,
`WorkerFrontierSummary`, `ClusterFrontier`, `CompleteThroughToken`) in
`rockstream-types`.

There are **two semantic gotchas** that block a naive trait swap and must be
handled deliberately (see §5). The migration is otherwise mechanical and
localized to one crate plus a handful of `use` statements.

---

## 2. What `antichain` provides

From `../antichain` (package `antichain` v0.3.1, `edition = "2024"`,
`rust-version = "1.85"`):

| Item | Kind | Notes |
|------|------|-------|
| `Lattice` | trait | `pub trait Lattice: PartialOrd { fn meet; fn join; }` — **note the `PartialOrd` supertrait** |
| `Antichain<T>` | struct | `empty`, `from_elem`, `insert`, `elements`, `len`, `is_empty`, `less_equal` |
| `Frontier<T>` | struct | `bottom`, `from_elem`, `from_elements`, `elements`, `less_equal`, `meet`, `join` |
| `ProductTimestamp<T1, T2>` | struct | product order; `outer`/`inner` fields |
| `Lexicographic<A, B>` | struct | epoch × offset / lexicographic order |
| `Max<T>` / `Min<T>` / `Bounded<T>` | structs | order modifiers (Phase 6) |
| `WithTop<T>` / `WithBottom<T>` | enums | structural sentinels (Phase 7) |
| `MapLattice<K, V>` / `SetLattice<T>` | structs | point-wise / powerset lattices |
| `antichain-intervals::IntervalSetLattice<T>` | struct | companion crate; coalesced `[start, end)` interval lattice |

Built-in `Lattice` impls: all integer primitives (`u8..=u128`, `i8..=i128`,
`usize`, `isize`), 2-tuples `(A, B)` (component-wise), and all the composition
types above.

**Feature flags:** `std` (default), `serde` (off by default; derives
`Serialize`/`Deserialize` for all public types via `serde/alloc`).

---

## 3. What RockStream has today

[`crates/rockstream-types/src/frontier.rs`](crates/rockstream-types/src/frontier.rs)
mixes two concerns:

### 3a. Generic primitives — candidates for deletion

| RockStream item | `antichain` equivalent | Compatibility |
|-----------------|------------------------|---------------|
| `trait Lattice { fn meet; fn join; }` | `trait Lattice: PartialOrd` | ⚠️ adds `PartialOrd` supertrait (see §5.1) |
| `impl Lattice for u64` | built-in (`u8..=i128`) | ✅ superset |
| `struct Antichain<T>` | `Antichain<T>` | ✅ identical method set |
| `struct Frontier<T>` | `Frontier<T>` | ⚠️ `empty()` → `bottom()`, missing `is_empty`/`advance` (see §5.2) |
| `struct ProductTimestamp<T1, T2>` | `ProductTimestamp<T1, T2>` | ⚠️ no `Hash` derive, `serde` is feature-gated (see §5.3) |

### 3b. Domain-specific progress types — **stay in RockStream**

These encode RockStream's three-layer frontier protocol (v0.32) and are *not*
part of `antichain`:

- `SourceProgress` — `(source_epoch, event_time_watermark_ms)` with a custom
  `Lattice` impl.
- `FreshnessToken` — vector clock over `SourceId → SourceProgress` plus a
  `cluster_frontier_hash`.
- `ShardFrontierReport`, `WorkerFrontierSummary`, `ClusterFrontier`,
  `CompleteThroughToken` — protocol message types (no `Lattice` impl).

---

## 4. Where the primitives are used (blast radius)

The **generic** `Frontier`/`Antichain`/`ProductTimestamp` types are only
exercised inside `frontier.rs` itself and its unit tests. No other crate
constructs a generic `Frontier` or `Antichain`. Production call sites use the
**domain** types:

| Consumer | Imports | Uses |
|----------|---------|------|
| [`rockstream-ops/src/op.rs`](crates/rockstream-ops/src/op.rs) | `frontier::FreshnessToken` | operator input frontier |
| [`rockstream-ops/src/zset.rs`](crates/rockstream-ops/src/zset.rs) | `frontier::FreshnessToken` | batch frontier |
| [`rockstream-ops/src/time_window.rs`](crates/rockstream-ops/src/time_window.rs) | `frontier::{FreshnessToken, SourceProgress}` | watermark state |
| [`rockstream-control/src/frontier.rs`](crates/rockstream-control/src/frontier.rs) | `frontier::{ClusterFrontier, ShardFrontierReport}` | aggregator |
| [`rockstream-storage/src/shard_db.rs`](crates/rockstream-storage/src/shard_db.rs) | `frontier::ShardFrontierReport` | shard reporting |
| [`rockstream-sim/tests/progress_soak.rs`](crates/rockstream-sim/tests/progress_soak.rs) | `frontier::{FreshnessToken, Lattice, SourceProgress}` | soak test — **only external `Lattice` consumer** |
| [`rockstream-connectors/tests/source_proof_tests.rs`](crates/rockstream-connectors/tests/source_proof_tests.rs) | `frontier::{FreshnessToken, SourceProgress}` | source proofs |

**Consequence:** the only code outside `frontier.rs` that names the `Lattice`
trait is one soak test. The migration touches very little.

---

## 5. Semantic gotchas (must address before swapping)

### 5.1 `Lattice` gains a `PartialOrd` supertrait — and two domain impls are inconsistent with it

`antichain::Lattice` requires `Self: PartialOrd`, with the documented contract
that `meet`/`join` are the GLB/LUB **consistent with** that `PartialOrd`. Two
RockStream impls violate this:

- **`FreshnessToken` is not a lattice at all.** Its `meet`/`join` combine the
  `cluster_frontier_hash` with XOR:
  ```rust
  let cluster_frontier_hash = self.cluster_frontier_hash ^ other.cluster_frontier_hash;
  ```
  So `meet(a, a)` yields `hash ^ hash == 0`, i.e. **`meet` is not idempotent**.
  `FreshnessToken` also does not derive `PartialOrd`. It therefore *cannot* and
  *should not* implement `antichain::Lattice`.

- **`SourceProgress`'s `Lattice` is inconsistent with its derived `PartialOrd`.**
  It derives `PartialOrd` (epoch first, then `Option<watermark>` where
  `None < Some`), but on an epoch tie its `meet` prefers `Some(w)` over `None`:
  ```rust
  (Some(w1), None) => Some(w1),   // but derived order says None < Some(w1)
  ```
  So `meet(a, b)` can be **greater** than `b` under the derived order — again
  violating the `Lattice: PartialOrd` contract.

**Recommendation:** Do **not** route the domain types through
`antichain::Lattice`. Instead keep a small RockStream-local trait for the
bespoke "merge" algebra used by progress tokens, e.g.:

```rust
// rockstream-types/src/frontier.rs (retained)
/// RockStream progress-merge algebra (NOT a mathematical lattice:
/// `FreshnessToken` is intentionally non-idempotent due to its hash field).
pub trait ProgressMerge {
    fn meet(&self, other: &Self) -> Self;
    fn join(&self, other: &Self) -> Self;
}
impl ProgressMerge for SourceProgress { /* moved from Lattice impl */ }
impl ProgressMerge for FreshnessToken { /* moved from Lattice impl */ }
```

Then update the single external consumer
([`progress_soak.rs`](crates/rockstream-sim/tests/progress_soak.rs)) to import
`ProgressMerge` instead of `Lattice`. This cleanly separates "real lattice math"
(`antichain::Lattice`) from "RockStream progress merge" (`ProgressMerge`) and
documents the non-idempotency that previously hid behind a shared trait name.

> Alternative (not recommended): keep one unified trait but lose the
> `antichain::Lattice` benefits for our timestamp types. Splitting is cheaper
> and more honest.

### 5.2 `Frontier` API renames / gaps

| RockStream | `antichain` | Action |
|------------|-------------|--------|
| `Frontier::empty()` | `Frontier::bottom()` | rename call sites (none in prod; tests only) |
| `Frontier::is_empty()` | — (not present) | drop or add a thin wrapper if needed |
| `Frontier::advance(&mut self, other)` | — (not present) | inline as `*f = f.join(other)` or add wrapper |
| `Antichain::empty()` | `Antichain::empty()` | ✅ no change |

`advance`/`is_empty`/`empty` on the generic `Frontier` are currently used only
inside `frontier.rs` tests, so the rename is contained.

### 5.3 `ProductTimestamp` derive differences

- RockStream derives `Hash` (and `Copy`); `antichain` derives neither `Hash`
  nor `Copy`. If any RockStream code hashes a `ProductTimestamp` (none found
  today), we lose that. Mitigation: upstream a `Hash` derive into `antichain`
  (it's a trivial, non-breaking addition) rather than forking.
- `antichain`'s `Serialize`/`Deserialize` for `ProductTimestamp` (and all
  types) is behind the `serde` feature. RockStream serializes frontiers, so we
  must enable `features = ["serde"]` (see §6).

### 5.4 Edition / MSRV

`antichain` is `edition = "2024"` / MSRV `1.85`. RockStream is `edition = "2021"`
/ MSRV `1.88` on toolchain `1.88`. Editions are per-crate, so a 2021 crate can
depend on a 2024 crate with no issue, and `1.88 > 1.85`. **No toolchain bump
required.**

---

## 6. Cargo wiring

Add `antichain` (with `serde`) to the workspace dependency table in the root
[`Cargo.toml`](Cargo.toml):

```toml
[workspace.dependencies]
# ... existing ...
antichain = { version = "0.3.1", features = ["serde"] }
# Optionally, the interval companion crate (see §8):
# antichain-intervals = { version = "0.3", features = ["serde"] }
```

Source options, in order of preference:

1. **Published crate (preferred, once on crates.io):**
   `antichain = { version = "0.3.1", features = ["serde"] }`
2. **Git pin (until published):**
   `antichain = { git = "https://github.com/trickle-labs/antichain", tag = "v0.3.1", features = ["serde"] }`
3. **Local path (dev only — do not commit for CI):**
   `antichain = { path = "../antichain", features = ["serde"] }`

Then in [`crates/rockstream-types/Cargo.toml`](crates/rockstream-types/Cargo.toml):

```toml
[dependencies]
antichain = { workspace = true }
```

---

## 7. Migration plan (step by step)

1. **Wire the dependency** (§6): add `antichain` to workspace + `rockstream-types`.
2. **Re-export the generic primitives** from `rockstream-types` so downstream
   `use rockstream_types::frontier::{Frontier, Antichain, ProductTimestamp}`
   paths keep working:
   ```rust
   // rockstream-types/src/frontier.rs (top)
   pub use antichain::{Antichain, Frontier, Lattice, ProductTimestamp};
   ```
3. **Delete** the local `struct Antichain`, `struct Frontier`,
   `struct ProductTimestamp`, `trait Lattice`, `impl Lattice for u64`, and the
   generic lattice unit tests from `frontier.rs` (now covered by `antichain`'s
   own test suite).
4. **Introduce `ProgressMerge`** (§5.1) and move the `SourceProgress` /
   `FreshnessToken` merge impls onto it. Keep `SourceProgress`,
   `FreshnessToken`, `ShardFrontierReport`, `WorkerFrontierSummary`,
   `ClusterFrontier`, `CompleteThroughToken` exactly as they are otherwise.
5. **Fix the one external trait consumer:** in
   [`progress_soak.rs`](crates/rockstream-sim/tests/progress_soak.rs) change
   `use rockstream_types::frontier::{FreshnessToken, Lattice, SourceProgress};`
   to `... ProgressMerge ...`.
6. **Reconcile the `Frontier` API** (§5.2): replace any `Frontier::empty()` with
   `Frontier::bottom()`, and inline `advance`/`is_empty` where used (tests
   only). Keep these as thin wrappers in `frontier.rs` if we want to preserve
   the exact surface.
7. **Build + test:**
   ```bash
   rtk cargo build --workspace
   rtk cargo test --workspace 2>&1 | grep -E "^test result:|FAILED|^error"
   rtk cargo clippy --workspace -- -D warnings
   ```
8. **Verify serialization parity:** the on-wire/on-disk encoding of
   `Frontier`/`ProductTimestamp` must be byte-compatible if any persisted state
   exists. `antichain`'s `Antichain` custom `Serialize` emits the element slice;
   confirm against a golden sample before shipping (see §9).

---

## 8. Optional follow-ups enabled by `antichain`

- **`IntervalSetLattice`** (`antichain-intervals`) could back watermark / "gaps"
  tracking where progress is a union of disjoint `[start, end)` ranges — a
  cleaner model than ad-hoc epoch bookkeeping in
  [`rockstream-ops/src/time_window.rs`](crates/rockstream-ops/src/time_window.rs).
- **`MapLattice<ShardId, Epoch>`** is a natural fit for
  `ClusterFrontier`/`WorkerFrontierSummary` aggregation in
  [`rockstream-control/src/frontier.rs`](crates/rockstream-control/src/frontier.rs),
  replacing manual `BTreeMap` min-folding with a proven point-wise meet.
- **`Lexicographic<Epoch, Offset>`** matches the epoch × offset pattern if we
  ever need a totally-ordered composite progress key.

These are *not* required for the swap; they are future simplifications the
dependency makes available.

---

## 9. Risks & test strategy

| Risk | Mitigation |
|------|------------|
| `Lattice: PartialOrd` contract violation hidden by trait reuse | Split off `ProgressMerge` (§5.1); add a doc comment noting non-idempotency |
| Serialization drift for `Frontier`/`ProductTimestamp` | Add a round-trip golden test against a pre-migration sample before deleting local types |
| Lost `Hash`/`Copy` on `ProductTimestamp` | Upstream the derives into `antichain` (non-breaking) |
| Dependency not yet on crates.io | Pin by git tag `v0.3.1` until published; switch to version req afterward |
| `serde` feature not enabled | Enable `features = ["serde"]` in workspace dep |

**Test gates:** existing `rockstream-sim` progress soak, control aggregator
tests, and connector source-proof tests already cover the domain behaviour;
`antichain`'s own property tests (commutativity / associativity / idempotence /
absorption / distributivity) cover the generic algebra we are deleting.

---

## 10. Recommendation

Proceed with the swap. It removes ~370 lines of duplicated lattice code from
`rockstream-types`, replaces it with a tested, documented, `no_std`-capable
crate, and surfaces a latent correctness smell (`FreshnessToken`'s
non-idempotent merge) that we should make explicit via a dedicated
`ProgressMerge` trait rather than the misnamed `Lattice`. Keep all domain
protocol types in `rockstream-types`; depend on `antichain` only for the generic
primitives.
