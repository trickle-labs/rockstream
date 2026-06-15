# RockStream Formal Modeling Conventions (FizzBee)

All `.fizz` specifications in this repository must adhere to the following conventions:

## 1. Role Naming & Parameterization

- Use capitalized nouns for roles corresponding to RockStream architecture components:
  - `Worker` (represents a worker node).
  - `Shard` (represents a single SlateDB shard instance).
  - `ControlPlane` (represents the centralized control plane / catalog).
  - `ObjectStore` (represents shared durable storage).
- Parameterize role instances using uppercase constructor keys (e.g., `Worker(id=1)`).

## 2. Durability Annotations

- Ephemeral (lost on crash) vs. Durable (survives crash) state must be explicitly declared using the `@state` decorator:
  - Declare a role's durable or ephemeral fields using `@state(ephemeral=["name", ...])` or `@state(durable=["name", ...])`.
  - `Shard` state must be entirely durable.
  - `Worker` state like `held_leases` or `current_epoch_buffer` must be declared ephemeral.

## 3. Communication Models

- Use channels or explicit message sets:
  - Idempotent and asynchronous messages (e.g. shard frontier reports) should use `unordered`, `atmost_once`, `fire_and_forget` channels.
  - Reordering-sensitive proofs should use explicit collections (`set` of messages) processed non-deterministically.

## 4. Finite State-Space Bounds

Keep specifications checkable in CI by applying these maximum bounds:
- `NUM_WORKERS` <= 3
- `NUM_SHARDS` <= 3
- `MAX_EPOCH` <= 3
- `MAX_CHECKPOINT` <= 2

## 5. Coverage Assertions

- Every spec must contain a `COV-<Model>` coverage assertion using `exists` to prevent vacuously-passing specs (e.g., ensuring crash and recovery states are actually reached in the model check).
