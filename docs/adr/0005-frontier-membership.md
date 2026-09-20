# ADR 0005: Membership-aware frontier publication

Status: Accepted

## Context

The scalar frontier report identified only a shard and an epoch. A publisher
could therefore retain `10` after admitting a new shard whose durable frontier
was `5`, even though the meet for the current registry was `5`. Monotone
publication is not sufficient when the membership set changes.

## Decision

- A frontier scope is identified by its deployment/view scope string and a
  monotonically increasing `FrontierGeneration`.
- Configured membership is authoritative; reporting shards are merely members
  that have supplied a valid report; only active members participate in the
  publication meet. An empty active set publishes no completeness (`None`).
- Every active shard has a `ShardIncarnation`. A report is admitted only when
  its version, scope, generation, shard, and incarnation match the active
  configuration, and its lease token is current for that shard. Scalar reports
  remain available for legacy aggregators but are rejected by configured
  aggregators.
- A new or replacement member is activated only with a durable bootstrap epoch
  at least as high as the currently published frontier. Retirements remove the
  member from the meet; replacement is one guarded generation transition.
- Generation overflow is rejected. Report epochs are exclusive: a frontier
  `F` means every epoch `e < F` is durable, not `e <= F`.
- Durable publications use a generation-scoped key. A restarted publisher must
  not import a previous configuration's publication into the new scope.
- `FreshnessToken` source progress and its XOR hash are separate semantics; the
  hash is diagnostic and is not treated as an idempotent lattice field.

## Consequences

The versioned worker envelope is required for the safe path. Existing scalar
workers continue to work only with the compatibility aggregator, and dynamic
membership qualification remains responsible for the authority that supplies
bootstrap epochs and configuration changes; the public service checks the
reporting connection and shard lease before admission.
