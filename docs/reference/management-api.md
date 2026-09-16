# Management API reference

The management API exposes typed status reads and long-running administrative operations. The protocol source is [`management.proto`](../../crates/rockstream-management-proto/proto/management/v1/management.proto).

**Qualification status:** v0.66 is signed off. `CreateBackup` runs only when the server has an attached shard store. Server-side authorization and health telemetry are absent; health therefore remains `unknown`.

## Endpoint and protocol

`control.management_addr` selects the server address. Its default is `127.0.0.1:9201`. The CLI uses that address unless you pass `--management`.

The service uses tonic over HTTP/2 without TLS or request authorization. Bind it to a trusted local interface. The current server does not authenticate remote callers.

The protocol version is `1`. Every request carries `protocol_version`. The server returns `FAILED_PRECONDITION` for an unsupported version.

## Methods

| Method | Behavior |
|---|---|
| `GetClusterStatus` | Returns the observed cluster state, node snapshot, operation counts, management request fill, and ACK waiter fill. |
| `ListNodes` | Returns a page of registered nodes. |
| `GetNode` | Returns one registered node or `NOT_FOUND`. |
| `ListShards` | Returns a page of current shard leases. |
| `GetShard` | Returns one current shard lease or `NOT_FOUND`. |
| `ListOperations` | Returns a page of retained operation records. |
| `GetOperation` | Returns one operation or `NOT_FOUND`. |
| `GetConfigSummary` | Returns effective configuration values and redaction flags. |
| `GetCapabilities` | Returns the methods attached to this server. |
| `GetHealth` | Returns `unknown` until the process registers authoritative health telemetry. |
| `DrainWorker` | Persists an idempotent operation request, then drains a worker asynchronously. |
| `MigrateShard` | Persists an idempotent request, flushes and closes the donor shard, transfers its lease, and waits for the recipient to open it. Before donor handoff, the v0.66 executor rejects a shard with a registered active workload deployment. |
| `CreateBackup` | Persists an idempotent operation, requests a durable checkpoint from every shard owner, exports checkpoint-pinned shard data and control state while excluding the audit and management-operation logs and live worker topology, and reports success only after validating its terminal marker. Workers rebuild their topology records when they register. The method appears in capabilities only when a backup source store is attached. |
| `CancelOperation` | Cancels a pending operation or a migration before lease transfer. During donor handoff, the executor waits for the worker acknowledgement and reopens the donor before returning to its caller. It returns `FAILED_PRECONDITION` after lease transfer starts. |

The control service attaches drain and migration executors. It attaches the backup executor only when it can read the authoritative shard store. Embedded `--role all` supports one local worker. Multi-worker backups require every shard owner to use the same configured shared object store.

The CLI sends `admin backup create` to `CreateBackup` only when you pass `--management`. Without that flag, it writes the legacy local manifest used by `admin backup inspect` and `admin backup verify`.

## CLI examples

These commands target a local management endpoint. Replace `<operation-id>` with an ID returned by an operation command.

```sh
rockstream --output json --management 127.0.0.1:9201 status
rockstream --output json --management 127.0.0.1:9201 health
rockstream --output json --management 127.0.0.1:9201 capabilities
rockstream --output json --management 127.0.0.1:9201 cluster workers list
rockstream --output json --management 127.0.0.1:9201 cluster workers status 1
rockstream --output json --management 127.0.0.1:9201 shard list
rockstream --output json --management 127.0.0.1:9201 shard show 0
rockstream --output json --management 127.0.0.1:9201 config summary
rockstream --output json --management 127.0.0.1:9201 admin operation list
rockstream --output json --management 127.0.0.1:9201 admin operation show <operation-id>
```

`CreateBackup` accepts a local path, a `file://` path, or an `s3://bucket/prefix` destination. It requires an idle cluster with no active workload deployments or source writes. The operation record stores the destination and checkpoint phase. The server stores its checkpoint manifest in the source store and writes the export under `checkpoint-exports/management-<operation_id>`. A missing worker or temporary storage error leaves the operation waiting. An invalid destination or checkpoint integrity error leaves it failed.

The management backup format is a committed checkpoint generation with inventory records and a terminal `commit` marker. It is separate from the local `manifest.json` format used by the legacy `admin backup inspect` and `admin backup verify` commands.

A successful `DrainWorker` result requires a matching recipient-open acknowledgement for every moved shard. A drain remains active while a recipient is unavailable or has not acknowledged its lease.

## Response fields

`observed_at` uses RFC 3339 UTC. `source_version` identifies the state source or record version. Compound status fields do not share a transactional snapshot across the catalog and lease store.

`request_fill` and `request_capacity` report the management request semaphore. `ack_waiter_fill` and `ack_waiter_capacity` report registered migration and backup worker acknowledgements. Each executor admits at most 64 ACK waiters. The reported ACK waiter capacity is 64 for each attached executor.

`Node` includes its registered address, role, capacity headroom, host and zone, health, registration time, and lifecycle state. Missing process-health telemetry remains `unknown`.

`Shard` reports its owner and lease token. `key_range_known` is false and `key_range` is empty because the current lease store does not publish key ranges.

`Operation` reports an identifier, kind, state, start and update times, progress, phase, error code, next steps, and source record version. The states are `Pending`, `Running`, `Waiting`, `Succeeded`, `Failed`, and `Cancelled`. The server persists times as Unix milliseconds and renders them as RFC 3339 UTC.

`GetConfigSummary` marks each redacted value with `redacted=true`. It does not return the original secret.

## Pagination and limits

`page_size=0` selects the default of 50. The maximum page size is 100. Page tokens are decimal offsets; malformed or out-of-range tokens return `INVALID_ARGUMENT`.

The server admits at most 64 concurrent management requests and 64 ACK waiters for each attached migration or backup executor. It limits gRPC messages to 1 MiB. The operation store retains at most 1,000 active records and 10,000 total records. Terminal records remain for seven days. Idempotency keys remain bound for 24 hours. Each operation accepts at most 64 state transitions.

## Idempotency and cancellation

`DrainWorker`, `MigrateShard`, and `CreateBackup` accept an `idempotency_key`. The server binds the key to the protocol version, operation kind, request digest, and operation ID before it dispatches work. An exact retry returns the same record. A changed request under the same key returns `ALREADY_EXISTS`. An expired key returns `FAILED_PRECONDITION`.

The valid status transitions are:

| Current state | Next states |
|---|---|
| `Pending` | `Running`, `Waiting`, `Failed`, `Cancelled` |
| `Running` | `Waiting`, `Succeeded`, `Failed`, `Cancelled` |
| `Waiting` | `Running`, `Failed`, `Cancelled` |
| `Succeeded` | None |
| `Failed` | None |
| `Cancelled` | None |

`Running` and `Waiting` can also receive progress metadata updates without changing state. Terminal states cannot change.

The server durably records `Pending` before it returns an operation ID. A transition uses a create-only record keyed by the previous record hash. This prevents two store instances from advancing the same operation state at once. A transition conflict returns `FAILED_PRECONDITION`.

Cancellation is allowed before lease transfer. A migration cancelled during donor handoff waits for the acknowledgement and reopens the donor under the unchanged lease before stopping. The operation record reports `cancelled_before_lease_transfer`. The server rejects cancellation after lease transfer starts. A rejected cancellation leaves the operation record unchanged.

## Current limitations

Management RPC errors retain their gRPC status. The CLI currently maps management status errors to `RS-0003`, so automation cannot distinguish every gRPC failure through the CLI output.

The management endpoint has no TLS or server-side role check. Do not expose it to an untrusted network. The CLI's local identity flags do not authenticate a caller to the management service.

The server has no authoritative process-health telemetry, so `GetHealth` returns `unknown`. This is the qualified behavior for v0.66; the limitation remains explicit until health telemetry is implemented.
