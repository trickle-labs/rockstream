# Management API reference

The management API exposes typed status reads and long-running administrative operations. The protocol source is [`management.proto`](../../crates/rockstream-management-proto/proto/management/v1/management.proto).

**Qualification status:** v0.66 is not signed off. `CreateBackup` has no executor, server-side authorization is absent, and the release-process transcripts are missing.

## Endpoint and protocol

`control.management_addr` selects the server address. Its default is `127.0.0.1:9201`. The CLI uses that address unless you pass `--management`.

The service uses tonic over HTTP/2 without TLS or request authorization. Bind it to a trusted local interface. The current server does not authenticate remote callers.

The protocol version is `1`. Every request carries `protocol_version`. The server returns `FAILED_PRECONDITION` for an unsupported version.

## Methods

| Method | Behavior |
|---|---|
| `GetClusterStatus` | Returns the observed cluster state, node snapshot, operation counts, and management request fill. |
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
| `CreateBackup` | The request defines an `idempotency_key`, but the server returns `UNAVAILABLE` before acceptance. No backup executor is attached, and this method is absent from capabilities. |
| `CancelOperation` | Cancels a pending operation or one still in its validation phase. It returns `FAILED_PRECONDITION` after worker handoff starts. |

The control service attaches the drain and migration executors. `CreateBackup` never reports success. A successful `DrainWorker` result is not qualified until recipient-open acknowledgments and real-process drain transcripts pass.

## Response fields

`observed_at` uses RFC 3339 UTC. `source_version` identifies the state source or record version. Compound status fields do not share a transactional snapshot across the catalog and lease store.

`Node` includes its registered address, role, capacity headroom, host and zone, health, registration time, and lifecycle state. Missing process-health telemetry remains `unknown`.

`Shard` reports its owner and lease token. `key_range_known` is false and `key_range` is empty because the current lease store does not publish key ranges.

`Operation` reports an identifier, kind, state, start and update times, progress, phase, error code, next steps, and source record version. The states are `Pending`, `Running`, `Waiting`, `Succeeded`, `Failed`, and `Cancelled`. The server persists times as Unix milliseconds and renders them as RFC 3339 UTC.

`GetConfigSummary` marks each redacted value with `redacted=true`. It does not return the original secret.

## Pagination and limits

`page_size=0` selects the default of 50. The maximum page size is 100. Page tokens are decimal offsets; malformed or out-of-range tokens return `INVALID_ARGUMENT`.

The server admits at most 64 concurrent management requests. It limits gRPC messages to 1 MiB. The operation store retains at most 1,000 active records and 10,000 total records. Terminal records remain for seven days. Idempotency keys remain bound for 24 hours. Each operation accepts at most 64 state transitions.

## Idempotency and cancellation

`DrainWorker` and `MigrateShard` accept an `idempotency_key`. The server binds the key to the protocol version, operation kind, canonical request digest, and operation ID before it dispatches work. An exact retry returns the same record. A changed request under the same key returns `ALREADY_EXISTS`. An expired key returns `FAILED_PRECONDITION`.

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

Cancellation is allowed before worker handoff. The server rejects cancellation after the operation enters a side-effect phase. A rejected cancellation leaves the operation record unchanged.

## Current limitations

Management RPC errors retain their gRPC status. The CLI currently maps management status errors to `RS-0003`, so automation cannot distinguish every gRPC failure through the CLI output.

The management endpoint has no TLS or server-side role check. Do not expose it to an untrusted network. The CLI's local identity flags do not authenticate a caller to the management service.

The current release work has no release-binary transcript tests for standalone and multi-process clusters. The v0.66 criteria remain incomplete until those tests and the backup executor pass.
