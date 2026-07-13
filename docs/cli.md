# RockStream CLI Reference

RockStream ships as a **single binary** named `rockstream`. Every node role is a
flag on this one binary — there is no separate server, worker, or gateway
executable. `main` is always runnable through it.

At **v0.1** the binary runs an **embedded no-op node**: it brings the node up,
runs a no-op pipeline to completion, writes an audit log and a support bundle,
and exits cleanly. Real operators, durability, SQL, and the distributed roles
are added in later versions; this page documents only what exists today.

## Synopsis

```bash
rockstream --help
rockstream --version
rockstream start --storage <dir> [--role <role>]
```

## Commands

### `rockstream start`

Starts a RockStream node. At v0.1 this runs the embedded no-op node, which:

1. validates the requested role,
2. creates the storage directory if it does not exist,
3. writes an audit log recording the node and no-op pipeline lifecycle,
4. writes a support bundle, and
5. exits with status `0`.

**Options**

| Option | Required | Default | Description |
|---|---|---|---|
| `--storage <dir>` | yes | — | Local storage directory for node state and artifacts. Created if missing. |
| `--role <role>` | no | `all` | Node role. One of `all`, `control`, `worker`, `gateway`, `frontier`. An unrecognised role, or a role requiring `--control` when it is omitted, is rejected with `RS-0002`. |
| `--control <url>` | no (required for `worker`/`frontier`) | — | Control service URL. Required for the `worker` and `frontier` roles; omitting it is rejected with `RS-0002`. |
| `--auth <mode>` | no | `off` | Authentication mode. One of `off`, `oidc`, `mtls`. |
| `--metrics-addr <addr>` | no | — | Metrics HTTP server listen address. |
| `--listen <addr>` | no | `127.0.0.1:5432` | PostgreSQL wire gateway listen address. Activates the live gateway server for the `gateway` and `all` roles. |
| `--raft-peers <list>` | no | — | v0.45.2 M7: comma-separated list of the *other* control nodes in this node's Raft group, `id@host:port,id@host:port`. Only meaningful for `--role=control`. When omitted, the control role runs exactly as before v0.45.2 (single embedded node, no Raft leader-only write gating). |
| `--raft-node-id <id>` | no (required with `--raft-peers`) | — | This node's id within its Raft group. |
| `--raft-bind <addr>` | no (required with `--raft-peers`) | — | Address this node's Raft peer-RPC listener binds to. |
| `--raft-bootstrap` | no | `false` | Start an election immediately on boot rather than waiting out a randomized timeout. Exactly one node in a freshly-bootstrapped Raft group should set this. |
| `--daemon` | no | `false` | v0.45.2 M7 S4: run the `control` role as a real long-lived daemon that blocks on SIGTERM / Ctrl-C, exactly like the `gateway`/`all` roles' live PostgreSQL wire server, instead of the short embedded no-op run. Only meaningful for `--role=control`; required for a real multi-node control-plane cluster. |
| `--control-bind <addr>` | no | `127.0.0.1:8000` | v0.45.2 M7 S4: overrides the address the control-plane's worker-facing `ControlService` TCP listener binds to. Only meaningful for `--role=control`/`--role=all`. Needed to bind `0.0.0.0:<port>` inside a container so peer control nodes and workers on other hosts can reach it. |
| `--control-shared-storage <dir>` | no | — | v0.45.2 M7 S4/S5: directory for state that must be *shared* across every control node in this node's Raft group — the Raft term/vote/log and the shard-lease-manager snapshot. Only meaningful for `--role=control` with `--raft-peers`. When omitted, each control node's Raft state lives under its own private `--storage` directory (no cross-process lease continuity). |

**Example**

```bash
rockstream start --storage ./data
```

**Artifacts written under `<storage>`**

- `audit.jsonl` — one JSON object per line, one per control-plane action. v0.1
  emits `server.started`, `pipeline.created`, `pipeline.started`,
  `pipeline.stopped`, and `server.stopped`.
- `support-bundle-<timestamp>.json` — a snapshot bundle containing
  `system_info` (version, OS, arch, role), a `metrics` snapshot (run duration
  and audit-event count), and the full `audit_events` list.

## Exit codes

| Exit code | Meaning |
|---|---|
| `0` | The node ran the no-op pipeline to completion and wrote its artifacts. |
| non-zero | A failure occurred; the error is printed to stderr with its `RS-XXXX` code and actionable next steps. |

## Error codes

Operator-visible failures carry an `RS-XXXX` code (see the registry in
`crates/rockstream-types/src/error_code.rs`). The `start` command can return:

| Code | Meaning | Next steps |
|---|---|---|
| `RS-0002` | Unknown node role passed to `--role`, or a required option (e.g. `--control` for `worker`/`frontier`) was omitted for the requested role. | Pass `--role` with one of: `all`, `control`, `worker`, `gateway`, `frontier`, and supply any options that role requires. |
| `RS-0003` | Could not create the storage directory or write an artifact. | Check that the parent path exists, is writable, and the disk is not full. |
| `RS-1731` | `control.not_leader` — a control-plane write (e.g. a shard-lease grant or shard-assignment write) was rejected because this control node is not the current Raft leader. Only relevant when `--raft-peers` is set. | Retry the write against the current Raft leader; callers should re-resolve control-plane leadership and route the request there. |

## Logging

Logs are emitted through `tracing`. Set the verbosity with the standard
`RUST_LOG` environment variable, e.g. `RUST_LOG=debug`:

```bash
RUST_LOG=info rockstream start --storage ./data
```
