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
| `--role <role>` | no | `all` | Node role. One of `all`, `control`, `worker`, `gateway`. v0.1 runs the embedded `all` profile regardless of the value; an unrecognised role is rejected with `RS-0002`. |

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
| `RS-0002` | Unknown node role passed to `--role`. | Pass `--role` with one of: `all`, `control`, `worker`, `gateway`. |
| `RS-0003` | Could not create the storage directory or write an artifact. | Check that the parent path exists, is writable, and the disk is not full. |

## Logging

Logs are emitted through `tracing`. Set the verbosity with the standard
`RUST_LOG` environment variable, e.g. `RUST_LOG=debug`:

```bash
RUST_LOG=info rockstream start --storage ./data
```
