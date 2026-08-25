# CLI reference

## `rockstream audit`

Audit log inspection commands

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream audit query`

Query audit log events matching a filter

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| filter | — | --filter | FILTER | no | — | — | Substring filter for actor, action, or resource |
| max | — | --max | MAX | no | 100 | — | Maximum events to return (max 1000) |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream audit tail`

Tail recent audit log events

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| max | — | --max | MAX | no | 100 | — | Maximum events to return (max 1000) |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream checkpoint`

Checkpoint inspection commands

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream checkpoint export`

Export latest committed checkpoint

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| destination | — | --destination | DESTINATION | yes | — | — | Destination object-store URL |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream checkpoint list`

List cluster checkpoints

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream checkpoint restore`

Restore committed export

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| source | — | --source | SOURCE | yes | — | — | Export object-store URL |
| storage | — | --storage | STORAGE | yes | — | — | Fresh target object-store URL |
| yes | — | --yes | YES | no | — | true, false | Confirm destructive action without interactive prompt |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream checkpoint show`

Show per-shard checkpoint alignment state

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| checkpoint_id | — | — | CHECKPOINT_ID | yes | — | — | Checkpoint ID |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream cluster`

Cluster administration and inspection commands

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream cluster quotas`

Show cluster quotas and capacity limits

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream cluster status`

Show cluster status and leadership

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream cluster workers`

Worker administration commands

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

#### `rockstream cluster workers drain`

Begin draining a worker

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| control | — | --control | CONTROL | no | — | — | Control-plane worker-facing TCP address |
| worker_id | — | — | WORKER_ID | yes | — | — | Worker ID to drain |
| yes | — | --yes | YES | no | — | true, false | Confirm destructive action without interactive prompt |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

#### `rockstream cluster workers list`

List all registered workers

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

#### `rockstream cluster workers status`

Show detailed worker status

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| worker_id | — | — | WORKER_ID | no | — | — | Optional worker ID |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream completions`

Generate shell completion scripts for Bash, Zsh, or Fish

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| shell | — | — | SHELL | yes | — | bash, zsh, fish | Target shell to generate completions for |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream config`

Configuration validation and effective printing

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream config print-effective`

Print the effective configuration resolved from defaults, config file, environment, and CLI flags

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| checkpoint_retention_count | — | --checkpoint-retention-count | CHECKPOINT_RETENTION_COUNT | no | — | — |  |
| exchange_direct_threshold_bytes | — | --exchange-direct-threshold-bytes | EXCHANGE_DIRECT_THRESHOLD_BYTES | no | — | — |  |
| exchange_domain_size | — | --exchange-domain-size | EXCHANGE_DOMAIN_SIZE | no | — | — |  |
| exchange_force_durable | — | --exchange-force-durable | EXCHANGE_FORCE_DURABLE | no | false | true, false |  |
| exchange_spill_threshold_mb | — | --exchange-spill-threshold-mb | EXCHANGE_SPILL_THRESHOLD_MB | no | — | — |  |
| file | — | --file | FILE | no | — | — | Path to configuration file (defaults to standard search paths) |
| max_exchange_compression_states | — | --max-exchange-compression-states | MAX_EXCHANGE_COMPRESSION_STATES | no | — | — |  |
| min_epoch_ms | — | --min-epoch-ms | MIN_EPOCH_MS | no | — | — |  |
| same_host_shm_segment_bytes | — | --same-host-shm-segment-bytes | SAME_HOST_SHM_SEGMENT_BYTES | no | — | — |  |
| same_host_shm_segments_per_peer | — | --same-host-shm-segments-per-peer | SAME_HOST_SHM_SEGMENTS_PER_PEER | no | — | — |  |
| show_origins | — | --show-origins | SHOW_ORIGINS | no | — | true, false | Include source origin annotations in the printed configuration |
| state_budget_gb | — | --state-budget-gb | STATE_BUDGET_GB | no | — | — |  |
| webhook_listen | — | --webhook-listen | WEBHOOK_LISTEN | no | — | — |  |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream config validate`

Validate RockStream configuration files for syntax, unknown keys, and semantic bounds

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| check_files | — | --check-files | CHECK_FILES | no | — | true, false | Validate accessibility of referenced TLS certificate and key files |
| file | — | --file | FILE | no | — | — | Path to configuration file to validate (defaults to standard search paths) |
| strict | — | --strict | STRICT | no | true | true, false | Enforce strict validation (fail on unknown or deprecated keys) |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream debug`

Low-level debugging and arrangement state inspection

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream debug arrangement`

Inspect intermediate arrangement Z-set state for an operator

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| epoch | — | --epoch | EPOCH | no | — | — | Historical epoch to inspect |
| key | — | — | KEY | yes | — | — | Key expression to inspect |
| op_id | — | — | OP_ID | yes | — | — | Operator ID to inspect |
| view | — | — | VIEW | yes | — | — | View name to inspect |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream demo`

Run an embedded demonstration scenario proving incremental view maintenance

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| keep | — | --keep | KEEP | no | — | true, false | Retain demo storage directory after execution |
| listen | — | --listen | LISTEN | no | 127.0.0.1:0 | — | PostgreSQL wire gateway listen address |
| scenario | — | --scenario | SCENARIO | no | orders | — | Demo scenario to execute (default: orders) |
| step_delay_ms | — | --step-delay-ms | STEP_DELAY_MS | no | 0 | — | Optional presentation delay in milliseconds between scenario steps (max 5000) |
| storage | — | --storage | STORAGE | no | — | — | Storage directory for local state and artifacts |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream doctor`

Run non-destructive diagnostic checks on binary, config, system, storage, and network reachability

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| config | — | --config | CONFIG | no | — | — | Path to configuration file to validate |
| control | — | --control | CONTROL | no | — | — | Control service URL to probe |
| deep | — | --deep | DEEP | no | — | true, false | Perform active storage write/read/delete probe |
| gateway | — | --gateway | GATEWAY | no | — | — | PostgreSQL wire gateway address (host:port) to probe |
| include_docker | — | --include-docker | INCLUDE_DOCKER | no | — | true, false | Include Docker daemon socket accessibility check |
| storage | — | --storage | STORAGE | no | — | — | Storage path or s3:// URL to validate |
| timeout | — | --timeout | TIMEOUT | no | 5 | — | Check execution timeout in seconds (default 5, max 30) |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream explain`

Explain the incremental execution plan for a view

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| estimate | — | --estimate | ESTIMATE | no | — | true, false | Show static cost and state memory estimates without deploying |
| op_ids | — | --op-ids | OP_IDS | no | — | true, false | Show operator IDs and addressability details for intermediate state |
| view | — | — | VIEW | yes | — | — | View name to explain |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream init`

Initialize a new RockStream project from a template

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| dir | — | --dir | DIR | no | — | — | Target directory to scaffold the project into (defaults to ./<name>) |
| force | — | --force | FORCE | no | false | true, false | Overwrite existing files in non-empty directory |
| name | — | — | NAME | no | my_project | — | Project name (defaults to "my_project") |
| template | — | --template | TEMPLATE | no | local | — | Project template: "local", "kafka", or "postgres-cdc" |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream manifest`

Evidence manifest verification and inspection

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream manifest validate`

Validate an evidence-manifest.json file

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| base_dir | — | --base-dir | BASE_DIR | no | — | — | Optional base directory containing referenced artifact files |
| path | — | — | PATH | yes | — | — | Path to the evidence-manifest.json file |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream migrate`

Migrate shard storage formats offline

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| from | — | --from | FROM | yes | — | — | Existing storage format version |
| storage | — | --storage | STORAGE | yes | — | — | Local path or s3://bucket/prefix containing shard databases |
| to | — | --to | TO | yes | — | — | Target storage format version |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream qualify`

Run release qualification suite or check prerequisites

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| check_prerequisites | — | --check-prerequisites | CHECK_PREREQUISITES | no | — | true, false | Check execution environment prerequisites fail-closed |
| output | — | --output | OUTPUT | no | — | — | Output file path for raw metrics and summary |
| suite | — | --suite | SUITE | no | — | — | Qualification test suite to execute |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream resource`

Resource usage inspection commands

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream resource cluster`

Show aggregate cluster resource usage

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream resource usage`

Show per-view and per-workload resource usage

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| workload | — | --workload | WORKLOAD | no | — | — | Optional workload name filter |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream schema`

Schema inspection commands

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream schema create`

Create a new schema table

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| columns | — | --columns | COLUMNS | no | — | — | Column specification |
| name | — | — | NAME | yes | — | — | Table name |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream schema drop`

Drop a schema table or view

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| name | — | — | NAME | yes | — | — | Table name |
| yes | — | --yes | YES | no | — | true, false | Confirm destructive action without interactive prompt |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream schema list`

List all tables and views in the schema

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream schema show`

Show schema columns for a table or view

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| name | — | — | NAME | yes | — | — | Table or view name |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream schema-evolution`

Schema evolution inspection commands

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream schema-evolution history`

Show schema evolution version history

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream schema-evolution status`

Show schema evolution status

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream shard`

Shard inspection commands

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream shard list`

List all shards and their lease assignments

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream shard migrate`

Migrate a shard to another worker

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| shard_id | — | — | SHARD_ID | yes | — | — | Shard ID |
| to | — | --to | TO | yes | — | — | Target worker ID |
| yes | — | --yes | YES | no | — | true, false | Confirm destructive action without interactive prompt |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream source`

Source inspection commands

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream source drop`

Drop a source connector

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| name | — | — | NAME | yes | — | — | Source name |
| yes | — | --yes | YES | no | — | true, false | Confirm destructive action without interactive prompt |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream source list`

List all sources

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream source pause`

Pause source ingestion

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| name | — | — | NAME | yes | — | — | Source name |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream source resume`

Resume paused source ingestion

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| name | — | — | NAME | yes | — | — | Source name |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream source show`

Show source connector detail

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| name | — | — | NAME | yes | — | — | Source name |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream sql`

Parse, lower, and explain a SQL query without deploying

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| query | — | — | QUERY | yes | — | — | SQL query to parse and lower |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream start`

Start a RockStream node

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| auth | — | --auth | AUTH | no | off | off, oidc, mtls | Authentication mode |
| availability_zone | — | --availability-zone | AVAILABILITY_ZONE | no | — | — | Availability zone advertised during worker registration |
| checkpoint_retention_count | — | --checkpoint-retention-count | CHECKPOINT_RETENTION_COUNT | no | — | — |  |
| control | — | --control | CONTROL | no | — | — | Control service URL (required for the worker and frontier roles) |
| control_bind | — | --control-bind | CONTROL_BIND | no | — | — | Override address worker-facing ControlService binds to |
| control_shared_storage | — | --control-shared-storage | CONTROL_SHARED_STORAGE | no | — | — | Directory for state shared across control nodes in Raft group |
| daemon | — | --daemon | DAEMON | no | false | true, false | Run the control role as a daemon |
| exchange_direct_threshold_bytes | — | --exchange-direct-threshold-bytes | EXCHANGE_DIRECT_THRESHOLD_BYTES | no | — | — |  |
| exchange_domain_size | — | --exchange-domain-size | EXCHANGE_DOMAIN_SIZE | no | — | — |  |
| exchange_force_durable | — | --exchange-force-durable | EXCHANGE_FORCE_DURABLE | no | false | true, false |  |
| exchange_spill_threshold_mb | — | --exchange-spill-threshold-mb | EXCHANGE_SPILL_THRESHOLD_MB | no | — | — |  |
| host_id | — | --host-id | HOST_ID | no | — | — | Stable same-host identity advertised during worker registration |
| listen | — | --listen | LISTEN | no | 127.0.0.1:5432 | — | PostgreSQL wire gateway listen address |
| max_exchange_compression_states | — | --max-exchange-compression-states | MAX_EXCHANGE_COMPRESSION_STATES | no | — | — |  |
| metrics_addr | — | --metrics-addr | METRICS_ADDR | no | — | — | Metrics HTTP server listen address |
| min_epoch_ms | — | --min-epoch-ms | MIN_EPOCH_MS | no | — | — |  |
| query_time_shard_dirs | — | --query-time-shard-dir | QUERY_TIME_SHARD_DIRS | no | — | — | Root directory of a non-local shard included in query-time scatter read |
| raft_bind | — | --raft-bind | RAFT_BIND | no | — | — | Address this node's Raft peer-RPC listener binds to |
| raft_bootstrap | — | --raft-bootstrap | RAFT_BOOTSTRAP | no | false | true, false | Start an election immediately on boot |
| raft_node_id | — | --raft-node-id | RAFT_NODE_ID | no | — | — | This node's ID within its Raft group |
| raft_peers | — | --raft-peers | RAFT_PEERS | no | — | — | Comma-separated list of other control nodes in Raft group |
| role | — | --role | ROLE | no | all | — | Node role |
| same_host_shm_segment_bytes | — | --same-host-shm-segment-bytes | SAME_HOST_SHM_SEGMENT_BYTES | no | — | — |  |
| same_host_shm_segments_per_peer | — | --same-host-shm-segments-per-peer | SAME_HOST_SHM_SEGMENTS_PER_PEER | no | — | — |  |
| state_budget_gb | — | --state-budget-gb | STATE_BUDGET_GB | no | — | — |  |
| storage | — | --storage | STORAGE | yes | — | — | Local storage directory for node state and artifacts |
| webhook_listen | — | --webhook-listen | WEBHOOK_LISTEN | no | — | — | Independent HTTP listener for `POST /webhook/<source>` ingestion |
| worker_id | — | --worker-id | WORKER_ID | no | — | — | Explicit worker ID advertised during worker registration |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream support`

Diagnostic support commands

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream support bundle`

Generate on-demand diagnostic support bundle

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| out | — | --out | OUT | no | — | — | Output file path for the support bundle |
| since | — | --since | SINCE | no | — | — | Optional duration filter |
| view | — | --view | VIEW | no | — | — | Optional view name filter |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream support diagnose`

Look up one runtime diagnostic and write its support bundle

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| code | — | --code | CODE | no | — | — | Look up the most recent occurrence for this catalog code |
| correlation_id | — | --correlation-id | CORRELATION_ID | no | — | — | Look up an occurrence by its correlation UUID |
| out | — | --out | OUT | no | — | — | Output file path for the support bundle |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream version`

Print candidate identity and version information

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| json | — | --json | JSON | no | — | true, false | Format version information as JSON |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream view`

View inspection commands

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream view list`

List all views

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream view pause`

Pause an active view

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| name | — | — | NAME | yes | — | — | View name |
| yes | — | --yes | YES | no | — | true, false | Confirm destructive action without interactive prompt |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream view query`

Query view results

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| limit | — | --limit | LIMIT | no | — | — | Maximum rows to return |
| name | — | — | NAME | yes | — | — | View name |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream view resume`

Resume a paused view

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| name | — | — | NAME | yes | — | — | View name |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream view show`

Show detailed view metadata

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| name | — | — | NAME | yes | — | — | View name |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream view status`

Show view lifecycle and freshness status

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| name | — | — | NAME | no | — | — | Optional view name filter |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream view subscribe`

Stream live view updates

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| from_epoch | — | --from-epoch | FROM_EPOCH | no | — | — | Start streaming from a specific epoch |
| name | — | — | NAME | yes | — | — | View name |
| snapshot | — | --snapshot | SNAPSHOT | no | — | true, false | Begin subscription with a baseline snapshot |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

## `rockstream workload`

Workload inspection commands

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream workload alter`

Alter an existing workload

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| freshness_slo_ms | — | --freshness-slo-ms | FRESHNESS_SLO_MS | no | — | — | Freshness SLO in milliseconds |
| max_parallelism | — | --max-parallelism | MAX_PARALLELISM | no | — | — | Maximum worker parallelism |
| memory_limit | — | --memory-limit | MEMORY_LIMIT | no | — | — | Memory limit in bytes |
| name | — | — | NAME | yes | — | — | Workload name |
| priority | — | --priority | PRIORITY | no | — | — | Scheduling priority |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream workload create`

Create a new workload

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| freshness_slo_ms | — | --freshness-slo-ms | FRESHNESS_SLO_MS | no | — | — | Freshness SLO in milliseconds |
| max_parallelism | — | --max-parallelism | MAX_PARALLELISM | no | — | — | Maximum worker parallelism |
| memory_limit | — | --memory-limit | MEMORY_LIMIT | no | — | — | Memory limit in bytes |
| name | — | — | NAME | yes | — | — | Workload name |
| priority | — | --priority | PRIORITY | no | — | — | Scheduling priority |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream workload drop`

Drop a workload

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| name | — | — | NAME | yes | — | — | Workload name |
| yes | — | --yes | YES | no | — | true, false | Confirm destructive action without interactive prompt |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream workload list`

List all workloads

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

### `rockstream workload show`

Show workload definition detail

Options

| Name | Short | Long | Value | Required | Default | Values | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| name | — | — | NAME | yes | — | — | Workload name |

Exit codes

| Code | Title | Description | Error codes |
| --- | --- | --- | --- |
| 0 | Success | Command completed successfully without error | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 1 | Execution Error | Runtime failure or operation error during execution | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |
| 2 | Usage Error | Invalid arguments, options, or flags provided to CLI | RS-0001, RS-1001, RS-1012, RS-1013, RS-2001 |

Error codes: `RS-0001`, `RS-1001`, `RS-1012`, `RS-1013`, `RS-2001`

