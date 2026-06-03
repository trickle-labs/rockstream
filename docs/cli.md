# RockStream CLI Subcommands Reference

This document covers all CLI subcommands, options, and structured JSON logging options.

## Subcommand Reference

### `rockstream start`
Starts a RockStream node (control, worker, or gateway role).

**Options:**
- `--storage <path>`: Local storage directory for data and WALs (default: `./data`).
- `--role <role>`: Selects services to run (`all`, `worker`, `control`, `gateway`, `frontier`).
- `--control <addr>`: Address of control node to register with when running as `worker` or `gateway`.
- `--control-bind <addr>`: Address to listen on when running `control` role (default: `127.0.0.1:7700`).
- `--tls-cert <path>`, `--tls-key <path>`, `--tls-ca-cert <path>`: Supply all three to enable mTLS.
- `--allow-law-operand-fallback`: Permits fallback to raw bytes on law operand corruption (emergency recovery).

### `rockstream bootstrap`
Bootstraps a cluster by connecting to a running control service.

**Options:**
- `--control <addr>`: Address of the control-plane node (default: `127.0.0.1:7700`).

### `rockstream explain`
Prints the operator graph with merge-law annotations for a view.

**Usage:**
```bash
rockstream explain sales_by_product
```

### `rockstream sql`
Runs a SQL query and outputs the planned IVM operator graph.

**Usage:**
```bash
rockstream sql "SELECT region, SUM(amount) FROM orders GROUP BY region"
```

### `rockstream describe`
Describes the status of a specific pipeline.

**Usage:**
```bash
rockstream describe orders_pipeline
```

### `rockstream debug arrangement`
Connects to the worker hosting the shard, decodes arrangement law headers, and prints the raw/parsed operand and tombstone density.

**Usage:**
```bash
rockstream debug arrangement orders_mv 3f2a "product_id=42"
```

### `rockstream support-bundle`
Generates a tarball containing system logs, catalog metrics, configuration details, and audit history.

**Options:**
- `--output <path>`: Output file path (default: `./support-bundle.tar.gz`).

## JSON Logging Options

Supply the environment variable `RUST_LOG=info` and the standard `--json` flag (if available) to format output as structured JSON.

Example JSON output structure:
```json
{
  "timestamp": "2026-06-03T07:53:20Z",
  "level": "INFO",
  "fields": {
    "message": "starting rockstream",
    "storage": "./data",
    "role": "all"
  },
  "target": "rockstream"
}
```
