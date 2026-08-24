# Contributor guide

Start with [CONTRIBUTING.md](../CONTRIBUTING.md) for setup, formatting,
linting, tests, dependency checks, and pull requests.

Run the commands in the [test-command taxonomy](test-commands.md) before you
open a pull request.

Useful source and contract references:

- [CLI reference](reference/cli.md)
- [Configuration reference](reference/configuration.md)
- [Language features](language-features.md)
- [Connector guarantees](connectors.md)
- [Error codes](reference/errors.md)
- [Dependency policy](../DEPENDENCY_POLICY.md)

Keep generated reference changes tied to their manifest or contributor source.

## Surface guides

| Change | Canonical source | Check |
|---|---|---|
| SQL/functions | `crates/rockstream-docgen/src/contributors/function.rs` and `contracts/sql-type-matrix.toml` | `make documentation` |
| Operators | `crates/rockstream-plan/src/lib.rs` | `make test` |
| Errors | `crates/rockstream-types/src/error_code.rs` | `make error-codes` |
| Catalogs | `crates/rockstream-docgen/src/contributors/catalog.rs` | `make documentation` |
| Configuration | `crates/rockstream-docgen/src/contributors/config.rs` | `make documentation` |
| Scenarios | No scenario DSL exists before v0.59.17. | Do not add a public scenario surface yet. |
