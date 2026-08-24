# RockStream documentation

Choose a path by what you need to do:

- [Get started](getting-started.md) with the demo or a local project.
- [Operate RockStream](operator.md), including configuration, connectors,
  recovery, and observability.
- [Contribute](contributors.md) with the repository workflow and source map.
- Run the [test-command taxonomy](test-commands.md) before opening a change.
- Apply the [schema-evolution cookbook](schema-evolution.md) when a view or
  PostgreSQL source changes.
- Read the [history](history.md) and [architecture decisions](adr/README.md).

## Reference

- [CLI](reference/cli.md)
- [Configuration](reference/configuration.md)
- [Functions](reference/functions.md)
- [SQL support](reference/sql-support.md)
- [Catalog](reference/catalog.md)
- [Metrics](reference/metrics.md)
- [Errors](reference/errors.md)

The older top-level reference pages remain as compatibility URLs.

## Terms

- A **view** is a saved query whose result RockStream maintains.
- An **epoch** is a batch of changes processed together.
- A **frontier** marks the latest point reached by an input.
- **CDC** means change data capture.
