# Contributing to RockStream

Start with the [contributor guide](docs/contributors.md) for the repository
workflow and documentation map.

Thank you for considering contributing to RockStream!

## Development Setup

1. Install Rust via [rustup](https://rustup.rs/).
2. Clone the repository.
3. Run `cargo build --workspace` to build all crates.
4. Run `cargo test --workspace` to run all tests.

Alternatively, use the dev container (`.devcontainer/`) which has all
dependencies pre-installed.

## Code Standards

- **Formatting**: `cargo fmt --all` before committing.
- **Linting**: `cargo clippy --workspace --all-targets -- -D warnings` must pass.
- **Tests**: `cargo test --workspace` must pass. No skipped tests.
- **Dependencies**: `cargo deny check` must pass. See `DEPENDENCY_POLICY.md`.
- **MSRV**: Code must compile on the Rust version specified in `rust-toolchain.toml`.

## Pull Request Process

1. Create a feature branch from `main`.
2. Make your changes with tests.
3. Ensure CI passes (fmt, clippy, test, deny).
4. Submit a pull request with a clear description.

## Error Codes

Every user-visible or operator-visible error must have an `RS-XXXX` error code.
See the error-code registry in `rockstream-types`.

## Commit Messages

Use conventional commit style:
- `feat:` for new features
- `fix:` for bug fixes
- `docs:` for documentation
- `ci:` for CI changes
- `refactor:` for refactoring
- `test:` for test additions

## License

By contributing, you agree that your contributions will be licensed under
the Apache License 2.0.
