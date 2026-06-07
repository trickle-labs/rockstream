# Agent Configuration

## Cost Prevention Principles

- **No HEREDOC:** Use direct string literals. HEREDOCs incur parsing overhead.
- **Prevent errors upfront:** Run `cargo fmt` + `cargo clippy` before commits; don't fix via re-reading.
- **Exact PR schemas:** No variations, no repetition. One-shot correctness.
- **Measure actual costs:** Use `rtk gain --history` to ground decisions in your real token patterns.

## Decision Framework: Task → Approach

**Direct tools (Bash, Read, Edit):**
- Single file edits
- Locating symbols/files in codebase (`grep`, `find`)
- Running tests and reading output
- Known-path operations

**Agents (general-purpose, Plan):**
- Multi-step research across scattered files (>3 lookups needed)
- Architectural decisions with multi-crate impact
- Decisions where context-gathering IS the bottleneck
- Complex debugging with dependent findings

**Cost reality:** Agent cost ≠ better. Depends on:
- How much context you need to gather vs. I can synthesize
- Whether the problem requires decision-making or just execution
- Your current conversation context size (grows with time)

## RTK Hook: Automated Optimization

All Bash commands automatically rewritten → `rtk <cmd>` (transparent, 0 overhead).

**What's optimized:**
- `cargo check/build`: Filtered output, parallelization
- `git status/log`: Pruned metadata
- `grep/find`: Relevant matches only

**Measure your savings:** `rtk gain --history` shows cumulative token delta across your recent commands.

## Batching Patterns (Avoid Round-Trips)

**Pattern: Iterative fix cycle (E2E tests)**
```
cargo test -p rockstream-e2e --test ddl_lifecycle_tests
  → Read error output
  → Edit implementation/test
  → cargo test (same compilation cache, faster)
  → Repeat
```
Why: Single compilation setup + multiple iterations avoids N recompiles.

**Pattern: Dependency audit**
```
cargo tree -p rockstream     (understand graph)
cargo update -p <crate>      (make change)
cargo check                  (validate)
Edit Cargo.toml if needed    (if manual edits required)
```
Why: Sequential steps let you abort early (e.g., if `check` fails, don't edit yet).

**Pattern: PR preparation**
```
git status + git diff        (parallel: see all changes)
cargo fmt + cargo clippy     (parallel: both run once)
git add <files> && git commit
```
Why: Single format/lint cycle; avoid re-parsing crate multiple times.

## File Read Strategy

**Large files:** Read in sections
- Cargo.lock: `--limit 50` (version scan)
- Tests: `--offset X --limit Y` (specific test functions)
- Implementation: Read section-by-section if >1000 lines

**Never re-read after Edit:** File state is tracked; I don't need confirmation.

**First pass:** If you're unsure of file size, use `wc -l` first.

## Rockstream Specifics

- **E2E tests:** `crates/rockstream-e2e/tests/` (isolated per-test, no shared state)
- **Workspace:** 7 crates; use `-p <crate>` to avoid rebuilding unrelated crates
- **Test data:** DDL migrations embedded in each test; isolated schemas
- **Common edits:** ddl_lifecycle_tests.rs, pgwire.rs (known paths, direct edit)

## Validation

Decisions in this file are principles, not laws. Validate with:
- `rtk gain` — shows actual token consumption for your workflows
- `rtk gain --history` — trends over time (which patterns actually save tokens?)
- Your own judgment — what works in practice for rockstream?
