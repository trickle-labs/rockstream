# Agent Configuration

## Rules

- Never use HEREDOC.
- After every git commit make sure that the commit messages isn't garbled.
- After creating or updating a pull request title or body make sure that they aren't garbled.

### PR Body Generation Rules
- **No Variations:** Follow the requested Markdown schema exactly.
- **Table Constraints:** If generating test matrices, use a strict format. Do not nest complex types inside table columns.
- **No Repetition:** If you find yourself repeating a phrase or token pattern, immediately truncate the section and move to the next header.
- **Code Block Integrity:** Never break out of inline code blocks (` `) or structural lines without closing them.
- **Confirm:** Make sure that PR body is not garbled. If so fix it. Then confirm one more time.


# RTK - Rust Token Killer (Google Antigravity)

**Usage**: Token-optimized CLI proxy for shell commands.

## Rule

Always prefix shell commands with `rtk` to minimize token consumption.

Examples:

```bash
rtk git status
rtk cargo test
rtk ls src/
rtk grep "pattern" src/
rtk find "*.rs" .
rtk docker ps
rtk gh pr list
```

## Meta Commands

```bash
rtk gain              # Show token savings
rtk gain --history    # Command history with savings
rtk discover          # Find missed RTK opportunities
rtk proxy <cmd>       # Run raw (no filtering, for debugging)
```

## Why

RTK filters and compresses command output before it reaches the LLM context, saving 60-90% tokens on common operations. Always use `rtk <cmd>` instead of raw commands.
