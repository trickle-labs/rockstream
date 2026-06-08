# Agent Configuration

You are an expert developer. When modifying code, NEVER output the entire file. You MUST ONLY output the exact lines to be replaced using standard unified diff format (or a specific search/replace block). Be extremely concise.

## Rules

- Never use HEREDOC.
- After every git commit make sure that the commit messages isn't garbled.
- After creating or updating a pull request title or body make sure that they aren't garbled.

Always use rtk wrapper for these high-verbosity commands:

- `rtk git log` instead of `git log`
- `rtk git status` instead of `git status`
- `rtk git diff` instead of `git diff`
- `rtk find "*.md" .` instead of `find . -name "*.md"`
- `rtk read <file>` instead of `cat <file>` (for large files >10K lines)
- `rtk ls .` instead of `ls -la`
- `rtk grep "pattern"` instead of `grep -r "pattern"`
- `rtk cargo test` instead of `cargo test`
- `rtk cargo build` instead of `cargo build`
- `rtk cargo clippy` instead of `cargo clippy`
- `rtk gh pr view <num>` instead of `gh pr view <num>`
- `rtk gh pr checks <num>` instead of `gh pr checks <num>`

## Code Exploration Protocol

When exploring a codebase or understanding a module:

1. **Structure first** — run the appropriate command for the language:

   Rust: `rg "^\s*(pub\s+)?(async\s+)?fn |^\s*(pub\s+)?(struct|enum|trait|impl)\s" src/ --no-heading -n`

   Use `^\s*` not `^` — Rust methods inside impl blocks are indented. The `^` pattern misses ~70% of them.

2. Identify 2-3 relevant functions from the signatures
3. Read only those functions with line offset (not the whole file)
4. Cross-reference callers with Grep if needed

Never read a file end-to-end when exploring. Structure first, drill second.

### PR Body Generation Rules
- **No Variations:** Follow the requested Markdown schema exactly.
- **Table Constraints:** If generating test matrices, use a strict format. Do not nest complex types inside table columns.
- **No Repetition:** If you find yourself repeating a phrase or token pattern, immediately truncate the section and move to the next header.
- **Code Block Integrity:** Never break out of inline code blocks (` `) or structural lines without closing them.
- **Confirm:** Make sure that PR body is not garbled. If so fix it. Then confirm one more time.

