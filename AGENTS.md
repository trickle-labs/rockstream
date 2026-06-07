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
