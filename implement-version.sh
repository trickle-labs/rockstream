#!/bin/zsh
set -Eeuo pipefail

if [[ -z "${ROCKSTREAM_VERSION:-}" ]]; then
    echo "Error: ROCKSTREAM_VERSION environment variable is required." >&2
    exit 1
fi

copilot --allow-all --model claude-sonnet-5 --effort medium -p "Run .github/prompts/implement-version-orient.prompt.md for v${ROCKSTREAM_VERSION}."
copilot --allow-all --model claude-sonnet-5 --effort high -p "Run .github/prompts/implement-version-plan.prompt.md for v${ROCKSTREAM_VERSION}."
copilot --allow-all --model claude-sonnet-5 --effort medium -p "Run .github/prompts/implement-version-implement-3a.prompt.md for v${ROCKSTREAM_VERSION}."
copilot --allow-all --model claude-sonnet-5 --effort medium -p "Run .github/prompts/implement-version-implement-3b.prompt.md for v${ROCKSTREAM_VERSION}."
copilot --allow-all --model claude-haiku-4.5 -p "Run .github/prompts/implement-version-prove.prompt.md for v${ROCKSTREAM_VERSION}."
copilot --allow-all --model claude-sonnet-5 --effort medium -p "Run .github/prompts/implement-version-signoff.prompt.md for v${ROCKSTREAM_VERSION}. Commit and push."
