# Domain Docs

## Before exploring, read these

- `CONTEXT.md` at the repo root, or `CONTEXT-MAP.md` if it exists.
- Relevant ADRs in `docs/adr/`.

If these files do not exist, proceed silently. The domain-modeling skill creates them when needed.

## File structure

This is a single-context repo:

```text
/
├── CONTEXT.md
├── docs/adr/
└── src/
```

## Use the glossary’s vocabulary

When naming domain concepts, use terms defined in `CONTEXT.md`. If a needed concept is missing, note it for domain modeling.

## Flag ADR conflicts

If output contradicts an existing ADR, surface the conflict explicitly.
