---
name: implement-version
description: 'Run the full rockstream "implement a roadmap version" pipeline end-to-end — Orient, Plan, Implement, Prove, Sign Off — by delegating each phase to a scoped subagent. Use when the user asks to "implement version X", "run the implement-version workflow", "do orient/plan/implement/prove/signoff for vX.Y", or wants all seven implement-version-*.prompt.md phases run in order automatically instead of manually running each prompt and typing /compact between them.'
argument-hint: 'version (e.g. 0.46)'
---

# Implement Version — Full Pipeline

Orchestrates the 7 phases already defined in `.github/prompts/implement-version-*.prompt.md`
for one roadmap version, running them **in order**, each in its own subagent so
that one phase's exploration/build/test noise never accumulates in the main
conversation. This replaces the manual `/compact` + "now run the next prompt"
steps baked into each of those prompt files — the subagent boundary *is* the
compaction point, and only a short status report crosses back into this
conversation.

## Phase → subagent map

| # | Phase | Prompt file | Subagent | Tools | Why this subagent |
|---|-------|-------------|----------|-------|--------------------|
| 0–1 | Orient | [implement-version-orient.prompt.md](../../prompts/implement-version-orient.prompt.md) | `Explore` (built-in) | read-only | Pure validation/read-back of the roadmap row + prior sign-off; no file writes, so the cheapest read-only agent is sufficient. |
| 2 | Plan | [implement-version-plan.prompt.md](../../prompts/implement-version-plan.prompt.md) | [`version-plan-writer`](../../agents/version-plan-writer.agent.md) | read, search, edit | Needs to write `.claude/<version>-plan.md` but must never touch source or run builds — narrower tool set = less risk, less cost. |
| 3 / 3a+3b | Implement | [implement-version-implement.prompt.md](../../prompts/implement-version-implement.prompt.md) or the [3a](../../prompts/implement-version-implement-3a.prompt.md)/[3b](../../prompts/implement-version-implement-3b.prompt.md) pair | [`version-builder`](../../agents/version-builder.agent.md) | read, search, edit, execute | Test-first coding needs full tool access. |
| 4 | Prove | [implement-version-prove.prompt.md](../../prompts/implement-version-prove.prompt.md) | [`version-builder`](../../agents/version-builder.agent.md) | read, search, edit, execute | Runs oracle/LFS/MinIO/SimRuntime/benchmark suites; writes evidence file. |
| 5–6 | Sign Off & Report | [implement-version-signoff.prompt.md](../../prompts/implement-version-signoff.prompt.md) | [`version-builder`](../../agents/version-builder.agent.md) | read, search, edit, execute | Runs `make approve`/exit-criteria scripts, edits `sign-offs/`, updates roadmap status. |

`implement-version-implement-3a`/`-3b` and `implement-version-implement` are
**alternatives**, not both-in-sequence — see the split decision in step 3 below.

## Procedure

1. **Resolve the version.** If `${input:version}` wasn't given, ask for it
   before doing anything else.

2. **Phase 0–1 Orient** — invoke a subagent (`agentName: "Explore"`, thoroughness
   `medium`) with a prompt telling it to: read
   `.github/prompts/implement-version-orient.prompt.md` and carry out Phases 0
   and 1 for `version=<X.Y>` exactly as written there (check the roadmap row
   exists, check the previous version's sign-off is fully checked off, restate
   the Proof obligations as a checkable list, and audit prior sign-offs for
   overlapping deferred items). Ask it to return only: pass/blocked, the
   restated proof-obligation list, and any overlapping deferred items found.
   - If it reports **blocked** (prior version incomplete, or version missing
     from the roadmap): stop immediately and report that to the user. Do not
     continue.

3. **Phase 2 Plan** — invoke `agentName: "version-plan-writer"` with a prompt
   telling it to follow `.github/prompts/implement-version-plan.prompt.md` for
   `version=<X.Y>`, using the proof obligations and deferred items from step 2
   as inputs, and to write `.claude/<version>-plan.md`.
   - Read its returned slice count/list. **Decide the implement split:**
     - If the plan has **more than ~5–6 vertical slices**, or explicitly
       contains multiple durability (LFS/MinIO) or coordination (SimRuntime)
       slices that are independent of each other, split Phase 3 into 3a + 3b
       (roughly first half / second half of the slice list).
     - Otherwise, run the single `implement-version-implement.prompt.md`
       pass — avoids paying for a second subagent round-trip when one pass
       comfortably covers the whole plan.

4. **Phase 3 Implement** — invoke `agentName: "version-builder"`:
   - Split path: first with prompt file `implement-version-implement-3a.prompt.md`
     for `version=<X.Y>`, wait for its report, then again with
     `implement-version-implement-3b.prompt.md` for the same version (it
     re-derives remaining work from `.claude/<version>-plan.md` + `git status`
     itself — don't restate the whole plan in the prompt, just point at the
     file).
   - Single path: one invocation with `implement-version-implement.prompt.md`.
   - If a report comes back **blocked** (root-caused failure it couldn't
     resolve): stop and report to the user with the exact blocker. Do not
     silently skip to Prove.

5. **Phase 4 Prove** — invoke `agentName: "version-builder"` with prompt file
   `implement-version-prove.prompt.md` for `version=<X.Y>`, pointing it at
   `.claude/<version>-plan.md` for the proof-claim → test mapping. It writes
   `.claude/<version>-evidence.md`.
   - If any Proof claim can't be satisfied: stop and report the gap. Do not
     proceed to sign-off with unproven claims.

6. **Phase 5–6 Sign Off & Report** — invoke `agentName: "version-builder"` with
   prompt file `implement-version-signoff.prompt.md` for `version=<X.Y>`,
   pointing it at `.claude/<version>-evidence.md`. It creates/edits
   `sign-offs/v<X.Y>.md`, flips the roadmap status, and runs
   `scripts/check-exit-criteria.sh` + `make e2e`.

7. **Relay to the user.** After each phase, post only the subagent's short
   status line(s) to the user (not its internal transcript). After Phase 6,
   present its final Definition-of-Done table/report as the overall result.

## Subagent prompt template

For every invocation, keep the prompt itself short — point at files rather
than pasting their contents (the subagent has its own read tools):

```
Read and follow .github/prompts/<prompt-file> for version <X.Y>.
<Any phase-specific pointer, e.g.: "Plan is at .claude/<X.Y>-plan.md.">
Return exactly one final message: pass/blocked status, the required summary
fields from that prompt's Exit section, and nothing else (no intermediate
reasoning, no full test logs).
```

## Token-cost optimizations this pipeline relies on

- **Context isolation per phase** — each subagent call starts fresh and only
  its final summary re-enters this conversation, so 7 phases' worth of
  exploration/build/test output never piles up in one context window.
- **Tool-scoped subagents** — `Explore` and `version-plan-writer` physically
  cannot run `cargo`/`make`, which also removes any temptation to burn tokens
  re-running builds during read/plan phases.
- **Disk, not chat, is the handoff** — `.claude/<version>-plan.md` and
  `.claude/<version>-evidence.md` carry state between phases; prompts to later
  subagents reference these paths instead of re-pasting their contents.
- **Skip the 3a/3b split when the plan is small** — a single
  `implement-version-implement.prompt.md` pass avoids a second subagent
  round-trip's fixed overhead when the plan doesn't need it.
- **Summary-only test output** — every `version-builder` invocation is
  instructed to grep test output down to `ok|FAILED|result:|error` lines
  before it even finishes its own turn, so pass-heavy noise never reaches this
  conversation or the next phase's subagent.

## Stopping conditions (always halt and report, never guess past these)

- Orient reports the previous version's sign-off is incomplete, or the version
  isn't on the roadmap.
- Implement reports a test failure it couldn't root-cause within the phase.
- Prove reports a Proof claim that cannot be satisfied.
- Sign Off reports any `- [ ]` remaining in `sign-offs/v<X.Y>.md`, or
  `scripts/check-exit-criteria.sh` / `make e2e` failing.
