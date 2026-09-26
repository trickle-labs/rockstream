# Captured source: GitHub issue #101

URL: https://github.com/trickle-labs/rockstream/issues/101
Retrieved: 2026-09-26T14:22:16Z
State at retrieval: OPEN
Attribution retained from the original contract: Rockstream test-suite audit dated 2026-09-21, section B1, revision `5d9c3b919682140ec0b365d7dba848cd5cf5ecbb`. The issue body below contains all five acceptance promises. The audit report itself is not in this repository tree.

## Title

Preserve catalog retry deduplication across snapshot recovery

## Issue body

### What to build

Fix the executed catalog recovery defect: retrying an already committed operation after snapshot and compaction can resurrect deleted metadata and regress the revision. Preserve enough durable operation identity for retries to remain no-ops, and add permanent regressions.

Source: Rockstream test suite audit dated 2026-09-21, section B1, revision `5d9c3b919682140ec0b365d7dba848cd5cf5ecbb`. Priority: P1.

### Acceptance criteria

- [ ] Create table T with operation 555 at revision 1, delete T with operation 556 at revision 2, snapshot, compact, and recover. Assert revision 2 and an empty complete table list before retry.
- [ ] Retry operation 555 and assert returned revision 2, current revision 2, and an empty complete table list. The audit reproduced revision 1 with T restored.
- [ ] Compare the complete stored-object inventory and bytes before and after retry; it creates or changes no log object.
- [ ] Repeat recovery and retry, and retry an old ALTER after a newer definition. Assert the complete latest metadata remains unchanged.
- [ ] Retain successful log-only replay and snapshot recovery coverage with full-record comparisons.

### Blocked by

- None (can start immediately).

## Relevant issue comments

- 2026-09-22 10:32:15Z, grove: contract v1 prepared from all five legacy criteria and linked from the issue.
- 2026-09-22 10:38:02Z, grove: readiness reconciliation complete; no unresolved contract gaps or unavailable prerequisites. No acceptance amendment.
