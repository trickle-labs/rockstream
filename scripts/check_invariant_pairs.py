#!/usr/bin/env python3
"""check_invariant_pairs.py — implementation for check-invariant-pairs.sh.

Parses every `always assertion NAME:` / `always eventually assertion NAME:` /
`exists assertion NAME:` line out of `formal/*.fizz`, extracts the leading
`M<n>_(S|L)<n>` / `COV_M<n>` token(s) from `NAME` (a name may encode more
than one ID), converts underscores to hyphens, and checks `crates/` for
coverage of that exact hyphenated ID.

An ID is considered covered if either:
  (a) a real `assert!`/`debug_assert!` call site references it — the ID
      text appears within a few lines of an `assert!(`/`debug_assert!(`
      invocation (matching this repo's convention of embedding the ID in
      the assertion's panic message or an immediately adjacent comment), or
  (b) an `// INVARIANT-BY-CONSTRUCTION: <ID> — <reason>` comment references
      it, documenting why no separate runtime assertion is needed.

Exit 0 with a summary if every ID is covered; exit 1 with the list of
missing IDs otherwise.
"""

import re
import sys
from pathlib import Path

ASSERTION_RE = re.compile(
    r"^(?:always(?: eventually)?|exists) assertion ([A-Za-z0-9_]+):", re.MULTILINE
)
ID_RE = re.compile(r"(COV_M\d+|M\d+_(?:S|L)\d+)")
ASSERT_MACRO_RE = re.compile(r"\b(?:debug_)?assert!\s*\(")
INVARIANT_BY_CONSTRUCTION_RE = re.compile(r"INVARIANT-BY-CONSTRUCTION:")

# How many lines around a raw ID-text match to search for an assert! call
# site (case (a)). Wide enough to cover this repo's multi-line panic
# messages and their preceding doc comments.
PROXIMITY_WINDOW = 8


def extract_ids(formal_dir: Path) -> set[str]:
    ids: set[str] = set()
    for fizz_file in sorted(formal_dir.glob("*.fizz")):
        text = fizz_file.read_text()
        for match in ASSERTION_RE.finditer(text):
            name = match.group(1)
            for id_match in ID_RE.finditer(name):
                ids.add(id_match.group(1).replace("_", "-"))
    return ids


def load_rust_files(crates_dir: Path) -> list[tuple[Path, list[str]]]:
    files = []
    for rust_file in sorted(crates_dir.rglob("*.rs")):
        if "/target/" in str(rust_file):
            continue
        try:
            lines = rust_file.read_text(errors="replace").splitlines()
        except OSError:
            continue
        files.append((rust_file, lines))
    return files


def is_covered(invariant_id: str, rust_files: list[tuple[Path, list[str]]]) -> bool:
    for _, lines in rust_files:
        mentions = [i for i, line in enumerate(lines) if invariant_id in line]
        if not mentions:
            continue
        # Case (b): INVARIANT-BY-CONSTRUCTION comment referencing this ID.
        if any(INVARIANT_BY_CONSTRUCTION_RE.search(lines[i]) for i in mentions):
            return True
        # Case (a): a real assert!/debug_assert! site referencing it — this
        # repo's convention is to embed the ID directly in the assert!'s
        # panic message, or in a doc comment on the line(s) immediately
        # preceding the assert! call it documents.
        for i in mentions:
            lo = max(0, i - PROXIMITY_WINDOW)
            hi = min(len(lines), i + PROXIMITY_WINDOW + 1)
            if any(ASSERT_MACRO_RE.search(w) for w in lines[lo:hi]):
                return True
    return False


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()
    formal_dir = root / "formal"
    crates_dir = root / "crates"

    ids = extract_ids(formal_dir)
    rust_files = load_rust_files(crates_dir)

    missing = sorted(i for i in ids if not is_covered(i, rust_files))

    if missing:
        print("Missing FizzBee invariant coverage in crates/ for:")
        for invariant_id in missing:
            print(f"  {invariant_id}")
        print("")
        print(
            "Each ID needs either a real assert!/debug_assert! site referencing "
            "it, or an `// INVARIANT-BY-CONSTRUCTION: <ID> — <reason>` comment."
        )
        return 1

    print(f"OK: all {len(ids)} FizzBee invariants have paired Rust coverage.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
