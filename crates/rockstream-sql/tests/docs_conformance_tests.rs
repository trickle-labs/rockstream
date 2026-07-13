//! v0.45.5 Slice 3 — `docs/language-features.md` conformance lock.
//!
//! Extracts every backtick-quoted keyword/clause listed under "Implemented
//! Today" in `docs/language-features.md` and asserts each one is recognized
//! by RockStream's real SQL surface: `rockstream-sql`'s parser/lowering code
//! (`crates/rockstream-sql/src`) and, for the RockStream-specific DDL/SHOW
//! statements that are dispatched as literal-prefix matches rather than
//! through DataFusion's SQL parser (e.g. `CREATE WORKLOAD`, `SHOW RESOURCE
//! USAGE`), `rockstream-gateway`'s query dispatch
//! (`crates/rockstream-gateway/src`) — together, these two source trees are
//! RockStream's entire SQL frontend. This does not hand-copy a parallel
//! keyword list in the test itself; it greps the parser's own source, so it
//! fails if a future PR adds an "Implemented Today" keyword that isn't
//! actually recognized anywhere in the real implementation.

use std::path::Path;

/// Returns the body of the "## Implemented Today" section (everything up to
/// the next `## ` heading).
fn implemented_today_section(doc: &str) -> &str {
    let start_marker = "## Implemented Today";
    let start = doc
        .find(start_marker)
        .expect("docs/language-features.md has no `## Implemented Today` heading")
        + start_marker.len();
    let rest = &doc[start..];
    let end = rest.find("\n## ").unwrap_or(rest.len());
    &rest[..end]
}

/// Extracts every backtick-quoted span (`` `...` ``) from `section`.
fn extract_backtick_spans(section: &str) -> Vec<String> {
    let mut spans = Vec::new();
    let mut chars = section.char_indices().peekable();
    while let Some((start, c)) = chars.next() {
        if c != '`' {
            continue;
        }
        // Find the matching closing backtick.
        let rest = &section[start + 1..];
        if let Some(len) = rest.find('`') {
            spans.push(rest[..len].to_string());
            // Skip past the consumed span in the outer iterator.
            let skip_to = start + 1 + len;
            while let Some(&(idx, _)) = chars.peek() {
                if idx <= skip_to {
                    chars.next();
                } else {
                    break;
                }
            }
        }
    }
    spans
}

/// A span is "checkable" as a SQL keyword/clause reference (rather than a
/// CRDT type name, function/file/identifier reference, or other lower-case
/// prose) if it contains no ASCII-lowercase letters and at least one
/// ASCII-uppercase letter.
fn is_checkable(span: &str) -> bool {
    !span.chars().any(|c| c.is_ascii_lowercase()) && span.chars().any(|c| c.is_ascii_uppercase())
}

/// Splits a checkable span into individual alphanumeric/underscore tokens,
/// dropping punctuation (`()*,.'`) and purely-numeric tokens (e.g. error-code
/// suffixes), and keeping only tokens of length >= 2.
fn tokenize(span: &str) -> Vec<String> {
    span.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|t| t.len() >= 2 && !t.chars().all(|c| c.is_ascii_digit()))
        .map(|t| t.to_string())
        .collect()
}

/// Whole-word, case-insensitive search for `needle` in `haystack`. Word
/// boundaries treat `_` as a separator (not a continuing character) so that
/// a bare doc keyword like `TUMBLE` matches Rust identifiers like
/// `tumble_node`/`TumbleWindow`, matching how RockStream source tends to
/// compose SQL-keyword-derived identifiers. Also falls back to an
/// underscore-insensitive substring match (e.g. the SQL keyword `TRY_CAST`
/// matching the Rust identifier `TryCast`), since RockStream's
/// SQL-keyword-to-Rust-identifier naming isn't 1:1.
fn contains_word(haystack: &str, needle: &str) -> bool {
    let haystack_lower = haystack.to_ascii_lowercase();
    let needle_lower = needle.to_ascii_lowercase();
    let mut start = 0;
    while let Some(pos) = haystack_lower[start..].find(&needle_lower) {
        let abs = start + pos;
        let before_ok = haystack_lower[..abs]
            .chars()
            .next_back()
            .map(|c| !c.is_ascii_alphanumeric())
            .unwrap_or(true);
        let after_idx = abs + needle_lower.len();
        let after_ok = haystack_lower[after_idx..]
            .chars()
            .next()
            .map(|c| !c.is_ascii_alphanumeric())
            .unwrap_or(true);
        if before_ok && after_ok {
            return true;
        }
        start = abs + 1;
    }
    if needle_lower.contains('_') {
        let haystack_no_us: String = haystack_lower.chars().filter(|c| *c != '_').collect();
        let needle_no_us: String = needle_lower.chars().filter(|c| *c != '_').collect();
        if haystack_no_us.contains(&needle_no_us) {
            return true;
        }
    }
    false
}

/// Recursively concatenates every `.rs` file under `dir` into one string.
fn concat_rust_sources(dir: &Path) -> String {
    let mut combined = String::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&current) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().map(|e| e == "rs").unwrap_or(false) {
                if let Ok(src) = std::fs::read_to_string(&path) {
                    combined.push_str(&src);
                    combined.push('\n');
                }
            }
        }
    }
    combined
}

#[test]
fn test_language_features_doc_keywords_are_parseable() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir.parent().unwrap().parent().unwrap();
    let doc_path = repo_root.join("docs/language-features.md");

    assert!(
        doc_path.exists(),
        "docs/language-features.md not found at {:?}",
        doc_path
    );

    let content =
        std::fs::read_to_string(&doc_path).expect("failed to read docs/language-features.md");
    let section = implemented_today_section(&content);

    let spans = extract_backtick_spans(section);
    assert!(
        !spans.is_empty(),
        "no backtick-quoted spans found under `## Implemented Today` in docs/language-features.md"
    );

    let checkable_spans: Vec<&String> = spans.iter().filter(|s| is_checkable(s)).collect();
    assert!(
        !checkable_spans.is_empty(),
        "no checkable (all-caps, no lowercase) backtick spans found under `## Implemented Today`"
    );

    // RockStream's entire SQL frontend: rockstream-sql's parser/lowering
    // code plus rockstream-gateway's query dispatch (the latter is where
    // RockStream-specific DDL/SHOW statements like `CREATE WORKLOAD` and
    // `SHOW RESOURCE USAGE` are recognized as literal-prefix matches rather
    // than through DataFusion's generic SQL parser).
    let sql_src = concat_rust_sources(&repo_root.join("crates/rockstream-sql/src"));
    let gateway_src = concat_rust_sources(&repo_root.join("crates/rockstream-gateway/src"));
    let combined_src = format!("{sql_src}\n{gateway_src}");

    let mut unrecognized = Vec::new();
    for span in &checkable_spans {
        let tokens = tokenize(span);
        for token in tokens {
            if !contains_word(&combined_src, &token) {
                unrecognized.push(format!(
                    "`{span}` (token `{token}`) not found anywhere in rockstream-sql's or \
                     rockstream-gateway's source"
                ));
            }
        }
    }

    assert!(
        unrecognized.is_empty(),
        "docs/language-features.md's \"Implemented Today\" section claims keywords/clauses not \
         recognized anywhere in rockstream-sql's or rockstream-gateway's source:\n{}",
        unrecognized.join("\n")
    );
}
