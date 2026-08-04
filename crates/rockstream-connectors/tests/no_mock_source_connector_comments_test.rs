//! Test asserting zero matches for `mock.*source connector` in shipped connector source files.

use std::fs;
use std::path::Path;

#[test]
fn test_no_mock_source_connector_comments() {
    let src_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut mock_matches = Vec::new();

    for entry in fs::read_dir(&src_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() && path.extension().map_or(false, |ext| ext == "rs") {
            let content = fs::read_to_string(&path).unwrap();
            for (line_no, line) in content.lines().enumerate() {
                let lower = line.to_lowercase();
                if let Some(idx) = lower.find("mock") {
                    if lower[idx..].contains("source connector") {
                        mock_matches.push(format!("{}:{}: {}", path.display(), line_no + 1, line));
                    }
                }
            }
        }
    }

    assert!(
        mock_matches.is_empty(),
        "Found mock source connector references in shipped connector files:\n{}",
        mock_matches.join("\n")
    );
}
