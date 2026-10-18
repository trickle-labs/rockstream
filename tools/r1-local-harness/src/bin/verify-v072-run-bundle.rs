//! Cross-file v0.72 run bundle integrity gate (#138, #141).
//! Checking this manifest does not prove independent oracle provenance, frozen
//! workload execution, real measurements, topology, or capacity qualification.

use anyhow::{bail, Context, Result};
use clap::Parser;
use r1_local_harness::artifact::sha256_file;
use r1_local_harness::oracle::compare_multisets;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

const FROZEN_PROFILE_SHA256: &str =
    "d5cf97b3d0a181cddd78dbf811e22224c4767bd1948143b45beb38390464719a";

#[derive(Parser)]
struct Args {
    #[arg(long)]
    manifest: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunManifest {
    schema_version: u32,
    run_id: String,
    profile_path: String,
    executable_path: String,
    executable_sha256: String,
    capture_path: String,
    capture_sha256: String,
    expected_rows_path: String,
    expected_rows_sha256: String,
    observed_rows_path: String,
    observed_rows_sha256: String,
}

#[derive(Debug, Deserialize)]
struct Capture {
    schema_version: u32,
    qualification_status: String,
    executable_sha256: String,
    pid: u32,
    samples: Vec<Sample>,
}

#[derive(Debug, Deserialize)]
struct Sample {
    monotonic_elapsed_ns: u64,
    user_cpu_ns: u64,
    system_cpu_ns: u64,
    rss_bytes: u64,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct Decision {
    schema_version: u32,
    run_id: String,
    status: &'static str,
    observed_pid: u32,
    sample_count: usize,
    oracle_row_count: usize,
}

fn trusted_file(root: &Path, name: &str) -> Result<PathBuf> {
    let relative = Path::new(name);
    if relative.components().next().is_none()
        || !relative
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        bail!("unsafe or empty run artifact path: {name}");
    }
    let resolved = root
        .join(relative)
        .canonicalize()
        .with_context(|| format!("resolve artifact {name}"))?;
    if !resolved.starts_with(root) || !resolved.is_file() {
        bail!("artifact escapes bundle root or is not a file: {name}");
    }
    Ok(resolved)
}

fn verify_file(root: &Path, name: &str, digest: &str) -> Result<PathBuf> {
    let path = trusted_file(root, name)?;
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("invalid expected SHA-256 for {name}");
    }
    if sha256_file(&path)? != digest {
        bail!("SHA-256 mismatch for {name}");
    }
    Ok(path)
}

fn verify_bundle(manifest: &Path) -> Result<Decision> {
    let manifest = manifest.canonicalize().context("resolve bundle manifest")?;
    let root = manifest.parent().context("manifest has no directory")?;
    let data: RunManifest =
        serde_json::from_slice(&fs::read(&manifest)?).context("parse bundle manifest")?;
    if data.schema_version != 1 || data.run_id.trim().is_empty() || data.run_id.len() > 128 {
        bail!("invalid run manifest version or run identifier");
    }

    let profile = trusted_file(root, &data.profile_path)?;
    if sha256_file(&profile)? != FROZEN_PROFILE_SHA256 {
        bail!("frozen v0.72 profile hash mismatch");
    }
    let executable = verify_file(root, &data.executable_path, &data.executable_sha256)?;
    let capture = verify_file(root, &data.capture_path, &data.capture_sha256)?;
    let expected = verify_file(root, &data.expected_rows_path, &data.expected_rows_sha256)?;
    let observed = verify_file(root, &data.observed_rows_path, &data.observed_rows_sha256)?;
    let artifacts = [&profile, &executable, &capture, &expected, &observed];
    if artifacts.iter().collect::<BTreeSet<_>>().len() != artifacts.len() {
        bail!("bundle paths alias the same artifact");
    }

    let record: Capture =
        serde_json::from_slice(&fs::read(&capture)?).context("parse process capture")?;
    if record.schema_version != 1
        || record.qualification_status != "observation_only_not_capacity_qualified"
        || record.executable_sha256 != data.executable_sha256
        || record.pid == 0
        || record.samples.len() < 2
    {
        bail!("invalid or unbound process capture");
    }
    if record.samples.iter().all(|sample| sample.rss_bytes == 0)
        || record.samples.windows(2).any(|w| {
            w[0].monotonic_elapsed_ns >= w[1].monotonic_elapsed_ns
                || w[0].user_cpu_ns > w[1].user_cpu_ns
                || w[0].system_cpu_ns > w[1].system_cpu_ns
        })
    {
        bail!("invalid process-counter time series");
    }

    let expected_rows: Vec<Vec<String>> = serde_json::from_slice(&fs::read(&expected)?)
        .context("parse complete independent-oracle rows")?;
    let observed_rows: Vec<Vec<String>> =
        serde_json::from_slice(&fs::read(&observed)?).context("parse complete observed rows")?;
    if expected_rows.is_empty() {
        bail!("empty oracle cannot qualify a v0.72 workload");
    }
    compare_multisets(&observed_rows, &expected_rows)
        .map_err(|error| anyhow::anyhow!("result oracle mismatch: {error}"))?;

    Ok(Decision {
        schema_version: 1,
        run_id: data.run_id,
        status: "cross_file_structural_only_not_capacity_qualified",
        observed_pid: record.pid,
        sample_count: record.samples.len(),
        oracle_row_count: expected_rows.len(),
    })
}

fn main() {
    if let Err(error) = run() {
        eprintln!("VIOLATION: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let decision = verify_bundle(&Args::parse().manifest)?;
    println!("{}", serde_json::to_string(&decision)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use r1_local_harness::artifact::sha256_file;
    use serde_json::{json, Value};

    const PROFILE: &[u8] = include_bytes!("../../../../benchmarks/r1-local/profile-v072.toml");

    fn fixture() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        fs::write(root.join("profile.toml"), PROFILE).unwrap();
        fs::write(root.join("rockstream"), b"mock-release-executable").unwrap();
        let binary_sha = sha256_file(&root.join("rockstream")).unwrap();
        fs::write(root.join("capture.json"), serde_json::to_vec(&json!({
            "schema_version": 1,
            "qualification_status": "observation_only_not_capacity_qualified",
            "executable_sha256": binary_sha,
            "pid": 3121,
            "samples": [
                {"monotonic_elapsed_ns": 1, "user_cpu_ns": 3, "system_cpu_ns": 2, "rss_bytes": 1024},
                {"monotonic_elapsed_ns": 6, "user_cpu_ns": 4, "system_cpu_ns": 2, "rss_bytes": 2048}
            ]
        })).unwrap()).unwrap();
        fs::write(root.join("expected.json"), br#"[["1"],["1"],["2"]]"#).unwrap();
        fs::write(root.join("observed.json"), br#"[["2"],["1"],["1"]]"#).unwrap();
        let manifest = root.join("manifest.json");
        fs::write(
            &manifest,
            serde_json::to_vec(&json!({
                "schema_version": 1,
                "run_id": "example-local-candidate",
                "profile_path": "profile.toml",
                "executable_path": "rockstream",
                "executable_sha256": binary_sha,
                "capture_path": "capture.json",
                "capture_sha256": sha256_file(&root.join("capture.json")).unwrap(),
                "expected_rows_path": "expected.json",
                "expected_rows_sha256": sha256_file(&root.join("expected.json")).unwrap(),
                "observed_rows_path": "observed.json",
                "observed_rows_sha256": sha256_file(&root.join("observed.json")).unwrap()
            }))
            .unwrap(),
        )
        .unwrap();
        (dir, manifest)
    }

    fn mutate_manifest(path: &Path, f: impl FnOnce(&mut Value)) {
        let mut document: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        f(&mut document);
        fs::write(path, serde_json::to_vec(&document).unwrap()).unwrap();
    }

    #[test]
    fn exact_success_remains_explicitly_unqualified() {
        let (_dir, manifest) = fixture();
        assert_eq!(
            verify_bundle(&manifest).unwrap(),
            Decision {
                schema_version: 1,
                run_id: "example-local-candidate".to_string(),
                status: "cross_file_structural_only_not_capacity_qualified",
                observed_pid: 3121,
                sample_count: 2,
                oracle_row_count: 3
            }
        );
    }

    #[test]
    fn modified_profile_is_never_accepted() {
        let (dir, manifest) = fixture();
        fs::write(dir.path().join("profile.toml"), b"fake").unwrap();
        assert!(verify_bundle(&manifest)
            .unwrap_err()
            .to_string()
            .contains("profile hash mismatch"));
    }

    #[test]
    fn modified_capture_or_binary_is_never_accepted() {
        let (dir, manifest) = fixture();
        fs::write(dir.path().join("capture.json"), b"{}").unwrap();
        assert!(verify_bundle(&manifest)
            .unwrap_err()
            .to_string()
            .contains("SHA-256 mismatch for capture.json"));
        let (dir, manifest) = fixture();
        fs::write(dir.path().join("rockstream"), b"replaced").unwrap();
        assert!(verify_bundle(&manifest)
            .unwrap_err()
            .to_string()
            .contains("SHA-256 mismatch for rockstream"));
    }

    #[test]
    fn forged_oracle_digest_rejected_before_comparison() {
        let (_dir, manifest) = fixture();
        mutate_manifest(&manifest, |doc| {
            doc["observed_rows_sha256"] = json!("0".repeat(64))
        });
        assert!(verify_bundle(&manifest)
            .unwrap_err()
            .to_string()
            .contains("SHA-256 mismatch for observed.json"));
    }

    #[test]
    fn duplicate_rows_must_match_independently_computed_multisets() {
        let (dir, manifest) = fixture();
        fs::write(dir.path().join("observed.json"), br#"[["1"],["2"],["2"]]"#).unwrap();
        mutate_manifest(&manifest, |doc| {
            doc["observed_rows_sha256"] =
                json!(sha256_file(&dir.path().join("observed.json")).unwrap());
        });
        assert!(verify_bundle(&manifest)
            .unwrap_err()
            .to_string()
            .contains("result oracle mismatch"));
    }

    #[test]
    fn traversal_and_aliases_cannot_leave_bundle_root() {
        let (_dir, manifest) = fixture();
        mutate_manifest(&manifest, |doc| {
            doc["capture_path"] = json!("../outside.json")
        });
        assert!(verify_bundle(&manifest)
            .unwrap_err()
            .to_string()
            .contains("unsafe"));
        let (_dir, manifest) = fixture();
        mutate_manifest(&manifest, |doc| {
            doc["observed_rows_path"] = doc["expected_rows_path"].clone();
            doc["observed_rows_sha256"] = doc["expected_rows_sha256"].clone();
        });
        assert!(verify_bundle(&manifest)
            .unwrap_err()
            .to_string()
            .contains("alias"));
    }

    #[test]
    fn incomplete_or_nonmonotonic_cpu_capture_cannot_pass() {
        let (dir, manifest) = fixture();
        let capture = dir.path().join("capture.json");
        let mut document: Value = serde_json::from_slice(&fs::read(&capture).unwrap()).unwrap();
        document["samples"][1]["system_cpu_ns"] = json!(1);
        fs::write(&capture, serde_json::to_vec(&document).unwrap()).unwrap();
        mutate_manifest(&manifest, |doc| {
            doc["capture_sha256"] = json!(sha256_file(&capture).unwrap())
        });
        assert!(verify_bundle(&manifest)
            .unwrap_err()
            .to_string()
            .contains("process-counter"));
    }

    #[test]
    fn empty_complete_oracle_is_not_a_qualified_run() {
        let (dir, manifest) = fixture();
        fs::write(dir.path().join("expected.json"), b"[]").unwrap();
        mutate_manifest(&manifest, |doc| {
            doc["expected_rows_sha256"] =
                json!(sha256_file(&dir.path().join("expected.json")).unwrap())
        });
        assert!(verify_bundle(&manifest)
            .unwrap_err()
            .to_string()
            .contains("empty oracle"));
    }

    #[cfg(unix)]
    #[test]
    fn symlink_escape_is_rejected() {
        use std::os::unix::fs::symlink;
        let (dir, manifest) = fixture();
        let outside = tempfile::NamedTempFile::new().unwrap();
        symlink(outside.path(), dir.path().join("escape")).unwrap();
        mutate_manifest(&manifest, |doc| doc["capture_path"] = json!("escape"));
        assert!(verify_bundle(&manifest)
            .unwrap_err()
            .to_string()
            .contains("escapes bundle root"));
    }
}
