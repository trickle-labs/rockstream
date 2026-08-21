use anyhow::{Context, Result};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

pub fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub fn sha256_file(path: &Path) -> Result<String> {
    Ok(sha256(
        &fs::read(path).with_context(|| format!("read {}", path.display()))?,
    ))
}

pub fn canonical_json<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).context("serialize canonical JSON")
}

pub fn canonical_rows(mut rows: Vec<Vec<String>>) -> Result<(Vec<Vec<String>>, String)> {
    rows.sort();
    let digest = sha256(&canonical_json(&rows)?);
    Ok((rows, digest))
}

pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("{} has no parent", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let temporary = temporary_path(path)?;
    let result = (|| {
        fs::write(&temporary, bytes).with_context(|| format!("write {}", temporary.display()))?;
        fs::rename(&temporary, path).with_context(|| format!("replace {}", path.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value).context("serialize JSON artifact")?;
    bytes.push(b'\n');
    atomic_write(path, &bytes)
}

pub fn append_jsonl<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let mut bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error).with_context(|| format!("read {}", path.display())),
    };
    serde_json::to_writer(&mut bytes, value).context("serialize JSONL record")?;
    bytes.push(b'\n');
    atomic_write(path, &bytes)
}

fn temporary_path(path: &Path) -> Result<PathBuf> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .with_context(|| format!("invalid artifact path {}", path.display()))?;
    Ok(path.with_file_name(format!(".{name}.{}.tmp", std::process::id())))
}
