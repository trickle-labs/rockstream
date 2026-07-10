//! In-memory role catalog (pg_authid-style) for SCRAM/MD5 authentication.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::auth::{compute_md5_stored_hash, gen_scram_verifiers_raw};

/// Maximum roles in the catalog.
/// Fill-level metric: `catalog.len()`.
/// Backpressure: `CREATE ROLE` returns RS-2402 when at capacity.
pub const MAX_ROLES: usize = 10_000;

/// A single role entry in the catalog.
#[derive(Debug, Clone)]
pub struct RoleEntry {
    pub username: String,
    /// PBKDF2-HMAC-SHA256(password, salt, iterations) — used to derive StoredKey/ServerKey.
    pub scram_salted_password: Vec<u8>,
    pub scram_salt: Vec<u8>,
    pub scram_iterations: u32,
    /// `"md5" + hex(md5(password + username))` — for MD5 wire authentication.
    pub md5_hash: Option<String>,
}

/// Thread-safe in-memory role catalog bounded by `MAX_ROLES`.
#[derive(Debug, Default)]
pub struct RoleCatalog {
    inner: Arc<RwLock<HashMap<String, RoleEntry>>>,
}

impl RoleCatalog {
    pub fn new() -> Self {
        RoleCatalog {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Current number of roles. Fill-level metric.
    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert a new role. Fails if the catalog is at capacity and the role is new.
    pub fn insert(&self, entry: RoleEntry) -> Result<(), String> {
        let mut map = self.inner.write().unwrap();
        if map.len() >= MAX_ROLES && !map.contains_key(&entry.username) {
            return Err(format!(
                "[RS-2402] auth.role_limit_reached: role catalog is full ({MAX_ROLES} roles). next_steps: Drop unused roles before creating new ones."
            ));
        }
        map.insert(entry.username.clone(), entry);
        Ok(())
    }

    /// Update the password for an existing role. Returns false if not found.
    pub fn update_password(&self, username: &str, password: &str) -> bool {
        let mut map = self.inner.write().unwrap();
        if let Some(entry) = map.get_mut(username) {
            let salt = entry.scram_salt.clone();
            let iters = entry.scram_iterations;
            let (salted_password, _, _) = gen_scram_verifiers_raw(password, &salt, iters);
            entry.scram_salted_password = salted_password.to_vec();
            entry.md5_hash = Some(compute_md5_stored_hash(username, password));
            true
        } else {
            false
        }
    }

    /// Remove a role. Returns false if not found.
    pub fn remove(&self, username: &str) -> bool {
        self.inner.write().unwrap().remove(username).is_some()
    }

    /// Get a role entry by username.
    pub fn get(&self, username: &str) -> Option<RoleEntry> {
        self.inner.read().unwrap().get(username).cloned()
    }
}

/// Generate all three SCRAM components from a plaintext password, salt, and iterations.
/// Returns `(scram_salted_password, stored_key, server_key)`.
pub fn gen_scram_verifiers(
    password: &str,
    salt: &[u8],
    iterations: u32,
) -> ([u8; 32], [u8; 32], [u8; 32]) {
    gen_scram_verifiers_raw(password, salt, iterations)
}

/// Build a `RoleEntry` from a username and plaintext password.
/// Generates a fresh random 16-byte salt.
pub fn create_role_entry(username: &str, password: &str) -> RoleEntry {
    use rand::RngCore;
    let mut salt = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut salt);
    let iterations = 4096u32;
    let (salted_password, _, _) = gen_scram_verifiers_raw(password, &salt, iterations);
    RoleEntry {
        username: username.to_string(),
        scram_salted_password: salted_password.to_vec(),
        scram_salt: salt.to_vec(),
        scram_iterations: iterations,
        md5_hash: Some(compute_md5_stored_hash(username, password)),
    }
}
