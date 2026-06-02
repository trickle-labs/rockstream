//! Secrets management types and helpers for RockStream (v0.49).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

/// A stored secret representation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecretEntry {
    pub name: String,
    pub secret_type: String,
    pub encrypted_value: Vec<u8>,
    pub kek_ref: String,
}

impl SecretEntry {
    /// Helper to decrypt the secret value using a mock KEK resolver.
    pub fn decrypt(&self, kek_resolver: &MockKekResolver) -> Result<String, String> {
        kek_resolver.decrypt(&self.encrypted_value, &self.kek_ref)
    }
}

/// A mock KEK envelope encryption manager.
pub struct MockKekResolver {
    keys: HashMap<String, Vec<u8>>,
}

impl MockKekResolver {
    pub fn new() -> Self {
        let mut keys = HashMap::new();
        keys.insert("kek-v1".to_string(), b"master-key-v1-bytes-16".to_vec());
        keys.insert("kek-v2".to_string(), b"master-key-v2-bytes-16".to_vec());
        Self { keys }
    }

    pub fn encrypt(&self, plain: &str, kek_ref: &str) -> Result<Vec<u8>, String> {
        let key = self
            .keys
            .get(kek_ref)
            .ok_or_else(|| format!("KEK not found: {kek_ref}"))?;
        let mut cipher = plain.as_bytes().to_vec();
        for (i, byte) in cipher.iter_mut().enumerate() {
            *byte ^= key[i % key.len()];
        }
        Ok(cipher)
    }

    pub fn decrypt(&self, cipher: &[u8], kek_ref: &str) -> Result<String, String> {
        let key = self
            .keys
            .get(kek_ref)
            .ok_or_else(|| format!("KEK not found: {kek_ref}"))?;
        let mut plain = cipher.to_vec();
        for (i, byte) in plain.iter_mut().enumerate() {
            *byte ^= key[i % key.len()];
        }
        String::from_utf8(plain).map_err(|e| e.to_string())
    }
}

impl Default for MockKekResolver {
    fn default() -> Self {
        Self::new()
    }
}

/// A global secrets registry to simulate rotation and lookup without restart.
pub struct SecretsRegistry {
    secrets: Mutex<HashMap<String, SecretEntry>>,
    kek_resolver: MockKekResolver,
}

impl SecretsRegistry {
    pub fn new() -> Self {
        Self {
            secrets: Mutex::new(HashMap::new()),
            kek_resolver: MockKekResolver::new(),
        }
    }

    pub fn register(
        &self,
        name: &str,
        secret_type: &str,
        plain: &str,
        kek_ref: &str,
    ) -> Result<(), String> {
        let encrypted_value = self.kek_resolver.encrypt(plain, kek_ref)?;
        let entry = SecretEntry {
            name: name.to_string(),
            secret_type: secret_type.to_string(),
            encrypted_value,
            kek_ref: kek_ref.to_string(),
        };
        self.secrets.lock().unwrap().insert(name.to_string(), entry);
        Ok(())
    }

    pub fn get_decrypted(&self, name: &str) -> Result<String, String> {
        let secrets = self.secrets.lock().unwrap();
        let entry = secrets
            .get(name)
            .ok_or_else(|| format!("Secret not found: {name}"))?;
        entry.decrypt(&self.kek_resolver)
    }

    pub fn list(&self) -> Vec<(String, String, String)> {
        let secrets = self.secrets.lock().unwrap();
        let mut list: Vec<_> = secrets
            .values()
            .map(|e| {
                (
                    e.name.clone(),
                    e.secret_type.clone(),
                    "[MASKED]".to_string(),
                )
            })
            .collect();
        list.sort_by(|a, b| a.0.cmp(&b.0));
        list
    }
}

impl Default for SecretsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

static GLOBAL_SECRETS: OnceLock<Arc<SecretsRegistry>> = OnceLock::new();

pub fn get_global_secrets() -> Arc<SecretsRegistry> {
    GLOBAL_SECRETS
        .get_or_init(|| Arc::new(SecretsRegistry::new()))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secret_registration_and_masking() {
        let registry = get_global_secrets();
        registry
            .register(
                "kafka_password",
                "SASL_PLAINTEXT",
                "super-secret-123",
                "kek-v1",
            )
            .unwrap();

        // Values are masked when listing secrets
        let listed = registry.list();
        let entry = listed.iter().find(|x| x.0 == "kafka_password").unwrap();
        assert_eq!(entry.1, "SASL_PLAINTEXT");
        assert_eq!(entry.2, "[MASKED]");

        // Values decrypted correctly
        let decrypted = registry.get_decrypted("kafka_password").unwrap();
        assert_eq!(decrypted, "super-secret-123");
    }
}
