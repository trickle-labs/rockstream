//! Secrets management types for RockStream (v0.55.1).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

/// Secret type discriminator.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretType {
    SaslPlain,
    PostgresPassword,
    BearerToken,
    #[serde(untagged)]
    Custom(String),
}

impl SecretType {
    pub fn as_str(&self) -> &str {
        match self {
            Self::SaslPlain => "sasl_plain",
            Self::PostgresPassword => "postgres_password",
            Self::BearerToken => "bearer_token",
            Self::Custom(s) => s.as_str(),
        }
    }
}

impl fmt::Display for SecretType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for SecretType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "sasl_plain" | "sasl_plaintext" => Self::SaslPlain,
            "postgres_password" | "postgres" => Self::PostgresPassword,
            "bearer_token" | "bearer" => Self::BearerToken,
            other => Self::Custom(other.to_string()),
        }
    }
}

impl From<String> for SecretType {
    fn from(s: String) -> Self {
        Self::from(s.as_str())
    }
}

/// Metadata associated with a stored secret.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecretMetadata {
    pub created_at: u64,
    pub updated_at: u64,
    pub version: u64,
    #[serde(default)]
    pub source_refs: Vec<String>,
}

impl Default for SecretMetadata {
    fn default() -> Self {
        Self {
            created_at: 0,
            updated_at: 0,
            version: 1,
            source_refs: Vec::new(),
        }
    }
}

/// Plaintext in-memory representation of a secret.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Secret {
    pub name: String,
    pub secret_type: SecretType,
    pub payload: HashMap<String, String>,
    pub metadata: SecretMetadata,
}

/// Envelope-encrypted stored representation of a secret.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncryptedSecret {
    pub name: String,
    pub secret_type: SecretType,
    pub kek_provider: String,
    pub wrapped_dek: Vec<u8>,
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
    pub metadata: SecretMetadata,
}

/// Short-lived secret token issued to a worker node identity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecretToken {
    pub token_id: String,
    pub secret_name: String,
    pub secret_type: SecretType,
    pub worker_identity: String,
    pub encrypted_payload: Vec<u8>,
    pub nonce: Vec<u8>,
    pub issued_at: u64,
    pub expires_at: u64,
}

impl SecretToken {
    /// Stable key derivation shared by the control plane and worker process.
    pub fn worker_key(identity: &str) -> [u8; 32] {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(b"rockstream-worker-secret-token-key-v1");
        hasher.update(identity.as_bytes());
        let digest = hasher.finalize();
        let mut key = [0u8; 32];
        key.copy_from_slice(&digest);
        key
    }

    pub fn is_expired(&self, current_time_secs: u64) -> bool {
        current_time_secs >= self.expires_at
    }
}

/// Notification emitted after a secret payload changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretRotation {
    pub secret_name: String,
    pub version: u64,
}

/// A stored secret representation (legacy compatibility helper).
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
    fn test_secret_types_and_models() {
        let meta = SecretMetadata {
            created_at: 100,
            updated_at: 200,
            version: 2,
            source_refs: vec!["src_kafka".to_string()],
        };
        let mut payload = HashMap::new();
        payload.insert("username".to_string(), "alice".to_string());
        payload.insert("password".to_string(), "secret123".to_string());

        let secret = Secret {
            name: "kafka_auth".to_string(),
            secret_type: SecretType::SaslPlain,
            payload,
            metadata: meta.clone(),
        };
        assert_eq!(secret.secret_type.as_str(), "sasl_plain");

        let token = SecretToken {
            token_id: "tok-1".to_string(),
            secret_name: "kafka_auth".to_string(),
            secret_type: SecretType::SaslPlain,
            worker_identity: "worker-1".to_string(),
            encrypted_payload: vec![1, 2, 3],
            nonce: vec![4, 5, 6],
            issued_at: 100,
            expires_at: 200,
        };
        assert!(!token.is_expired(150));
        assert!(token.is_expired(200));
        assert!(token.is_expired(250));
    }

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
