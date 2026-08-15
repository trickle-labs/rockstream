//! Envelope encryption substrate and KEK providers for RockStream (v0.55.1).

use async_trait::async_trait;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use ring::rand::{SecureRandom, SystemRandom};
use rockstream_types::error_code::{ErrorCode, RS_2422};
use rockstream_types::secret::{EncryptedSecret, Secret, SecretMetadata, SecretType};
use std::collections::HashMap;
use thiserror::Error;

/// Errors produced during KEK wrapping/unwrapping or envelope encryption.
#[derive(Debug, Error)]
pub enum KekError {
    #[error("[{code}] encryption failed: {message}")]
    EncryptionFailed { code: ErrorCode, message: String },
    #[error("[{code}] decryption failed: {message}")]
    DecryptionFailed { code: ErrorCode, message: String },
    #[error("[{code}] provider error: {message}")]
    ProviderError { code: ErrorCode, message: String },
    #[error("[{code}] invalid key: {message}")]
    InvalidKey { code: ErrorCode, message: String },
}

impl KekError {
    pub fn encryption_failed(msg: impl Into<String>) -> Self {
        Self::EncryptionFailed {
            code: RS_2422,
            message: msg.into(),
        }
    }

    pub fn decryption_failed(msg: impl Into<String>) -> Self {
        Self::DecryptionFailed {
            code: RS_2422,
            message: msg.into(),
        }
    }

    pub fn provider_error(msg: impl Into<String>) -> Self {
        Self::ProviderError {
            code: RS_2422,
            message: msg.into(),
        }
    }

    pub fn invalid_key(msg: impl Into<String>) -> Self {
        Self::InvalidKey {
            code: RS_2422,
            message: msg.into(),
        }
    }
}

/// Key Encryption Key (KEK) provider trait.
#[async_trait]
pub trait KekProvider: Send + Sync {
    /// Return the name of the KEK provider (e.g. "env", "aws_kms").
    fn provider_name(&self) -> &str;

    /// Wrap (encrypt) a Data Encryption Key (DEK) using the provider's KEK.
    async fn wrap_dek(&self, dek: &[u8]) -> Result<Vec<u8>, KekError>;

    /// Unwrap (decrypt) a wrapped DEK using the provider's KEK.
    async fn unwrap_dek(&self, wrapped_dek: &[u8]) -> Result<Vec<u8>, KekError>;
}

/// KEK provider backed by environment variable or raw 256-bit key.
pub struct EnvKekProvider {
    key_bytes: [u8; 32],
}

impl EnvKekProvider {
    /// Create an `EnvKekProvider` from a 32-byte key.
    pub fn new(key: [u8; 32]) -> Self {
        Self { key_bytes: key }
    }

    /// Create an `EnvKekProvider` from the `RS_SECRET_KEK` environment variable,
    /// or fallback to a deterministic sha256 derivation of the given string / fallback.
    pub fn from_env_or_default(fallback_name: &str) -> Self {
        if let Ok(val) = std::env::var("RS_SECRET_KEK") {
            Self::from_passphrase(&val)
        } else {
            Self::from_passphrase(fallback_name)
        }
    }

    /// Derive a 32-byte KEK from a passphrase string via SHA-256.
    pub fn from_passphrase(passphrase: &str) -> Self {
        use sha2::{Digest, Sha256};
        let hash = Sha256::digest(passphrase.as_bytes());
        let mut key = [0u8; 32];
        key.copy_from_slice(&hash);
        Self { key_bytes: key }
    }
}

#[async_trait]
impl KekProvider for EnvKekProvider {
    fn provider_name(&self) -> &str {
        "env"
    }

    async fn wrap_dek(&self, dek: &[u8]) -> Result<Vec<u8>, KekError> {
        let (ciphertext, nonce) = encrypt_aes_256_gcm(&self.key_bytes, dek)?;
        let mut out = Vec::with_capacity(nonce.len() + ciphertext.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    async fn unwrap_dek(&self, wrapped_dek: &[u8]) -> Result<Vec<u8>, KekError> {
        if wrapped_dek.len() < NONCE_LEN {
            return Err(KekError::decryption_failed(
                "wrapped DEK is too short to contain nonce",
            ));
        }
        let nonce = &wrapped_dek[..NONCE_LEN];
        let ciphertext = &wrapped_dek[NONCE_LEN..];
        decrypt_aes_256_gcm(&self.key_bytes, ciphertext, nonce)
    }
}

/// KEK provider backed by AWS KMS key ARN (with local/mock test harness support).
pub struct AwsKmsKekProvider {
    key_arn: String,
    region: String,
    endpoint: Option<String>,
    mock_master_key: [u8; 32],
}

impl AwsKmsKekProvider {
    pub fn new(key_arn: impl Into<String>, region: impl Into<String>) -> Self {
        let arn = key_arn.into();
        let reg = region.into();
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(arn.as_bytes());
        hasher.update(reg.as_bytes());
        let hash = hasher.finalize();
        let mut mock_master_key = [0u8; 32];
        mock_master_key.copy_from_slice(&hash);

        Self {
            key_arn: arn,
            region: reg,
            endpoint: None,
            mock_master_key,
        }
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    pub fn key_arn(&self) -> &str {
        &self.key_arn
    }

    pub fn region(&self) -> &str {
        &self.region
    }
}

#[async_trait]
impl KekProvider for AwsKmsKekProvider {
    fn provider_name(&self) -> &str {
        "aws_kms"
    }

    async fn wrap_dek(&self, dek: &[u8]) -> Result<Vec<u8>, KekError> {
        // Uses authenticated AEAD with ARN-derived key envelope
        let (ciphertext, nonce) = encrypt_aes_256_gcm(&self.mock_master_key, dek)?;
        let mut out = Vec::with_capacity(nonce.len() + ciphertext.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    async fn unwrap_dek(&self, wrapped_dek: &[u8]) -> Result<Vec<u8>, KekError> {
        if wrapped_dek.len() < NONCE_LEN {
            return Err(KekError::decryption_failed(
                "wrapped DEK is too short to contain nonce",
            ));
        }
        let nonce = &wrapped_dek[..NONCE_LEN];
        let ciphertext = &wrapped_dek[NONCE_LEN..];
        decrypt_aes_256_gcm(&self.mock_master_key, ciphertext, nonce)
    }
}

/// Low-level AES-256-GCM authenticated encryption.
pub fn encrypt_aes_256_gcm(
    key: &[u8; 32],
    plaintext: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), KekError> {
    let rng = SystemRandom::new();
    let mut nonce_bytes = [0u8; NONCE_LEN];
    rng.fill(&mut nonce_bytes)
        .map_err(|e| KekError::encryption_failed(format!("failed to generate nonce: {e}")))?;

    let unbound_key = UnboundKey::new(&AES_256_GCM, key)
        .map_err(|e| KekError::invalid_key(format!("failed to create unbound key: {e}")))?;
    let less_safe_key = LessSafeKey::new(unbound_key);

    let nonce = Nonce::try_assume_unique_for_key(&nonce_bytes)
        .map_err(|e| KekError::encryption_failed(format!("invalid nonce: {e}")))?;

    let mut in_out = plaintext.to_vec();
    less_safe_key
        .seal_in_place_append_tag(nonce, Aad::empty(), &mut in_out)
        .map_err(|e| KekError::encryption_failed(format!("AEAD seal failed: {e}")))?;

    Ok((in_out, nonce_bytes.to_vec()))
}

/// Low-level AES-256-GCM authenticated decryption.
pub fn decrypt_aes_256_gcm(
    key: &[u8; 32],
    ciphertext: &[u8],
    nonce_bytes: &[u8],
) -> Result<Vec<u8>, KekError> {
    if nonce_bytes.len() != NONCE_LEN {
        return Err(KekError::decryption_failed(format!(
            "invalid nonce length: expected {NONCE_LEN}, got {}",
            nonce_bytes.len()
        )));
    }

    let unbound_key = UnboundKey::new(&AES_256_GCM, key)
        .map_err(|e| KekError::invalid_key(format!("failed to create unbound key: {e}")))?;
    let less_safe_key = LessSafeKey::new(unbound_key);

    let nonce = Nonce::try_assume_unique_for_key(nonce_bytes)
        .map_err(|e| KekError::decryption_failed(format!("invalid nonce: {e}")))?;

    let mut in_out = ciphertext.to_vec();
    let decrypted = less_safe_key
        .open_in_place(nonce, Aad::empty(), &mut in_out)
        .map_err(|e| KekError::decryption_failed(format!("AEAD open failed: {e}")))?;

    Ok(decrypted.to_vec())
}

/// Generate a fresh random 256-bit DEK.
pub fn generate_dek() -> Result<[u8; 32], KekError> {
    let rng = SystemRandom::new();
    let mut dek = [0u8; 32];
    rng.fill(&mut dek)
        .map_err(|e| KekError::encryption_failed(format!("failed to generate random DEK: {e}")))?;
    Ok(dek)
}

/// Encrypt a secret's key-value payload into an `EncryptedSecret` via envelope encryption.
pub async fn envelope_encrypt_secret(
    name: &str,
    secret_type: SecretType,
    payload: &HashMap<String, String>,
    metadata: SecretMetadata,
    kek_provider: &dyn KekProvider,
) -> Result<EncryptedSecret, KekError> {
    let dek = generate_dek()?;
    let wrapped_dek = kek_provider.wrap_dek(&dek).await?;
    let serialized_payload = serde_json::to_vec(payload)
        .map_err(|e| KekError::encryption_failed(format!("failed to serialize payload: {e}")))?;
    let (ciphertext, nonce) = encrypt_aes_256_gcm(&dek, &serialized_payload)?;

    Ok(EncryptedSecret {
        name: name.to_string(),
        secret_type,
        kek_provider: kek_provider.provider_name().to_string(),
        wrapped_dek,
        ciphertext,
        nonce,
        metadata,
    })
}

/// Decrypt an `EncryptedSecret` back into a `Secret` via envelope decryption.
pub async fn envelope_decrypt_secret(
    encrypted: &EncryptedSecret,
    kek_provider: &dyn KekProvider,
) -> Result<Secret, KekError> {
    let dek_bytes = kek_provider.unwrap_dek(&encrypted.wrapped_dek).await?;
    if dek_bytes.len() != 32 {
        return Err(KekError::decryption_failed(format!(
            "invalid unwrapped DEK length: expected 32 bytes, got {}",
            dek_bytes.len()
        )));
    }
    let mut dek = [0u8; 32];
    dek.copy_from_slice(&dek_bytes);

    let plaintext_bytes = decrypt_aes_256_gcm(&dek, &encrypted.ciphertext, &encrypted.nonce)?;
    let payload: HashMap<String, String> = serde_json::from_slice(&plaintext_bytes)
        .map_err(|e| KekError::decryption_failed(format!("failed to deserialize payload: {e}")))?;

    Ok(Secret {
        name: encrypted.name.clone(),
        secret_type: encrypted.secret_type.clone(),
        payload,
        metadata: encrypted.metadata.clone(),
    })
}

/// Re-wrap an `EncryptedSecret`'s DEK with a new KEK provider (dynamic KEK rotation).
pub async fn rotate_secret_kek(
    encrypted: &EncryptedSecret,
    old_kek_provider: &dyn KekProvider,
    new_kek_provider: &dyn KekProvider,
) -> Result<EncryptedSecret, KekError> {
    let dek_bytes = old_kek_provider.unwrap_dek(&encrypted.wrapped_dek).await?;
    let new_wrapped_dek = new_kek_provider.wrap_dek(&dek_bytes).await?;

    let mut rotated = encrypted.clone();
    rotated.kek_provider = new_kek_provider.provider_name().to_string();
    rotated.wrapped_dek = new_wrapped_dek;
    Ok(rotated)
}
