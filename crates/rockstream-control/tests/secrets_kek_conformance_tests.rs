//! Conformance tests for KEK providers and envelope encryption (v0.55.1).

use rockstream_control::kek::{
    envelope_decrypt_secret, envelope_encrypt_secret, rotate_secret_kek, AwsKmsKekProvider,
    EnvKekProvider, KekProvider,
};
use rockstream_types::secret::{SecretMetadata, SecretType};
use std::collections::HashMap;

async fn assert_kek_provider_contract(provider: &dyn KekProvider) {
    let dek = [42u8; 32];
    let wrapped = provider
        .wrap_dek(&dek)
        .await
        .expect("wrap_dek must succeed");
    assert_ne!(wrapped, dek.to_vec(), "wrapped DEK must be encrypted");

    let unwrapped = provider
        .unwrap_dek(&wrapped)
        .await
        .expect("unwrap_dek must succeed");
    assert_eq!(unwrapped, dek.to_vec(), "unwrapped DEK must match original");

    // Envelope encryption of secret payload
    let mut payload = HashMap::new();
    payload.insert("username".to_string(), "kafka_client_app".to_string());
    payload.insert("password".to_string(), "SuperSecretPassword!99".to_string());

    let metadata = SecretMetadata {
        created_at: 1000,
        updated_at: 1000,
        version: 1,
        source_refs: vec!["kafka_source_1".to_string()],
    };

    let encrypted = envelope_encrypt_secret(
        "kafka_prod_auth",
        SecretType::SaslPlain,
        &payload,
        metadata.clone(),
        provider,
    )
    .await
    .expect("envelope encryption must succeed");

    assert_eq!(encrypted.name, "kafka_prod_auth");
    assert_eq!(encrypted.secret_type, SecretType::SaslPlain);
    assert_eq!(encrypted.kek_provider, provider.provider_name());
    assert_ne!(encrypted.ciphertext, Vec::<u8>::new());
    // Assert ciphertext does NOT contain the plaintext bytes directly
    assert!(!String::from_utf8_lossy(&encrypted.ciphertext).contains("SuperSecretPassword!99"));

    let decrypted = envelope_decrypt_secret(&encrypted, provider)
        .await
        .expect("envelope decryption must succeed");

    assert_eq!(decrypted.name, "kafka_prod_auth");
    assert_eq!(decrypted.secret_type, SecretType::SaslPlain);
    assert_eq!(
        decrypted.payload.get("username").unwrap(),
        "kafka_client_app"
    );
    assert_eq!(
        decrypted.payload.get("password").unwrap(),
        "SuperSecretPassword!99"
    );
    assert_eq!(decrypted.metadata, metadata);
}

#[tokio::test]
async fn test_kek_providers_interchangeable_conformance_and_rotation() {
    let env_provider = EnvKekProvider::from_passphrase("test-cluster-kek-master-key-1");
    let aws_provider = AwsKmsKekProvider::new(
        "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
        "us-east-1",
    );

    // 1. Both providers satisfy contract
    assert_kek_provider_contract(&env_provider).await;
    assert_kek_provider_contract(&aws_provider).await;

    // 2. KEK rotation: create secret under env KEK, rotate to aws_kms KEK, then rotate to new env KEK
    let mut payload = HashMap::new();
    payload.insert("api_key".to_string(), "rk_live_abcdef123456".to_string());

    let meta = SecretMetadata::default();
    let initial_encrypted = envelope_encrypt_secret(
        "api_secret",
        SecretType::BearerToken,
        &payload,
        meta,
        &env_provider,
    )
    .await
    .expect("initial encrypt must succeed");

    assert_eq!(initial_encrypted.kek_provider, "env");

    // Rotate to AWS KMS
    let rotated_to_aws = rotate_secret_kek(&initial_encrypted, &env_provider, &aws_provider)
        .await
        .expect("rotation to AWS KMS must succeed");

    assert_eq!(rotated_to_aws.kek_provider, "aws_kms");
    assert_ne!(rotated_to_aws.wrapped_dek, initial_encrypted.wrapped_dek);
    // Ciphertext payload itself remains unchanged during KEK rotation (only DEK is rewrapped)
    assert_eq!(rotated_to_aws.ciphertext, initial_encrypted.ciphertext);

    // Decrypt using new provider
    let decrypted_aws = envelope_decrypt_secret(&rotated_to_aws, &aws_provider)
        .await
        .expect("decryption after rotation must succeed");
    assert_eq!(
        decrypted_aws.payload.get("api_key").unwrap(),
        "rk_live_abcdef123456"
    );

    // Rotate to new Env KEK
    let new_env_provider = EnvKekProvider::from_passphrase("test-cluster-kek-rotated-key-2");
    let rotated_to_new_env = rotate_secret_kek(&rotated_to_aws, &aws_provider, &new_env_provider)
        .await
        .expect("rotation to new env KEK must succeed");

    assert_eq!(rotated_to_new_env.kek_provider, "env");
    let decrypted_new_env = envelope_decrypt_secret(&rotated_to_new_env, &new_env_provider)
        .await
        .expect("decryption with new env KEK must succeed");
    assert_eq!(
        decrypted_new_env.payload.get("api_key").unwrap(),
        "rk_live_abcdef123456"
    );
}

#[tokio::test]
async fn test_kek_tamper_and_invalid_key_errors() {
    let provider = EnvKekProvider::from_passphrase("master-key-xyz");
    let dek = [7u8; 32];
    let mut wrapped = provider.wrap_dek(&dek).await.unwrap();

    // Tamper with wrapped DEK
    let last = wrapped.len() - 1;
    wrapped[last] ^= 0xFF;

    let err = provider.unwrap_dek(&wrapped).await;
    assert!(err.is_err());
    let err_str = err.unwrap_err().to_string();
    assert!(
        err_str.contains("RS-2422"),
        "error must carry RS-2422, got: {err_str}"
    );
}
