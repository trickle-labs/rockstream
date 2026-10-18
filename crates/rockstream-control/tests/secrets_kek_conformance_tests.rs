//! Conformance tests for KEK providers and envelope encryption (v0.55.1).

use base64::{engine::general_purpose::STANDARD, Engine};
use object_store::aws::AwsCredential;
use object_store::StaticCredentialProvider;
use rockstream_control::kek::{
    envelope_decrypt_secret, envelope_encrypt_secret, rotate_secret_kek, AwsKmsKekProvider,
    EnvKekProvider, KekProvider,
};
use rockstream_types::secret::{Secret, SecretMetadata, SecretType};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[path = "support/kms.rs"]
mod kms;

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

    assert_eq!(
        decrypted,
        Secret {
            name: "kafka_prod_auth".into(),
            secret_type: SecretType::SaslPlain,
            payload,
            metadata,
        }
    );
}

#[tokio::test]
async fn test_kek_providers_interchangeable_conformance_and_rotation() {
    let env_provider = EnvKekProvider::from_passphrase("test-cluster-kek-master-key-1");
    // The endpoint owns opaque wrapped keys independently of the public provider.
    let keys = Mutex::new(HashMap::<String, String>::new());
    let fixture = kms::KmsFixture::start(move |request| {
        assert_eq!(request.method, "POST / HTTP/1.1");
        assert_eq!(
            request.body["KeyId"],
            "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
        );
        let mut keys = keys.lock().unwrap();
        match request.headers["x-amz-target"].as_str() {
            "TrentService.Encrypt" => {
                let ciphertext = STANDARD.encode(format!("server-owned-key-{}", keys.len()));
                keys.insert(
                    ciphertext.clone(),
                    request.body["Plaintext"].as_str().unwrap().into(),
                );
                (200, serde_json::json!({"CiphertextBlob": ciphertext}))
            }
            "TrentService.Decrypt" => {
                let ciphertext = request.body["CiphertextBlob"].as_str().unwrap();
                (200, serde_json::json!({"Plaintext": keys[ciphertext]}))
            }
            target => panic!("unexpected target: {target}"),
        }
    })
    .await;
    let aws_provider = AwsKmsKekProvider::new(
        "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
        "us-east-1",
    )
    .with_endpoint(&fixture.endpoint)
    .with_credentials(Arc::new(StaticCredentialProvider::new(AwsCredential {
        key_id: "fixture-access".into(),
        secret_key: "fixture-secret".into(),
        token: None,
    })));

    // 1. Both providers satisfy contract
    assert_kek_provider_contract(&env_provider).await;
    assert_kek_provider_contract(&aws_provider).await;

    // 2. KEK rotation: create secret under env KEK, rotate to aws_kms KEK, then rotate to new env KEK
    let mut payload = HashMap::new();
    payload.insert("api_key".to_string(), "rk_live_abcdef123456".to_string());
    payload.insert("scope".to_string(), "synthetic-scope-secret".to_string());

    let meta = SecretMetadata::default();
    let initial_encrypted = envelope_encrypt_secret(
        "api_secret",
        SecretType::BearerToken,
        &payload,
        meta.clone(),
        &env_provider,
    )
    .await
    .expect("initial encrypt must succeed");

    assert_eq!(initial_encrypted.kek_provider, "env");
    let expected = Secret {
        name: "api_secret".into(),
        secret_type: SecretType::BearerToken,
        payload,
        metadata: meta,
    };
    let untouched_payload = HashMap::from([
        ("username".into(), "untouched-user".into()),
        ("password".into(), "untouched-password-secret".into()),
    ]);
    let untouched = envelope_encrypt_secret(
        "untouched",
        SecretType::SaslPlain,
        &untouched_payload,
        SecretMetadata::default(),
        &env_provider,
    )
    .await
    .unwrap();
    let expected_untouched = Secret {
        name: "untouched".into(),
        secret_type: SecretType::SaslPlain,
        payload: untouched_payload,
        metadata: SecretMetadata::default(),
    };

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
    assert_eq!(decrypted_aws, expected);
    assert_eq!(
        envelope_decrypt_secret(&untouched, &env_provider)
            .await
            .unwrap(),
        expected_untouched
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
    assert_eq!(decrypted_new_env, expected);
    assert_eq!(
        envelope_decrypt_secret(&untouched, &env_provider)
            .await
            .unwrap(),
        expected_untouched
    );
    assert_eq!(rotated_to_new_env.nonce, initial_encrypted.nonce);
    assert_eq!(rotated_to_new_env.ciphertext, initial_encrypted.ciphertext);
    for envelope in [
        &initial_encrypted,
        &rotated_to_aws,
        &rotated_to_new_env,
        &untouched,
    ] {
        let catalog = serde_json::to_string(envelope).unwrap();
        let diagnostics = format!("{envelope:?}");
        for literal in expected
            .payload
            .values()
            .chain(expected_untouched.payload.values())
        {
            assert!(!catalog.contains(literal));
            assert!(!diagnostics.contains(literal));
            for bytes in [&envelope.ciphertext, &envelope.wrapped_dek] {
                assert!(!bytes
                    .windows(literal.len())
                    .any(|window| window == literal.as_bytes()));
            }
        }
    }
    assert_eq!(
        fixture
            .requests
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.headers["x-amz-target"].as_str())
            .collect::<Vec<_>>(),
        [
            "TrentService.Encrypt",
            "TrentService.Decrypt",
            "TrentService.Encrypt",
            "TrentService.Decrypt",
            "TrentService.Encrypt",
            "TrentService.Decrypt",
            "TrentService.Decrypt",
        ]
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
