use base64::{engine::general_purpose::STANDARD, Engine};
use hmac::{Hmac, Mac};
use object_store::aws::AwsCredential;
use object_store::StaticCredentialProvider;
use rockstream_control::kek::{AwsKmsKekProvider, KekProvider};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[path = "support/kms.rs"]
mod kms;
use kms::{KmsFixture, Request};

const KEY: &str = "arn:aws:kms:us-east-1:123456789012:key/fixture";

fn provider(endpoint: &str, key: &str) -> AwsKmsKekProvider {
    AwsKmsKekProvider::new(key, "us-east-1")
        .with_endpoint(endpoint)
        .with_credentials(Arc::new(StaticCredentialProvider::new(AwsCredential {
            key_id: "fixture-access".into(),
            secret_key: "fixture-signing-secret".into(),
            token: Some("fixture-session-token".into()),
        })))
}

fn hmac(key: &[u8], input: &str) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(input.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

fn assert_signed_request(request: &Request) {
    assert_eq!(request.method, "POST / HTTP/1.1");
    assert_eq!(
        request.headers["content-type"],
        "application/x-amz-json-1.1"
    );
    assert_eq!(
        request.headers["x-amz-security-token"],
        "fixture-session-token"
    );
    let digest = hex::encode(Sha256::digest(request.body.to_string()));
    assert_eq!(request.headers["x-amz-content-sha256"], digest);
    let signed =
        "content-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token;x-amz-target";
    let canonical_headers: String = signed
        .split(';')
        .map(|name| format!("{name}:{}\n", request.headers[name]))
        .collect();
    let canonical_request = format!("POST\n/\n\n{canonical_headers}\n{signed}\n{digest}");
    let timestamp = &request.headers["x-amz-date"];
    let date = &timestamp[..8];
    let scope = format!("{date}/us-east-1/kms/aws4_request");
    let signing_key = hmac(b"AWS4fixture-signing-secret", date);
    let signing_key = hmac(&signing_key, "us-east-1");
    let signing_key = hmac(&signing_key, "kms");
    let signing_key = hmac(&signing_key, "aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request))
    );
    let signature = hex::encode(hmac(&signing_key, &to_sign));
    assert_eq!(request.headers["authorization"], format!("AWS4-HMAC-SHA256 Credential=fixture-access/{scope}, SignedHeaders={signed}, Signature={signature}"));
}

#[tokio::test]
async fn authorized_operations_use_signed_kms_requests_and_independent_responses() {
    let fixture = KmsFixture::start(|request| {
        assert_signed_request(request);
        match request.headers["x-amz-target"].as_str() {
            "TrentService.Encrypt" => {
                assert_eq!(request.body, json!({"KeyId": KEY, "Plaintext": STANDARD.encode([42; 32])}));
                (200, json!({"CiphertextBlob": STANDARD.encode(b"independent-kms-ciphertext")}))
            }
            "TrentService.Decrypt" => {
                assert_eq!(request.body, json!({"KeyId": KEY, "CiphertextBlob": STANDARD.encode(b"independent-kms-ciphertext")}));
                (200, json!({"Plaintext": STANDARD.encode([42; 32])}))
            }
            target => panic!("unexpected target: {target}"),
        }
    }).await;
    let provider = provider(&fixture.endpoint, KEY);
    assert_eq!(provider.provider_name(), "aws_kms");
    assert_eq!(provider.key_arn(), KEY);
    assert_eq!(provider.region(), "us-east-1");
    assert_eq!(
        provider.wrap_dek(&[42; 32]).await.unwrap(),
        b"independent-kms-ciphertext"
    );
    assert_eq!(
        provider
            .unwrap_dek(b"independent-kms-ciphertext")
            .await
            .unwrap(),
        [42; 32]
    );
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(|r| r.headers["x-amz-target"].as_str())
            .collect::<Vec<_>>(),
        ["TrentService.Encrypt", "TrentService.Decrypt"]
    );
}

#[tokio::test]
async fn denied_operations_fail_without_local_fallback_or_leaking_response_bodies() {
    let fixture = KmsFixture::start(|request| {
        assert_signed_request(request);
        (
            400,
            json!({"__type": "AccessDeniedException", "message": "sensitive-response-literal"}),
        )
    })
    .await;
    let provider = provider(&fixture.endpoint, KEY);
    assert_eq!(
        provider.wrap_dek(&[42; 32]).await.unwrap_err().to_string(),
        "[RS-2422] provider error: AWS KMS Encrypt failed (HTTP 400)"
    );
    assert_eq!(
        provider
            .unwrap_dek(b"captured-envelope")
            .await
            .unwrap_err()
            .to_string(),
        "[RS-2422] provider error: AWS KMS Decrypt failed (HTTP 400)"
    );
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(|r| (&r.headers["x-amz-target"], &r.body))
            .collect::<Vec<_>>(),
        vec![
            (
                &"TrentService.Encrypt".to_string(),
                &json!({"KeyId": KEY, "Plaintext": STANDARD.encode([42; 32])})
            ),
            (
                &"TrentService.Decrypt".to_string(),
                &json!({"KeyId": KEY, "CiphertextBlob": STANDARD.encode(b"captured-envelope")})
            ),
        ]
    );
}

#[tokio::test]
async fn wrong_key_decryption_is_rejected_by_the_selected_kms_boundary() {
    let fixture = KmsFixture::start(|request| {
        assert_eq!(request.headers["x-amz-target"], "TrentService.Decrypt");
        assert_eq!(request.body, json!({"KeyId": "alias/wrong-key", "CiphertextBlob": STANDARD.encode(b"captured-envelope")}));
        (400, json!({"__type": "IncorrectKeyException"}))
    }).await;
    assert_eq!(
        provider(&fixture.endpoint, "alias/wrong-key")
            .unwrap_dek(b"captured-envelope")
            .await
            .unwrap_err()
            .to_string(),
        "[RS-2422] provider error: AWS KMS Decrypt failed (HTTP 400)"
    );
}

#[tokio::test]
async fn invalid_kms_success_responses_fail_explicitly() {
    for response in [
        json!({}),
        json!({"Plaintext": "%%%"}),
        json!({"Plaintext": ""}),
        json!({"Plaintext": 42}),
    ] {
        let fixture = KmsFixture::start(move |_| (200, response.clone())).await;
        assert_eq!(
            provider(&fixture.endpoint, KEY)
                .unwrap_dek(b"ciphertext")
                .await
                .unwrap_err()
                .to_string(),
            "[RS-2422] provider error: AWS KMS Decrypt returned an invalid Plaintext"
        );
    }
}

#[tokio::test]
async fn unavailable_kms_fails_both_operations() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let provider = provider(&endpoint, KEY);
    assert_eq!(
        provider.wrap_dek(&[42; 32]).await.unwrap_err().to_string(),
        "[RS-2422] provider error: AWS KMS Encrypt request failed"
    );
    assert_eq!(
        provider
            .unwrap_dek(b"ciphertext")
            .await
            .unwrap_err()
            .to_string(),
        "[RS-2422] provider error: AWS KMS Decrypt request failed"
    );
}

#[tokio::test]
async fn unsafe_endpoints_fail_before_sending_credentials() {
    for endpoint in [
        "http://example.com",
        "ftp://127.0.0.1",
        "https://user:password@example.com",
        "https://example.com/?token=secret",
        "https://example.com/#fragment",
    ] {
        assert_eq!(provider(endpoint, KEY).wrap_dek(&[42; 32]).await.unwrap_err().to_string(), "[RS-2422] provider error: AWS KMS endpoint must use HTTPS (HTTP is allowed only on loopback) and contain no credentials, query or fragment");
    }
}

#[tokio::test]
async fn invalid_credentials_fail_without_panicking_or_exposing_their_values() {
    for (key_id, token) in [
        ("fixture-access", "sensitive\ninvalid-token"),
        ("sensitive\ninvalid-id", "fixture-token"),
    ] {
        let provider = AwsKmsKekProvider::new(KEY, "us-east-1")
            .with_endpoint("http://127.0.0.1:9")
            .with_credentials(Arc::new(StaticCredentialProvider::new(AwsCredential {
                key_id: key_id.into(),
                secret_key: "fixture-secret".into(),
                token: Some(token.into()),
            })));
        assert_eq!(
            provider.wrap_dek(&[42; 32]).await.unwrap_err().to_string(),
            "[RS-2422] provider error: invalid AWS KMS credentials"
        );
    }
}

#[tokio::test]
async fn redirects_are_not_followed() {
    // Even a redirect response containing valid-looking plaintext must fail closed.
    let fixture =
        KmsFixture::start(|_| (307, json!({"Plaintext": STANDARD.encode([42; 32])}))).await;
    assert_eq!(
        provider(&fixture.endpoint, KEY)
            .unwrap_dek(b"ciphertext")
            .await
            .unwrap_err()
            .to_string(),
        "[RS-2422] provider error: AWS KMS Decrypt failed (HTTP 307)"
    );
}

#[tokio::test]
#[ignore = "requires explicitly provisioned authorized/wrong-key/denied-principal AWS KMS fixtures; see docs/security.md"]
async fn real_kms_enforces_key_and_principal_permissions() {
    use rockstream_control::kek::{envelope_decrypt_secret, envelope_encrypt_secret};
    use rockstream_types::secret::{Secret, SecretMetadata, SecretType};
    use std::collections::HashMap;

    let key = std::env::var("ROCKSTREAM_TEST_KMS_KEY_ARN").expect("scoped KMS key ARN required");
    let wrong_key = std::env::var("ROCKSTREAM_TEST_KMS_WRONG_KEY_ARN")
        .expect("different scoped KMS key ARN required");
    assert_ne!(key, wrong_key);
    let region = std::env::var("ROCKSTREAM_TEST_KMS_REGION").expect("scoped KMS region required");
    let authorized = AwsKmsKekProvider::new(&key, &region);
    let denied = AwsKmsKekProvider::new(&key, &region).with_credentials(Arc::new(
        StaticCredentialProvider::new(AwsCredential {
            key_id: std::env::var("ROCKSTREAM_TEST_KMS_DENIED_ACCESS_KEY_ID")
                .expect("denied principal required"),
            secret_key: std::env::var("ROCKSTREAM_TEST_KMS_DENIED_SECRET_ACCESS_KEY")
                .expect("denied principal required"),
            token: std::env::var("ROCKSTREAM_TEST_KMS_DENIED_SESSION_TOKEN").ok(),
        }),
    ));
    let known_dek = [42; 32];
    let wrapped = authorized.wrap_dek(&known_dek).await.unwrap();
    assert_eq!(authorized.unwrap_dek(&wrapped).await.unwrap(), known_dek);
    assert_eq!(
        AwsKmsKekProvider::new(wrong_key, region)
            .unwrap_dek(&wrapped)
            .await
            .unwrap_err()
            .to_string(),
        "[RS-2422] provider error: AWS KMS Decrypt failed (HTTP 400)"
    );
    for (result, operation) in [
        (denied.wrap_dek(&known_dek).await, "Encrypt"),
        (denied.unwrap_dek(&wrapped).await, "Decrypt"),
    ] {
        assert_eq!(
            result.unwrap_err().to_string(),
            format!("[RS-2422] provider error: AWS KMS {operation} failed (HTTP 400)")
        );
    }
    for (name, secret_type, payload) in [
        (
            "kms-fixture-a",
            SecretType::SaslPlain,
            HashMap::from([
                ("username".into(), "synthetic-kms-user".into()),
                ("password".into(), "synthetic-kms-password".into()),
            ]),
        ),
        (
            "kms-fixture-b",
            SecretType::BearerToken,
            HashMap::from([
                ("token".into(), "synthetic-kms-token".into()),
                ("scope".into(), "synthetic-kms-scope".into()),
            ]),
        ),
    ] {
        let expected = Secret {
            name: name.into(),
            secret_type: secret_type.clone(),
            payload: payload.clone(),
            metadata: SecretMetadata::default(),
        };
        let encrypted = envelope_encrypt_secret(
            name,
            secret_type,
            &payload,
            expected.metadata.clone(),
            &authorized,
        )
        .await
        .unwrap();
        assert_eq!(
            envelope_decrypt_secret(&encrypted, &authorized)
                .await
                .unwrap(),
            expected
        );
        assert_eq!(
            envelope_decrypt_secret(&encrypted, &denied)
                .await
                .unwrap_err()
                .to_string(),
            "[RS-2422] provider error: AWS KMS Decrypt failed (HTTP 400)"
        );
    }
}
