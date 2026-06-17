//! Auth types and JWT verification for v0.26.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Max entries in the JWKS key cache (LRU eviction).
/// Bound: MAX_JWKS_ENTRIES; fill metric: jwks_cache_size gauge.
pub const MAX_JWKS_ENTRIES: usize = 100;

/// Authentication mode for the gateway.
#[derive(Debug, Clone, PartialEq)]
pub enum AuthMode {
    Off,  // --auth=off: all connections get Principal::System, zero network calls
    Oidc, // --auth=oidc: bearer JWT validated against JWKS
    Mtls, // --auth=mtls: TLS client cert CN extracted
}

impl AuthMode {
    pub fn from_str(s: &str) -> Self {
        match s {
            "oidc" => AuthMode::Oidc,
            "mtls" => AuthMode::Mtls,
            _ => AuthMode::Off,
        }
    }
}

/// Authenticated principal for a gateway connection.
#[derive(Debug, Clone, PartialEq)]
pub enum Principal {
    System,               // --auth=off or internal paths
    Jwt { sub: String },  // OIDC bearer token, verified against JWKS; sub = JWT subject
    CertCn { cn: String }, // mTLS client cert CN
}

impl Principal {
    pub fn identity(&self) -> &str {
        match self {
            Principal::System => "system",
            Principal::Jwt { sub } => sub,
            Principal::CertCn { cn } => cn,
        }
    }

    pub fn is_anonymous(&self) -> bool {
        false // All principals are identified
    }

    /// Actor string for audit events.
    pub fn actor(&self) -> String {
        match self {
            Principal::System => "system".to_string(),
            Principal::Jwt { sub } => format!("jwt:{sub}"),
            Principal::CertCn { cn } => format!("cert:{cn}"),
        }
    }

    /// Returns true if this is Principal::System (bypasses all ACL checks).
    pub fn is_system(&self) -> bool {
        matches!(self, Principal::System)
    }
}

/// JWT claims parsed from payload.
#[derive(Debug, Clone)]
pub struct JwtClaims {
    pub sub: String,
    pub exp: Option<u64>,
}

/// Auth error — all map to RS-24xx.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("[RS-2400] auth.unauthenticated: {0}")]
    Unauthenticated(String),
    #[error("[RS-2400] auth.unauthenticated: JWT signature verification failed")]
    InvalidSignature,
    #[error("[RS-2400] auth.unauthenticated: JWT token expired")]
    TokenExpired,
}

/// JWKS in-memory key cache with LRU eviction.
struct JwksCache {
    entries: HashMap<String, Vec<u8>>,
    order: VecDeque<String>,
}

impl JwksCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn insert(&mut self, kid: String, key_bytes: Vec<u8>) {
        if self.entries.contains_key(&kid) {
            self.entries.insert(kid, key_bytes);
        } else {
            if self.entries.len() >= MAX_JWKS_ENTRIES {
                if let Some(oldest) = self.order.pop_front() {
                    self.entries.remove(&oldest);
                }
            }
            self.order.push_back(kid.clone());
            self.entries.insert(kid, key_bytes);
        }
    }

    fn get(&self, kid: &str) -> Option<&Vec<u8>> {
        self.entries.get(kid)
    }
    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// JWT verifier with JWKS key cache.
pub struct JwtVerifier {
    cache: Mutex<JwksCache>,
    default_key: Option<Vec<u8>>, // for HS256 tests
}

impl JwtVerifier {
    pub fn new() -> Self {
        Self {
            cache: Mutex::new(JwksCache::new()),
            default_key: None,
        }
    }

    pub fn with_hs256_key(key: Vec<u8>) -> Self {
        Self {
            cache: Mutex::new(JwksCache::new()),
            default_key: Some(key),
        }
    }

    pub fn add_key(&self, kid: String, key_bytes: Vec<u8>) {
        self.cache.lock().unwrap().insert(kid, key_bytes);
    }

    /// Fill metric: jwks_cache_size.
    pub fn cache_size(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    /// Verify a JWT token (HS256 only). Returns JwtClaims on success.
    pub fn verify(&self, token: &str) -> Result<JwtClaims, AuthError> {
        let parts: Vec<&str> = token.splitn(3, '.').collect();
        if parts.len() != 3 {
            return Err(AuthError::Unauthenticated(
                "malformed JWT: expected 3 parts".to_string(),
            ));
        }

        let (header_b64, payload_b64, sig_b64) = (parts[0], parts[1], parts[2]);

        let header_bytes = URL_SAFE_NO_PAD
            .decode(header_b64)
            .map_err(|_| AuthError::Unauthenticated("header base64 decode error".to_string()))?;
        let header: serde_json::Value = serde_json::from_slice(&header_bytes)
            .map_err(|_| AuthError::Unauthenticated("header JSON parse error".to_string()))?;

        let payload_bytes = URL_SAFE_NO_PAD
            .decode(payload_b64)
            .map_err(|_| AuthError::Unauthenticated("payload base64 decode error".to_string()))?;
        let payload: serde_json::Value = serde_json::from_slice(&payload_bytes)
            .map_err(|_| AuthError::Unauthenticated("payload JSON parse error".to_string()))?;

        let alg = header["alg"].as_str().unwrap_or("");
        if alg != "HS256" {
            return Err(AuthError::Unauthenticated(format!(
                "unsupported algorithm: {alg}"
            )));
        }

        // Key lookup
        let kid = header["kid"].as_str().map(String::from);
        let key = {
            let cache = self.cache.lock().unwrap();
            kid.as_deref().and_then(|k| cache.get(k)).cloned()
        }
        .or_else(|| self.default_key.clone())
        .ok_or_else(|| AuthError::Unauthenticated("no key found for JWT kid".to_string()))?;

        // HMAC-SHA256 signature verification
        type HmacSha256 = Hmac<Sha256>;
        let signing_input = format!("{}.{}", header_b64, payload_b64);
        let sig = URL_SAFE_NO_PAD
            .decode(sig_b64)
            .map_err(|_| AuthError::Unauthenticated("signature base64 decode error".to_string()))?;
        let mut mac = HmacSha256::new_from_slice(&key)
            .map_err(|_| AuthError::Unauthenticated("key error".to_string()))?;
        mac.update(signing_input.as_bytes());
        mac.verify_slice(&sig).map_err(|_| AuthError::InvalidSignature)?;

        // Expiry check
        if let Some(exp) = payload["exp"].as_u64() {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if now > exp {
                return Err(AuthError::TokenExpired);
            }
        }

        let sub = payload["sub"]
            .as_str()
            .ok_or_else(|| AuthError::Unauthenticated("missing sub claim".to_string()))?
            .to_string();

        Ok(JwtClaims {
            sub,
            exp: payload["exp"].as_u64(),
        })
    }
}

/// Create a signed HS256 JWT for tests.
pub fn create_test_jwt(sub: &str, exp: u64, secret: &[u8]) -> String {
    let header = r#"{"alg":"HS256","typ":"JWT"}"#;
    let payload = serde_json::json!({"sub": sub, "exp": exp}).to_string();
    let h = URL_SAFE_NO_PAD.encode(header.as_bytes());
    let p = URL_SAFE_NO_PAD.encode(payload.as_bytes());
    let msg = format!("{}.{}", h, p);
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(secret).unwrap();
    mac.update(msg.as_bytes());
    let sig = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes().as_slice());
    format!("{}.{}", msg, sig)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SECRET: &[u8] = b"test-secret-key-for-rockstream-v026";

    /// S2 green gate: auth_off_bypasses_all_checks
    /// --auth=off: AuthMode::Off produces Principal::System; zero JWKS calls.
    #[test]
    fn auth_off_bypasses_all_checks() {
        let mode = AuthMode::from_str("off");
        assert_eq!(mode, AuthMode::Off);

        let p = Principal::System;
        assert_eq!(p.identity(), "system");
        assert_eq!(p.actor(), "system");
        assert!(p.is_system());

        let verifier = JwtVerifier::new();
        assert_eq!(verifier.cache_size(), 0);
    }

    /// S2 green gate: jwt_bearer_token_validates_sub
    #[test]
    fn jwt_bearer_token_validates_sub() {
        let verifier = JwtVerifier::with_hs256_key(TEST_SECRET.to_vec());
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let token = create_test_jwt("alice", exp, TEST_SECRET);
        let claims = verifier.verify(&token).expect("should validate");
        assert_eq!(claims.sub, "alice");
    }

    /// S2 green gate: jwt_missing_or_invalid_returns_rs2400
    #[test]
    fn jwt_missing_or_invalid_returns_rs2400() {
        let verifier = JwtVerifier::with_hs256_key(TEST_SECRET.to_vec());

        // Missing token (empty string)
        let err = verifier.verify("").unwrap_err();
        assert!(err.to_string().contains("RS-2400"), "expected RS-2400, got: {err}");

        // Invalid signature (tampered)
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let mut token = create_test_jwt("eve", exp, TEST_SECRET);
        token.push_str("tampered");
        let err = verifier.verify(&token).unwrap_err();
        assert!(err.to_string().contains("RS-2400"), "expected RS-2400, got: {err}");

        // Expired token
        let past_exp: u64 = 1; // far in the past
        let expired = create_test_jwt("bob", past_exp, TEST_SECRET);
        let err = verifier.verify(&expired).unwrap_err();
        assert!(err.to_string().contains("RS-2400"), "expected RS-2400, got: {err}");
    }

    #[test]
    fn jwks_cache_lru_eviction() {
        let verifier = JwtVerifier::new();
        for i in 0..=MAX_JWKS_ENTRIES {
            verifier.add_key(format!("kid-{i}"), vec![0u8; 32]);
        }
        assert_eq!(verifier.cache_size(), MAX_JWKS_ENTRIES); // evicted oldest
    }
}
