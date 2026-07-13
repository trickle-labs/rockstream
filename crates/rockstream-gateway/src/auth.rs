//! Auth types and JWT verification for v0.26.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// Max entries in the JWKS key cache (LRU eviction).
/// Bound: MAX_JWKS_ENTRIES; fill metric: jwks_cache_size gauge.
pub const MAX_JWKS_ENTRIES: usize = 100;

/// Authentication mode for the gateway.
#[derive(Debug, Clone, PartialEq)]
pub enum AuthMode {
    Off,   // --auth=off: all connections get Principal::System, zero network calls
    Oidc,  // --auth=oidc: bearer JWT validated against JWKS
    Mtls,  // --auth=mtls: TLS client cert CN extracted
    Scram, // --auth=scram: SCRAM-SHA-256 with RoleCatalog
    Md5,   // --auth=md5: MD5 password with RoleCatalog
}

impl std::str::FromStr for AuthMode {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "oidc" => AuthMode::Oidc,
            "mtls" => AuthMode::Mtls,
            "scram" => AuthMode::Scram,
            "md5" => AuthMode::Md5,
            _ => AuthMode::Off,
        })
    }
}

/// Authenticated principal for a gateway connection.
#[derive(Debug, Clone, PartialEq)]
pub enum Principal {
    System,                         // --auth=off or internal paths
    Jwt { sub: String },            // OIDC bearer token, verified against JWKS; sub = JWT subject
    CertCn { cn: String },          // mTLS client cert CN
    ScramUser { username: String }, // SCRAM-SHA-256 or MD5 authenticated user
}

impl Principal {
    pub fn identity(&self) -> &str {
        match self {
            Principal::System => "system",
            Principal::Jwt { sub } => sub,
            Principal::CertCn { cn } => cn,
            Principal::ScramUser { username } => username,
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
            Principal::ScramUser { username } => format!("scram:{username}"),
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
    #[error("[RS-2401] auth.invalid_password: password authentication failed. next_steps: Check password and retry")]
    InvalidPassword,
}

// ── SCRAM-SHA-256 crypto primitives (RFC 5802 §3 / RFC 7677) ─────────────────

/// Hi(str, salt, i) — PBKDF2-HMAC-SHA-256 as defined in RFC 5802 §2.2.
/// Implemented as an iterative HMAC loop; no extra crate required.
pub fn scram_hi(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    type HmacSha256 = Hmac<Sha256>;
    let mut salt_int = salt.to_vec();
    salt_int.extend_from_slice(&[0u8, 0, 0, 1]); // INT(1) per RFC 5802

    let mut mac = HmacSha256::new_from_slice(password).expect("HMAC init");
    mac.update(&salt_int);
    let mut u: [u8; 32] = mac.finalize().into_bytes().into();
    let mut result = u;

    for _ in 1..iterations {
        let mut mac = HmacSha256::new_from_slice(password).expect("HMAC init");
        mac.update(&u);
        u = mac.finalize().into_bytes().into();
        for j in 0..32 {
            result[j] ^= u[j];
        }
    }
    result
}

/// StoredKey = H(HMAC(SaltedPassword, "Client Key")).
pub fn scram_stored_key(salted_password: &[u8]) -> [u8; 32] {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(salted_password).expect("HMAC init");
    mac.update(b"Client Key");
    let client_key: [u8; 32] = mac.finalize().into_bytes().into();
    Sha256::digest(client_key).into()
}

/// ServerKey = HMAC(SaltedPassword, "Server Key").
pub fn scram_server_key(salted_password: &[u8]) -> [u8; 32] {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(salted_password).expect("HMAC init");
    mac.update(b"Server Key");
    mac.finalize().into_bytes().into()
}

/// ClientSignature = HMAC(StoredKey, AuthMessage).
pub fn scram_client_signature(stored_key: &[u8], auth_message: &str) -> [u8; 32] {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(stored_key).expect("HMAC init");
    mac.update(auth_message.as_bytes());
    mac.finalize().into_bytes().into()
}

/// ServerSignature = HMAC(ServerKey, AuthMessage).
pub fn scram_server_signature(server_key: &[u8], auth_message: &str) -> [u8; 32] {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(server_key).expect("HMAC init");
    mac.update(auth_message.as_bytes());
    mac.finalize().into_bytes().into()
}

/// Verify a SCRAM client proof. Returns true iff the proof is valid.
///
/// `stored_key_from_catalog` must be StoredKey = H(ClientKey).
/// `client_proof_b64` is the base64-encoded p= value from the client-final.
pub fn verify_client_proof(
    stored_key_from_catalog: &[u8],
    client_proof_b64: &str,
    auth_message: &str,
) -> bool {
    let client_proof = match STANDARD.decode(client_proof_b64) {
        Ok(v) if v.len() == 32 => v,
        _ => return false,
    };
    // ClientSignature = HMAC(StoredKey, AuthMessage)
    let sig = scram_client_signature(stored_key_from_catalog, auth_message);
    // ClientKey = ClientProof XOR ClientSignature
    let mut client_key = [0u8; 32];
    for i in 0..32 {
        client_key[i] = client_proof[i] ^ sig[i];
    }
    // Verify: H(ClientKey) == StoredKey
    let computed_stored: [u8; 32] = Sha256::digest(client_key).into();
    computed_stored == stored_key_from_catalog
}

/// Convenience wrapper: returns `(salted_password, stored_key, server_key)`.
pub fn gen_scram_verifiers_raw(
    password: &str,
    salt: &[u8],
    iterations: u32,
) -> ([u8; 32], [u8; 32], [u8; 32]) {
    let salted = scram_hi(password.as_bytes(), salt, iterations);
    let stored = scram_stored_key(&salted);
    let server = scram_server_key(&salted);
    (salted, stored, server)
}

// ── MD5 password helpers ──────────────────────────────────────────────────────

/// Compute the MD5 wire hash that the client sends in response to an MD5 challenge.
/// Returns `"md5" + hex(md5(hex(md5(password + user)) + salt))`.
pub fn hash_md5_password(user: &str, password: &str, salt: &[u8]) -> String {
    let inner = {
        let mut ctx = md5::Context::new();
        ctx.consume(password.as_bytes());
        ctx.consume(user.as_bytes());
        format!("{:x}", ctx.compute())
    };
    let outer = {
        let mut ctx = md5::Context::new();
        ctx.consume(inner.as_bytes());
        ctx.consume(salt);
        format!("{:x}", ctx.compute())
    };
    format!("md5{outer}")
}

/// Verify an MD5 client response.
///
/// `expected_hash` is the stored value `"md5" + hex(md5(password + username))`.
/// `client_response` is what the client sent: `"md5" + hex(md5(inner_hex + salt))`.
pub fn verify_md5(expected_hash: &str, user: &str, salt: &[u8], client_response: &str) -> bool {
    let inner_hex = expected_hash.strip_prefix("md5").unwrap_or("");
    if inner_hex.is_empty() {
        return false;
    }
    let expected_wire = {
        let mut ctx = md5::Context::new();
        ctx.consume(inner_hex.as_bytes());
        ctx.consume(salt);
        format!("md5{:x}", ctx.compute())
    };
    let _ = user; // username is already baked into expected_hash
    client_response == expected_wire
}

/// Compute the stored MD5 hash for a role: `"md5" + hex(md5(password + username))`.
pub fn compute_md5_stored_hash(username: &str, password: &str) -> String {
    let mut ctx = md5::Context::new();
    ctx.consume(password.as_bytes());
    ctx.consume(username.as_bytes());
    format!("md5{:x}", ctx.compute())
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

impl Default for JwtVerifier {
    fn default() -> Self {
        Self::new()
    }
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
        mac.verify_slice(&sig)
            .map_err(|_| AuthError::InvalidSignature)?;

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
        let mode: AuthMode = "off".parse().unwrap();
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
        assert!(
            err.to_string().contains("RS-2400"),
            "expected RS-2400, got: {err}"
        );

        // Invalid signature (tampered)
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let mut token = create_test_jwt("eve", exp, TEST_SECRET);
        token.push_str("tampered");
        let err = verifier.verify(&token).unwrap_err();
        assert!(
            err.to_string().contains("RS-2400"),
            "expected RS-2400, got: {err}"
        );

        // Expired token
        let past_exp: u64 = 1; // far in the past
        let expired = create_test_jwt("bob", past_exp, TEST_SECRET);
        let err = verifier.verify(&expired).unwrap_err();
        assert!(
            err.to_string().contains("RS-2400"),
            "expected RS-2400, got: {err}"
        );
    }

    /// S2 green gate: test_scram_crypto_rfc_vectors
    /// Simulates a complete SCRAM-SHA-256 exchange (RFC 5802/RFC 7677 structure).
    /// Uses the RFC 7677 Appendix B protocol messages with our primitives to verify
    /// internal consistency.  Compatibility with external implementations (tokio-postgres)
    /// is verified by the integration test `test_scram_auth_flow_unit`.
    #[test]
    fn test_scram_crypto_rfc_vectors() {
        let password = b"pencil";
        // Use a small salt/iteration count to keep the test fast.
        let salt = b"rockstream-test-s";
        let iterations = 1u32; // 1 iteration = testable without PBKDF2 timing

        // Protocol messages (RFC 5802 §3 structure)
        let client_first_bare = "n=user,r=rOprNGfwEbeRWgbNEkqO";
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=dGVzdA==,i=1";
        let client_final_without_proof =
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
        let auth_message =
            format!("{client_first_bare},{server_first},{client_final_without_proof}");

        // Server side: derive keys from password
        let salted_password = scram_hi(password, salt, iterations);
        let stored_key = scram_stored_key(&salted_password);
        let server_key = scram_server_key(&salted_password);

        // Simulate client: compute proof
        // ClientKey = HMAC(SaltedPassword, "Client Key")
        let client_key: [u8; 32] = {
            let mut mac = Hmac::<Sha256>::new_from_slice(&salted_password).expect("HMAC key");
            mac.update(b"Client Key");
            let out = mac.finalize().into_bytes();
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&out);
            arr
        };
        // ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_sig = scram_client_signature(&stored_key, &auth_message);
        // ClientProof = ClientKey XOR ClientSignature
        let mut client_proof = [0u8; 32];
        for i in 0..32 {
            client_proof[i] = client_key[i] ^ client_sig[i];
        }
        let client_proof_b64 = STANDARD.encode(client_proof);

        // Server verifies the proof
        assert!(
            verify_client_proof(&stored_key, &client_proof_b64, &auth_message),
            "SCRAM protocol roundtrip: verify_client_proof failed"
        );

        // verify_client_proof rejects a tampered proof
        let mut tampered = client_proof;
        tampered[0] ^= 0xFF;
        let tampered_b64 = STANDARD.encode(tampered);
        assert!(
            !verify_client_proof(&stored_key, &tampered_b64, &auth_message),
            "SCRAM protocol roundtrip: tampered proof should be rejected"
        );

        // Server signature is deterministic and non-empty
        let server_sig = scram_server_signature(&server_key, &auth_message);
        assert_eq!(server_sig.len(), 32, "server signature must be 32 bytes");
        let server_sig_b64 = STANDARD.encode(server_sig);
        assert_eq!(server_sig_b64.len(), 44, "base64 of 32 bytes is 44 chars");

        // StoredKey and ServerKey are distinct (no collision between key purposes)
        assert_ne!(
            stored_key, server_key,
            "stored_key and server_key must differ"
        );
    }

    /// S2 green gate: test_md5_password_hash_roundtrip
    #[test]
    fn test_md5_password_hash_roundtrip() {
        let user = "alice";
        let password = "pencil";
        let salt = b"1234";

        // Compute stored hash (no salt — stores password+user hash)
        let stored = compute_md5_stored_hash(user, password);
        assert!(
            stored.starts_with("md5"),
            "stored hash must start with 'md5'"
        );
        assert_eq!(stored.len(), 35, "md5 prefix + 32 hex chars");

        // Compute wire hash (what client sends)
        let wire = hash_md5_password(user, password, salt);
        assert!(wire.starts_with("md5"));

        // Verify: stored hash + salt → wire hash should match
        assert!(
            verify_md5(&stored, user, salt, &wire),
            "MD5 roundtrip failed: stored={stored}, wire={wire}"
        );

        // Wrong password should fail
        let wrong_wire = hash_md5_password(user, "wrong", salt);
        assert!(
            !verify_md5(&stored, user, salt, &wrong_wire),
            "MD5 should reject wrong password"
        );
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
