use std::{
    collections::HashMap,
    net::IpAddr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use parking_lot::RwLock;
use reqwest::{
    header::{AGE, CACHE_CONTROL, CONTENT_LENGTH, DATE, EXPIRES},
    redirect::Policy,
    Client,
};
use ring::signature::{RsaPublicKeyComponents, RSA_PKCS1_2048_8192_SHA256};
use serde_json::Value;
use tokio::sync::Mutex;

use super::{AuthError, JwtClaims};

pub const MAX_OIDC_TOKEN_BYTES: usize = 16 * 1024;
pub const MAX_JWKS_RESPONSE_BYTES: usize = 1024 * 1024;
pub const MAX_JWKS_KEYS: usize = 100;
const MAX_JWKS_MODULUS_BYTES: usize = 1024;
const MAX_RSA_EXPONENT_BYTES: usize = 8;
const JWKS_CACHE_TTL: Duration = Duration::from_secs(60);
const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(5);
const JWKS_RETRY_DELAY: Duration = Duration::from_millis(250);

#[derive(Clone, Debug)]
pub struct OidcConfig {
    issuer: String,
    audience: String,
    jwks_url: reqwest::Url,
}

impl OidcConfig {
    pub fn new(
        issuer: impl Into<String>,
        audience: impl Into<String>,
        jwks_url: impl AsRef<str>,
    ) -> Result<Self, AuthError> {
        let issuer = issuer.into();
        let audience = audience.into();
        if issuer.trim().is_empty() || audience.trim().is_empty() {
            return Err(unauthenticated(
                "OIDC issuer and audience must be configured",
            ));
        }

        let jwks_url = reqwest::Url::parse(jwks_url.as_ref())
            .map_err(|_| unauthenticated("OIDC JWKS URL is invalid"))?;
        if !is_https_or_loopback_http(&jwks_url)
            || !jwks_url.username().is_empty()
            || jwks_url.password().is_some()
            || jwks_url.fragment().is_some()
        {
            return Err(unauthenticated(
                "OIDC JWKS URL must use HTTPS or loopback HTTP without credentials or a fragment",
            ));
        }

        Ok(Self {
            issuer,
            audience,
            jwks_url,
        })
    }
}

#[derive(Clone, Debug)]
struct RsaJwk {
    modulus: Vec<u8>,
    exponent: Vec<u8>,
}

struct CachedJwks {
    keys: HashMap<String, RsaJwk>,
    expires_at: Instant,
}

struct FetchedJwks {
    keys: HashMap<String, RsaJwk>,
    ttl: Duration,
}

enum CachedKey {
    Fresh(RsaJwk),
    FreshMiss,
    Expired,
}

struct FetchFailure {
    error: AuthError,
    retryable: bool,
}

/// RS256-only OIDC verifier with a bounded, expiring JWKS cache.
///
/// Construct this before opening the gateway listener. `new` fetches and
/// validates the configured JWKS first, so a broken issuer cannot leave an
/// apparently authenticated listener running.
pub struct OidcVerifier {
    config: OidcConfig,
    client: Client,
    cache: RwLock<Option<CachedJwks>>,
    refresh: Mutex<()>,
}

impl OidcVerifier {
    pub async fn new(config: OidcConfig) -> Result<Self, AuthError> {
        let client = Client::builder()
            .timeout(JWKS_FETCH_TIMEOUT)
            .redirect(Policy::none())
            .build()
            .map_err(|_| unauthenticated("OIDC JWKS client initialization failed"))?;
        let verifier = Self {
            config,
            client,
            cache: RwLock::new(None),
            refresh: Mutex::new(()),
        };
        let fetched = verifier.fetch_jwks().await?;
        verifier.store_jwks(fetched);
        Ok(verifier)
    }

    pub async fn verify(&self, token: &str) -> Result<JwtClaims, AuthError> {
        if token.len() > MAX_OIDC_TOKEN_BYTES {
            return Err(unauthenticated("OIDC token exceeds the 16 KiB limit"));
        }
        let mut parts = token.splitn(3, '.');
        let header_b64 = parts.next().unwrap_or_default();
        let payload_b64 = parts.next().unwrap_or_default();
        let signature_b64 = parts.next().unwrap_or_default();
        if header_b64.is_empty() || payload_b64.is_empty() || signature_b64.is_empty() {
            return Err(unauthenticated("malformed JWT: expected 3 parts"));
        }

        let header_bytes = URL_SAFE_NO_PAD
            .decode(header_b64)
            .map_err(|_| unauthenticated("JWT header base64 decode failed"))?;
        let header: Value = serde_json::from_slice(&header_bytes)
            .map_err(|_| unauthenticated("JWT header JSON parse failed"))?;
        if header.get("alg").and_then(Value::as_str) != Some("RS256") {
            return Err(unauthenticated("OIDC accepts only RS256 tokens"));
        }
        if header.get("crit").is_some() {
            return Err(unauthenticated(
                "JWT critical header parameters are unsupported",
            ));
        }
        let kid = header
            .get("kid")
            .and_then(Value::as_str)
            .filter(|kid| !kid.is_empty())
            .ok_or_else(|| unauthenticated("JWT header is missing kid"))?;

        let signature = URL_SAFE_NO_PAD
            .decode(signature_b64)
            .map_err(|_| unauthenticated("JWT signature base64 decode failed"))?;
        let jwk = self.key_for_kid(kid).await?;
        let signing_input = format!("{header_b64}.{payload_b64}");
        RsaPublicKeyComponents {
            n: &jwk.modulus,
            e: &jwk.exponent,
        }
        .verify(
            &RSA_PKCS1_2048_8192_SHA256,
            signing_input.as_bytes(),
            &signature,
        )
        .map_err(|_| AuthError::InvalidSignature)?;

        let payload_bytes = URL_SAFE_NO_PAD
            .decode(payload_b64)
            .map_err(|_| unauthenticated("JWT payload base64 decode failed"))?;
        let payload: Value = serde_json::from_slice(&payload_bytes)
            .map_err(|_| unauthenticated("JWT payload JSON parse failed"))?;
        self.validate_claims(&payload)
    }

    /// Current usable key count for the `jwks_cache_size` fill metric.
    pub fn cache_size(&self) -> usize {
        self.cache
            .read()
            .as_ref()
            .filter(|cache| Instant::now() < cache.expires_at)
            .map_or(0, |cache| cache.keys.len())
    }

    fn validate_claims(&self, payload: &Value) -> Result<JwtClaims, AuthError> {
        if payload.get("iss").and_then(Value::as_str) != Some(self.config.issuer.as_str()) {
            return Err(unauthenticated(
                "OIDC issuer claim does not match configuration",
            ));
        }
        let audience_matches = match payload.get("aud") {
            Some(Value::String(audience)) => audience == &self.config.audience,
            Some(Value::Array(audiences)) => audiences
                .iter()
                .any(|audience| audience.as_str() == Some(self.config.audience.as_str())),
            _ => false,
        };
        if !audience_matches {
            return Err(unauthenticated(
                "OIDC audience claim does not match configuration",
            ));
        }
        let sub = payload
            .get("sub")
            .and_then(Value::as_str)
            .filter(|sub| !sub.trim().is_empty())
            .ok_or_else(|| unauthenticated("OIDC subject claim is missing"))?
            .to_string();
        let exp = payload
            .get("exp")
            .and_then(Value::as_u64)
            .ok_or_else(|| unauthenticated("OIDC expiration claim is missing or invalid"))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now >= exp {
            return Err(AuthError::TokenExpired);
        }
        if let Some(nbf) = payload.get("nbf") {
            let nbf = nbf
                .as_u64()
                .ok_or_else(|| unauthenticated("OIDC not-before claim is invalid"))?;
            if now < nbf {
                return Err(unauthenticated("OIDC token is not yet valid"));
            }
        }
        Ok(JwtClaims {
            issuer: Some(self.config.issuer.clone()),
            sub,
            exp: Some(exp),
        })
    }

    async fn key_for_kid(&self, kid: &str) -> Result<RsaJwk, AuthError> {
        match self.cached_lookup(kid) {
            CachedKey::Fresh(key) => return Ok(key),
            CachedKey::FreshMiss => {
                return Err(unauthenticated("no signing key found for JWT kid"));
            }
            CachedKey::Expired => {}
        }

        // Coalesce cache-expiry races into one issuer request. Recheck after
        // acquiring the lock so waiters use the refreshed cache entry.
        let _refresh = self.refresh.lock().await;
        match self.cached_lookup(kid) {
            CachedKey::Fresh(key) => return Ok(key),
            CachedKey::FreshMiss => {
                return Err(unauthenticated("no signing key found for JWT kid"));
            }
            CachedKey::Expired => {}
        }

        let fetched = self.fetch_jwks().await?;
        let key = fetched.keys.get(kid).cloned();
        self.store_jwks(fetched);
        key.ok_or_else(|| unauthenticated("no signing key found for JWT kid"))
    }

    fn cached_lookup(&self, kid: &str) -> CachedKey {
        let cache = self.cache.read();
        let Some(cache) = cache.as_ref() else {
            return CachedKey::Expired;
        };
        if Instant::now() >= cache.expires_at {
            return CachedKey::Expired;
        }
        cache
            .keys
            .get(kid)
            .cloned()
            .map_or(CachedKey::FreshMiss, CachedKey::Fresh)
    }

    fn store_jwks(&self, fetched: FetchedJwks) {
        if fetched.ttl.is_zero() {
            *self.cache.write() = None;
            return;
        }
        *self.cache.write() = Some(CachedJwks {
            keys: fetched.keys,
            expires_at: Instant::now() + fetched.ttl.min(JWKS_CACHE_TTL),
        });
    }

    async fn fetch_jwks(&self) -> Result<FetchedJwks, AuthError> {
        for attempt in 0..=1 {
            match self.fetch_jwks_once().await {
                Ok(fetched) => return Ok(fetched),
                Err(failure) if attempt == 0 && failure.retryable => {
                    tokio::time::sleep(JWKS_RETRY_DELAY).await;
                }
                Err(failure) => return Err(failure.error),
            }
        }
        Err(unauthenticated("OIDC JWKS request failed"))
    }

    async fn fetch_jwks_once(&self) -> Result<FetchedJwks, FetchFailure> {
        let response = self
            .client
            .get(self.config.jwks_url.clone())
            .send()
            .await
            .map_err(|error| FetchFailure {
                error: unauthenticated("OIDC JWKS request failed"),
                retryable: error.is_timeout() || error.is_connect(),
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(FetchFailure {
                error: unauthenticated("OIDC JWKS endpoint returned an unsuccessful status"),
                retryable: status.as_u16() == 429 || status.is_server_error(),
            });
        }
        if let Some(length) = response.headers().get(CONTENT_LENGTH) {
            let length = length
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| FetchFailure {
                    error: unauthenticated("OIDC JWKS content length is malformed"),
                    retryable: false,
                })?;
            if length > MAX_JWKS_RESPONSE_BYTES as u64 {
                return Err(FetchFailure {
                    error: unauthenticated("OIDC JWKS response exceeds the 1 MiB limit"),
                    retryable: false,
                });
            }
        }
        let ttl = cache_ttl(response.headers()).map_err(|error| FetchFailure {
            error,
            retryable: false,
        })?;
        let mut response = response;
        let mut body = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    if body.len().saturating_add(chunk.len()) > MAX_JWKS_RESPONSE_BYTES {
                        return Err(FetchFailure {
                            error: unauthenticated("OIDC JWKS response exceeds the 1 MiB limit"),
                            retryable: false,
                        });
                    }
                    body.extend_from_slice(&chunk);
                }
                Ok(None) => break,
                Err(error) => {
                    return Err(FetchFailure {
                        error: unauthenticated("OIDC JWKS response body could not be read"),
                        retryable: error.is_timeout() || error.is_connect(),
                    });
                }
            }
        }
        let keys = parse_jwks(&body).map_err(|error| FetchFailure {
            error,
            retryable: false,
        })?;
        Ok(FetchedJwks { keys, ttl })
    }
}

fn parse_jwks(body: &[u8]) -> Result<HashMap<String, RsaJwk>, AuthError> {
    if body.len() > MAX_JWKS_RESPONSE_BYTES {
        return Err(unauthenticated(
            "OIDC JWKS response exceeds the 1 MiB limit",
        ));
    }
    let document: Value =
        serde_json::from_slice(body).map_err(|_| unauthenticated("OIDC JWKS JSON parse failed"))?;
    let keys = document
        .get("keys")
        .and_then(Value::as_array)
        .ok_or_else(|| unauthenticated("OIDC JWKS has no keys array"))?;
    if keys.len() > MAX_JWKS_KEYS {
        return Err(unauthenticated("OIDC JWKS contains more than 100 keys"));
    }

    let mut parsed = HashMap::new();
    for key in keys {
        if key.get("kty").and_then(Value::as_str) != Some("RSA") {
            continue;
        }
        if ["d", "p", "q", "dp", "dq", "qi", "oth"]
            .iter()
            .any(|private_field| key.get(private_field).is_some())
        {
            return Err(unauthenticated("OIDC JWKS contains private key material"));
        }
        if let Some(usage) = key.get("use") {
            let usage = usage
                .as_str()
                .ok_or_else(|| unauthenticated("OIDC RSA key use is malformed"))?;
            if usage != "sig" {
                continue;
            }
        }
        if let Some(alg) = key.get("alg") {
            let alg = alg
                .as_str()
                .ok_or_else(|| unauthenticated("OIDC RSA key algorithm is malformed"))?;
            if alg != "RS256" {
                continue;
            }
        }
        if let Some(key_ops) = key.get("key_ops") {
            let ops = key_ops
                .as_array()
                .ok_or_else(|| unauthenticated("OIDC RSA key operations are malformed"))?;
            let can_verify = ops.iter().any(|op| op.as_str() == Some("verify"));
            if !can_verify {
                continue;
            }
        }
        let kid = key
            .get("kid")
            .and_then(Value::as_str)
            .filter(|kid| !kid.is_empty())
            .ok_or_else(|| unauthenticated("OIDC RSA signing key is missing kid"))?
            .to_string();
        let modulus = decode_jwk_integer(key.get("n"), "modulus")?;
        let exponent = decode_jwk_integer(key.get("e"), "exponent")?;
        if modulus.len() < 256
            || modulus.len() > MAX_JWKS_MODULUS_BYTES
            || modulus.first().is_none_or(|first| first & 0x80 == 0)
        {
            return Err(unauthenticated("OIDC RSA modulus size is unsupported"));
        }
        let mut exponent_bytes = [0u8; MAX_RSA_EXPONENT_BYTES];
        if exponent.is_empty()
            || exponent.len() > MAX_RSA_EXPONENT_BYTES
            || exponent.first() == Some(&0)
        {
            return Err(unauthenticated("OIDC RSA exponent size is unsupported"));
        }
        exponent_bytes[MAX_RSA_EXPONENT_BYTES - exponent.len()..].copy_from_slice(&exponent);
        let exponent_value = u64::from_be_bytes(exponent_bytes);
        if exponent_value < 3 || exponent_value % 2 == 0 {
            return Err(unauthenticated("OIDC RSA exponent is invalid"));
        }
        if parsed.insert(kid, RsaJwk { modulus, exponent }).is_some() {
            return Err(unauthenticated(
                "OIDC JWKS contains duplicate signing key IDs",
            ));
        }
    }
    if parsed.is_empty() {
        return Err(unauthenticated(
            "OIDC JWKS contains no usable RS256 signing keys",
        ));
    }
    Ok(parsed)
}

fn decode_jwk_integer(value: Option<&Value>, field: &str) -> Result<Vec<u8>, AuthError> {
    let encoded = value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| unauthenticated(format!("OIDC RSA key is missing {field}")))?;
    URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| unauthenticated(format!("OIDC RSA {field} is malformed")))
}

fn cache_ttl(headers: &reqwest::header::HeaderMap) -> Result<Duration, AuthError> {
    let mut ttl = JWKS_CACHE_TTL;
    if let Some(control) = headers.get(CACHE_CONTROL) {
        let control = control
            .to_str()
            .map_err(|_| unauthenticated("OIDC JWKS cache policy is malformed"))?;
        for directive in control.split(',').map(str::trim) {
            let normalized = directive.to_ascii_lowercase();
            let (name, value) = normalized
                .split_once('=')
                .map_or((normalized.as_str(), None), |(name, value)| {
                    (name.trim(), Some(value.trim()))
                });
            if name == "no-store" || name == "no-cache" {
                return Ok(Duration::ZERO);
            }
            if name == "max-age" {
                let value = value
                    .ok_or_else(|| unauthenticated("OIDC JWKS max-age cache policy is malformed"))?
                    .trim_matches('"')
                    .parse::<u64>()
                    .map_err(|_| unauthenticated("OIDC JWKS max-age cache policy is malformed"))?;
                ttl = ttl.min(Duration::from_secs(value));
            }
        }
    }

    let age = match headers.get(AGE) {
        Some(value) => value
            .to_str()
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| unauthenticated("OIDC JWKS age header is malformed"))?,
        None => 0,
    };
    ttl = ttl.saturating_sub(Duration::from_secs(age));

    if let Some(expires) = headers.get(EXPIRES) {
        let expires = parse_http_date(expires)?;
        let now = SystemTime::now();
        let (lifetime, apparent_age) = if let Some(date) = headers.get(DATE) {
            let date = parse_http_date(date)?;
            (
                expires.duration_since(date).unwrap_or_default(),
                now.duration_since(date).unwrap_or_default(),
            )
        } else {
            (
                expires.duration_since(now).unwrap_or_default(),
                Duration::ZERO,
            )
        };
        let current_age = Duration::from_secs(age).saturating_add(apparent_age);
        ttl = ttl.min(lifetime.saturating_sub(current_age));
    }
    Ok(ttl.min(JWKS_CACHE_TTL))
}

fn parse_http_date(value: &reqwest::header::HeaderValue) -> Result<SystemTime, AuthError> {
    let value = value
        .to_str()
        .map_err(|_| unauthenticated("OIDC JWKS expiry date is malformed"))?;
    let parsed = chrono::DateTime::parse_from_rfc2822(value)
        .map_err(|_| unauthenticated("OIDC JWKS expiry date is malformed"))?;
    let seconds = parsed.timestamp();
    if seconds < 0 {
        return Ok(UNIX_EPOCH);
    }
    Ok(UNIX_EPOCH + Duration::from_secs(seconds as u64))
}

fn is_https_or_loopback_http(url: &reqwest::Url) -> bool {
    if url.scheme() == "https" {
        return true;
    }
    if url.scheme() != "http" {
        return false;
    }
    match url.host_str() {
        Some(host) if host.eq_ignore_ascii_case("localhost") => true,
        Some(host) => host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback()),
        None => false,
    }
}

fn unauthenticated(message: impl Into<String>) -> AuthError {
    AuthError::Unauthenticated(message.into())
}

#[cfg(test)]
const TEST_JWKS: &str = r#"{"keys":[{"kty":"RSA","use":"sig","kid":"test-rsa","alg":"RS256","n":"uC5TZBsRzpdMEuqLcWOwt2Lpxpgm9ndeGckqfIDXk91vhkJkwZmnI6flKMl7Q76xTKPmElATzyjc0SCthFS9Y2LFuunjfWdz1mBqrldJy90LZ3Jc65o3ZHxq4XPZCcSF7ysbFhBYC85LApxCOwn2ibtmOAA2K_5G4ZuXZkavlqXfHE8ETbWBghh7pUlOqAfx915ge_2vTs0Z1CeBShizn4ZRvPnlSCzoVt4fOHIpE_mESnJ1hYtS3zz9b_8fILwIzo2vxJAn3FMxvzb5UJIUJGfaqQVcvjXEHYwjiLncIeMSYEWgCFYdBuQTjI1YqFmNMFZSFJqZNoeOgTvrZmKx8w","e":"AQAB"}]}"#;
#[cfg(test)]
pub(crate) const VALID_TOKEN: &str = "eyJhbGciOiJSUzI1NiIsImtpZCI6InRlc3QtcnNhIiwidHlwIjoiSldUIn0.eyJpc3MiOiJodHRwczovL2lzc3Vlci5leGFtcGxlIiwiYXVkIjoicm9ja3N0cmVhbSIsInN1YiI6ImFsaWNlIiwiZXhwIjo0MTAyNDQ0ODAwfQ.MS2GUK9CGRv3nRBI7cjJiTAq5Uq7_TKUbwbnghcxnl8uTaV3hC3k1GXN2knEZ3W2R7ZtVgsvG085Qizuf4u6O0jJS-txM2uqUJg4M2q59OzV0nmDKmZBwqF_w0h63Vy4aO5tFrNkY-1F9fsMQ_WVBr1_Sid4aBZrcdhn3xVCQEnm7MRfD4U_tDLEyDeBeOYegJl5WDZDIwA8vmdCteyj80aqsnndyntqgTvCzBfwLZQKKwzoKNAMubKk-4npFishD19Hvmgbp0hX-ITc7DMTrszIb18oKq3AJOuCHiZ9kNsM2mwP-DVPlbcV6jQqHHosrfXAE0O7DdcaQqUkwB5mBQ";

#[cfg(test)]
pub(crate) async fn verifier_with_local_jwks() -> OidcVerifier {
    verifier_with_local_jwks_for("https://issuer.example", "rockstream").await
}

#[cfg(test)]
pub(crate) async fn verifier_with_local_jwks_for(issuer: &str, audience: &str) -> OidcVerifier {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 2048];
        let _ = stream.read(&mut request).await.unwrap();
        let body = TEST_JWKS.as_bytes();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: max-age=120\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
    });
    let config = OidcConfig::new(issuer, audience, format!("http://{address}/jwks")).unwrap();
    let verifier = OidcVerifier::new(config).await.unwrap();
    server.await.unwrap();
    verifier
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[tokio::test]
    async fn loads_bounded_jwks_before_verifying_rs256_and_claims() {
        let verifier = verifier_with_local_jwks().await;
        assert_eq!(verifier.cache_size(), 1);
        let claims = verifier.verify(VALID_TOKEN).await.unwrap();
        assert_eq!(claims.sub, "alice");
        assert_eq!(claims.exp, Some(4_102_444_800));
    }

    #[tokio::test]
    async fn retries_one_transient_jwks_failure_before_startup() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 2048];
            let _ = first.read(&mut request).await.unwrap();
            first
                .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();

            let (mut second, _) = listener.accept().await.unwrap();
            let _ = second.read(&mut request).await.unwrap();
            let body = TEST_JWKS.as_bytes();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: max-age=60\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            second.write_all(response.as_bytes()).await.unwrap();
            second.write_all(body).await.unwrap();
        });
        let config = OidcConfig::new(
            "https://issuer.example",
            "rockstream",
            format!("http://{address}/jwks"),
        )
        .unwrap();
        let verifier = OidcVerifier::new(config).await.unwrap();
        server.await.unwrap();
        assert_eq!(verifier.cache_size(), 1);
    }

    #[tokio::test]
    async fn expired_jwks_are_not_used_when_refresh_fails() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 2048];
            let _ = stream.read(&mut request).await.unwrap();
            let body = TEST_JWKS.as_bytes();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: max-age=0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });
        let config = OidcConfig::new(
            "https://issuer.example",
            "rockstream",
            format!("http://{address}/jwks"),
        )
        .unwrap();
        let verifier = OidcVerifier::new(config).await.unwrap();
        server.await.unwrap();
        assert_eq!(verifier.cache_size(), 0);
        assert_eq!(
            verifier.verify(VALID_TOKEN).await.unwrap_err().to_string(),
            "[RS-2400] auth.unauthenticated: OIDC JWKS request failed"
        );
    }

    #[tokio::test]
    async fn concurrent_expired_cache_refreshes_share_one_jwks_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            async fn respond(mut stream: tokio::net::TcpStream, delay: Duration) {
                let mut request = [0u8; 2048];
                let _ = stream.read(&mut request).await.unwrap();
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                let body = TEST_JWKS.as_bytes();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: max-age=60\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            }

            let (initial, _) = listener.accept().await.unwrap();
            respond(initial, Duration::ZERO).await;

            let mut request_count = 1;
            loop {
                match tokio::time::timeout(Duration::from_millis(200), listener.accept()).await {
                    Ok(Ok((stream, _))) => {
                        request_count += 1;
                        respond(stream, Duration::from_millis(50)).await;
                    }
                    Ok(Err(error)) => panic!("JWKS listener failed: {error}"),
                    Err(_) => break,
                }
            }
            request_count
        });

        let config = OidcConfig::new(
            "https://issuer.example",
            "rockstream",
            format!("http://{address}/jwks"),
        )
        .unwrap();
        let verifier = OidcVerifier::new(config).await.unwrap();
        verifier.cache.write().as_mut().unwrap().expires_at = Instant::now();

        let (first, second) =
            tokio::join!(verifier.verify(VALID_TOKEN), verifier.verify(VALID_TOKEN));
        for claims in [first.unwrap(), second.unwrap()] {
            assert_eq!(claims.issuer.as_deref(), Some("https://issuer.example"));
            assert_eq!(claims.sub, "alice");
            assert_eq!(claims.exp, Some(4_102_444_800));
        }
        assert_eq!(server.await.unwrap(), 2);
    }

    #[tokio::test]
    async fn rejects_non_rs256_and_oversized_tokens_with_exact_errors() {
        let verifier = verifier_with_local_jwks().await;
        let mut parts = VALID_TOKEN.split('.');
        let _header = parts.next().unwrap();
        let payload = parts.next().unwrap();
        let signature = parts.next().unwrap();
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","kid":"test-rsa"}"#);
        let wrong_algorithm = format!("{header}.{payload}.{signature}");
        assert_eq!(
            verifier
                .verify(&wrong_algorithm)
                .await
                .unwrap_err()
                .to_string(),
            "[RS-2400] auth.unauthenticated: OIDC accepts only RS256 tokens"
        );
        let unknown_key_header = URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","kid":"unknown-key"}"#);
        let unknown_key = format!("{unknown_key_header}.{payload}.{signature}");
        assert_eq!(
            verifier.verify(&unknown_key).await.unwrap_err().to_string(),
            "[RS-2400] auth.unauthenticated: no signing key found for JWT kid"
        );
        assert_eq!(
            verifier
                .verify(&"x".repeat(MAX_OIDC_TOKEN_BYTES + 1))
                .await
                .unwrap_err()
                .to_string(),
            "[RS-2400] auth.unauthenticated: OIDC token exceeds the 16 KiB limit"
        );
    }

    #[tokio::test]
    async fn validates_issuer_audience_subject_expiration_and_not_before() {
        let verifier = verifier_with_local_jwks().await;
        let base = serde_json::json!({
            "iss": "https://issuer.example",
            "aud": "rockstream",
            "sub": "alice",
            "exp": 4_102_444_800u64
        });
        let mut wrong_issuer = base.clone();
        wrong_issuer["iss"] = Value::String("https://other.example".to_string());
        assert_eq!(
            verifier
                .validate_claims(&wrong_issuer)
                .unwrap_err()
                .to_string(),
            "[RS-2400] auth.unauthenticated: OIDC issuer claim does not match configuration"
        );
        let mut wrong_audience = base.clone();
        wrong_audience["aud"] = Value::String("other-client".to_string());
        assert_eq!(
            verifier
                .validate_claims(&wrong_audience)
                .unwrap_err()
                .to_string(),
            "[RS-2400] auth.unauthenticated: OIDC audience claim does not match configuration"
        );
        let mut missing_subject = base.clone();
        missing_subject["sub"] = Value::String("  ".to_string());
        assert_eq!(
            verifier
                .validate_claims(&missing_subject)
                .unwrap_err()
                .to_string(),
            "[RS-2400] auth.unauthenticated: OIDC subject claim is missing"
        );
        let mut missing_expiration = base.clone();
        missing_expiration.as_object_mut().unwrap().remove("exp");
        assert_eq!(
            verifier
                .validate_claims(&missing_expiration)
                .unwrap_err()
                .to_string(),
            "[RS-2400] auth.unauthenticated: OIDC expiration claim is missing or invalid"
        );
        let mut future_not_before = base.clone();
        future_not_before["nbf"] = Value::from(4_102_444_801u64);
        assert_eq!(
            verifier
                .validate_claims(&future_not_before)
                .unwrap_err()
                .to_string(),
            "[RS-2400] auth.unauthenticated: OIDC token is not yet valid"
        );
        let mut expired = base.clone();
        expired["exp"] = Value::from(0);
        assert_eq!(
            verifier.validate_claims(&expired).unwrap_err().to_string(),
            "[RS-2400] auth.unauthenticated: JWT token expired"
        );
    }

    #[test]
    fn honors_jwks_cache_directives_and_key_count_bound() {
        let mut headers = HeaderMap::new();
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("max-age=300"));
        assert_eq!(cache_ttl(&headers).unwrap(), JWKS_CACHE_TTL);
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("max-age=1"));
        assert!(cache_ttl(&headers).unwrap() <= Duration::from_secs(1));
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("MAX-AGE = \"3\""));
        assert!(cache_ttl(&headers).unwrap() <= Duration::from_secs(3));
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        assert_eq!(cache_ttl(&headers).unwrap(), Duration::ZERO);

        let oversized = serde_json::json!({
            "keys": (0..=MAX_JWKS_KEYS).map(|_| serde_json::json!({"kty":"EC"})).collect::<Vec<_>>()
        });
        assert_eq!(
            parse_jwks(oversized.to_string().as_bytes())
                .unwrap_err()
                .to_string(),
            "[RS-2400] auth.unauthenticated: OIDC JWKS contains more than 100 keys"
        );
    }

    #[test]
    fn rejects_insecure_remote_jwks_urls_and_private_jwks_keys() {
        assert_eq!(
            OidcConfig::new("https://issuer.example", "rockstream", "http://keys.example/jwks")
                .unwrap_err()
                .to_string(),
            "[RS-2400] auth.unauthenticated: OIDC JWKS URL must use HTTPS or loopback HTTP without credentials or a fragment"
        );
        let private_key = TEST_JWKS.replace("\"e\":\"AQAB\"", "\"e\":\"AQAB\",\"d\":\"private\"");
        assert_eq!(
            parse_jwks(private_key.as_bytes()).unwrap_err().to_string(),
            "[RS-2400] auth.unauthenticated: OIDC JWKS contains private key material"
        );
    }
}
