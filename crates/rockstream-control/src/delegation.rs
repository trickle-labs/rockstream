//! Short-lived, service-signed actor assertions for cross-process requests.

use std::{fs, path::Path, sync::Arc, time::SystemTime};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ring::signature::{
    RsaKeyPair, RsaPublicKeyComponents, RSA_PKCS1_2048_8192_SHA256, RSA_PKCS1_SHA256,
};
use rockstream_types::{
    authorization::{PolicyScope, SecurityAction},
    ids::PrincipalId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_ACTOR_ASSERTION_LIFETIME_SECS: u64 = 60;
pub const MAX_ACTOR_ASSERTION_BYTES: usize = 16 * 1024;
const ASSERTION_ISSUER: &str = "rockstream-gateway";
const CONTROL_SERVICE: &str = "rockstream-control";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActorAssertionClaims {
    version: u8,
    iss: String,
    sub: PrincipalId,
    aud: String,
    deployment_id: String,
    iat: u64,
    exp: u64,
    jti: String,
    policy_revision: u64,
    action: SecurityAction,
    scope: PolicyScope,
    request_digest: String,
}

impl ActorAssertionClaims {
    pub fn principal_id(&self) -> PrincipalId {
        self.sub
    }

    pub fn policy_revision(&self) -> u64 {
        self.policy_revision
    }

    pub fn action(&self) -> SecurityAction {
        self.action
    }

    pub fn scope(&self) -> &PolicyScope {
        &self.scope
    }
}

pub struct ActorAssertionSigner {
    key_pair: Arc<RsaKeyPair>,
    deployment_id: String,
}

impl ActorAssertionSigner {
    pub fn from_pkcs8_pem(path: &Path, deployment_id: impl Into<String>) -> Result<Self, String> {
        let key = crate::tls::load_key(path)?;
        let key_pair = match &key {
            rustls::pki_types::PrivateKeyDer::Pkcs8(key) => {
                RsaKeyPair::from_pkcs8(key.secret_pkcs8_der())
            }
            rustls::pki_types::PrivateKeyDer::Pkcs1(key) => {
                RsaKeyPair::from_der(key.secret_pkcs1_der())
            }
            rustls::pki_types::PrivateKeyDer::Sec1(_) => {
                return Err("actor assertion signing key must be RSA".to_owned())
            }
            _ => return Err("actor assertion signing key type is unsupported".to_owned()),
        }
        .map_err(|_| "actor assertion signing key is not a supported RSA key".to_owned())?;
        let deployment_id = deployment_id.into();
        validate_deployment_id(&deployment_id)?;
        Ok(Self {
            key_pair: Arc::new(key_pair),
            deployment_id,
        })
    }

    pub fn uses_same_key_as(&self, path: &Path) -> Result<bool, String> {
        let key = crate::tls::load_key(path)?;
        let key_pair = match &key {
            rustls::pki_types::PrivateKeyDer::Pkcs8(key) => {
                RsaKeyPair::from_pkcs8(key.secret_pkcs8_der())
            }
            rustls::pki_types::PrivateKeyDer::Pkcs1(key) => {
                RsaKeyPair::from_der(key.secret_pkcs1_der())
            }
            rustls::pki_types::PrivateKeyDer::Sec1(_) => return Ok(false),
            _ => return Ok(false),
        }
        .map_err(|_| "TLS RSA private key is invalid".to_owned())?;
        Ok(self.key_pair.public().as_ref() == key_pair.public().as_ref())
    }

    pub fn issue(
        &self,
        principal_id: PrincipalId,
        policy_revision: u64,
        action: SecurityAction,
        scope: PolicyScope,
        request_digest: [u8; 32],
    ) -> Result<String, String> {
        if principal_id.0 == 0 || policy_revision == 0 {
            return Err(
                "actor assertion requires a persisted principal and policy revision".into(),
            );
        }
        scope
            .validate()
            .map_err(|error| format!("actor assertion scope is invalid: {error}"))?;

        let now = unix_time_secs()?;
        let claims = ActorAssertionClaims {
            version: 1,
            iss: ASSERTION_ISSUER.to_owned(),
            sub: principal_id,
            aud: control_audience(&self.deployment_id),
            deployment_id: self.deployment_id.clone(),
            iat: now,
            exp: now + MAX_ACTOR_ASSERTION_LIFETIME_SECS,
            jti: uuid::Uuid::new_v4().to_string(),
            policy_revision,
            action,
            scope,
            request_digest: hex::encode(request_digest),
        };
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","typ":"JWT"}"#);
        let payload =
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).map_err(|error| error.to_string())?);
        let signing_input = format!("{header}.{payload}");
        let mut signature = vec![0; self.key_pair.public().modulus_len()];
        self.key_pair
            .sign(
                &RSA_PKCS1_SHA256,
                &ring::rand::SystemRandom::new(),
                signing_input.as_bytes(),
                &mut signature,
            )
            .map_err(|_| "actor assertion signing failed".to_owned())?;
        Ok(format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(signature)
        ))
    }
}

pub struct ActorAssertionVerifier {
    modulus: Vec<u8>,
    exponent: Vec<u8>,
    deployment_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RsaJwk {
    kty: String,
    alg: String,
    #[serde(rename = "use")]
    key_use: String,
    n: String,
    e: String,
}

impl ActorAssertionVerifier {
    /// Load the deployment-managed public key as a single RSA RS256 JWK.
    pub fn from_jwk_file(path: &Path, deployment_id: impl Into<String>) -> Result<Self, String> {
        let bytes = fs::read(path).map_err(|error| {
            format!(
                "failed to read actor assertion verification key {}: {error}",
                path.display()
            )
        })?;
        if bytes.len() > MAX_ACTOR_ASSERTION_BYTES {
            return Err("actor assertion verification key is too large".to_owned());
        }
        let jwk: RsaJwk = serde_json::from_slice(&bytes)
            .map_err(|_| "actor assertion verification key is not a valid RSA JWK".to_owned())?;
        if jwk.kty != "RSA" || jwk.alg != "RS256" || jwk.key_use != "sig" {
            return Err("actor assertion verification key must be an RSA RS256 signing key".into());
        }
        let modulus = URL_SAFE_NO_PAD
            .decode(jwk.n)
            .map_err(|_| "actor assertion RSA modulus is invalid".to_owned())?;
        let exponent = URL_SAFE_NO_PAD
            .decode(jwk.e)
            .map_err(|_| "actor assertion RSA exponent is invalid".to_owned())?;
        if !(256..=1024).contains(&modulus.len()) || exponent.is_empty() || exponent.len() > 8 {
            return Err("actor assertion RSA key size is unsupported".to_owned());
        }
        let deployment_id = deployment_id.into();
        validate_deployment_id(&deployment_id)?;
        Ok(Self {
            modulus,
            exponent,
            deployment_id,
        })
    }

    pub fn verify(
        &self,
        token: &str,
        request_digest: [u8; 32],
        now: u64,
    ) -> Result<ActorAssertionClaims, String> {
        if token.len() > MAX_ACTOR_ASSERTION_BYTES {
            return Err("actor assertion is too large".to_owned());
        }
        let mut parts = token.split('.');
        let (Some(header), Some(payload), Some(signature), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err("actor assertion must be a compact JWT".to_owned());
        };
        let header: AssertionHeader = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(header)
                .map_err(|_| "actor assertion header encoding is invalid".to_owned())?,
        )
        .map_err(|_| "actor assertion header is invalid".to_owned())?;
        if header.alg != "RS256" || header.typ != "JWT" {
            return Err("actor assertion algorithm or type is not supported".to_owned());
        }
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| "actor assertion signature encoding is invalid".to_owned())?;
        let signing_input = token.rsplit_once('.').map(|(prefix, _)| prefix).unwrap();
        RsaPublicKeyComponents {
            n: &self.modulus,
            e: &self.exponent,
        }
        .verify(
            &RSA_PKCS1_2048_8192_SHA256,
            signing_input.as_bytes(),
            &signature,
        )
        .map_err(|_| "actor assertion signature is invalid".to_owned())?;

        let claims: ActorAssertionClaims = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(payload)
                .map_err(|_| "actor assertion payload encoding is invalid".to_owned())?,
        )
        .map_err(|_| "actor assertion claims are invalid".to_owned())?;
        if claims.version != 1
            || claims.iss != ASSERTION_ISSUER
            || claims.aud != control_audience(&self.deployment_id)
            || claims.deployment_id != self.deployment_id
            || claims.sub.0 == 0
            || claims.policy_revision == 0
            || claims.jti.is_empty()
            || claims.jti.len() > 128
            || claims.iat == 0
            || claims.iat > now
            || claims.exp <= now
            || claims.exp.saturating_sub(claims.iat) > MAX_ACTOR_ASSERTION_LIFETIME_SECS
            || claims.request_digest != hex::encode(request_digest)
        {
            return Err(
                "actor assertion claims are expired or do not match this request".to_owned(),
            );
        }
        claims
            .scope
            .validate()
            .map_err(|error| format!("actor assertion scope is invalid: {error}"))?;
        Ok(claims)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssertionHeader {
    alg: String,
    typ: String,
}

pub fn request_digest<T: Serialize>(request: &T) -> Result<[u8; 32], String> {
    let request = serde_json::to_vec(request).map_err(|error| error.to_string())?;
    Ok(Sha256::digest(request).into())
}

pub fn control_audience(deployment_id: &str) -> String {
    format!("{CONTROL_SERVICE}:{deployment_id}")
}

fn validate_deployment_id(deployment_id: &str) -> Result<(), String> {
    if deployment_id.trim().is_empty()
        || deployment_id.len() > 256
        || deployment_id.chars().any(char::is_control)
    {
        return Err("actor assertion deployment ID is invalid".to_owned());
    }
    Ok(())
}

fn unix_time_secs() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| "system clock is before the Unix epoch".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::{authorization::ResourceKind, ids::WorkloadId, topology::WorkerMessage};

    #[test]
    fn assertions_bind_actor_scope_deployment_request_and_expiry() {
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let signer = ActorAssertionSigner::from_pkcs8_pem(
            &fixtures.join("actor-assertion-test-key.pem"),
            "test-deployment",
        )
        .unwrap();
        let verifier = ActorAssertionVerifier::from_jwk_file(
            &fixtures.join("actor-assertion-test-public.jwk"),
            "test-deployment",
        )
        .unwrap();
        let scope = PolicyScope::Resource {
            namespace: "public".to_owned(),
            kind: ResourceKind::View,
            name: "orders_view".to_owned(),
        };
        let request = WorkerMessage::ReadWorkload {
            workload_id: WorkloadId(42),
        };
        let digest = request_digest(&request).unwrap();
        let token = signer
            .issue(
                PrincipalId(7),
                3,
                SecurityAction::DataRead,
                scope.clone(),
                digest,
            )
            .unwrap();
        let now = unix_time_secs().unwrap();
        let claims = verifier.verify(&token, digest, now).unwrap();
        assert_eq!(claims.principal_id(), PrincipalId(7));
        assert_eq!(claims.policy_revision(), 3);
        assert_eq!(claims.scope(), &scope);
        assert!(verifier.verify(&token, [0; 32], now).is_err());
        assert!(verifier
            .verify(&token, digest, now + MAX_ACTOR_ASSERTION_LIFETIME_SECS + 1)
            .is_err());

        let other_deployment = ActorAssertionVerifier::from_jwk_file(
            &fixtures.join("actor-assertion-test-public.jwk"),
            "other-deployment",
        )
        .unwrap();
        assert!(other_deployment.verify(&token, digest, now).is_err());
    }
}
