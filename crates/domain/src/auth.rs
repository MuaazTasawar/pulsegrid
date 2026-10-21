use std::fmt;

use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::errors::AppError;

/// Default device-token lifetime: 30 days.
pub const DEFAULT_TOKEN_TTL_HOURS: i64 = 24 * 30;
/// Minimum length of any secret / API key when running in production.
pub const MIN_PRODUCTION_SECRET_LEN: usize = 32;

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
    #[serde(default)]
    pub iat: usize,
}

/// Issues device JWTs. Supports HS256 (shared secret) and EdDSA/Ed25519
/// (private key). Only the coordinator needs one.
#[derive(Clone)]
pub struct TokenSigner {
    algorithm: Algorithm,
    key: EncodingKey,
    ttl_hours: i64,
}

impl TokenSigner {
    pub fn hs256(secret: &str, ttl_hours: i64) -> Self {
        Self {
            algorithm: Algorithm::HS256,
            key: EncodingKey::from_secret(secret.as_bytes()),
            ttl_hours,
        }
    }

    /// `private_pem` is a PKCS#8 PEM Ed25519 private key, e.g. produced by
    /// `openssl genpkey -algorithm ED25519`.
    pub fn eddsa_from_pem(private_pem: &[u8], ttl_hours: i64) -> Result<Self, AppError> {
        let key = EncodingKey::from_ed_pem(private_pem)
            .map_err(|e| AppError::Config(format!("invalid Ed25519 private key PEM: {e}")))?;
        Ok(Self {
            algorithm: Algorithm::EdDSA,
            key,
            ttl_hours,
        })
    }

    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    pub fn ttl_hours(&self) -> i64 {
        self.ttl_hours
    }

    pub fn issue(&self, device_id: Uuid) -> Result<String, AppError> {
        let now = Utc::now();
        let claims = Claims {
            sub: device_id.to_string(),
            exp: (now + Duration::hours(self.ttl_hours)).timestamp().max(0) as usize,
            iat: now.timestamp().max(0) as usize,
        };
        encode(&Header::new(self.algorithm), &claims, &self.key)
            .map_err(|e| AppError::Internal(e.into()))
    }
}

/// Verifies device JWTs. The accepted algorithm is fixed at construction,
/// so a token cannot pick its own algorithm (no algorithm-confusion).
#[derive(Clone)]
pub struct TokenVerifier {
    key: DecodingKey,
    validation: Validation,
}

impl TokenVerifier {
    fn build(algorithm: Algorithm, key: DecodingKey) -> Self {
        let mut validation = Validation::new(algorithm);
        validation.set_required_spec_claims(&["exp", "sub"]);
        Self { key, validation }
    }

    pub fn hs256(secret: &str) -> Self {
        Self::build(
            Algorithm::HS256,
            DecodingKey::from_secret(secret.as_bytes()),
        )
    }

    /// `public_pem` is an Ed25519 public key PEM (SubjectPublicKeyInfo).
    pub fn eddsa_from_pem(public_pem: &[u8]) -> Result<Self, AppError> {
        let key = DecodingKey::from_ed_pem(public_pem)
            .map_err(|e| AppError::Config(format!("invalid Ed25519 public key PEM: {e}")))?;
        Ok(Self::build(Algorithm::EdDSA, key))
    }

    /// Returns the device id the token was issued for.
    pub fn verify(&self, token: &str) -> Result<Uuid, AppError> {
        let data = decode::<Claims>(token, &self.key, &self.validation)
            .map_err(|_| AppError::Unauthorized)?;
        Uuid::parse_str(&data.claims.sub).map_err(|_| AppError::Unauthorized)
    }
}

/// Issues an HS256 token with the default 30-day lifetime.
/// Kept for callers that only have a shared secret.
pub fn issue_token(device_id: Uuid, secret: &str) -> Result<String, AppError> {
    TokenSigner::hs256(secret, DEFAULT_TOKEN_TTL_HOURS).issue(device_id)
}

/// Verifies an HS256 token and returns the device id it was issued for.
pub fn verify_token(token: &str, secret: &str) -> Result<Uuid, AppError> {
    TokenVerifier::hs256(secret).verify(token)
}

/// Generates a single-use WebSocket ticket: 64 hex characters (two random
/// v4 UUIDs, ~244 bits from the OS CSPRNG). The ticket is stored in Redis
/// with a short TTL and consumed atomically on connect, so it never has to
/// carry the long-lived JWT in a URL.
pub fn new_ws_ticket() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

/// True for empty values and the `change-me...` placeholders shipped in the
/// env templates.
pub fn is_placeholder_secret(value: &str) -> bool {
    let v = value.trim();
    v.is_empty() || v.to_ascii_lowercase().starts_with("change-me")
}

/// Rejects empty secrets everywhere, and placeholder or short secrets when
/// `production` is true. `name` is only used in the error message.
pub fn validate_secret_strength(name: &str, value: &str, production: bool) -> Result<(), AppError> {
    if value.trim().is_empty() {
        return Err(AppError::Config(format!("{name} must not be empty")));
    }
    if production {
        if is_placeholder_secret(value) {
            return Err(AppError::Config(format!(
                "{name} is still a placeholder; run scripts/gen-secrets.ps1"
            )));
        }
        if value.len() < MIN_PRODUCTION_SECRET_LEN {
            return Err(AppError::Config(format!(
                "{name} must be at least {MIN_PRODUCTION_SECRET_LEN} characters in production"
            )));
        }
    }
    Ok(())
}

/// A set of operator API keys (sent as the `X-API-Key` header).
/// An empty set rejects everything: operator endpoints are closed unless a
/// key is configured.
#[derive(Clone, Default)]
pub struct ApiKeys {
    keys: Vec<Vec<u8>>,
}

impl ApiKeys {
    /// Parses a comma-separated list. Blank entries are ignored. In
    /// production at least one strong key is required.
    pub fn from_csv(csv: &str, production: bool) -> Result<Self, AppError> {
        let mut keys = Vec::new();
        for raw in csv.split(',') {
            let key = raw.trim();
            if key.is_empty() {
                continue;
            }
            validate_secret_strength("OPERATOR_API_KEYS entry", key, production)?;
            keys.push(key.as_bytes().to_vec());
        }
        if production && keys.is_empty() {
            return Err(AppError::Config(
                "OPERATOR_API_KEYS must contain at least one key in production".into(),
            ));
        }
        Ok(Self { keys })
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Constant-time comparison against every configured key (no early
    /// exit), so response time does not reveal which key matched.
    pub fn verify(&self, presented: &str) -> bool {
        let presented = presented.as_bytes();
        let mut matched = 0u8;
        for key in &self.keys {
            matched |= key.as_slice().ct_eq(presented).unwrap_u8();
        }
        matched == 1
    }
}

impl fmt::Debug for ApiKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ApiKeys({} configured)", self.keys.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST-ONLY throwaway Ed25519 keys. Never used outside unit tests.
    const TEST_PRIV: &str = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIBdWliWBNxPVRVGNuJcFBEdSpDmmFHDpF3CbIbRJDI/m\n-----END PRIVATE KEY-----\n";
    const TEST_PUB: &str = "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAUaSbdJyBNsZkUote1GDMqT99UGSCwto0HHs3sMPEZxY=\n-----END PUBLIC KEY-----\n";
    const OTHER_PUB: &str = "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAStiFiYmd0+urJCHuOBbq3thVyTVlZtmVyC0eUxBmo5g=\n-----END PUBLIC KEY-----\n";

    #[test]
    fn hs256_roundtrip() {
        let id = Uuid::new_v4();
        let token = issue_token(id, "a-secret").unwrap();
        assert_eq!(verify_token(&token, "a-secret").unwrap(), id);
    }

    #[test]
    fn hs256_wrong_secret_rejected() {
        let token = issue_token(Uuid::new_v4(), "a-secret").unwrap();
        assert!(matches!(
            verify_token(&token, "other"),
            Err(AppError::Unauthorized)
        ));
    }

    #[test]
    fn expired_token_rejected() {
        let signer = TokenSigner::hs256("s", -2);
        let token = signer.issue(Uuid::new_v4()).unwrap();
        assert!(matches!(
            TokenVerifier::hs256("s").verify(&token),
            Err(AppError::Unauthorized)
        ));
    }

    #[test]
    fn garbage_token_rejected() {
        assert!(TokenVerifier::hs256("s").verify("not.a.jwt").is_err());
        assert!(TokenVerifier::hs256("s").verify("").is_err());
    }

    #[test]
    fn eddsa_roundtrip() {
        let signer = TokenSigner::eddsa_from_pem(TEST_PRIV.as_bytes(), 1).unwrap();
        let verifier = TokenVerifier::eddsa_from_pem(TEST_PUB.as_bytes()).unwrap();
        let id = Uuid::new_v4();
        let token = signer.issue(id).unwrap();
        assert_eq!(verifier.verify(&token).unwrap(), id);
    }

    #[test]
    fn eddsa_wrong_public_key_rejected() {
        let signer = TokenSigner::eddsa_from_pem(TEST_PRIV.as_bytes(), 1).unwrap();
        let verifier = TokenVerifier::eddsa_from_pem(OTHER_PUB.as_bytes()).unwrap();
        let token = signer.issue(Uuid::new_v4()).unwrap();
        assert!(verifier.verify(&token).is_err());
    }

    #[test]
    fn algorithm_confusion_rejected() {
        // An attacker signs an HS256 token using the *public* key bytes as
        // the HMAC secret. An EdDSA verifier must refuse it.
        let forged = TokenSigner::hs256(TEST_PUB, 1)
            .issue(Uuid::new_v4())
            .unwrap();
        let verifier = TokenVerifier::eddsa_from_pem(TEST_PUB.as_bytes()).unwrap();
        assert!(verifier.verify(&forged).is_err());
        // ...and an HS256 verifier must refuse an EdDSA token.
        let real = TokenSigner::eddsa_from_pem(TEST_PRIV.as_bytes(), 1)
            .unwrap()
            .issue(Uuid::new_v4())
            .unwrap();
        assert!(TokenVerifier::hs256(TEST_PUB).verify(&real).is_err());
    }

    #[test]
    fn bad_pem_is_a_config_error() {
        assert!(matches!(
            TokenSigner::eddsa_from_pem(b"nope", 1),
            Err(AppError::Config(_))
        ));
        assert!(matches!(
            TokenVerifier::eddsa_from_pem(b"nope"),
            Err(AppError::Config(_))
        ));
    }

    #[test]
    fn secret_strength_rules() {
        assert!(validate_secret_strength("S", "", false).is_err());
        assert!(validate_secret_strength("S", "tiny", false).is_ok());
        assert!(validate_secret_strength("S", "tiny", true).is_err());
        assert!(validate_secret_strength("S", "change-me-in-production", true).is_err());
        assert!(validate_secret_strength("S", &"x".repeat(31), true).is_err());
        assert!(validate_secret_strength("S", &"x".repeat(32), true).is_ok());
        assert!(is_placeholder_secret("CHANGE-ME-dev"));
        assert!(!is_placeholder_secret("a1b2c3"));
    }

    #[test]
    fn api_keys_verify_any_configured_key() {
        let keys = ApiKeys::from_csv(" key-one , key-two ,, ", false).unwrap();
        assert!(!keys.is_empty());
        assert!(keys.verify("key-one"));
        assert!(keys.verify("key-two"));
        assert!(!keys.verify("key-thr"));
        assert!(!keys.verify(""));
        assert!(!keys.verify("key-one,key-two"));
    }

    #[test]
    fn empty_api_keys_reject_everything() {
        let keys = ApiKeys::from_csv("", false).unwrap();
        assert!(keys.is_empty());
        assert!(!keys.verify(""));
        assert!(!keys.verify("anything"));
    }

    #[test]
    fn api_keys_production_rules() {
        assert!(ApiKeys::from_csv("", true).is_err());
        assert!(ApiKeys::from_csv("short", true).is_err());
        assert!(ApiKeys::from_csv(&"k".repeat(40), true).is_ok());
        assert_eq!(
            format!("{:?}", ApiKeys::from_csv("abc", false).unwrap()),
            "ApiKeys(1 configured)"
        );
    }

    #[test]
    fn ws_tickets_are_long_and_unique() {
        let a = new_ws_ticket();
        let b = new_ws_ticket();
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }
}
