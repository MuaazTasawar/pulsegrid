use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::errors::AppError;

const TOKEN_TTL_HOURS: i64 = 24 * 30;

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
}

/// Issues a JWT for a device. Shared by api-coordinator (issuing on
/// registration) so both binaries can never drift on token shape.
pub fn issue_token(device_id: Uuid, secret: &str) -> Result<String, AppError> {
    let claims = Claims {
        sub: device_id.to_string(),
        exp: (Utc::now() + Duration::hours(TOKEN_TTL_HOURS)).timestamp() as usize,
    };
    encode(&Header::default(), &claims, &EncodingKey::from_secret(secret.as_bytes()))
        .map_err(|e| AppError::Internal(e.into()))
}

/// Verifies a JWT and returns the device_id it was issued for. Used by
/// api-coordinator's auth extractor AND fanout-worker's WebSocket
/// upgrade handler -- the gap this closes is that the WS endpoint
/// previously accepted any device_id with no proof of ownership at all.
pub fn verify_token(token: &str, secret: &str) -> Result<Uuid, AppError> {
    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &Validation::default(),
    )
    .map_err(|_| AppError::Unauthorized)?;
    Uuid::parse_str(&data.claims.sub).map_err(|_| AppError::Unauthorized)
}