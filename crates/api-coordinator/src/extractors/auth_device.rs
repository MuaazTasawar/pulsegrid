use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::RequestPartsExt;
use axum_extra::headers::{authorization::Bearer, Authorization};
use axum_extra::TypedHeader;
use uuid::Uuid;

use crate::errors::ApiError;
use crate::AppState;
use domain::AppError;

/// An authenticated device: the request carried a valid, unexpired device
/// JWT, and the device has not been revoked or deregistered since.
pub struct AuthDevice {
    pub device_id: Uuid,
}

impl FromRequestParts<AppState> for AuthDevice {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|_| ApiError(AppError::Unauthorized))?;

        let device_id = state
            .verifier
            .verify(bearer.token())
            .map_err(|_| ApiError(AppError::Unauthorized))?;

        // A valid signature is not enough: deregistered or revoked devices
        // must stop working immediately, not when their token expires. If
        // the revocation list cannot be read we refuse (fail closed).
        match state.revocation_store.is_revoked(device_id).await {
            Ok(false) => Ok(AuthDevice { device_id }),
            Ok(true) => Err(ApiError(AppError::Unauthorized)),
            Err(e) => {
                tracing::error!(error = ?e, "cannot check the revocation list");
                Err(ApiError(AppError::ServiceUnavailable(
                    "authentication backend unavailable".into(),
                )))
            }
        }
    }
}
