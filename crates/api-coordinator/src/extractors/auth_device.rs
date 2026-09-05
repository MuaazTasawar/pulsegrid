use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::RequestPartsExt;
use axum_extra::headers::{authorization::Bearer, Authorization};
use axum_extra::TypedHeader;
use uuid::Uuid;

use crate::errors::ApiError;
use crate::AppState;
use domain::AppError;

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

        let device_id = domain::verify_token(bearer.token(), &state.jwt_secret)
            .map_err(|_| ApiError(AppError::Unauthorized))?;

        Ok(AuthDevice { device_id })
    }
}