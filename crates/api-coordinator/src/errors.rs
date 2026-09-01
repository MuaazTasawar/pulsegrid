use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use domain::AppError;
use serde_json::json;

/// Local wrapper around domain::AppError so we can implement axum's
/// IntoResponse here without violating the orphan rule (neither
/// AppError nor IntoResponse is defined in this crate). Every handler
/// returns Result<_, ApiError> and uses `?` on anything that produces
/// AppError -- the From impl below makes that conversion automatic.
pub struct ApiError(pub AppError);

impl From<AppError> for ApiError {
    fn from(err: AppError) -> Self {
        Self(err)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = if self.0.is_client_error() {
            match &self.0 {
                AppError::DeviceNotFound(_) => StatusCode::NOT_FOUND,
                AppError::Unauthorized => StatusCode::UNAUTHORIZED,
                _ => StatusCode::BAD_REQUEST,
            }
        } else {
            tracing::error!(error = ?self.0, "internal error");
            StatusCode::INTERNAL_SERVER_ERROR
        };

        let message = if status == StatusCode::INTERNAL_SERVER_ERROR {
            "internal server error".to_string()
        } else {
            self.0.to_string()
        };

        (status, Json(json!({ "error": message }))).into_response()
    }
}