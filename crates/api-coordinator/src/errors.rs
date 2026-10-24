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
#[derive(Debug)]
pub struct ApiError(pub AppError);

impl From<AppError> for ApiError {
    fn from(err: AppError) -> Self {
        Self(err)
    }
}

impl ApiError {
    /// The HTTP status each kind of error maps to.
    pub fn status(&self) -> StatusCode {
        match &self.0 {
            AppError::DeviceNotFound(_) | AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::Unauthorized => StatusCode::UNAUTHORIZED,
            AppError::Forbidden(_) => StatusCode::FORBIDDEN,
            AppError::Conflict(_) => StatusCode::CONFLICT,
            AppError::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            other if other.is_client_error() => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();

        let message = if status == StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!(error = ?self.0, "internal error");
            "internal server error".to_string()
        } else {
            if status == StatusCode::SERVICE_UNAVAILABLE {
                tracing::warn!(error = ?self.0, "service unavailable");
            }
            self.0.to_string()
        };

        (status, Json(json!({ "error": message }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status_of(e: AppError) -> StatusCode {
        ApiError(e).status()
    }

    #[test]
    fn errors_map_to_the_right_status() {
        assert_eq!(
            status_of(AppError::DeviceNotFound("x".into())),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_of(AppError::NotFound("x".into())),
            StatusCode::NOT_FOUND
        );
        assert_eq!(status_of(AppError::Unauthorized), StatusCode::UNAUTHORIZED);
        assert_eq!(
            status_of(AppError::Forbidden("x".into())),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            status_of(AppError::Conflict("x".into())),
            StatusCode::CONFLICT
        );
        assert_eq!(
            status_of(AppError::ServiceUnavailable("x".into())),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status_of(AppError::Validation("x".into())),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status_of(AppError::InvalidRadius(0.0)),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status_of(AppError::InvalidLocation {
                lat: 99.0,
                lon: 0.0
            }),
            StatusCode::BAD_REQUEST
        );
        for server_side in [
            AppError::Database("x".into()),
            AppError::Cache("x".into()),
            AppError::Messaging("x".into()),
            AppError::Config("x".into()),
            AppError::GeohashError("x".into()),
        ] {
            assert_eq!(status_of(server_side), StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    #[test]
    fn internal_errors_do_not_leak_details() {
        let response = ApiError(AppError::Database("password=hunter2".into())).into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
