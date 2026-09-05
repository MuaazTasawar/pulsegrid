use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use domain::AlertSeverity;
use serde::Deserialize;
use validator::Validate;

use crate::errors::ApiError;
use crate::AppState;
use domain::AppError;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SeverityInput {
    Info,
    Warning,
    Critical,
}

impl From<SeverityInput> for AlertSeverity {
    fn from(s: SeverityInput) -> Self {
        match s {
            SeverityInput::Info => AlertSeverity::Info,
            SeverityInput::Warning => AlertSeverity::Warning,
            SeverityInput::Critical => AlertSeverity::Critical,
        }
    }
}

#[derive(Debug, Deserialize, Validate)]
pub struct DispatchAlertRequest {
    #[validate(range(min = -90.0, max = 90.0))]
    pub lat: f64,
    #[validate(range(min = -180.0, max = 180.0))]
    pub lon: f64,
    #[validate(range(min = 1.0, max = 500000.0))]
    pub radius_meters: f64,
    pub severity: SeverityInput,
    #[validate(length(min = 1, max = 200))]
    pub title: String,
    #[validate(length(min = 1, max = 2000))]
    pub message: String,
}

pub async fn dispatch_alert(
    State(state): State<AppState>,
    Json(payload): Json<DispatchAlertRequest>,
) -> Result<impl IntoResponse, ApiError> {
    payload.validate().map_err(|e| AppError::Validation(e.to_string()))?;

    let result = state
        .alert_dispatch_service
        .dispatch(
            payload.lat,
            payload.lon,
            payload.radius_meters,
            payload.severity.into(),
            payload.title,
            payload.message,
        )
        .await?;

    let status = if result.failed_shards.is_empty() {
        StatusCode::OK
    } else {
        StatusCode::MULTI_STATUS
    };

    Ok((status, Json(result)))
}