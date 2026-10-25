use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use domain::{Alert, AlertId, AlertSeverity};
use serde::Deserialize;
use uuid::Uuid;
use validator::Validate;

use crate::errors::ApiError;
use crate::services::alert_dispatch_service::AlertDetails;
use crate::AppState;
use domain::AppError;

#[derive(Debug, Deserialize, Validate)]
pub struct DispatchAlertRequest {
    #[validate(range(min = -90.0, max = 90.0))]
    pub lat: f64,
    #[validate(range(min = -180.0, max = 180.0))]
    pub lon: f64,
    #[validate(range(min = 1.0, max = 500000.0))]
    pub radius_meters: f64,
    /// `"info"`, `"warning"` or `"critical"`.
    pub severity: AlertSeverity,
    #[validate(length(min = 1, max = 200))]
    pub title: String,
    #[validate(length(min = 1, max = 2000))]
    pub message: String,
}

/// `POST /alerts` (operators). Answers 200 when every shard was published,
/// 207 when some failed (see `failed_shards`).
pub async fn dispatch_alert(
    State(state): State<AppState>,
    Json(payload): Json<DispatchAlertRequest>,
) -> Result<impl IntoResponse, ApiError> {
    payload
        .validate()
        .map_err(|e| AppError::Validation(e.to_string()))?;

    let result = state
        .alert_dispatch_service
        .dispatch(
            payload.lat,
            payload.lon,
            payload.radius_meters,
            payload.severity,
            payload.title,
            payload.message,
        )
        .await?;

    let status = if result.is_partial() {
        StatusCode::MULTI_STATUS
    } else {
        StatusCode::OK
    };

    Ok((status, Json(result)))
}

#[derive(Debug, Deserialize)]
pub struct ListAlertsQuery {
    pub limit: Option<i64>,
}

/// Default 20, at most 100, at least 1.
pub fn clamp_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(20).clamp(1, 100)
}

/// `GET /alerts?limit=N` (operators): newest alerts first.
pub async fn list_alerts(
    State(state): State<AppState>,
    Query(query): Query<ListAlertsQuery>,
) -> Result<Json<Vec<Alert>>, ApiError> {
    let alerts = state
        .alert_dispatch_service
        .recent(clamp_limit(query.limit))
        .await?;
    Ok(Json(alerts))
}

/// `GET /alerts/{id}` (operators): the alert, which shards it was published
/// to, and what the workers actually delivered to live sockets.
pub async fn get_alert(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<AlertDetails>, ApiError> {
    let details = state
        .alert_dispatch_service
        .alert_details(AlertId(id))
        .await?;
    Ok(Json(details))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_limit_is_clamped() {
        assert_eq!(clamp_limit(None), 20);
        assert_eq!(clamp_limit(Some(5)), 5);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(-3)), 1);
        assert_eq!(clamp_limit(Some(10_000)), 100);
    }

    #[test]
    fn severity_is_lowercase_in_requests() {
        let ok: Result<DispatchAlertRequest, _> = serde_json::from_str(
            r#"{"lat":1,"lon":2,"radius_meters":100,"severity":"critical","title":"t","message":"m"}"#,
        );
        assert!(ok.is_ok());
        let upper: Result<DispatchAlertRequest, _> = serde_json::from_str(
            r#"{"lat":1,"lon":2,"radius_meters":100,"severity":"Critical","title":"t","message":"m"}"#,
        );
        assert!(upper.is_err());
    }
}
