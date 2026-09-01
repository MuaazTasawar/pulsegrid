use axum::extract::State;
use axum::Json;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::errors::ApiError;
use crate::extractors::auth_device::AuthDevice;
use crate::AppState;
use domain::AppError;

#[derive(Debug, Deserialize, Validate)]
pub struct RegisterDeviceRequest {
    #[validate(range(min = -90.0, max = 90.0))]
    pub lat: f64,
    #[validate(range(min = -180.0, max = 180.0))]
    pub lon: f64,
}

#[derive(Debug, Serialize)]
pub struct RegisterDeviceResponse {
    pub device_id: String,
    pub token: String,
    pub shard_prefix: String,
}

pub async fn register(
    State(state): State<AppState>,
    Json(payload): Json<RegisterDeviceRequest>,
) -> Result<Json<RegisterDeviceResponse>, ApiError> {
    payload
        .validate()
        .map_err(|e| AppError::Validation(e.to_string()))?;

    let (device, token) = state.device_service.register(payload.lat, payload.lon).await?;

    Ok(Json(RegisterDeviceResponse {
        device_id: device.id.to_string(),
        token,
        shard_prefix: device.shard_prefix,
    }))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateLocationRequest {
    #[validate(range(min = -90.0, max = 90.0))]
    pub lat: f64,
    #[validate(range(min = -180.0, max = 180.0))]
    pub lon: f64,
}

#[derive(Debug, Serialize)]
pub struct UpdateLocationResponse {
    pub shard_prefix: String,
    pub shard_changed: bool,
}

pub async fn update_location(
    State(state): State<AppState>,
    auth: AuthDevice,
    Json(payload): Json<UpdateLocationRequest>,
) -> Result<Json<UpdateLocationResponse>, ApiError> {
    payload
        .validate()
        .map_err(|e| AppError::Validation(e.to_string()))?;

    let (shard_prefix, shard_changed) = state
        .device_service
        .update_location(auth.device_id, payload.lat, payload.lon)
        .await?;

    Ok(Json(UpdateLocationResponse {
        shard_prefix,
        shard_changed,
    }))
}