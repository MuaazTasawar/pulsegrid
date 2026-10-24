use axum::extract::State;
use axum::http::StatusCode;
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
    /// Base WebSocket URL of the fanout-worker instance that currently
    /// owns this device's shard. None if no worker has registered
    /// ownership of that shard yet (e.g. it hasn't started, or nothing
    /// owns that geographic area in this deployment).
    pub ws_url: Option<String>,
}

pub async fn register(
    State(state): State<AppState>,
    Json(payload): Json<RegisterDeviceRequest>,
) -> Result<Json<RegisterDeviceResponse>, ApiError> {
    payload
        .validate()
        .map_err(|e| AppError::Validation(e.to_string()))?;

    let (device, token) = state
        .device_service
        .register(payload.lat, payload.lon)
        .await?;
    state.metrics.devices_registered.inc();

    let ws_url = state
        .shard_registry
        .lookup_shard(&device.shard_prefix)
        .await
        .unwrap_or(None);

    Ok(Json(RegisterDeviceResponse {
        device_id: device.id.to_string(),
        token,
        shard_prefix: device.shard_prefix,
        ws_url,
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
    /// Where to reconnect when `shard_changed` is true. The worker that
    /// holds the device's current socket also receives a `shard_changed`
    /// control message and forwards it to the device.
    pub ws_url: Option<String>,
}

pub async fn update_location(
    State(state): State<AppState>,
    auth: AuthDevice,
    Json(payload): Json<UpdateLocationRequest>,
) -> Result<Json<UpdateLocationResponse>, ApiError> {
    payload
        .validate()
        .map_err(|e| AppError::Validation(e.to_string()))?;

    let update = state
        .device_service
        .update_location(auth.device_id, payload.lat, payload.lon)
        .await?;
    if update.shard_changed {
        state.metrics.shard_changes.inc();
    }

    Ok(Json(UpdateLocationResponse {
        shard_prefix: update.shard_prefix,
        shard_changed: update.shard_changed,
        ws_url: update.ws_url,
    }))
}

/// `POST /devices/heartbeat`: "this device is still alive".
pub async fn heartbeat(
    State(state): State<AppState>,
    auth: AuthDevice,
) -> Result<StatusCode, ApiError> {
    state.device_service.heartbeat(auth.device_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Serialize)]
pub struct WsTicketResponse {
    /// Single-use. Pass it to the worker as `?ticket=...`.
    pub ticket: String,
    pub shard_prefix: String,
    pub ws_url: Option<String>,
    pub expires_in_secs: u64,
}

/// `POST /devices/ws-ticket`: trade the device JWT for a one-time ticket to
/// open the WebSocket, so the long-lived token never appears in a URL.
pub async fn ws_ticket(
    State(state): State<AppState>,
    auth: AuthDevice,
) -> Result<Json<WsTicketResponse>, ApiError> {
    let ticket = state.device_service.issue_ws_ticket(auth.device_id).await?;
    state.metrics.ws_tickets_issued.inc();
    Ok(Json(WsTicketResponse {
        ticket: ticket.ticket,
        shard_prefix: ticket.shard_prefix,
        ws_url: ticket.ws_url,
        expires_in_secs: ticket.expires_in_secs,
    }))
}

/// `DELETE /devices/me`: deregister the calling device.
pub async fn delete_me(
    State(state): State<AppState>,
    auth: AuthDevice,
) -> Result<StatusCode, ApiError> {
    state.device_service.delete(auth.device_id).await?;
    state.metrics.devices_deleted.inc();
    Ok(StatusCode::NO_CONTENT)
}
