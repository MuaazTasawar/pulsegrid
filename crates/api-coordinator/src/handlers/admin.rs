use axum::extract::{Path, State};
use axum::http::StatusCode;
use uuid::Uuid;

use crate::errors::ApiError;
use crate::AppState;

/// `POST /admin/devices/{id}/revoke` (operators): the device's token stops
/// working at once and any live socket is closed. The device record stays.
pub async fn revoke_device(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    state.device_service.revoke(id).await?;
    state.metrics.admin_revocations.inc();
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /admin/devices/{id}` (operators): deregister a device.
pub async fn delete_device(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    state.device_service.delete(id).await?;
    state.metrics.devices_deleted.inc();
    Ok(StatusCode::NO_CONTENT)
}
