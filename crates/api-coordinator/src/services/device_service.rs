use chrono::{Duration, Utc};
use domain::device::Location;
use domain::{AppError, Device, DeviceId};
use infra::DeviceRepository;
use jsonwebtoken::{encode, EncodingKey, Header};
use uuid::Uuid;

use crate::extractors::auth_device::Claims;

const TOKEN_TTL_HOURS: i64 = 24 * 30; // devices stay authenticated for 30 days

#[derive(Clone)]
pub struct DeviceService {
    repo: DeviceRepository,
    jwt_secret: String,
}

impl DeviceService {
    pub fn new(repo: DeviceRepository, jwt_secret: String) -> Self {
        Self { repo, jwt_secret }
    }

    /// Registers a new device and issues it a long-lived JWT. Returns
    /// both the device record and the token -- the caller (the register
    /// handler) is responsible for shaping the HTTP response.
    pub async fn register(&self, lat: f64, lon: f64) -> Result<(Device, String), AppError> {
        let location = Location::new(lat, lon)?;
        let device = Device::register(location)?;
        self.repo.insert(&device).await?;
        let token = self.issue_token(device.id)?;
        Ok((device, token))
    }

    /// Updates a device's location. Returns the device's (possibly new)
    /// shard_prefix and whether it changed from before -- the caller
    /// uses `shard_changed` to tell the client it needs to reconnect its
    /// WebSocket to a different fanout-worker instance (Phase 5).
    pub async fn update_location(
        &self,
        device_id: Uuid,
        lat: f64,
        lon: f64,
    ) -> Result<(String, bool), AppError> {
        let mut device = self.repo.find_by_id(DeviceId(device_id)).await?;
        let new_location = Location::new(lat, lon)?;
        let shard_changed = device.update_location(new_location)?;
        self.repo
            .update_location(
                device.id,
                device.location,
                &device.geohash,
                &device.shard_prefix,
            )
            .await?;
        Ok((device.shard_prefix, shard_changed))
    }

    fn issue_token(&self, device_id: DeviceId) -> Result<String, AppError> {
        let claims = Claims {
            sub: device_id.0.to_string(),
            exp: (Utc::now() + Duration::hours(TOKEN_TTL_HOURS)).timestamp() as usize,
        };
        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.jwt_secret.as_bytes()),
        )
        .map_err(|e| AppError::Internal(e.into()))
    }
}