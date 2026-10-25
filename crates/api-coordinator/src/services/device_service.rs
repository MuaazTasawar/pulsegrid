use domain::device::Location;
use domain::{AppError, Device, DeviceId, ServerMessage, TokenSigner};
use infra::redis::DeviceLocationRecord;
use infra::{DeviceLocationStore, DeviceRepository, RevocationStore, ShardRegistry, WsTicketStore};
use uuid::Uuid;

/// Everything the device service needs, bundled so `DeviceService::new`
/// stays readable.
#[derive(Clone)]
pub struct DeviceServiceParts {
    pub repo: DeviceRepository,
    pub signer: TokenSigner,
    pub locations: DeviceLocationStore,
    pub revocations: RevocationStore,
    pub tickets: WsTicketStore,
    pub shards: ShardRegistry,
    pub nats: infra::nats::Client,
    /// How long a device's location record stays in Redis.
    pub location_ttl_secs: u64,
    /// How long a revocation outlives the longest-lived token.
    pub revocation_ttl_secs: u64,
    pub ws_ticket_ttl_secs: u64,
}

/// Result of a location update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationUpdate {
    pub shard_prefix: String,
    pub shard_changed: bool,
    /// The worker that owns the new shard, when the shard changed.
    pub ws_url: Option<String>,
}

/// A one-time WebSocket ticket and where to use it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsTicket {
    pub ticket: String,
    pub shard_prefix: String,
    pub ws_url: Option<String>,
    pub expires_in_secs: u64,
}

#[derive(Clone)]
pub struct DeviceService {
    p: DeviceServiceParts,
}

impl DeviceService {
    pub fn new(parts: DeviceServiceParts) -> Self {
        Self { p: parts }
    }

    /// Writes the device's current location where the fanout workers can
    /// read it (to verify its shard and to filter by alert radius).
    async fn publish_location(&self, device: &Device) -> Result<(), AppError> {
        self.p
            .locations
            .set(
                device.id.0,
                &DeviceLocationRecord {
                    lat: device.location.lat,
                    lon: device.location.lon,
                    shard_prefix: device.shard_prefix.clone(),
                },
                self.p.location_ttl_secs,
            )
            .await
    }

    /// Best effort: telling a worker to notify the device is a courtesy and
    /// must never fail the request that triggered it.
    async fn notify_device(&self, device_id: Uuid, message: &ServerMessage) {
        if let Err(e) = infra::nats::publish_control(&self.p.nats, device_id, message).await {
            tracing::warn!(%device_id, error = ?e, "could not publish control message");
        }
    }

    pub async fn register(&self, lat: f64, lon: f64) -> Result<(Device, String), AppError> {
        let location = Location::new(lat, lon)?;
        let device = Device::register(location)?;
        self.p.repo.insert(&device).await?;

        if let Err(e) = self.publish_location(&device).await {
            // Do not leave a device behind that no worker could ever verify.
            if let Err(cleanup) = self.p.repo.delete(device.id).await {
                tracing::error!(error = ?cleanup, "could not roll back device registration");
            }
            return Err(e);
        }

        let token = self.p.signer.issue(device.id.0)?;
        Ok((device, token))
    }

    pub async fn update_location(
        &self,
        device_id: Uuid,
        lat: f64,
        lon: f64,
    ) -> Result<LocationUpdate, AppError> {
        let mut device = self.p.repo.find_by_id(DeviceId(device_id)).await?;
        let new_location = Location::new(lat, lon)?;
        let shard_changed =
            new_location != device.location && device.update_location(new_location)?;
        self.p
            .repo
            .update_location(
                device.id,
                device.location,
                &device.geohash,
                &device.shard_prefix,
            )
            .await?;
        self.publish_location(&device).await?;

        let mut ws_url = None;
        if shard_changed {
            ws_url = self
                .p
                .shards
                .lookup_shard(&device.shard_prefix)
                .await
                .unwrap_or(None);
            self.notify_device(
                device_id,
                &ServerMessage::ShardChanged {
                    shard_prefix: device.shard_prefix.clone(),
                    ws_url: ws_url.clone(),
                },
            )
            .await;
        }

        Ok(LocationUpdate {
            shard_prefix: device.shard_prefix,
            shard_changed,
            ws_url,
        })
    }

    /// Marks the device as alive (and keeps its location record from expiring).
    pub async fn heartbeat(&self, device_id: Uuid) -> Result<(), AppError> {
        let device = self.p.repo.find_by_id(DeviceId(device_id)).await?;
        self.p.repo.touch(device.id).await?;
        self.publish_location(&device).await
    }

    /// Issues a single-use ticket for opening the device's WebSocket.
    pub async fn issue_ws_ticket(&self, device_id: Uuid) -> Result<WsTicket, AppError> {
        let device = self.p.repo.find_by_id(DeviceId(device_id)).await?;
        // Refresh the location record too, so the worker can always verify
        // the device even if Redis was flushed since it last moved.
        self.publish_location(&device).await?;

        let ticket = domain::new_ws_ticket();
        self.p
            .tickets
            .issue(&ticket, device_id, self.p.ws_ticket_ttl_secs)
            .await?;

        let ws_url = self
            .p
            .shards
            .lookup_shard(&device.shard_prefix)
            .await
            .unwrap_or(None);
        Ok(WsTicket {
            ticket,
            shard_prefix: device.shard_prefix,
            ws_url,
            expires_in_secs: self.p.ws_ticket_ttl_secs,
        })
    }

    /// Operator action: the device's token stops working at once and any live
    /// socket is told to close, but the device record is kept.
    pub async fn revoke(&self, device_id: Uuid) -> Result<(), AppError> {
        // Check the device exists, so a typo cannot fill the revocation list.
        self.p.repo.find_by_id(DeviceId(device_id)).await?;
        self.p
            .revocations
            .revoke(device_id, self.p.revocation_ttl_secs)
            .await?;
        self.notify_device(device_id, &ServerMessage::Revoked).await;
        Ok(())
    }

    /// Deregisters the device: its token stops working at once, its data is
    /// deleted, and any live socket is told to close.
    pub async fn delete(&self, device_id: Uuid) -> Result<(), AppError> {
        // Revoke first, so the token is already useless even if a later
        // step fails.
        self.p
            .revocations
            .revoke(device_id, self.p.revocation_ttl_secs)
            .await?;
        let existed = self.p.repo.delete(DeviceId(device_id)).await?;
        if let Err(e) = self.p.locations.delete(device_id).await {
            tracing::warn!(%device_id, error = ?e, "could not delete the device location record");
        }
        self.notify_device(device_id, &ServerMessage::Revoked).await;

        if existed {
            Ok(())
        } else {
            Err(AppError::DeviceNotFound(device_id.to_string()))
        }
    }
}
