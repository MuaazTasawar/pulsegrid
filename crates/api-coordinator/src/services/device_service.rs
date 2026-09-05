use domain::device::Location;
use domain::{AppError, Device, DeviceId};
use infra::DeviceRepository;
use uuid::Uuid;

#[derive(Clone)]
pub struct DeviceService {
    repo: DeviceRepository,
    jwt_secret: String,
}

impl DeviceService {
    pub fn new(repo: DeviceRepository, jwt_secret: String) -> Self {
        Self { repo, jwt_secret }
    }

    pub async fn register(&self, lat: f64, lon: f64) -> Result<(Device, String), AppError> {
        let location = Location::new(lat, lon)?;
        let device = Device::register(location)?;
        self.repo.insert(&device).await?;
        let token = domain::issue_token(device.id.0, &self.jwt_secret)?;
        Ok((device, token))
    }

    pub async fn update_location(
        &self,
        device_id: Uuid,
        lat: f64,
        lon: f64,
    ) -> Result<(String, bool), AppError> {
        let mut device = self.repo.find_by_id(DeviceId(device_id)).await?;
        let new_location = Location::new(lat, lon)?;
        let shard_changed = new_location != device.location && device.update_location(new_location)?;
        self.repo
            .update_location(device.id, device.location, &device.geohash, &device.shard_prefix)
            .await?;
        Ok((device.shard_prefix, shard_changed))
    }
}