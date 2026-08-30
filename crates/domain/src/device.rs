use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::errors::AppError;
use crate::geo;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Location {
    pub lat: f64,
    pub lon: f64,
}

impl Location {
    pub fn new(lat: f64, lon: f64) -> Result<Self, AppError> {
        if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
            return Err(AppError::InvalidLocation { lat, lon });
        }
        Ok(Self { lat, lon })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeviceId(pub Uuid);

impl DeviceId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for DeviceId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for DeviceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Device {
    pub id: DeviceId,
    pub location: Location,
    /// Full-precision geohash for this device's exact location.
    pub geohash: String,
    /// Coarser prefix identifying which fanout-worker shard owns this
    /// device's live WebSocket connection.
    pub shard_prefix: String,
    pub registered_at: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}

impl Device {
    /// Registers a new device at the given location, computing its geohash
    /// and shard prefix immediately so the caller can persist + route it
    /// in one step without a second geo lookup.
    pub fn register(location: Location) -> Result<Self, AppError> {
        let geohash = geo::encode_location(&location)?;
        let shard_prefix = geo::shard_prefix_of(&geohash);
        let now = Utc::now();
        Ok(Self {
            id: DeviceId::new(),
            location,
            geohash,
            shard_prefix,
            registered_at: now,
            last_seen: now,
        })
    }

    /// Updates the device's location, recomputing geohash/shard_prefix.
    /// Returns whether the shard assignment changed — the caller (the
    /// coordinator's device_service in Phase 3) needs this to know
    /// whether the device's live connection must migrate to a different
    /// fanout-worker instance.
    pub fn update_location(&mut self, location: Location) -> Result<bool, AppError> {
        let new_geohash = geo::encode_location(&location)?;
        let new_shard_prefix = geo::shard_prefix_of(&new_geohash);
        let shard_changed = new_shard_prefix != self.shard_prefix;
        self.location = location;
        self.geohash = new_geohash;
        self.shard_prefix = new_shard_prefix;
        self.last_seen = Utc::now();
        Ok(shard_changed)
    }
}